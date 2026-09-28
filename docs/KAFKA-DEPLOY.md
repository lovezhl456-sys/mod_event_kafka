# Kafka 部署说明（Lab 与生产参考）

> **Lab 专用栈 ≠ 生产。** 本文以本机 Grok Bot 盒子上的真实路径与命令为准。  
> FreeSWITCH / 客户端 **bootstrap 只能走 toxiproxy**，禁止直连生产或云端 Kafka。

## 1. Lab 拓扑（已验证）

本机 Phase1 **未**跑通 3 节点 KRaft 选主（嵌套/vfs 环境不稳定），采用：

| 组件 | 容器名 | 说明 |
|------|--------|------|
| 单节点 KRaft | `lab-kafka-1` | `apache/kafka:3.8.1`，heap `-Xmx384M`，桥接网 `lab-kafka`，**不**直接对宿主暴露业务口 |
| toxiproxy | `lab-toxiproxy` | 3 个前端全部转发到同一 broker `EXTERNAL:9094` |
| FreeSWITCH + 模块 | `lab-freeswitch` | 镜像 `lab-freeswitch:1.10.12-kafka`，**`--network host`** |

客户端路径（唯一支持的 bootstrap）：

```
127.0.0.1:19092  →  toxiproxy(kafka1) → kafka-1:9094
127.0.0.1:19093  →  toxiproxy(kafka2) → kafka-1:9094
127.0.0.1:19094  →  toxiproxy(kafka3) → kafka-1:9094
```

- **Bootstrap：** `127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094`
- **Toxiproxy API：** `http://127.0.0.1:8474`
- **Topic：** `fs_events`（partitions=3，RF=1）
- **事件头：** `x-fs-event-id`（去重验收键，勿改名）

为何三个入口指向同一 broker：QA 可分别/同时 `disable` `kafka1/2/3`，模拟「客户端侧整集群不可达」，而 broker 进程本身仍可存活。

## 2. 本机路径一览

| 用途 | 路径 |
|------|------|
| Compose / Phase1 | 仓库 `lab/`；共享机 `/workspace/lab-mod-event-kafka/` |
| 切流脚本 | `lab/toxiproxy_cut_restore.sh`（共享机：`/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh`） |
| 拨测 | `lab/dialtest_fast.sh`、`lab/dialtest_originate.sh`（共享机同名脚本在 `/workspace/lab-mod-event-kafka/`） |
| FS 前缀/模块缓存 | `/workspace/lab-mod-event-kafka/fs/` |
| Lab conf（含 TTL） | `/workspace/lab-mod-event-kafka/fs/conf/autoload_configs/event_kafka.conf.xml` |
| 模块源码（TTL 版） | `/workspace/mod_event_kafka-fix/work/` + `include/` + `src/` |
| 验收脚本 | `/workspace/mod_event_kafka-fix/scripts/verify_event_ids.py` |
| L-16 一键 | `/workspace/mod_event_kafka-fix/scripts/run_l16_abc.sh` |
| 证据示例 | `/workspace/mod_event_kafka-fix/reports/l16-abc-20260925-215439/` |

Docker 命令统一用：`sg docker -c '...'`（用户在 `docker` 组，需通过 `sg` 生效）。

### 2.1 与本仓库 `lab/` 的对照

演练主命令在仓库根目录用相对路径。上表里的 `/workspace/lab-mod-event-kafka/…` 是共享实验室机器上的绝对路径，仍然可用（脚本在该目录根下，不带 `lab/` 前缀）。本仓库 `lab/` 是其中一部分的同步副本。

| 实验室机器 | 本仓库 |
|------------|--------|
| `/workspace/lab-mod-event-kafka/docker-compose.yml` | `lab/docker-compose.yml` |
| `/workspace/lab-mod-event-kafka/README.md` | `lab/README.md` |
| `/workspace/lab-mod-event-kafka/README-PHASE2.md` | `lab/README-PHASE2.md` |
| `/workspace/lab-mod-event-kafka/toxiproxy.json` | `lab/toxiproxy.json` |
| `/workspace/lab-mod-event-kafka/dialtest_originate.sh` | `lab/dialtest_originate.sh` |
| `/workspace/lab-mod-event-kafka/dialtest_fast.sh` | `lab/dialtest_fast.sh` |
| `/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh` | `lab/toxiproxy_cut_restore.sh` |
| `/workspace/lab-mod-event-kafka/fs/conf/autoload_configs/event_kafka.conf.xml` | 参数对照见 `lab/event_kafka.fs.conf.xml`（仓库示例，不是容器里正在用的那份） |
| `/workspace/mod_event_kafka-fix/`（含 `scripts/verify_event_ids.py`、`scripts/run_l16_abc.sh`、`reports/`） | 本仓库模块源码在根目录 `include/`、`src/`、`mod_event_kafka.cpp`；仓内验收脚本是 `scripts/verify_event_ids.py`（目录参数，见演练手册） |

## 3. 起栈（Phase1：Kafka + toxiproxy）

```bash
# 本机曾出现桥接互通失败：FORWARD 策略 DROP → UNRECORDED / connection refused
sudo iptables-legacy -P FORWARD ACCEPT

# 仓库内；共享机改为 cd /workspace/lab-mod-event-kafka
cd lab
sg docker -c 'docker compose up -d'
sg docker -c 'docker compose ps'   # lab-kafka-1 应 healthy

# 建 topic（走 broker 内部监听）
sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic fs_events --partitions 3 --replication-factor 1"
```

### 3.1 经 toxiproxy 冒烟（必须 host 网络）

`EXTERNAL` 通告为 `127.0.0.1:19092`。在 bridge 容器内解析 `127.0.0.1` 会指到容器自身，**不要**在 `lab-kafka-1` 内对宿主映射口做 client 冒烟。

```bash
BOOT=127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094
IMG=apache/kafka:3.8.1

echo 'hello-fs-events' | sg docker -c "docker run --rm -i --network host $IMG \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $BOOT --topic fs_events"

sg docker -c "docker run --rm --network host $IMG \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BOOT \
  --topic fs_events --from-beginning --timeout-ms 15000"
```

### 3.2 切流 API 自检

```bash
curl -fsS http://127.0.0.1:8474/proxies | head
lab/toxiproxy_cut_restore.sh 2   # 仓库根目录；2 秒切断再恢复
# 共享机：/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh 2
```

## 4. 起 FreeSWITCH + mod_event_kafka（Phase2）

镜像：`lab-freeswitch:1.10.12-kafka`（标签兼容脚本；历史构建曾基于本机 prefix，实际二进制版本可能显示为 `1.10.7-dev`，以容器内 `freeswitch -version` 为准）。

```bash
sg docker -c 'docker rm -f lab-freeswitch' || true
sg docker -c 'docker run -d --name lab-freeswitch --network host lab-freeswitch:1.10.12-kafka \
  bash -c "export LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib; \
    /usr/local/freeswitch/bin/freeswitch -nonat -nf -nc -nosql -rp"'

# 确认模块
sg docker -c 'docker exec lab-freeswitch env LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib \
  /usr/local/freeswitch/bin/fs_cli -x "module_exists mod_event_kafka"'
```

容器内关键路径（当前镜像布局）：

- 模块：`/usr/local/freeswitch/mod/mod_event_kafka.so`
- 配置：`/usr/local/freeswitch/conf/autoload_configs/event_kafka.conf.xml`
- Outbox：`/usr/local/freeswitch/var/lib/freeswitch/db/event_kafka_outbox.db`

### 4.1 Lab conf 要点（参数名保持英文）

```xml
<param name="bootstrap-servers" value="127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"/>
<param name="topic" value="fs_events"/>
<param name="buffer-size" value="100000"/>
<param name="outbox-path" value="/usr/local/freeswitch/var/lib/freeswitch/db/event_kafka_outbox.db"/>
<param name="outbox-ttl-ms" value="120000"/>   <!-- 0=关闭过期 -->
<param name="message-timeout-ms" value="30000"/>
```

加载成功日志应含：`outbox-ttl-ms: 120000`。

### 4.2 更新模块后如何生效

仅 `unload`/`load` 可能仍映射旧 `.so`。本机可靠做法：

1. `docker cp` 新 `.so` + conf 进运行中容器  
2. `fs_cli -x 'fsctl shutdown now'` 后 `docker start lab-freeswitch`（保留可写层）  
3. 或重启容器后立刻再 `docker cp`，再 `unload`/`load`  
4. 验证通过后可：`docker commit lab-freeswitch lab-freeswitch:1.10.12-kafka`

## 5. 资源与限制（本机事实）

- 存储驱动常为 **vfs**，日志宜少、堆宜小（Kafka ~384M）。
- 真·3 broker RF≥2：**未**在本机 empirically 验证，仅作生产参考目标。
- 拆栈（保留镜像与目录）：

```bash
sg docker -c 'docker rm -f lab-freeswitch lab-toxiproxy lab-kafka-1'
# 或：cd lab && sg docker -c 'docker compose down -v --remove-orphans'
# 共享机：cd /workspace/lab-mod-event-kafka && sg docker -c 'docker compose down -v --remove-orphans'
# 再单独 rm lab-freeswitch（compose 不含 FS）
```

## 6. 生产参考（文档草案 / 未 empirically）

以下**不是**本机 lab 命令，仅作规划对照：

| 项 | Lab | 生产参考方向 |
|----|-----|--------------|
| 集群 | 单 KRaft + 三 proxy 入口 | 真多 broker / 受管云 Kafka |
| 故障注入 | toxiproxy disable | 网络分区、滚动重启、ACL/配额（需另案） |
| Bootstrap | 仅 toxiproxy 端口 | 业务 bootstrap；**勿**把 lab proxy 指到生产 |
| 安全 | PLAINTEXT | SASL_SSL / SSL；conf 中 `security-protocol`、`ssl-ca-location` |
| RF / ISR | RF=1 | RF≥2，min.insync.replicas≥2 |
| 容量 | heap 384M、vfs | 独立磁盘、监控 outbox 行数与 `outbox_expired` |

部署模块仍遵循 `docs/DEPLOY-ROLLBACK.md`：先装 librdkafka/sqlite，再装 `.so` + conf，确认 outbox 目录可写。

## 7. 相关文档

- 故障场景清单：`docs/FAULT-SCENARIOS.md`（由质检补 L-xx 映射）
- 一步步演练：`docs/DRILL-RUNBOOK.md`
- 设计与可靠性：`docs/DESIGN.md`、`docs/RELIABILITY.md`
- 用例：`docs/TEST-PLAN.md`
