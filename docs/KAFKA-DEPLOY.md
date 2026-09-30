# Kafka 部署说明（实验室与生产参考）

> **实验室专用栈 ≠ 生产环境。** 本文以本机（Grok Bot 主机）上的真实路径与命令为准。  
> FreeSWITCH / 客户端的 **bootstrap 只能走 toxiproxy**，禁止直连生产或云端 Kafka。

## 1. 实验室拓扑（已验证）

Phase1 在本机**未能**跑通 3 节点 KRaft 选主（嵌套/vfs 环境不稳定），因此采用：

| 组件 | 容器名 | 说明 |
|------|--------|------|
| 单节点 KRaft | `lab-kafka-1` | `apache/kafka:3.8.1`，heap `-Xmx384M`，bridge 网络 `lab-kafka`，**不**直接向宿主机暴露业务端口 |
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
- **事件消息头：** `x-fs-event-id`（去重与验收所用的键，勿改名）

为什么三个入口指向同一 broker：QA 可以单独或同时 `disable` `kafka1/2/3`，模拟「从客户端看整个集群不可达」，而 broker 进程本身仍保持运行。

## 2. 本机路径一览

| 用途 | 路径 |
|------|------|
| Compose / Phase1 | 仓库 `lab/`；共享机 `/workspace/lab-mod-event-kafka/` |
| 断流脚本 | `lab/toxiproxy_cut_restore.sh`（共享机：`/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh`） |
| 拨测 | `lab/dialtest_fast.sh`、`lab/dialtest_originate.sh`（共享机同名脚本在 `/workspace/lab-mod-event-kafka/`） |
| FS 安装前缀/模块缓存 | `/workspace/lab-mod-event-kafka/fs/` |
| 实验室配置（含 TTL） | `/workspace/lab-mod-event-kafka/fs/conf/autoload_configs/event_kafka.conf.xml` |
| 模块源码（TTL 版） | `/workspace/mod_event_kafka-fix/work/` + `include/` + `src/` |
| 验收脚本 | `/workspace/mod_event_kafka-fix/scripts/verify_event_ids.py` |
| L-16 一键脚本 | `/workspace/mod_event_kafka-fix/scripts/run_l16_abc.sh` |
| 证据示例 | `/workspace/mod_event_kafka-fix/reports/l16-abc-20260925-215439/` |

Docker 命令统一使用 `sg docker -c '...'`（当前用户已加入 `docker` 组，需通过 `sg` 使组权限生效）。

### 2.1 与本仓库 `lab/` 的对照

演练的主要命令在仓库根目录下使用相对路径执行。上表中的 `/workspace/lab-mod-event-kafka/…` 是共享实验室机器上的绝对路径，仍然可用（脚本在该目录根下，不带 `lab/` 前缀）。本仓库的 `lab/` 是其中部分文件的同步副本。

| 实验室机器 | 本仓库 |
|------------|--------|
| `/workspace/lab-mod-event-kafka/docker-compose.yml` | `lab/docker-compose.yml` |
| `/workspace/lab-mod-event-kafka/README.md` | `lab/README.md` |
| `/workspace/lab-mod-event-kafka/README-PHASE2.md` | `lab/README-PHASE2.md` |
| `/workspace/lab-mod-event-kafka/toxiproxy.json` | `lab/toxiproxy.json` |
| `/workspace/lab-mod-event-kafka/dialtest_originate.sh` | `lab/dialtest_originate.sh` |
| `/workspace/lab-mod-event-kafka/dialtest_fast.sh` | `lab/dialtest_fast.sh` |
| `/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh` | `lab/toxiproxy_cut_restore.sh` |
| `/workspace/lab-mod-event-kafka/fs/conf/autoload_configs/event_kafka.conf.xml` | 参数对照见 `lab/event_kafka.fs.conf.xml`（仓库内的示例，不是容器里实际使用的那份） |
| `/workspace/mod_event_kafka-fix/`（含 `scripts/verify_event_ids.py`、`scripts/run_l16_abc.sh`、`reports/`） | 本仓库模块源码在根目录 `include/`、`src/`、`mod_event_kafka.cpp`；仓内验收脚本是 `scripts/verify_event_ids.py`（目录参数，见演练手册） |

## 3. 启动栈（Phase1：Kafka + toxiproxy）

```bash
# 本机曾出现 bridge 网络上容器互通失败：FORWARD 策略为 DROP → UNRECORDED / connection refused
sudo iptables-legacy -P FORWARD ACCEPT

# 在仓库内执行；共享机上改为 cd /workspace/lab-mod-event-kafka
cd lab
sg docker -c 'docker compose up -d'
sg docker -c 'docker compose ps'   # lab-kafka-1 应 healthy

# 创建 topic（经 broker 内部监听器）
sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic fs_events --partitions 3 --replication-factor 1"
```

### 3.1 经 toxiproxy 冒烟（必须使用 host 网络）

`EXTERNAL` 的通告地址为 `127.0.0.1:19092`。在 bridge 网络的容器内，`127.0.0.1` 指向容器自身，因此**不要**在 `lab-kafka-1` 内对宿主机映射端口做客户端冒烟。

```bash
BOOT=127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094
IMG=apache/kafka:3.8.1

echo 'hello-fs-events' | sg docker -c "docker run --rm -i --network host $IMG \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $BOOT --topic fs_events"

sg docker -c "docker run --rm --network host $IMG \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BOOT \
  --topic fs_events --from-beginning --timeout-ms 15000"
```

### 3.2 断流 API 自检

```bash
curl -fsS http://127.0.0.1:8474/proxies | head
lab/toxiproxy_cut_restore.sh 2   # 在仓库根目录执行；断开 2 秒后恢复
# 共享机：/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh 2
```

## 4. 启动 FreeSWITCH + mod_event_kafka（Phase2）

镜像：`lab-freeswitch:1.10.12-kafka`（沿用该标签是为了兼容脚本；历史构建曾基于本机安装前缀，实际二进制版本可能显示为 `1.10.7-dev`，以容器内 `freeswitch -version` 的输出为准）。

```bash
sg docker -c 'docker rm -f lab-freeswitch' || true
sg docker -c 'docker run -d --name lab-freeswitch --network host lab-freeswitch:1.10.12-kafka \
  bash -c "export LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib; \
    /usr/local/freeswitch/bin/freeswitch -nonat -nf -nc -nosql -rp"'

# 确认模块已加载
sg docker -c 'docker exec lab-freeswitch env LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib \
  /usr/local/freeswitch/bin/fs_cli -x "module_exists mod_event_kafka"'
```

容器内关键路径（当前镜像布局）：

- 模块：`/usr/local/freeswitch/mod/mod_event_kafka.so`
- 配置：`/usr/local/freeswitch/conf/autoload_configs/event_kafka.conf.xml`
- Outbox：`/usr/local/freeswitch/var/lib/freeswitch/db/event_kafka_outbox.db`

### 4.1 实验室配置要点（参数名保持英文）

```xml
<param name="bootstrap-servers" value="127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"/>
<param name="topic" value="fs_events"/>
<param name="buffer-size" value="100000"/>
<param name="outbox-path" value="/usr/local/freeswitch/var/lib/freeswitch/db/event_kafka_outbox.db"/>
<param name="outbox-ttl-ms" value="120000"/>   <!-- 0=关闭过期 -->
<param name="message-timeout-ms" value="30000"/>
```

加载成功时，日志中应包含 `outbox-ttl-ms: 120000`。

### 4.2 更新模块后如何生效

仅执行 `unload`/`load` 可能仍会映射旧的 `.so`。本机上可靠的做法：

1. 用 `docker cp` 把新的 `.so` 与配置文件拷入运行中的容器  
2. `fs_cli -x 'fsctl shutdown now'` 后 `docker start lab-freeswitch`（保留可写层）  
3. 或者重启容器后立即再次 `docker cp`，然后 `unload`/`load`  
4. 验证通过后，可执行 `docker commit lab-freeswitch lab-freeswitch:1.10.12-kafka` 固化镜像

## 5. 资源与限制（本机事实）

- 存储驱动通常是 **vfs**，日志宜少、堆宜小（Kafka 约 384M）。
- 真实 3 broker、RF≥2：**未**在本机 empirically 验证，仅作为生产参考目标。
- 拆栈（保留镜像与目录）：

```bash
sg docker -c 'docker rm -f lab-freeswitch lab-toxiproxy lab-kafka-1'
# 或：cd lab && sg docker -c 'docker compose down -v --remove-orphans'
# 共享机：cd /workspace/lab-mod-event-kafka && sg docker -c 'docker compose down -v --remove-orphans'
# 之后再单独 rm lab-freeswitch（compose 不包含 FS）
```

## 6. 生产参考（文档草案 / 未 empirically）

以下**不是**本机实验室命令，仅作规划对照：

| 项 | 实验室 | 生产参考方向 |
|----|-----|--------------|
| 集群 | 单 KRaft + 三个代理入口 | 真实多 broker / 云托管 Kafka |
| 故障注入 | toxiproxy disable | 网络分区、滚动重启、ACL/配额（需另行立项） |
| Bootstrap | 仅 toxiproxy 端口 | 业务 bootstrap；**勿**把实验室代理指向生产 |
| 安全 | PLAINTEXT | SASL_SSL / SSL；conf 中 `security-protocol`、`ssl-ca-location` |
| RF / ISR | RF=1 | RF≥2，min.insync.replicas≥2 |
| 容量 | heap 384M、vfs | 独立磁盘、监控 outbox 行数与 `outbox_expired` |

模块部署仍按 `docs/DEPLOY-ROLLBACK.md` 执行：先安装 librdkafka/sqlite，再安装 `.so` 与配置文件，并确认 outbox 目录可写。

## 7. 相关文档

- 故障场景清单：`docs/FAULT-SCENARIOS.md`（L-xx 映射由质检补充）
- 分步演练：`docs/DRILL-RUNBOOK.md`
- 设计与可靠性：`docs/DESIGN.md`、`docs/RELIABILITY.md`
- 用例：`docs/TEST-PLAN.md`
