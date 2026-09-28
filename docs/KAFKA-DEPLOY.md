# Kafka 部署（实验室与生产参照）

本页描述 **mod_event_kafka** 拨测用的实验室 Kafka，并列出和生产参照之间的差异。证据与步骤都来自实验室机器，**未经生产验证**。候选补丁 `0001-kafka-restart-resilience.patch` 的状态是 **NOT_VERIFIED**，只作反面参考，不作为部署依据。

模块安装与回滚见 [DEPLOY-ROLLBACK.md](DEPLOY-ROLLBACK.md)。故障映射见 [FAULT-SCENARIOS.md](FAULT-SCENARIOS.md)（已 empirically 的是 FS-01、FS-09）。断连演练见 [DRILL-RUNBOOK.md](DRILL-RUNBOOK.md)。验收门是 `scripts/verify_event_ids.py`。

## 两套路径

实验室机器上的运行目录，和本仓库同步进来的副本，是同一套 Phase1 材料的两个位置。命令以实验室机器上的文件为准。

| 用途 | 实验室机器 | 本仓库（同步后） |
|------|------------|------------------|
| 运行目录 / compose | `/workspace/lab-mod-event-kafka/` | `lab/docker-compose.yml` |
| Phase1 说明 | `/workspace/lab-mod-event-kafka/README.md` | [lab/README.md](../lab/README.md) |
| Phase2（FreeSWITCH） | `/workspace/lab-mod-event-kafka/README-PHASE2.md` | [lab/README-PHASE2.md](../lab/README-PHASE2.md) |
| 代理定义 | `/workspace/lab-mod-event-kafka/toxiproxy.json` | `lab/toxiproxy.json` |
| 模块示例配置 | 同上目录中的 FS 配置副本 | [lab/event_kafka.fs.conf.xml](../lab/event_kafka.fs.conf.xml) |
| 拨测 | `/workspace/lab-mod-event-kafka/dialtest_originate.sh` | `lab/dialtest_originate.sh` |
| 快速拨测 | `/workspace/lab-mod-event-kafka/dialtest_fast.sh` | 仓库 `lab/` 尚未收入该脚本 |
| 切断 / 恢复 | `/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh` | 仓库 `lab/` 尚未收入该脚本 |

`dialtest_fast.sh` 与 `toxiproxy_cut_restore.sh` 的用法以实验室目录里的脚本为准（见 [DRILL-RUNBOOK.md](DRILL-RUNBOOK.md)）。

## 实验室拓扑

Phase1 是 **单个 KRaft 进程**（broker 与 controller 合一），前面放 toxiproxy。三个宿主机端口都转发到同一个 `kafka-1:9094`。这套布局用来模拟客户端看不到入口，**没有三个独立 broker**。3 broker、副本因子 2 的布局在实验室说明里记为推迟。

| 容器 | 镜像 | 角色 |
|------|------|------|
| `lab-kafka-1` | `apache/kafka:3.8.1` | 单个 KRaft broker+controller。不对宿主机发布端口。堆 `-Xmx384M -Xms256M` |
| `lab-toxiproxy` | `shopify/toxiproxy:2.1.4` | 三个前端 → 同一 broker 的 `EXTERNAL:9094`。宿主机端口 19092–19094，管理端口 8474 |

客户端路径（唯一支持的 bootstrap）：

```
127.0.0.1:19092  →  toxiproxy(kafka1) → kafka-1:9094
127.0.0.1:19093  →  toxiproxy(kafka2) → kafka-1:9094
127.0.0.1:19094  →  toxiproxy(kafka3) → kafka-1:9094
```

Bootstrap 字符串（模块 `bootstrap-servers` 只写这一条）：

```
127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094
```

- 管理 API：`http://127.0.0.1:8474`
- 测试主题：`fs_events`（partitions=3，replication-factor=1）
- 监听安全协议：`PLAINTEXT`（controller / internal / external）
- 副本相关项在 compose 里都是 1：`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`、`KAFKA_DEFAULT_REPLICATION_FACTOR`、`KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR`，`MIN_ISR=1`
- 存储驱动是 `vfs`，broker 日志在容器内 `/tmp`（`KAFKA_LOG_DIRS=/tmp/kraft-combined-logs`）

`EXTERNAL` 通告的是 `127.0.0.1:19092`。一次性客户端要用 `--network host`（或直接在宿主机上跑）。在 `lab-kafka-1` 里面访问宿主机上的 19092 会连到容器自己。

## 启动与停止

在实验室机器上执行。容器之间如果出现 `UNRECORDED` votes 或 connection refused，先把 iptables-legacy 的 `FORWARD` 策略改成 `ACCEPT`（该机器上曾经是 `DROP`，桥接流量会被丢掉）：

```bash
cd /workspace/lab-mod-event-kafka
sudo iptables-legacy -P FORWARD ACCEPT
sg docker -c 'docker compose up -d'
sg docker -c 'docker compose ps'
```

停止并清掉卷：

```bash
cd /workspace/lab-mod-event-kafka
sg docker -c 'docker compose down -v --remove-orphans'
```

只拿到本仓库时，compose 文件在 `lab/docker-compose.yml`，内容与实验室目录中的 compose 一致。在仓库里启动时把 `cd` 换成 `lab/` 所在目录；拨测、切断脚本仍使用上面的实验室绝对路径。

创建主题、经 toxiproxy 做生产/消费冒烟的命令写在 [lab/README.md](../lab/README.md) 的「健康检查 / 冒烟」一节。管理用的 `kafka-topics.sh` 走 broker 内部 `localhost:9092`；业务客户端只走 toxiproxy bootstrap。

FreeSWITCH 不在这份 compose 里。Phase2 容器 `lab-freeswitch`（镜像 `lab-freeswitch:1.10.12-kafka`，`--network host`）的启动方式见 [lab/README-PHASE2.md](../lab/README-PHASE2.md)。

## 模块配置要点（实验室）

运行中的文件：

`/usr/local/freeswitch/etc/freeswitch/autoload_configs/event_kafka.conf.xml`

仓库里的对照副本：[lab/event_kafka.fs.conf.xml](../lab/event_kafka.fs.conf.xml)。

| 参数 | 实验室值 | 说明 |
|------|----------|------|
| `bootstrap-servers` | `127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094` | 只经 toxiproxy。不要写成 broker 容器网或生产集群地址 |
| `topic` | `fs_events` | 与上面创建的主题一致 |
| `buffer-size` | `100000` | XML 键名是 `buffer-size`。这是既有的 librdkafka 缓冲参数，和下面的 `mem-queue-max` 不是同一个队列 |
| `outbox-ttl-ms` | `120000` | 模块默认也是 `120000`（2 分钟）。`0` 关闭过期。超过该时长的 **pending** 行标为 `dead`，`last_error=expired_ttl`，不再投递 |
| `outbox-path` | `/usr/local/freeswitch/var/lib/freeswitch/db/event_kafka_outbox.db` | 实验室路径。设计文档中的默认路径是 `/var/lib/freeswitch/event_kafka_outbox.db` |
| `mem-queue-max` | `10000` | 有界内存队列深度（模块默认） |
| `message-timeout-ms` | `30000` | 须经 topic 配置生效 |

Kafka 记录头名是 `x-fs-event-id`（正文 JSON 不改）。L-16 归档运行时模块配置为 `outbox-ttl-ms=120000`（对应仓库提交 `37e89154`）。

## 实验室与生产参照

| 项 | 实验室（本页） | 生产参照（尚未在本仓库验证） |
|----|----------------|------------------------------|
| Broker | 1 个 KRaft 进程 | 多个独立 broker |
| 客户端入口 | 3 个 toxiproxy 端口，上游是同一个 `kafka-1:9094` | 真实的多地址 bootstrap，每个地址背后是对应 broker |
| 副本 | replication-factor=1，`MIN_ISR=1` | 按集群要求设置副本与 ISR；单 broker 宕机时仍要满足 ISR |
| 安全 | `PLAINTEXT`，用户名密码为空 | `security-protocol` 取 `SASL_PLAINTEXT` / `SASL_SSL` / `SSL`，并配置 `ssl-ca-location` |
| 故障注入 | toxiproxy 禁用入口或（计划中的）toxics | 由变更窗口、滚动重启和真实网络策略执行，不把生产集群接到这套 toxiproxy |
| 堆与磁盘 | 约 384M 堆，`vfs`，日志在容器 `/tmp` | 按容量规划的堆、持久化日志目录与监控 |

不要把 FreeSWITCH、生产 Kafka 或云端 Kafka 接到这套实验室栈上。生产环境的安装顺序仍以 [DEPLOY-ROLLBACK.md](DEPLOY-ROLLBACK.md) 为准：`bootstrap-servers` 填目标环境自己的地址，`outbox-path` 对 FreeSWITCH 用户可写。
