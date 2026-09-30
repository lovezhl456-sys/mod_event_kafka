# FS-11 / Err-87 复现方法与整体思路

> **状态：已 empirically（旧无 outbox + FS 1.6.20 lab）。未经生产验证。**  
> 本文是 FS-11 的总览，只讲整体思路和复现方法，给运维按图索骥用。分步命令、验收门与逐项证据仍以 [FAULT-SCENARIOS.md](FAULT-SCENARIOS.md) 的 FS-11、[DRILL-RUNBOOK.md](DRILL-RUNBOOK.md) §10、[STATUS.md](STATUS.md) 为准，本文不重复展开。

**先记住三件事：**

1. 用户说的 **Err-87** 是 Kafka broker 返回的 **+87 `INVALID_RECORD`**（broker 校验记录失败），**不是** librdkafka 本地的 **-187 `ALL_BROKERS_DOWN`**（断连）。两者不得混用。
2. toxiproxy 短断（35s / 150s）制造的是断连，对应 **-187** 和 FS-01 / FS-09 演练，**不能**复现 FS-11。
3. 本文的实证全部来自实验室，**未经生产验证**。

**三条路径（原样）：**

| 用途 | 路径 |
|------|------|
| 工具链 | `/workspace/lab-mod-event-kafka/fs16/`（含 1.6.20 前缀 / `rebuild-mod.sh`） |
| 主证据 | `reports/fs11-err87-fs16-20260930-162827/` |
| 对照 | `reports/fs11-err87-20260930-154534/` |

---

## 1. 背景与现象

云 Kafka 升级后，生产 FreeSWITCH 1.6 上的 `mod_event_kafka` 开始持续打出：

```text
[ERR] mod_event_kafka.cpp:197 Message delivery failed Err-87
```

`Err-87` 就是 broker 错误码 **+87**，名字是 `INVALID_RECORD`：broker 收到了记录，但记录没通过 broker 的校验，于是拒收。链路本身是通的，这不是网络中断。

## 2. 根因思路（按机制推导，已在实验室复现）

1. 旧模块（upstream，**无 outbox**）用事件头 `Channel-Call-UUID` 作为 Kafka key：

   ```text
   PublishEvent: char *uuid = switch_event_get_header(event, "Channel-Call-UUID");
   send(event_json, uuid, 0);
   send(): key_length = key == NULL ? 0 : strlen(key);
           rd_kafka_produce(..., key, key_length, ...);
   ```

2. `event-filter` 为空时订阅 `SWITCH_EVENT_ALL`。HEARTBEAT、RE_SCHEDULE 等系统事件本来就没有 `Channel-Call-UUID`，于是以 **null key** 发出。
3. 如果 topic 是 `cleanup.policy=compact`，broker 要求每条记录都有非空 key（按 key 保留最新值），null key 在校验阶段被拒绝，返回 **+87 `INVALID_RECORD`**。
4. 旧模块在投递回调 `dr_msg_cb`（`mod_event_kafka.cpp:197`，格式串 `Message delivery failed %s`）里打出 ERR。旧模块没有 outbox，也没有重试流水线：直接 produce，失败只记日志。

**对照（排除法）：** 同一条 null-key 路径换到 `cleanup.policy=delete` 的 topic，不出现 **+87**（R2）。较早的 FS 1.10.x 对照一轮里另做过独立冒烟：compact + 非空 key 也是 OK。所以问题出在「compact topic + null key」的组合上，与断连无关。

> 可能的生产诱因：云 Kafka 升级后，目标 topic 的 `cleanup.policy` 变成（或本来就是）`compact`。这是推断，需要到生产侧用 `kafka-topics.sh --describe` 核对 topic 配置，本文不宣称已确认。

## 3. 错误码对照（不得混用）

| 代码 | 名称 | 谁产生 | 日志里长什么样 | 属于哪个场景 |
|------|------|--------|----------------|--------------|
| **+87** | `INVALID_RECORD` | Kafka **broker** 校验拒收 | 新一些的 librdkafka：`Message delivery failed Broker: Broker failed to validate record`；旧 librdkafka 不认识该码时印成 `Err-87?`，生产日志里是 `Err-87` | **FS-11**（本文） |
| **-187** | `ALL_BROKERS_DOWN` | librdkafka **本地**错误（所有 broker 断连） | 断连 / 断流类日志 | FS-01 / FS-09，toxiproxy 短断演练 |

- **+87** 是正号，来自 broker；**-187** 是负号，来自客户端本地。不要把 `Err-87` 解释成 `ALL_BROKERS_DOWN`。
- toxiproxy 短断（`lab/toxiproxy_cut_restore.sh`，35s / 150s）只覆盖 **-187** / 断连演练，**不是** FS-11。任何 **-187** 日志都不能当作 FS-11 PASS。

## 4. 复现路径（运维可跟）

整体分三步，全部由主证据目录里的 `reproduce.sh` 完成。命令以该脚本原文为准，本文只摘要，不另编命令或镜像标签。

```bash
bash /workspace/mod_event_kafka-fix/reports/fs11-err87-fs16-20260930-162827/reproduce.sh
```

| 步骤 | 做什么 | 期望 | 结果（RESULT，2026-09-30 16:29:59 +0800 Asia/Shanghai） |
|------|--------|------|------|
| **R0** 起栈 | `cd /workspace/lab-mod-event-kafka && docker compose up -d`（Kafka `apache/kafka:3.8.1` + `shopify/toxiproxy:2.1.4`）；在 `lab-kafka-1` 上建两个 topic（各 1 分区、RF=1）：`fs_events_compact`（`cleanup.policy=compact`）与 `fs_events_delete`（`cleanup.policy=delete`）；用镜像 `lab-freeswitch:1.6.20-kafka` 起容器 `lab-freeswitch-16`（`--network host`），拷入旧 `.so` | FS 1.6.20 + Kafka 起来，`module_exists=true` | PASS |
| **R1** compact + null key | 拷入证据目录 `conf/event_kafka.conf.xml`（topic `fs_events_compact`），`fsctl shutdown now` 后 `docker start` | 出现 **+87**：`[ERR] mod_event_kafka.cpp:197  Message delivery failed Broker: Broker failed to validate record` | PASS，`R1_INVALID_COUNT=670` |
| **R2** delete 对照 | 先用 `wc -c` 记下日志长度，换上 `conf/event_kafka.conf.delete.xml`（topic `fs_events_delete`），同样 shutdown 再 start，只看这次重启之后的新日志 | 没有 `Message delivery failed` / `Broker failed` | PASS，`R2_INVALID_COUNT=0` |

跟做时注意：

- 脚本开头会执行 `sudo iptables-legacy -P FORWARD ACCEPT`，并通过 `sg docker` 调 docker，需要 sudo 与 docker 组权限。
- 脚本会 `docker rm -f lab-freeswitch-16` 后重建容器；topic 用 `--if-not-exists` 创建，重复执行不会报错。
- toxiproxy 只作为 bootstrap 的转发代理在栈里跑着，本流程**不**切断它，也**不**调用 `toxiproxy_cut_restore.sh`。
- 1.6.20 镜像的模块目录是 `/usr/local/freeswitch/lib/freeswitch/mod`，配置目录是 `/usr/local/freeswitch/etc/freeswitch/autoload_configs`，与 1.10.x 镜像的 `/usr/local/freeswitch/mod` 不同。
- **栈目前仍在跑**（RESULT 记录：`lab-kafka-1`、`lab-toxiproxy`、`lab-freeswitch-16` 在 host 网络上 UP，FS 当前挂的是 delete topic 的配置；构建容器 `fs16-build` 可能也还在）。直接看现状即可，不必先拆栈；重跑 `reproduce.sh` 会重建 `lab-freeswitch-16`。

### 重编旧 `.so`（需要时）

旧模块在主证据目录里已有编好的 `build-upstream/mod_event_kafka.so`（md5 `063e55cb0a584839f68d44a22dab62e6`）。如需重编，按主证据目录的 `REBUILD-SO.md`，工具链在：

- 工具链：`/workspace/lab-mod-event-kafka/fs16/`（含 1.6.20 前缀 / `rebuild-mod.sh`）
  - 辅助脚本：`/workspace/lab-mod-event-kafka/fs16/scripts/rebuild-mod.sh`
  - FS 1.6.20 前缀：`/workspace/lab-mod-event-kafka/fs16/prefix`
  - 上游旧模块源码副本：`/workspace/lab-mod-event-kafka/fs16/upstream`（从 `/workspace/mod_event_kafka-fix/upstream/` 同步）
- 构建容器 `fs16-build`（debian:buster-slim）里把 Makefile 的 `-std=c++17` 改成 `-std=c++14` 再 `make`；librdkafka 用实验室的 1.9.2（`/usr/local`）

编出的 `.so` 不要提交进本仓库。

## 5. 环境要件

| 项 | 值 |
|----|----|
| FreeSWITCH | **1.6.20**：`FreeSWITCH version: 1.6.20+git~20180123T214909Z~987c9b9a2a~64bit`（git `987c9b9`，2018-01-23 21:49:09Z 64bit） |
| 镜像 | `lab-freeswitch:1.6.20-kafka`（id `f057ade5fc2f`，约 192MB） |
| 容器 | `lab-freeswitch-16`（`--network host`） |
| 镜像来源 | signalwire/freeswitch 标签 v1.6.20，在 debian:buster-slim 上源码构建，OpenSSL 1.0.2u（`/opt/openssl10`）；头文件 / pkg-config 为 `freeswitch` 1.6.20，前缀 `/usr/local/freeswitch` |
| 模块 | upstream **旧模块、无 outbox**，按 FS 1.6.20 头文件编译；失败日志在 `dr_msg_cb`、`mod_event_kafka.cpp:197` |
| Kafka | 实验室 `apache/kafka:3.8.1`，经 `shopify/toxiproxy:2.1.4` 转发 |

**关于 FS 1.6 曾被挡住：** 更早一轮拉的是 `praekeltfoundation/freeswitch:1.6`（Debian Jessie，仅运行时），没有头文件和 librdkafka，Jessie apt 源也已 EOL，编不出 1.6 的模块，所以那一轮改在 FS 1.10.x 上跑（即下面的对照）。后来用源码构建的 `lab-freeswitch:1.6.20-kafka` 解除了这个阻塞，R1/R2 在 1.6.20 上重跑并通过。不要写成「FS 1.6 一直可用」。

## 6. 证据索引

`reports/` 被 `.gitignore` 忽略，证据留在实验室机器上，本仓库只记路径，不提交证据或 `.so`。

**主证据（FS 1.6.20）：** `reports/fs11-err87-fs16-20260930-162827/`  
实验室绝对路径：`/workspace/mod_event_kafka-fix/reports/fs11-err87-fs16-20260930-162827/`

| 文件 | 看什么 |
|------|--------|
| RESULT | R0/R1/R2 均 PASS；FS 1.6.20、镜像与容器；Jessie 阻塞已被源码构建取代 |
| `fs-version.txt` | `freeswitch -version` 与 `fs_cli` 均为 1.6.20；`module_exists=true` |
| `topic-config.txt` | `fs_events_compact` 为 `cleanup.policy=compact`，`fs_events_delete` 为 `cleanup.policy=delete` |
| broker-reason | `R1_INVALID_COUNT=670`，以及第 197 行 `Broker failed to validate record` 原文 |
| R2 计数 | `R2_INVALID_COUNT=0` |
| 模块说明 | 旧模块 key 路径、第 197 行格式串、安装路径与 md5；无 outbox |
| `reproduce.sh` | 实际执行的复现脚本（目标镜像 `lab-freeswitch:1.6.20-kafka`） |
| `REBUILD-SO.md` | 在 `/workspace/lab-mod-event-kafka/fs16/` 上重编旧模块 |

**对照（FS 1.10.x，保留不删）：** `reports/fs11-err87-20260930-154534/`  
实验室绝对路径：`/workspace/mod_event_kafka-fix/reports/fs11-err87-20260930-154534/`

- 旧无 outbox + FS 1.10.x lab：镜像 `lab-freeswitch:1.10.12-kafka`（二进制报告 1.10.7-dev），容器 `lab-freeswitch`
- R1 窗口 482 行 `INVALID_RECORD`；独立冒烟 `DR_FAIL err=87 ... topic=fs_events_compact key_len=0`；R2 delivery-fail = 0
- 该目录的 `reproduce.sh` 针对 1.10.x 镜像，不要拿它跑 1.6.20；其中的 `fs16-blocker.txt` 记录的是当时的阻塞，已被上面的 1.6.20 源码构建取代
- 这一轮不代替 1.6.20 主证据

只有一行 `Err-87?`、没有 topic 配置和 broker 侧拒收原因，不算证据齐。

## 7. 与现网 / 当前模块的关系

- 上面的实证针对的是 **旧无 outbox 模块**，即生产现象里那条 `mod_event_kafka.cpp:197` 日志的来源。
- 当前 master 已换成 outbox 流水线：加载日志为 `KafkaEventPublisher Initialising (outbox pipeline)...`，投递回调在 `KafkaPipeline::on_delivery`，**不会**再打出 `mod_event_kafka.cpp:197` 那一行。用当前 master 的 `.so` 做同样实验，不能宣称复现了这一行。
- 代码阅读（**实验室未跑，未经验证**）：当前 master 仍以 `Channel-Call-UUID` 作 key，缺头时同样发 null key；`KafkaPipeline::is_permanent_error` 没有把 `INVALID_RECORD` 列为永久错误，按代码会走 `mark_retry` 退避重试，而不是 `mark_dead`。也就是说，topic 若是 compact，换成当前 master 并不会让 +87 自动消失。
- R3（收紧 `message.timestamp.*.max.ms`，另一条可能打出 **+87** 的路径）未跑，不计入。
- 以上全部 **未经生产验证**。

## 8. 相关文档

- [FAULT-SCENARIOS.md](FAULT-SCENARIOS.md) — FS-11 场景定义、如何注入、验收门、+87 / -187 对照
- [DRILL-RUNBOOK.md](DRILL-RUNBOOK.md) §10 — FS-11 分步复现（R0/R1/R2）、证据文件清单、明确不做的事
- [STATUS.md](STATUS.md) — FS-11 已跑结果与证据路径
- [KAFKA-DEPLOY.md](KAFKA-DEPLOY.md) — 实验室 Kafka / toxiproxy 拓扑

以上文档的 FS-11 段落与本文口径一致：主证据为 FS 1.6.20 lab，1.10.x 一轮保留为对照。
