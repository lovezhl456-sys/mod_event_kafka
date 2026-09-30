# Kafka / mod_event_kafka 演练手册（分步）

> 目标：在本机实验室上按固定步骤复现「短断自愈 / 超 TTL 过期 / 恢复后新呼叫」。  
> §0–§9 的命令均取自本机已跑通的流程。参数名、`x-fs-event-id`、状态字面量保持英文。  
> §10（FS-11）记录 2026-09-30 已跑通的 R1/R2：**已 empirically（旧无 outbox + FS 1.10.x lab）**。生产目标仍是 FS 1.6，这次没有在 FS 1.6 上跑，也不宣称生产验证。  
> 本文步骤均在仓库根目录执行，主要命令使用相对路径（`lab/toxiproxy_cut_restore.sh`、`lab/dialtest_fast.sh`、`lab/dialtest_originate.sh`）。共享实验室机器上仍可用绝对路径 `/workspace/lab-mod-event-kafka/…`（脚本在该目录根下，不带 `lab/` 前缀）。对照见 [KAFKA-DEPLOY.md](KAFKA-DEPLOY.md) §2.1。

对应故障场景：短断是 [FAULT-SCENARIOS.md](FAULT-SCENARIOS.md) 的 FS-01；超 TTL 与恢复后新呼叫是 FS-09。§10 是 FS-11（broker **+87** `INVALID_RECORD`），R1/R2 已在 FS 1.10.x + 旧模块上 PASS。不要用 §3 / §4 的断流脚本去跑它。

## 0. 前置检查

```bash
# Docker 可用
sg docker -c 'docker info' >/dev/null

# bridge 网络容器互通（本机曾需要此设置）
sudo iptables-legacy -P FORWARD ACCEPT

# 检查镜像是否已在本地（FS 镜像需事先 build/commit）
sg docker -c 'docker images | rg "apache/kafka|toxiproxy|lab-freeswitch"'
```

确认配置中包含 `outbox-ttl-ms=120000`，且加载日志中可见该值（这是超 TTL 演练的硬性前提）。

## 1. 启动栈

```bash
# 仓库内的 compose 在 lab/；共享机上的目录为 /workspace/lab-mod-event-kafka
cd lab
sg docker -c 'docker compose up -d'
sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic fs_events --partitions 3 --replication-factor 1"

sg docker -c 'docker rm -f lab-freeswitch' || true
sg docker -c 'docker run -d --name lab-freeswitch --network host lab-freeswitch:1.10.12-kafka \
  bash -c "export LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib; \
    /usr/local/freeswitch/bin/freeswitch -nonat -nf -nc -nosql -rp"'

# 等待 FS 就绪
sleep 5
sg docker -c 'docker exec lab-freeswitch env LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib \
  /usr/local/freeswitch/bin/fs_cli -x "module_exists mod_event_kafka"'
# 期望：true
```

可选：经 toxiproxy 做 Kafka 生产/消费冒烟（见 `KAFKA-DEPLOY.md` §3.1）。

## 2. 基线拨测

优先使用快拨测（避免 park 路径因长时间 `NO_ANSWER` 而阻塞）：

```bash
lab/dialtest_fast.sh 5
# 或：lab/dialtest_originate.sh 2
```

验收要点：

- `module_exists=true`
- FS 日志出现 `enqueued event_id=...`
- 经 toxiproxy 消费时可见消息头 `x-fs-event-id:...`

## 3. 短断演练（约 35s，小于 TTL）

对应 L-02 / L-07 / L-16a 的思路：断连期间事件写入 outbox，恢复后**无需 reload 模块**即可排空。

```bash
# 终端 A：断开全部入口，35 秒后自动恢复
lab/toxiproxy_cut_restore.sh 35

# 终端 B：在断连期间拨测（与 A 时间重叠）
lab/dialtest_fast.sh 10
```

恢复后等待若干秒，让 worker 排空，然后执行：

```bash
python3 /workspace/mod_event_kafka-fix/scripts/verify_event_ids.py \
  --injected <注入 id 列表文件> \
  --consumed <消费到的 x-fs-event-id 列表文件>
```

期望：注入集合与消费集合一致（历史证据：`reports/l02-l07-fs-20260925-133618/`、`reports/l16-abc-20260925-215439/l16a`）。STATUS 记录的 L-16 根目录是 `reports/l16-abc-20260925-215439/`。

本仓库的 `scripts/verify_event_ids.py` 只接受一个目录参数：`scripts/verify_event_ids.py reports/<run-id>/`，该目录中须包含 `injected_ids.txt` 与 `consumed_ids.txt`。上面的 `--injected` / `--consumed` 是实验室机器 `/workspace/mod_event_kafka-fix/scripts/verify_event_ids.py` 的用法。调用哪一份，就使用哪一份的参数；两者的退出码 `0` 都表示 VERIFY_OK。

一键脚本（本机上已有）：

```bash
# 包含短断 + 超 TTL + 恢复后新呼叫；结果写入 reports/l16-abc-<时间戳>/
bash /workspace/mod_event_kafka-fix/scripts/run_l16_abc.sh
```

## 4. 超 TTL 全断演练（约 150s，大于默认的 120s）

对应 L-16b：pending 行超过 TTL → `state=dead`，`last_error=expired_ttl`，**不会写入 topic**。

```bash
lab/toxiproxy_cut_restore.sh 150
# 在断连期间拨测，注入一批 id
lab/dialtest_fast.sh 10
```

恢复后：

1. 检查 outbox / 指标：过期计数看 `outbox_expired`；死信的 `last_error` 为 `expired_ttl`
2. 消费 topic：这批过期 id **不应**出现在消费到的 `x-fs-event-id` 中（`expired_still_in_topic=0`）

## 5. 恢复后新呼叫（L-16c）

在 §4 恢复完成、代理已为 `enabled=true` 之后：

```bash
lab/dialtest_fast.sh 10
# verify_event_ids.py → 新的一批应 VERIFY_OK（历史结果：30/30）
```

期望：TTL 丢弃过期事件后，新事件仍可正常投递；同样**无需**为 Kafka 恢复而 reload 模块。

## 6. 常用辅助命令

```bash
# 查看代理状态
curl -fsS http://127.0.0.1:8474/proxies

# 手动关闭/开启单个入口
curl -fsS -X POST http://127.0.0.1:8474/proxies/kafka1 \
  -H 'Content-Type: application/json' -d '{"enabled":false}'

# 带消息头消费
sg docker -c 'docker run --rm --network host apache/kafka:3.8.1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 127.0.0.1:19092 --topic fs_events \
  --from-beginning --timeout-ms 20000 \
  --property print.headers=true --property print.value=false'
```

## 7. 收尾：拆除栈

```bash
sg docker -c 'docker rm -f lab-freeswitch lab-toxiproxy lab-kafka-1'
# 镜像、lab/（共享机上为 /workspace/lab-mod-event-kafka）与模块源码均保留，下次回归时再启动
```

## 8. 证据与对照

| 步骤 | 用例 | 本机证据目录（示例） |
|------|------|----------------------|
| 短断自愈 | L-02 / L-07 / L-16a | `reports/l02-l07-fs-20260925-133618/`、`reports/l16-abc-20260925-215439/l16a` |
| 超 TTL 转死信 | L-16b | `reports/l16-abc-20260925-215439/l16b` |
| 恢复后新呼叫 | L-16c | `reports/l16-abc-20260925-215439/l16c` |
| FS-11 R1/R2 | broker **+87** `INVALID_RECORD`（FS 1.10.x + 旧模块，不是 FS 1.6） | `reports/fs11-err87-20260930-154534/` |

本手册 §10 记录 FS-11 的 R1/R2，证据在 `reports/fs11-err87-20260930-154534/`（实验室机器 `/workspace/mod_event_kafka-fix/reports/fs11-err87-20260930-154534/`）。这是 FS 1.10.x + 无 outbox 旧模块，不是 FS 1.6，也不要和已 PASS 的 L-16 断流结论混成同一个错误。尚未逐步展开的项：单入口 disable、延迟/带宽 toxic、杀 broker 进程、磁盘写满，以及 FS-11 的可选 R3（`message.timestamp.*.max.ms`）。需要时再扩充脚本。

## 9. 安全提醒

- 演练用的 bootstrap **只能**是 toxiproxy 的三个端口。  
- 禁止把实验室配置改成生产/云端 Kafka 地址。  
- 本文不宣称已在生产环境验证。  
- §10 复现的是实验室 Kafka 3.x 上的 broker 校验拒收，不是把模块指到云端 Kafka。

## 10. FS-11 Err-87 `INVALID_RECORD`（已 empirically：旧无 outbox + FS 1.10.x lab）

> **R0/R1/R2 已于 2026-09-30 PASS。** 证据目录 `reports/fs11-err87-20260930-154534/`（实验室机器 `/workspace/mod_event_kafka-fix/reports/fs11-err87-20260930-154534/`）。`reports/` 不入库，与 L-16 相同。  
> **不是**「已在 FS 1.6 实证」。生产目标仍是 FS 1.6；这次用的是镜像 `lab-freeswitch:1.10.12-kafka`，二进制报告 FreeSWITCH **1.10.7-dev**。未经生产验证。  
> 场景定义见 [FAULT-SCENARIOS.md](FAULT-SCENARIOS.md) 的 FS-11。  
> **不要**执行 `lab/toxiproxy_cut_restore.sh`（也不要执行共享机上的 `/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh`）来做本节。短断 35s 与超 TTL 断连 150s 制造的是客户端断连，对应 **-187** `ALL_BROKERS_DOWN`，不是 **+87**。

用户日志里的 `Err-87` 是 Kafka **broker** 错误码 **+87**，名字是 `INVALID_RECORD`。旧版 librdkafka 不认识该码时会印成 `Err-87?`。本次实验室打出的是同一码的具名文本：

```text
[ERR] mod_event_kafka.cpp:197  Message delivery failed Broker: Broker failed to validate record
```

独立冒烟：`DR_FAIL err=87 (Broker: Broker failed to validate record) topic=fs_events_compact key_len=0`。

librdkafka **本地**错误 **-187** 的名字是 `ALL_BROKERS_DOWN`，属于断连/断流。FS-01 等短断演练覆盖的是这一类。它不是 Err-87。书写时保持两个码分开：**+87** 是 broker 校验拒收，**-187** 是断连。不要把 `Err-87` 解释成 `ALL_BROKERS_DOWN`。

| 代码 | 名称 | 本次运行 |
|------|------|----------|
| **+87** | `INVALID_RECORD` | R1 看到的就是这个码。文本是 `Broker: Broker failed to validate record`（`err=87`） |
| **-187** | `ALL_BROKERS_DOWN` | 本次没有出现。短断/断连日志归 FS-01 / FS-09，不能当作 FS-11 PASS |

已通过的模块是 upstream **旧模块、无 outbox**。失败日志在 `dr_msg_cb`、`mod_event_kafka.cpp:197`。加载日志是第 87 行的 `KafkaEventPublisher Initialising...`，以及 `Subscribed to ALL events`。当前 master 走 outbox 流水线，加载时会写 `Initialising (outbox pipeline)`，投递报告在 `KafkaPipeline::on_delivery`，**不会**打出第 197 行。用当前 master 的 `.so` 做同样的 topic 实验，不能宣称已经复现了这一行。

### 10.1 这次实际用的 FS（R0 PASS）与 FS 1.6 阻塞

`RESULT.txt`：

- R0 FS+Kafka：PASS
- 版本字符串：`FreeSWITCH version: 1.10.7-dev+git~20210825T173719Z~dd2411336f~64bit`（git `dd24113`，2021-08-25 17:37:19Z 64bit）
- 镜像标签：`lab-freeswitch:1.10.12-kafka`（二进制报告 1.10.7-dev）
- 模块：upstream OLD（无 outbox），`cpp:197` `dr_msg_cb`
- 源码在实验室机器 `/workspace/mod_event_kafka-fix/upstream/`，按 FS 1.10.x 头文件（`/workspace/lab-mod-event-kafka/fs/prefix`）编译后装入容器 `/usr/local/freeswitch/mod/mod_event_kafka.so`。本仓库不提交该 `.so`

`fs16-blocker.txt`（FS 1.6 尝试失败，因此 **没有** FS 1.6 实证）：

- 拉取过 `praekeltfoundation/freeswitch:1.6`（FreeSWITCH 1.6.20，Debian Jessie）
- 镜像里没有 freeswitch-dev / `/usr/include/freeswitch` 头文件
- 镜像里没有 librdkafka；Jessie 的 apt 源已 EOL（要归档镜像并重编才行）
- 实验室现成头文件与前缀是 FS 1.10.12（bookworm），与 1.6 ABI 不兼容
- 从源码做一整套 FS 1.6 工具链不在这次运行里
- 决定：继续用 FS 1.10.12 镜像 + 旧 upstream `.so`（同一条 cpp:197 null-key 路径）
- 生产目标仍是 FS 1.6。Err-87 的拒收是模块与 broker 的行为，不是 FS 大版本专有。文档不得写成「已在 FS 1.6 实证」

Kafka 仍是 `apache/kafka:3.8.1`，bootstrap 只走 `127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094`。代理保持启用。本节不 disable toxiproxy。

### 10.2 已跑通的复现脚本

证据目录里的 `reproduce.sh` 就是这次执行的脚本。在实验室机器上：

```bash
bash /workspace/mod_event_kafka-fix/reports/fs11-err87-20260930-154534/reproduce.sh
```

脚本要点（与证据一致，便于对照；不要改去调用 `toxiproxy_cut_restore.sh`）：

1. `cd /workspace/lab-mod-event-kafka`，`docker compose up -d`
2. 创建两个 topic，都是 partitions=1、RF=1：
   - `fs_events_compact`，`--config cleanup.policy=compact`
   - `fs_events_delete`，`--config cleanup.policy=delete`
3. 用 `lab-freeswitch:1.10.12-kafka` 起 FS（`--network host`），把证据目录里编好的旧 `.so` 拷进 `/usr/local/freeswitch/mod/mod_event_kafka.so`
4. **R1：** 拷入 compact 配置（证据目录 `conf/event_kafka.conf.xml`），`fsctl shutdown` 后再 `docker start`，让旧模块重新加载。`event-filter` 为空，日志是 `Found 0 subscriptions` / `Subscribed to ALL events`，topic 为 `fs_events_compact`
5. **R2：** 记下当时日志长度，换上 delete 配置（`conf/event_kafka.conf.delete.xml`），同样重启。topic 为 `fs_events_delete`。只看这次重启之后的新日志

`topic-config.txt` 里两次 `--describe` 分别为 `cleanup.policy=compact` 与 `cleanup.policy=delete`。

### 10.3 R1：compact + 缺少 `Channel-Call-UUID`（PASS）

旧模块用 `Channel-Call-UUID` 当 Kafka key。头不存在时 key 指针为 NULL，`rd_kafka_produce` 发出 null key。`cleanup.policy=compact` 的 topic 需要非空 key，才能按 key 留下最新一条；null key 在 broker 校验阶段被拒绝，返回 **+87** `INVALID_RECORD`。

这次没有改源码去强制 null key。模块说明写明：强制 null key 的实验目录编过，但 **R1 没用它**。`event-filter` 为空，启动时的 HEARTBEAT、RE_SCHEDULE，以及不少 CUSTOM/系统事件没有 `Channel-Call-UUID`，于是走出这条路径。

通过时 FS 日志为上面的第 197 行。R1 窗口里 `INVALID_RECORD` 行数是 482。独立冒烟同时记下：`err=87`、`topic=fs_events_compact`、`key_len=0`。

`broker-reason.txt` 的归因：compact + NULL → err=87；delete + NULL → OK；compact + 非空 key → OK。证据日志里另外两条 `DR_OK`（`key_len=0` 与 `key_len=13`）是这组独立对照的成功侧，不是 R1 的 FS 失败日志。

### 10.4 R2：delete topic，同一条 null-key 路径（PASS）

同一旧模块，topic `fs_events_delete`，`cleanup.policy=delete`。`RESULT.txt` 与 R2 日志：delivery-fail = 0，`INVALID_RECORD` = 0。消费抽样里能看到被接受的 `HEARTBEAT` 与 `MODULE_LOAD`。

这是「同一路径在 delete 上不出现 **+87**」的对照。对照里如果出现 **-187**，那是另一次断连，既不能充当 R2，也不能充当 R1。

### 10.5 证据目录里要有的文件

`reports/fs11-err87-20260930-154534/`（实验室绝对路径见本节开头）至少包括：

| 文件 | 作用 |
|------|------|
| `RESULT.txt` | R0/R1/R2 PASS，FS 版本，以及「FS 1.6 这次没用」 |
| `broker-reason.txt` | broker 拒收原因：compact 要求非空 key；null key → **+87**；并写明不是 **-187** |
| `topic-config.txt` | `fs_events_compact` / `fs_events_delete` 的 `cleanup.policy` |
| `reproduce.sh` | 实际执行的复现命令 |
| `fs16-blocker.txt` | FS 1.6 镜像为何没能编过旧模块 |
| 模块说明 | 旧模块 key 路径、第 197 行格式串、R1 未使用强制 null key 补丁 |
| R1/R2 日志 | 第 197 行，以及 R2 的 0 次失败 |

只有一行 `Err-87?`、没有 topic 配置和 `broker-reason.txt`，不算本场景证据齐。本次实验室文本已经是 `Broker: Broker failed to validate record`，并用 `err=87` 对上 **+87**。

### 10.6 可选 R3（本次未跑）

收紧 `message.timestamp.*.max.ms` 仍是另一条可能打出 **+87** 的路径，**没有**出现在这次 PASS 里。不要把 R1 的 compact + null key 写成时间戳窗口。若以后要跑，放在 `cleanup.policy=delete` 的 topic 上，避免和 R1 的原因叠在一起，并在 `broker-reason.txt` 里写时间戳窗口。未跑之前不要给 R3 标 empirically。

### 10.7 本节明确不做的事

- 不用 `lab/toxiproxy_cut_restore.sh`，也不要 disable `kafka1`/`kafka2`/`kafka3` 来制造 Err-87。
- 不把 **-187** `ALL_BROKERS_DOWN` 写成 FS-11 通过。
- 不在当前 master 的 outbox 模块上寻找 `mod_event_kafka.cpp:197`。
- 不把这次结果写成已在 FS 1.6 上实证。
- 不把实验室 bootstrap 改成生产或云端 Kafka。
- 不用 `scripts/verify_event_ids.py` 的集合相等作为本节通过条件。旧模块不带消息头 `x-fs-event-id`。R1 被拒的记录不应留在 compact topic 里。
