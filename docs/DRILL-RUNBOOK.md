# Kafka / mod_event_kafka 演练手册（分步）

> 目标：在本机实验室上按固定步骤复现「短断自愈 / 超 TTL 过期 / 恢复后新呼叫」。  
> §0–§9 的命令均取自本机已跑通的流程。参数名、`x-fs-event-id`、状态字面量保持英文。  
> §10（FS-11）记录 2026-09-30 已跑通的 R1/R2：**已 empirically（旧无 outbox + FS 1.6.20 lab）**。这是与生产目标版本匹配的实验室实证。未经生产验证。更早的 FS 1.10.x 一轮保留为对照。  
> 本文步骤均在仓库根目录执行，主要命令使用相对路径（`lab/toxiproxy_cut_restore.sh`、`lab/dialtest_fast.sh`、`lab/dialtest_originate.sh`）。共享实验室机器上仍可用绝对路径 `/workspace/lab-mod-event-kafka/…`（脚本在该目录根下，不带 `lab/` 前缀）。对照见 [KAFKA-DEPLOY.md](KAFKA-DEPLOY.md) §2.1。§0–§7 的短断/超 TTL 栈仍用镜像 `lab-freeswitch:1.10.12-kafka`、容器 `lab-freeswitch`。§10 的 FS-11 主路径改用 `lab-freeswitch:1.6.20-kafka`、容器 `lab-freeswitch-16`。

对应故障场景：短断是 [FAULT-SCENARIOS.md](FAULT-SCENARIOS.md) 的 FS-01；超 TTL 与恢复后新呼叫是 FS-09。§10 是 FS-11（broker **+87** `INVALID_RECORD`），R1/R2 已在 FS 1.6.20 + 旧模块上重跑 PASS。不要用 §3 / §4 的断流脚本去跑它。

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
| FS-11 R1/R2（主证据） | broker **+87** `INVALID_RECORD`（旧无 outbox + FS 1.6.20 lab） | `reports/fs11-err87-fs16-20260930-162827/` |
| FS-11 R1/R2（对照） | 同一症状的较早一轮（旧无 outbox + FS 1.10.x lab；当时 1.6 被 Jessie 运行时镜像挡住） | `reports/fs11-err87-20260930-154534/` |

本手册 §10 记录 FS-11 的 R1/R2。主证据在 `reports/fs11-err87-fs16-20260930-162827/`（实验室机器 `/workspace/mod_event_kafka-fix/reports/fs11-err87-fs16-20260930-162827/`），状态为已 empirically（旧无 outbox + FS 1.6.20 lab）。对照 `reports/fs11-err87-20260930-154534/`（旧无 outbox + FS 1.10.x lab）保留，不删除。不要和已 PASS 的 L-16 断流结论混成同一个错误。尚未逐步展开的项：单入口 disable、延迟/带宽 toxic、杀 broker 进程、磁盘写满，以及 FS-11 的可选 R3（`message.timestamp.*.max.ms`）。需要时再扩充脚本。

## 9. 安全提醒

- 演练用的 bootstrap **只能**是 toxiproxy 的三个端口。  
- 禁止把实验室配置改成生产/云端 Kafka 地址。  
- 本文不宣称已在生产环境验证。  
- §10 复现的是实验室 Kafka 3.x 上的 broker 校验拒收，不是把模块指到云端 Kafka。

## 10. FS-11 Err-87 `INVALID_RECORD`（已 empirically：旧无 outbox + FS 1.6.20 lab）

> **R0/R1/R2 已于 2026-09-30 在 FS 1.6.20 上 PASS。** 主证据目录 `reports/fs11-err87-fs16-20260930-162827/`（实验室机器 `/workspace/mod_event_kafka-fix/reports/fs11-err87-fs16-20260930-162827/`）。`reports/` 不入库，与 L-16 相同。本仓库不提交 `.so`。  
> 这是与生产目标版本匹配的实验室实证。未经生产验证。  
> 更早的一轮 `reports/fs11-err87-20260930-154534/`（旧无 outbox + FS 1.10.x lab）保留为对照，见 §10.8。当时 Jessie 运行时镜像挡住了 1.6，所以那一轮改在 1.10.x 上跑。该阻塞已解除，R1/R2 在 1.6.20 上重跑并通过。不得写成「FS 1.6 一直可用」。  
> 场景定义见 [FAULT-SCENARIOS.md](FAULT-SCENARIOS.md) 的 FS-11。  
> **不要**执行 `lab/toxiproxy_cut_restore.sh`（也不要执行共享机上的 `/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh`）来做本节。短断 35s 与超 TTL 断连 150s 制造的是客户端断连，对应 **-187** `ALL_BROKERS_DOWN`，不是 **+87**。

用户日志里的 `Err-87` 是 Kafka **broker** 错误码 **+87**，名字是 `INVALID_RECORD`。旧版 librdkafka 不认识该码时会印成 `Err-87?`。FS 1.6.20 这一轮打出的是同一码的具名文本（broker-reason，`R1_INVALID_COUNT=670`）：

```text
[ERR] mod_event_kafka.cpp:197  Message delivery failed Broker: Broker failed to validate record
```

模块说明把该文本标为 `RD_KAFKA_RESP_ERR_INVALID_RECORD` / broker **+87**。格式串是 `Message delivery failed %s`。

librdkafka **本地**错误 **-187** 的名字是 `ALL_BROKERS_DOWN`，属于断连/断流。FS-01 等短断演练覆盖的是这一类。它不是 Err-87。书写时保持两个码分开：**+87** 是 broker 校验拒收，**-187** 是断连。不要把 `Err-87` 解释成 `ALL_BROKERS_DOWN`。

| 代码 | 名称 | FS 1.6.20 这一轮 |
|------|------|------------------|
| **+87** | `INVALID_RECORD` | R1 看到的就是这个码。文本是 `Broker: Broker failed to validate record`（`mod_event_kafka.cpp:197` ×670） |
| **-187** | `ALL_BROKERS_DOWN` | 这一轮的 `reproduce.sh` 没有制造它。短断/断连日志归 FS-01 / FS-09，不能当作 FS-11 PASS |

已通过的模块是 upstream **旧模块、无 outbox**。失败日志在 `dr_msg_cb`、`mod_event_kafka.cpp:197`。模块说明：没有 outbox / 重试流水线，只有直接 produce + `dr_msg_cb`。当前 master 走 outbox 流水线，加载时会写 `Initialising (outbox pipeline)`，投递报告在 `KafkaPipeline::on_delivery`，**不会**打出第 197 行。用当前 master 的 `.so` 做同样的 topic 实验，不能宣称已经复现了这一行。

### 10.1 这次实际用的 FS（R0 PASS）与已被取代的 Jessie 阻塞

主证据 RESULT（2026-09-30 16:29:59 +0800 Asia/Shanghai）：

- R0 FS+Kafka：PASS（lab）
- R1 compact+null key → **+87** `INVALID_RECORD`：PASS
- R2 delete topic 同一路径 → 无 **+87**：PASS
- 版本字符串：`FreeSWITCH version: 1.6.20+git~20180123T214909Z~987c9b9a2a~64bit`（git `987c9b9`，2018-01-23 21:49:09Z 64bit）。`fs_cli` 版本字符串相同。`module_exists=true`
- 镜像：`lab-freeswitch:1.6.20-kafka`（id `f057ade5fc2f`，约 192MB）
- 容器：`lab-freeswitch-16`（`--network host`）
- 构建：signalwire/freeswitch 标签 v1.6.20，基底 debian:buster-slim，OpenSSL 1.0.2u（`/opt/openssl10`）
- 头文件 / pkg-config：`freeswitch` 1.6.20，位于 `/usr/local/freeswitch`
- 模块：upstream OLD（无 outbox），`cpp:197` `dr_msg_cb`
- 源码在实验室机器 `/workspace/mod_event_kafka-fix/upstream/`，按 FS 1.6.20 头文件编译。证据目录内 `.so` 为 `build-upstream/mod_event_kafka.so`（md5 `063e55cb0a584839f68d44a22dab62e6`），装入容器 `/usr/local/freeswitch/lib/freeswitch/mod/mod_event_kafka.so`。本仓库不提交该 `.so`

先前的 `fs16-blocker` 叙述（记录在对照目录 `reports/fs11-err87-20260930-154534/fs16-blocker.txt`，**已被这次源码构建取代**）：

- 当时拉取过 `praekeltfoundation/freeswitch:1.6`（FreeSWITCH 1.6.20，Debian Jessie，仅运行时）
- 该镜像没有 freeswitch-dev / 头文件，没有 librdkafka；Jessie 的 apt 源已 EOL
- 当时实验室现成头文件是 FS 1.10.12（bookworm），与 1.6 ABI 不兼容
- 那一轮因此改用 `lab-freeswitch:1.10.12-kafka` + 旧 `.so`，**没有**在 FS 1.6 上跑
- 本次用源码构建的 `lab-freeswitch:1.6.20-kafka` 解除了该阻塞，并在 1.6.20 上重跑 R1/R2。RESULT 写明：Prior blocker (Jessie praekelt runtime-only) superseded by this source build
- 不得把「当时 Jessie 镜像编不过」写成「FS 1.6 一直可用」，也不得把本实验室实证写成生产验证

Kafka 仍是 `apache/kafka:3.8.1`，bootstrap 只走 `127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094`。代理保持启用。本节不 disable toxiproxy。

### 10.2 已跑通的复现脚本（目标是 FS 1.6.20）

主证据目录里的 `reproduce.sh` 就是这次执行的脚本，文件头写明复现对象是 FreeSWITCH 1.6.20，依赖镜像 `lab-freeswitch:1.6.20-kafka`。在实验室机器上：

```bash
bash /workspace/mod_event_kafka-fix/reports/fs11-err87-fs16-20260930-162827/reproduce.sh
```

脚本里的名字（与 `reproduce.sh` 一致；不要改去调用 `toxiproxy_cut_restore.sh`，也不要改回 §1 的 `lab-freeswitch` / `lab-freeswitch:1.10.12-kafka`）：

```bash
FS_IMAGE=lab-freeswitch:1.6.20-kafka
FS_NAME=lab-freeswitch-16
LD="LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/opt/openssl10/lib:/usr/local/lib"
MODDIR=/usr/local/freeswitch/lib/freeswitch/mod
CONFDIR=/usr/local/freeswitch/etc/freeswitch/autoload_configs
```

启动与装模块（脚本原文要点）：

```bash
sg docker -c "docker rm -f lab-freeswitch-16" || true
sg docker -c "docker run -d --name lab-freeswitch-16 --network host lab-freeswitch:1.6.20-kafka \
  bash -c 'export LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/opt/openssl10/lib:/usr/local/lib; \
  exec /usr/local/freeswitch/bin/freeswitch -nonat -nf -nc -nosql -rp'"
sg docker -c "docker cp ${UPSTREAM_SO} lab-freeswitch-16:/usr/local/freeswitch/lib/freeswitch/mod/mod_event_kafka.so"
sg docker -c "docker exec lab-freeswitch-16 env LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/opt/openssl10/lib:/usr/local/lib \
  /usr/local/freeswitch/bin/fs_cli -x 'fsctl shutdown now'" || true
sg docker -c "docker start lab-freeswitch-16"
```

步骤：

1. `cd /workspace/lab-mod-event-kafka`，`docker compose up -d`（需要镜像 `apache/kafka:3.8.1`、`shopify/toxiproxy:2.1.4`、`lab-freeswitch:1.6.20-kafka`）
2. 在 `lab-kafka-1` 上创建两个 topic，都是 partitions=1、RF=1：
   - `fs_events_compact`，`--config cleanup.policy=compact`
   - `fs_events_delete`，`--config cleanup.policy=delete`
3. 按上面的 `docker run` 起容器 `lab-freeswitch-16`。模块目录是 `/usr/local/freeswitch/lib/freeswitch/mod`，配置目录是 `/usr/local/freeswitch/etc/freeswitch/autoload_configs`。这与 §1 里 1.10 镜像的 `/usr/local/freeswitch/mod` 不同
4. **R1：** 把证据目录 `conf/event_kafka.conf.xml` 拷到 `event_kafka.conf.xml`，`fsctl shutdown now` 后再 `docker start`。topic 为 `fs_events_compact`
5. **R2：** 用 `wc -c` 记下当时日志长度，换上 `conf/event_kafka.conf.delete.xml`，同样 shutdown 再 start。只看这次重启之后的新日志（`tail -c +$((MARK+1))`）。topic 为 `fs_events_delete`

`topic-config.txt` 里 `--describe` 分别为 `fs_events_compact` 的 `cleanup.policy=compact` 与 `fs_events_delete` 的 `cleanup.policy=delete`（各 1 分区、RF=1）。

若要重编旧 `.so`，见主证据目录的 `REBUILD-SO.md`，不要把编出来的二进制提交进本仓库：

- 工具链目录：`/workspace/lab-mod-event-kafka/fs16/`
- 辅助脚本：`/workspace/lab-mod-event-kafka/fs16/scripts/rebuild-mod.sh`
- 主机前缀：`/workspace/lab-mod-event-kafka/fs16/prefix`
- 编译 FS 用的 OpenSSL 1.0.2u：`/workspace/lab-mod-event-kafka/fs16/opt-openssl10`
- 构建容器（若仍在）：`fs16-build`（debian:buster-slim）。该环境把 Makefile 的 `-std=c++17` 改成 `-std=c++14` 后再 `make`。librdkafka 为实验室的 1.9.2（`/usr/local`）
- 上游源码副本：`/workspace/lab-mod-event-kafka/fs16/upstream`（从 `/workspace/mod_event_kafka-fix/upstream/` 同步）
- 运行时拷贝目标：`lab-freeswitch-16:/usr/local/freeswitch/lib/freeswitch/mod/mod_event_kafka.so`

### 10.3 R1：compact + 缺少 `Channel-Call-UUID`（PASS）

旧模块用 `Channel-Call-UUID` 当 Kafka key。模块说明里的路径是：

```text
PublishEvent: char *uuid = switch_event_get_header(event, "Channel-Call-UUID");
send(event_json, uuid, 0);
send(): key_length = key == NULL ? 0 : strlen(key);
rd_kafka_produce(..., key, key_length, ...);
```

头不存在时 key 指针为 NULL，发出 null key。`cleanup.policy=compact` 的 topic 需要非空 key，才能按 key 留下最新一条；null key 在 broker 校验阶段被拒绝，返回 **+87** `INVALID_RECORD`。

`event-filter` 为空，因此订阅 `SWITCH_EVENT_ALL`。启动时的 HEARTBEAT、RE_SCHEDULE 等没有 `Channel-Call-UUID`，于是走出这条路径。这一轮的模块说明记录的是这条真实缺头路径。

通过时 FS 日志为上面的第 197 行。含 `Broker failed to validate record` 的行数是 670（`R1_INVALID_COUNT=670`）。

### 10.4 R2：delete topic，同一条 null-key 路径（PASS）

同一旧模块，topic `fs_events_delete`，`cleanup.policy=delete`。RESULT：R2 日志窗口 0 次投递失败 / 0 次 `INVALID_RECORD`，topic 为 `fs_events_delete`。计数文件：`R2_INVALID_COUNT=0`。

这是「同一路径在 delete 上不出现 **+87**」的对照。对照里如果出现 **-187**，那是另一次断连，既不能充当 R2，也不能充当 R1。toxiproxy 短断仍然不是 FS-11。

### 10.5 主证据目录里要有的文件

`reports/fs11-err87-fs16-20260930-162827/`（实验室绝对路径见本节开头）至少包括：

| 文件 | 作用 |
|------|------|
| RESULT | R0/R1/R2 PASS，FS 1.6.20 版本与镜像/容器，以及 Jessie 阻塞已被源码构建取代 |
| `fs-version.txt` | `freeswitch -version` / `fs_cli` 均为 1.6.20；`module_exists=true`；镜像 id 与容器名 |
| `topic-config.txt` | `fs_events_compact` / `fs_events_delete` 的 `cleanup.policy` |
| `reproduce.sh` | 实际执行的复现命令，目标镜像 `lab-freeswitch:1.6.20-kafka` |
| `REBUILD-SO.md` | 在 `/workspace/lab-mod-event-kafka/fs16/` 上重编旧模块 |
| broker-reason | `R1_INVALID_COUNT=670`，以及第 197 行 `Broker failed to validate record` |
| 模块说明 | 旧模块 key 路径、第 197 行格式串、安装路径与 md5；无 outbox |
| R2 计数 | `R2_INVALID_COUNT=0` |

只有一行 `Err-87?`、没有 topic 配置和 broker 侧拒收文本，不算本场景证据齐。这一轮的实验室文本已经是 `Broker: Broker failed to validate record`，模块说明把它对上 broker **+87**。

### 10.6 可选 R3（本次未跑）

收紧 `message.timestamp.*.max.ms` 仍是另一条可能打出 **+87** 的路径，**没有**出现在 FS 1.6.20 这次 PASS 里，也没有出现在 §10.8 的 1.10.x 对照里。不要把 R1 的 compact + null key 写成时间戳窗口。若以后要跑，放在 `cleanup.policy=delete` 的 topic 上，避免和 R1 的原因叠在一起，并在 broker 原因记录里写时间戳窗口。未跑之前不要给 R3 标 empirically。

### 10.7 本节明确不做的事

- 不用 `lab/toxiproxy_cut_restore.sh`，也不要 disable `kafka1`/`kafka2`/`kafka3` 来制造 Err-87。
- 不把 **-187** `ALL_BROKERS_DOWN` 写成 FS-11 通过。
- 不在当前 master 的 outbox 模块上寻找 `mod_event_kafka.cpp:197`。
- 不把 Jessie 运行时镜像当时编不过，写成「FS 1.6 一直可用」。1.6.20 实证来自后来的源码构建镜像，R1/R2 是在该镜像上重跑的。
- 不把这次实验室实证写成已经生产验证。未经生产验证。
- 不拿 §10.8 的 FS 1.10.x 对照代替本节的 1.6.20 主证据，也不删除该对照。
- 不把实验室 bootstrap 改成生产或云端 Kafka。
- 不用 `scripts/verify_event_ids.py` 的集合相等作为本节通过条件。旧模块不带消息头 `x-fs-event-id`。R1 被拒的记录不应留在 compact topic 里。

### 10.8 对照：较早的 FS 1.10.x 一轮（保留，不删除）

这是 1.6 被 Jessie 运行时镜像挡住时的实验室通过，**不是**当前主证据。状态口径是旧无 outbox + FS 1.10.x lab。证据目录 `reports/fs11-err87-20260930-154534/`（实验室机器 `/workspace/mod_event_kafka-fix/reports/fs11-err87-20260930-154534/`）。`RESULT.txt` 时间 2026-09-30 15:48:11 +0800。R0/R1/R2 在该目录里也是 PASS。

当时实际用的 FS：

- 版本字符串：`FreeSWITCH version: 1.10.7-dev+git~20210825T173719Z~dd2411336f~64bit`（git `dd24113`，2021-08-25 17:37:19Z 64bit）
- 镜像标签：`lab-freeswitch:1.10.12-kafka`（二进制报告 1.10.7-dev），容器名是 §1 的 `lab-freeswitch`，不是 `lab-freeswitch-16`
- 模块：同一条 upstream 旧模块、无 outbox、`cpp:197` `dr_msg_cb`。按 FS 1.10.x 头文件（`/workspace/lab-mod-event-kafka/fs/prefix`）编译，装入 `/usr/local/freeswitch/mod/mod_event_kafka.so`
- 加载日志是第 87 行的 `KafkaEventPublisher Initialising...`，以及 `Found 0 subscriptions` / `Subscribed to ALL events`
- R1 窗口里 `INVALID_RECORD` 行数是 482。独立冒烟：`DR_FAIL err=87 (Broker: Broker failed to validate record) topic=fs_events_compact key_len=0`
- `broker-reason.txt`：compact + NULL → err=87；delete + NULL → OK；compact + 非空 key → OK。另有两条 `DR_OK`（`key_len=0` 与 `key_len=13`），是这组独立对照的成功侧
- R2：delivery-fail = 0，`INVALID_RECORD` = 0。消费抽样里能看到被接受的 `HEARTBEAT` 与 `MODULE_LOAD`
- 强制 null key 的实验补丁当时编过，**那一轮 R1 没有用它**
- 复现脚本：`bash /workspace/mod_event_kafka-fix/reports/fs11-err87-20260930-154534/reproduce.sh`（镜像是 `lab-freeswitch:1.10.12-kafka`，不要拿它去跑 1.6.20）

`fs16-blocker.txt` 留在这个对照目录里，说明的是 **当时** 为什么没有 FS 1.6 实证。读到它时，以 §10.1 为准：该阻塞已经由 `lab-freeswitch:1.6.20-kafka` 取代。
