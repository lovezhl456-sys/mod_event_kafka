# Kafka / mod_event_kafka 演练手册（分步）

> 目标：在本机实验室上按固定步骤复现「短断自愈 / 超 TTL 过期 / 恢复后新呼叫」。  
> §0–§9 的命令均取自本机已跑通的流程。参数名、`x-fs-event-id`、状态字面量保持英文。  
> §10（FS-11）是 **文档草案 / 未 empirically**：命令尚未跑通，不得当成已 PASS，也不宣称生产验证。  
> 本文步骤均在仓库根目录执行，主要命令使用相对路径（`lab/toxiproxy_cut_restore.sh`、`lab/dialtest_fast.sh`、`lab/dialtest_originate.sh`）。共享实验室机器上仍可用绝对路径 `/workspace/lab-mod-event-kafka/…`（脚本在该目录根下，不带 `lab/` 前缀）。对照见 [KAFKA-DEPLOY.md](KAFKA-DEPLOY.md) §2.1。

对应故障场景：短断是 [FAULT-SCENARIOS.md](FAULT-SCENARIOS.md) 的 FS-01；超 TTL 与恢复后新呼叫是 FS-09。§10 是 FS-11（broker **+87** `INVALID_RECORD`）的草案，不要用 §3 / §4 的断流脚本去跑它。

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
| FS-11 草案（R1 未跑） | broker **+87** `INVALID_RECORD` | （无；占位 `reports/fs11-err87-<ts>/`） |

本手册 §10 已写出 FS-11 的草案步骤，状态仍是「文档草案 / 未 empirically」，没有证据目录，勿与已 PASS 的 L-16 结论混淆。尚未逐步展开的项：单入口 disable、延迟/带宽 toxic、杀 broker 进程、磁盘写满等。需要时再扩充脚本。

## 9. 安全提醒

- 演练用的 bootstrap **只能**是 toxiproxy 的三个端口。  
- 禁止把实验室配置改成生产/云端 Kafka 地址。  
- 本文不宣称已在生产环境验证。  
- §10 复现的是实验室 Kafka 3.x 上的 broker 校验拒收，不是把模块指到云端 Kafka。

## 10. FS-11 Err-87 `INVALID_RECORD`（文档草案 / 未 empirically）

> **整节都是文档草案。** R1 尚未跑过，不得标 PASS，不宣称生产验证。  
> 场景定义见 [FAULT-SCENARIOS.md](FAULT-SCENARIOS.md) 的 FS-11。  
> **不要**执行 `lab/toxiproxy_cut_restore.sh`（也不要执行共享机上的 `/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh`）来做本节。短断 35s 与超 TTL 断连 150s 制造的是客户端断连。

用户日志里的 `Err-87` 是 Kafka **broker** 错误码 **+87**，名字是 `INVALID_RECORD`：记录没通过 broker 校验，被拒绝。旧版 librdkafka 的错误表里若没有这个码，投递报告里会印成 `Err-87?`。问号只表示库不认识该码，数值仍是 **+87**。

librdkafka **本地**错误 **-187** 的名字是 `ALL_BROKERS_DOWN`，属于断连/断流。FS-01 等短断演练覆盖的是这一类。它不是 Err-87。书写时保持两个码分开：**+87** 是 broker 校验拒收，**-187** 是断连。不要把 `Err-87` 解释成 `ALL_BROKERS_DOWN`。

| 代码 | 名称 | 本节是否接受为通过 |
|------|------|--------------------|
| **+87** | `INVALID_RECORD` | 接受。日志文本须是 `INVALID_RECORD`，或旧库的 `Err-87?`（对应 **+87**），并且目录里有 broker 侧原因 |
| **-187** | `ALL_BROKERS_DOWN` | 不接受。短断/断连日志归 FS-01 / FS-09 |

旧模块（引入 outbox 之前）在 `dr_msg_cb` 里打印失败，源码约在 `mod_event_kafka.cpp` 第 197 行：

```text
Message delivery failed <rd_kafka_err2str>
```

FreeSWITCH 日志形态与生产症状一致时，类似：

```text
[ERR] mod_event_kafka.cpp:197 Message delivery failed Err-87
```

旧库不认识 **+87** 时，同一位置打印 `Err-87?`。`Err-87` 与 `Err-87?` 都表示 broker **+87** `INVALID_RECORD`，并且证据目录里还要有 broker 侧原因。字符串里的 `87` 不是 **-187**。

当前 master 走 outbox 流水线，投递报告在 `KafkaPipeline::on_delivery`，**不会**打出上面这一行。本节的通过日志以 **FS 1.6 + 无 outbox 旧模块** 为准。在当前 master 上做同样的 topic 实验，不能用来宣称已经复现了第 197 行。

### 10.1 前置（R0）

- FreeSWITCH **1.6** 实验室镜像。仓库里现有的 `lab-freeswitch:1.10.12-kafka` 是 1.10 演练镜像，不能直接当作本场景的 FS。镜像名由安装组填入下面的占位。
- 加载会打印 `Message delivery failed` 的 **outbox 之前** 的 `mod_event_kafka.so`。确认加载的是这份旧模块之后再做 R1。
- Kafka 3.x / KRaft。实验室可继续用 `apache/kafka:3.8.1`（`lab-kafka-1`）。bootstrap 仍只能是 `127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094`。
- 代理保持 `enabled=true`。本节不禁用 toxiproxy。

Kafka + toxiproxy 的启动方式与 §1 相同（`cd lab && sg docker -c 'docker compose up -d'`）。FS 1.6 容器的启动命令按现场镜像替换，下面只保留要核对的事项：

```bash
# 占位：换成现场的 FS 1.6 镜像名与安装前缀
# sg docker -c 'docker run -d --name lab-freeswitch --network host <fs-1.6-image> ...'

sg docker -c 'docker exec lab-freeswitch <fs_cli> -x "module_exists mod_event_kafka"'
# 期望：true
# 再确认日志里的模块路径是旧模块：出现 Message delivery failed 的二进制，
# 而不是 "KafkaEventPublisher Initialising (outbox pipeline)..."
```

### 10.2 创建 compact topic（R1）

专用 topic，避免改动已有演练用的 `fs_events`。

```bash
sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic fs_events_err87 --partitions 3 --replication-factor 1 \
  --config cleanup.policy=compact"

sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 --entity-type topics \
  --entity-name fs_events_err87 --describe"
```

`--describe` 的输出里应能看到 `cleanup.policy=compact`。把这段输出存进证据目录（见 §10.5）。

### 10.3 把旧模块指到该 topic

在 FS 1.6 实际加载的 `event_kafka.conf.xml` 里把 `topic` 指到 `fs_events_err87`（键名保持 `topic`）。`bootstrap-servers` 仍只写三个 toxiproxy 端口。改完后按 FS 1.6 的习惯 `reload mod_event_kafka` 或重启 FS，使旧模块重新建 producer。

```xml
<param name="bootstrap-servers" value="127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"/>
<param name="topic" value="fs_events_err87"/>
```

### 10.4 注入 null key（R1）

意图：旧模块 `PublishEvent` 用 `Channel-Call-UUID` 当 Kafka key。头不存在时，key 指针为 NULL，`rd_kafka_produce` 发出的是 **null key**。`cleanup.policy=compact` 的 topic 在 broker 校验阶段拒绝 null key，返回 **+87** `INVALID_RECORD`。

空字符串 key（指针非 NULL、长度为 0）和 null key 不是同一件事。本节以「头不存在 → key 指针为 NULL」为准。不要把「只是空字符串」写成已经 empirically 等价。

拨测或 `event-filter` 的精确写法现场尚未钉死，下面是占位。成功与否只看 §10.5 的日志和 topic 配置，不看占位命令本身是否原样可跑。

**占位 A（优先，不改源码）：** 让进入模块的事件没有 `Channel-Call-UUID`。安装组按现场习惯填实，例如把 `event-filter` 订到确认不含该头的事件；若选用 `SWITCH_EVENT_HEARTBEAT` 一类非通话事件，先抓一条 JSON，确认头不存在，再当作 R1 的注入。通话类 `CHANNEL_*` 通常带有 `Channel-Call-UUID`，用它们做 R1 时 key 不是 null，不能期望 **+87**。

**占位 B（仅当 A 无法去掉该头）：** 在实验室的旧模块副本里，调用 `send()` 时强制 key 为 NULL。该改动只留在 FS 1.6 实验副本上，不要提交进当前 master。通过标准仍然是同一条 `Message delivery failed` 加上 broker **+87**。

注入之后在 FS 日志里查找：

```text
Message delivery failed
```

通过时，同一条里的错误文本是 `INVALID_RECORD`，或者旧库的 `Err-87?`。两条都表示 broker **+87**。若文本是 “All broker connections are down” 或错误码为 **-187** `ALL_BROKERS_DOWN`，这是断连，回到 FS-01，不是本节通过。

### 10.5 收集证据

```bash
TS=$(date +%Y%m%d-%H%M%S)
OUT=reports/fs11-err87-$TS
mkdir -p "$OUT"

# 1) 复现命令：把本节实际执行的命令抄进该文件
# 2) topic 配置（broker 侧原因的一部分）
sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 --entity-type topics \
  --entity-name fs_events_err87 --describe" | tee "$OUT/topic-config.txt"

# 3) FS 日志片段：只保留含 Message delivery failed 的行
# sg docker -c 'docker logs lab-freeswitch' | rg "Message delivery failed" | tee "$OUT/fs.log.snippet"
```

目录里还要有一份人工可读的 `broker-reason.txt`，写明：

- 看到的是 broker **+87** `INVALID_RECORD`（若日志原文是 `Err-87?`，注明这是旧 librdkafka 对 **+87** 的打印）
- 拒收原因：topic `fs_events_err87` 的 `cleanup.policy=compact`，且该条记录的 key 为 null（没有 `Channel-Call-UUID`，或占位 B 强制 NULL）
- 明确写一句：本次不是 **-187** `ALL_BROKERS_DOWN`，也没有用断流脚本

只有 `Err-87?` 一行、没有 `topic-config.txt` 和 `broker-reason.txt` 的目录，不算 FS-11 证据齐。在 R1 按上面重复打出 **+87** 之前，本节保持 **文档草案 / 未 empirically**。

### 10.6 对照（R2）

两条对照都可以，做一条即可。对照的期望是：**不**出现 **+87** / `INVALID_RECORD` / `Err-87?`。

**对照 1：** 新建 `cleanup.policy=delete` 的 topic，旧模块改指到它，再用与 R1 相同的 null key 事件发送。

```bash
sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic fs_events_err87_ctl --partitions 3 --replication-factor 1 \
  --config cleanup.policy=delete"
```

**对照 2：** 仍用 `fs_events_err87`（`cleanup.policy=compact`），但事件始终带非空 `Channel-Call-UUID`（正常通话事件）。key 非空时，compact 这条拒收原因不成立。

把对照的 topic 配置和 FS 日志摘要写入 `$OUT/r2-control.txt`。对照里如果出现 **-187**，那是另一次断连，既不能充当 R2「没有 +87」，也不能充当 R1 通过。

### 10.7 可选：收紧时间戳窗口（R3）

仍是草案，未跑。这是第二条可能打出 **+87** `INVALID_RECORD` 的路径，用来和「compact + null key」区分开。优先仍以 R1 为主路径。

在 **delete** 策略的 topic 上收紧 `message.timestamp.*.max.ms`，避免和 R1 的 compact 原因叠在一起。键名随 broker 版本：较新的是 `message.timestamp.before.max.ms` 与 `message.timestamp.after.max.ms`；更老的集群可能仍是 `message.timestamp.difference.max.ms`。下面只说明意图，数值是否被该镜像接受，以现场 `--describe` 为准：

```bash
sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic fs_events_err87_ts --partitions 1 --replication-factor 1 \
  --config cleanup.policy=delete"

sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 --entity-type topics \
  --entity-name fs_events_err87_ts --alter \
  --add-config message.timestamp.before.max.ms=1,message.timestamp.after.max.ms=1"
```

事件时间戳落在窗口外时，broker 也可能返回 **+87** `INVALID_RECORD`。若要把它当作 FS-11 的证据，`broker-reason.txt` 里要写时间戳窗口，而不是写成 compact + null key。未跑通之前不要标 empirically。

### 10.8 本节明确不做的事

- 不用 `lab/toxiproxy_cut_restore.sh`，也不要 disable `kafka1`/`kafka2`/`kafka3` 来「制造 Err-87」。
- 不把 **-187** `ALL_BROKERS_DOWN` 的日志放进 FS-11 的通过结论。
- 不在当前 master 的 outbox 模块上寻找 `mod_event_kafka.cpp:197`。
- 不把实验室 bootstrap 改成生产或云端 Kafka。
- 不用 `scripts/verify_event_ids.py` 的集合相等作为本节通过条件。旧模块通常也不带消息头 `x-fs-event-id`。被拒记录不应出现在 topic 中。
