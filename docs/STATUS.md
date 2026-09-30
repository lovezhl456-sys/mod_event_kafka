# 状态 — 2026-09-25（Asia/Shanghai）；FS-11 增补 2026-09-30（1.6.20 重跑）

实验室源码已合入本仓库根目录（`include/`、`src/`、`mod_event_kafka.cpp`）。下文提到的 `work/` 指产生证据的实验室机器上的目录树，并不是本 PR 里的第二份副本。

## 摘要
- FS Phase2 拨测 **L-02/L-07=PASS**
- 证据路径：`reports/l02-l07-fs-20260925-133618/`
- `verify_event_ids.py` VERIFY_OK 60/60，未 reload 模块
- FS + TTL 实验室 **L-16a=PASS**，**L-16b=PASS**，**L-16c=PASS**
- 证据路径：`reports/l16-abc-20260925-215439/`
- 模块配置为 `outbox-ttl-ms=120000`（master `37e89154`）
- 短断 35 秒：全部送达（verify 通过；注入 60 条，匹配 60 条）
- 断连 150 秒 > TTL：过期行的状态为 `dead/expired_ttl`，`expired_still_in_topic=0`；consumed_matched=30 / 60（未过期的均已送达）
- 自愈之后：新呼叫 VERIFY_OK 30/30
- 候选补丁 0001 = **NOT_VERIFIED**
- FS-11 **已 empirically（旧无 outbox + FS 1.6.20 lab）**：R1 compact+null key → **+87** `INVALID_RECORD` PASS（670 行）；R2 delete 同一路径无 **+87** PASS（`R2_INVALID_COUNT=0`）
- 主证据路径：`reports/fs11-err87-fs16-20260930-162827/`（实验室机器 `/workspace/mod_event_kafka-fix/reports/fs11-err87-fs16-20260930-162827/`）
- FS 二进制为 **1.6.20**（镜像 `lab-freeswitch:1.6.20-kafka`，容器 `lab-freeswitch-16`）。这是与生产目标版本匹配的实验室实证
- 对照（保留，不删除）：先前 Jessie 运行时镜像挡住 FS 1.6 时的一轮，`reports/fs11-err87-20260930-154534/`（旧无 outbox + FS 1.10.x lab）。该阻塞已由源码构建的 `lab-freeswitch:1.6.20-kafka` 解除，R1/R2 在 1.6.20 上重跑并通过

## 已完成
- 已实现 `outbox-ttl-ms`（默认 `120000`；`0` 表示关闭）。超过 TTL 的 pending 行被标为 `dead` / `last_error=expired_ttl`，且不会被投递。
- FS + TTL 实验室 **L-16a=PASS**，**L-16b=PASS**，**L-16c=PASS**。证据路径：`reports/l16-abc-20260925-215439/`。模块配置为 `outbox-ttl-ms=120000`（master `37e89154`）。短断 35 秒：全部送达（verify 通过；注入 60 条，匹配 60 条）。断连 150 秒 > TTL：过期行的状态为 `dead/expired_ttl`，`expired_still_in_topic=0`；consumed_matched=30 / 60（未过期的均已送达）。自愈之后：新呼叫 VERIFY_OK 30/30。
- 设计已定稿：SQLite outbox + 有界队列 + worker/poll（候选补丁 0001 未被采纳为最终方案）。
- 已构建 Outbox + 有界队列库；ASan 单元测试 PASS（`test_outbox`）。
- FS 模块粘合层已接通：事件回调只做深拷贝并入队；worker 先插入 SQLite outbox 行再 produce；poll 线程在投递完成后 ACK。
- FS Phase2 拨测 **L-02/L-07=PASS**。证据路径：`reports/l02-l07-fs-20260925-133618/`。`verify_event_ids.py` VERIFY_OK 60/60，未 reload 模块。
- TEST-PLAN 由测试质检撰写，见 `docs/TEST-PLAN.md`。
- 实验室 compose 由安装软件大师撰写，位于 `/workspace/lab-mod-event-kafka/`（Kafka×3 + toxiproxy）。
- Phase1 实验室 READY（单个 KRaft + 3 个 toxiproxy 前端）。此后 Phase2 FS 拨测也已通过（见下文）；但 Phase1 跑的仍不是真正的 3 broker 集群拓扑。

## 未宣称
- L-03–L-15 未完整跑过。
- 未经生产验证。
- 候选补丁 0001 = **NOT_VERIFIED**（仅作反面参考）。不得将其标为已通过生产验证。
- FS-11 不得写成「Jessie 那一轮就能在 FS 1.6 上编译」。`praekeltfoundation/freeswitch:1.6`（1.6.20 / Debian Jessie，仅运行时，缺头文件与 librdkafka）当时挡住了 1.6。该阻塞后来才由源码构建的 `lab-freeswitch:1.6.20-kafka` 解除；解除之后的 R1/R2 见下文 FS-11，仍未经生产验证。不得写成「FS 1.6 一直可用」。
- FS-11 的 R3（`message.timestamp.*.max.ms`）未跑。FS-11 没有对应的 TEST-PLAN L-xx。
- 断连类 **-187** `ALL_BROKERS_DOWN` 不是 FS-11。本次证据不是 toxiproxy 断流。

## 客户端 bootstrap（实验室健康时）
仅经 toxiproxy：`127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094`。

## 实验室冒烟（稳定流水线）
- 2026-09-25T13:08:26+08:00：`stable/build/test_pipeline_smoke 127.0.0.1:19092 fs_events` → **SMOKE_PASS**（enqueued=produce_ok=acked=20）。根因已修复：poll_loop 在 `rd_kafka_poll` 期间不再持有 `rk_mu_`。
- 候选补丁 0001 = **NOT_VERIFIED**（仅作反面参考）。此后 FS Phase2 拨测 **L-02/L-07=PASS**；证据路径 `reports/l02-l07-fs-20260925-133618/`。

## Phase1 实验室 READY
- Bootstrap：经 toxiproxy 的 `127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094`；topic `fs_events`。
- 2026-09-25T13:08:45+08:00：用完整 bootstrap 再次确认稳定冒烟通过。
- L-02/L-07 与 `scripts/verify_event_ids.py` 已不再阻塞 QA，并已在稳定测试架与 FS Phase2 拨测上通过。

## L-02 / L-07（稳定测试架，Phase1 实验室）
- 运行：topic `fs_events_l02b` 上的 `reports/l02-l07-run1790313218/`
- 断连：toxiproxy 禁用 kafka1/2/3 **35 秒**（> `message_timeout_ms=30s`）后再启用；期间**不**停止/reload 流水线
- reload 前指标：enqueued=60 produce_ok=90 acked=60 delivery_fail=30 producer_rebuilds=0 outbox 已排空（pending/in_flight/dead=0）healed_without_reload=1
- `verify_event_ids.py`：退出码 0（injected=consumed_unique=60，missing=0）
- 结果：**L-02=PASS**，**L-07=PASS**
- 范围说明：本节针对稳定的 `KafkaPipeline` 测试架（不是 FreeSWITCH `.so`）。带模块证据的 FS Phase2 拨测见下一节。Phase1 = 单个 KRaft + 3 个 toxiproxy 前端
- 候选补丁 **0001 = NOT_VERIFIED**（仅作反面参考）
- 测试架：`stable/build/test_lab_l02_l07`；核对时要求消息头 `x-fs-event-id`

## L-02 / L-07（FS Phase2 拨测，2026-09-25）
本节记录 FS Phase2 的证据。

- FS Phase2 拨测 **L-02/L-07=PASS**
- 证据路径：`reports/l02-l07-fs-20260925-133618/`
- FS 1.10.12 + `work/` 下的 `mod_event_kafka.so`；配置 buffer-size=100000；bootstrap 仅走 toxiproxy；topic `fs_events`
- 拨号：`dialtest_originate.sh` loopback/park，断连前 ×5 + 断连期间 ×5（toxiproxy 禁用 kafka1/2/3 共 35 秒）；未 reload 模块
- `verify_event_ids.py` VERIFY_OK 60/60，未 reload 模块（injected=60，consumed_matched=60，missing=0）
- 未经 reload 即自愈；module_exists=true
- 候选补丁 0001 = **NOT_VERIFIED**（仅作反面参考）
- 限制：park 路径可能没有 ANSWER，只有 CREATE/HANGUP*
- 限制：L-03–L-15 未完整跑过；未经生产验证

## L-16a/b/c（FS + TTL，2026-09-25 夜间）
- **L-16a=PASS**，**L-16b=PASS**，**L-16c=PASS**
- 证据路径：`reports/l16-abc-20260925-215439/`
- 模块：lab-freeswitch，`outbox-ttl-ms=120000`（master `37e89154`）
- **L-16a=PASS**（短断 35 秒，verify_rc=0）：全部送达；注入 60 条，匹配 60 条
- **L-16b=PASS**（断连 150 秒 > TTL）：过期行的状态为 `dead/expired_ttl`；“注入期间 / 已超龄”集合中 expired_ttl dead=30；`expired_still_in_topic=0`；consumed_matched=30 / 60（未过期的均已送达）
- **L-16c=PASS**（自愈后拨测，verify_rc=0）：新呼叫 VERIFY_OK 30/30
- 拨号辅助脚本：`lab-mod-event-kafka/dialtest_fast.sh`（bgapi；避免长时间 NO_ANSWER 挂起）
- 候选补丁 0001 = **NOT_VERIFIED**（仅作反面参考）
- 限制：L-03–L-15 未完整跑过；未经生产验证

## FS-11（Err-87 `INVALID_RECORD`，2026-09-30，主证据为 FS 1.6.20）
- **已 empirically（旧无 outbox + FS 1.6.20 lab）**。这是与生产目标版本匹配的实验室实证。未经生产验证。
- **R0 FS+Kafka = PASS**，**R1 = PASS**，**R2 = PASS**（RESULT，2026-09-30 16:29:59 +0800 Asia/Shanghai）
- 主证据路径：`reports/fs11-err87-fs16-20260930-162827/`（实验室机器 `/workspace/mod_event_kafka-fix/reports/fs11-err87-fs16-20260930-162827/`，含 RESULT、`fs-version.txt`、`topic-config.txt`、`reproduce.sh`、`REBUILD-SO.md`、broker-reason、模块说明与 R1/R2 计数）。`reports/` 不入库，本仓库不提交 `.so`
- FS：`FreeSWITCH version: 1.6.20+git~20180123T214909Z~987c9b9a2a~64bit`（git `987c9b9`，2018-01-23 21:49:09Z 64bit）。镜像 `lab-freeswitch:1.6.20-kafka`（id `f057ade5fc2f`，约 192MB），容器 `lab-freeswitch-16`（`--network host`）。由 signalwire/freeswitch 标签 v1.6.20 在 debian:buster-slim 上源码构建，OpenSSL 1.0.2u（`/opt/openssl10`）。头文件 / pkg-config：`freeswitch` 1.6.20，前缀 `/usr/local/freeswitch`。`module_exists=true`（`fs_cli` 版本字符串与 `freeswitch -version` 相同）
- 模块：upstream 旧模块（无 outbox），源码 `/workspace/mod_event_kafka-fix/upstream/`，按 FreeSWITCH 1.6.20 头文件编译。证据目录内 `.so` 为 `build-upstream/mod_event_kafka.so`（md5 `063e55cb0a584839f68d44a22dab62e6`），装入 `/usr/local/freeswitch/lib/freeswitch/mod/mod_event_kafka.so`。失败日志在 `dr_msg_cb`、`mod_event_kafka.cpp:197`（格式串 `Message delivery failed %s`）。无 outbox / 重试流水线，只有直接 produce + `dr_msg_cb`。当前 master 的 outbox 流水线 **不会**打出第 197 行
- **R1：** topic `fs_events_compact`，`cleanup.policy=compact`（partitions=1，RF=1）。`event-filter` 为空，订阅 `SWITCH_EVENT_ALL`。启动时的 HEARTBEAT、RE_SCHEDULE 等没有 `Channel-Call-UUID`，`PublishEvent` 取出的 key 为 NULL，`send()` 里 `key_length = key == NULL ? 0 : strlen(key)`，再交给 `rd_kafka_produce`。日志：`Message delivery failed Broker: Broker failed to validate record`（`mod_event_kafka.cpp:197`）。`R1_INVALID_COUNT=670`。模块说明把该文本标为 `RD_KAFKA_RESP_ERR_INVALID_RECORD` / broker **+87**
- **R2：** 同一旧模块、同一 null-key 路径，换成 topic `fs_events_delete`，`cleanup.policy=delete`。`R2_INVALID_COUNT=0`。R2 日志窗口：0 次投递失败 / 0 次 `INVALID_RECORD`，topic 为 `fs_events_delete`
- 不是 **-187** `ALL_BROKERS_DOWN`。`reproduce.sh` 没有调用 toxiproxy 断流。短断 35s / 超 TTL 断连 150s 仍归 FS-01 / FS-09，不能当作 FS-11 PASS
- **Jessie 阻塞已解除，不是「1.6 一直可用」。** 更早曾拉取 `praekeltfoundation/freeswitch:1.6`（FreeSWITCH 1.6.20，Debian Jessie，仅运行时）：没有头文件，没有 librdkafka，Jessie apt 已 EOL，且与当时实验室的 FS 1.10.12 头文件 ABI 不兼容，所以那一轮没有在 1.6 上跑。该阻塞由本次源码构建镜像 `lab-freeswitch:1.6.20-kafka` 取代（见主证据 RESULT 与 `REBUILD-SO.md`）。重编工具链在 `/workspace/lab-mod-event-kafka/fs16/`，脚本 `scripts/rebuild-mod.sh`。R1/R2 在 FS 1.6.20 上重跑并通过
- **对照（保留，不删除）：** `reports/fs11-err87-20260930-154534/`（实验室机器 `/workspace/mod_event_kafka-fix/reports/fs11-err87-20260930-154534/`），**旧无 outbox + FS 1.10.x lab**。镜像 `lab-freeswitch:1.10.12-kafka`，二进制 `FreeSWITCH version: 1.10.7-dev+git~20210825T173719Z~dd2411336f~64bit`（git `dd24113`）。`RESULT.txt` 时间 2026-09-30 15:48:11 +0800。R1 窗口 482 行 `INVALID_RECORD`；R2 delivery-fail = 0。独立冒烟记下 `DR_FAIL err=87 ... key_len=0`，以及 compact+NULL → err=87、delete+NULL → OK、compact+非空 key → OK。目录里的 `fs16-blocker.txt` 记录的是当时 Jessie 运行时镜像为何编不过，该叙述已被上面的 1.6.20 源码构建取代。这一轮是 1.6 被挡住时的较早实验室通过，不代替 1.6.20 主证据
- 1.6.20 这一轮走的是真实缺 `Channel-Call-UUID` 的路径（模块说明）。R3（`message.timestamp.*.max.ms`）未跑
- 无对应 L-xx。未经生产验证。候选补丁 0001 = **NOT_VERIFIED**（仅作反面参考）
