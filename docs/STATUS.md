# 状态 — 2026-09-25（Asia/Shanghai）；FS-11 增补 2026-09-30

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
- FS-11 **已 empirically（旧无 outbox + FS 1.10.x lab）**：R1 compact+null key → **+87** `INVALID_RECORD` PASS；R2 delete 同一路径无 **+87** PASS
- 证据路径：`reports/fs11-err87-20260930-154534/`（实验室机器 `/workspace/mod_event_kafka-fix/reports/fs11-err87-20260930-154534/`）
- FS 二进制为 1.10.7-dev（镜像 `lab-freeswitch:1.10.12-kafka`）。**不是** FS 1.6 实证

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
- FS-11 没有在 FS 1.6 上跑（`fs16-blocker.txt`：`praekeltfoundation/freeswitch:1.6` 为 1.6.20 / Debian Jessie，缺头文件与 librdkafka，且与实验室 1.10.12 头文件 ABI 不兼容）。不得写成已在 FS 1.6 实证。生产目标仍是 FS 1.6。
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

## FS-11（Err-87 `INVALID_RECORD`，2026-09-30）
- **已 empirically（旧无 outbox + FS 1.10.x lab）**
- **R0 FS+Kafka = PASS**，**R1 = PASS**，**R2 = PASS**（`RESULT.txt`，2026-09-30 15:48:11 +0800）
- 证据路径：`reports/fs11-err87-20260930-154534/`（实验室机器 `/workspace/mod_event_kafka-fix/reports/fs11-err87-20260930-154534/`，含 `RESULT.txt`、`broker-reason.txt`、`topic-config.txt`、`reproduce.sh`、`fs16-blocker.txt`、模块说明与 R1/R2 日志）。`reports/` 不入库
- FS：`FreeSWITCH version: 1.10.7-dev+git~20210825T173719Z~dd2411336f~64bit`（git `dd24113`，2021-08-25 17:37:19Z 64bit）。镜像标签 `lab-freeswitch:1.10.12-kafka`（二进制报告 1.10.7-dev）
- 模块：upstream 旧模块（无 outbox），`mod_event_kafka.cpp:197` `dr_msg_cb`。加载日志是 `KafkaEventPublisher Initialising...`（约第 87 行），不是当前 master 的 outbox 流水线。当前 master **不会**打出第 197 行
- **R1：** topic `fs_events_compact`，`cleanup.policy=compact`。`event-filter` 为空（订阅 ALL）。缺少 `Channel-Call-UUID` 的事件（HEARTBEAT、RE_SCHEDULE 等）以 null key 发送。日志：`Message delivery failed Broker: Broker failed to validate record`（`mod_event_kafka.cpp:197`）。R1 窗口 482 行。独立冒烟：`DR_FAIL err=87 (Broker: Broker failed to validate record) topic=fs_events_compact key_len=0`
- **R2：** 同一旧模块、同一 null-key 路径，topic `fs_events_delete`，`cleanup.policy=delete`。delivery-fail = 0，`INVALID_RECORD` = 0
- 独立对照（`broker-reason.txt`）：compact+NULL → err=87；delete+NULL → OK；compact+非空 key → OK
- 不是 **-187** `ALL_BROKERS_DOWN`，没有用 toxiproxy 断流
- **FS 1.6 未跑。** `fs16-blocker.txt`：`praekeltfoundation/freeswitch:1.6`（FreeSWITCH 1.6.20，Debian Jessie）没有 freeswitch-dev / 头文件，没有 librdkafka，Jessie apt 已 EOL；实验室头文件是 FS 1.10.12（bookworm），与 1.6 ABI 不兼容。决定改用 FS 1.10.x + 旧 `.so`。生产目标仍是 FS 1.6。不得写成已在 FS 1.6 实证
- 强制 null key 的实验补丁没有用于 R1；走的是真实缺 `Channel-Call-UUID` 的路径
- R3（`message.timestamp.*.max.ms`）未跑
- 无对应 L-xx。未经生产验证。候选补丁 0001 = **NOT_VERIFIED**（仅作反面参考）
