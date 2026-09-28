# 状态 — 2026-09-25（Asia/Shanghai）

实验室源码已合入本仓库根目录（`include/`、`src/`、`mod_event_kafka.cpp`）。下文提到 `work/` 的地方，描述的是产生证据的实验室机器目录树；它们不是本 PR 里的第二份副本。

## 摘要
- FS Phase2 拨测 **L-02/L-07=PASS**
- 证据路径：`reports/l02-l07-fs-20260925-133618/`
- `verify_event_ids.py` VERIFY_OK 60/60，未 reload 模块
- FS + TTL 实验室 **L-16a=PASS**，**L-16b=PASS**，**L-16c=PASS**
- 证据路径：`reports/l16-abc-20260925-215439/`
- 模块配置为 `outbox-ttl-ms=120000`（master `37e89154`）
- 短切断 35 秒：完整送达（verify 成功；注入 60 条，匹配 60 条）
- 切断 150 秒 > TTL：过期行 `dead/expired_ttl`，`expired_still_in_topic=0`；consumed_matched=30 / 60（未过期的已送达）
- 自愈之后：新呼叫 VERIFY_OK 30/30
- 候选补丁 0001 = **NOT_VERIFIED**

## 已完成
- 已实现 `outbox-ttl-ms`（默认 `120000`；`0` 关闭）。超过 TTL 的 pending 行被标为 `dead` / `last_error=expired_ttl`，且不会被投递。
- FS + TTL 实验室 **L-16a=PASS**，**L-16b=PASS**，**L-16c=PASS**。证据路径：`reports/l16-abc-20260925-215439/`。模块配置为 `outbox-ttl-ms=120000`（master `37e89154`）。短切断 35 秒：完整送达（verify 成功；注入 60 条，匹配 60 条）。切断 150 秒 > TTL：过期行 `dead/expired_ttl`，`expired_still_in_topic=0`；consumed_matched=30 / 60（未过期的已送达）。自愈之后：新呼叫 VERIFY_OK 30/30。
- 设计已锁定：SQLite outbox + 有界队列 + worker/poll（候选补丁 0001 未被接受为最终方案）。
- 已构建 Outbox + 有界队列库；ASan 单元测试 PASS（`test_outbox`）。
- FS 模块粘合层已接上：事件回调只做深拷贝并入队；worker 插入 SQLite outbox 行后再 produce；poll 线程在投递之后 ACK。
- FS Phase2 拨测 **L-02/L-07=PASS**。证据路径：`reports/l02-l07-fs-20260925-133618/`。`verify_event_ids.py` VERIFY_OK 60/60，未 reload 模块。
- TEST-PLAN 由测试质检撰写，位于 `docs/TEST-PLAN.md`。
- 实验室 compose 由安装软件大师撰写，位于 `/workspace/lab-mod-event-kafka/`（Kafka×3 + toxiproxy）。
- Phase1 实验室 READY（单个 KRaft + 3 个 toxiproxy 前端）。Phase2 FS 拨测此后已通过（见下文）；真正的 3 broker 集群仍不是 Phase1 所跑的拓扑。

## 未宣称
- L-03–L-15 未完整跑过。
- 未经生产验证。
- 候选补丁 0001 = **NOT_VERIFIED**（仅为反面参考）。不得将其标为已通过生产验证。

## 客户端 bootstrap（实验室健康时）
仅经 toxiproxy：`127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094`。

## 实验室冒烟（稳定流水线）
- 2026-09-25T13:08:26+08:00：`stable/build/test_pipeline_smoke 127.0.0.1:19092 fs_events` → **SMOKE_PASS**（enqueued=produce_ok=acked=20）。根因已修复：poll_loop 不再在 `rd_kafka_poll` 期间持有 `rk_mu_`。
- 候选补丁 0001 = **NOT_VERIFIED**（仅为反面参考）。此后 FS Phase2 拨测 **L-02/L-07=PASS**；证据路径 `reports/l02-l07-fs-20260925-133618/`。

## Phase1 实验室 READY
- Bootstrap：经 toxiproxy 的 `127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094`；主题 `fs_events`。
- 2026-09-25T13:08:45+08:00：针对完整 bootstrap 再次确认稳定冒烟。
- L-02/L-07 与 `scripts/verify_event_ids.py` 已对 QA 解除阻塞，并已在稳定测试架与 FS Phase2 拨测上通过。

## L-02 / L-07（稳定流水线测试架，Phase1 实验室）
- 运行：主题 `fs_events_l02b` 上的 `reports/l02-l07-run1790313218/`
- 切断：toxiproxy 禁用 kafka1/2/3 **35 秒**（> `message_timeout_ms=30s`）；再启用；**不**停止/reload 流水线
- reload 前指标：enqueued=60 produce_ok=90 acked=60 delivery_fail=30 producer_rebuilds=0 outbox 已排空（pending/in_flight/dead=0）healed_without_reload=1
- `verify_event_ids.py`：退出码 0（injected=consumed_unique=60，missing=0）
- 结果：**L-02=PASS**，**L-07=PASS**
- 范围说明：本行是稳定的 `KafkaPipeline` 测试架（不是 FreeSWITCH `.so`）。拥有模块证据的 FS Phase2 拨测见下一节。Phase1 = 单个 KRaft + 3 个 toxiproxy 前端
- 候选补丁 **0001 = NOT_VERIFIED**（仅为反面参考）
- 测试架：`stable/build/test_lab_l02_l07`；核对要求头 `x-fs-event-id`

## L-02 / L-07（FS Phase2 拨测，2026-09-25）
本节拥有 FS Phase2 证据。

- FS Phase2 拨测 **L-02/L-07=PASS**
- 证据路径：`reports/l02-l07-fs-20260925-133618/`
- FS 1.10.12 + `work/` 下的 `mod_event_kafka.so`；配置 buffer-size=100000；bootstrap 仅 toxiproxy；主题 `fs_events`
- 拨号：`dialtest_originate.sh` loopback/park，切断前 ×5 + 切断期间 ×5（toxiproxy 禁用 kafka1/2/3 共 35 秒）；未 reload 模块
- `verify_event_ids.py` VERIFY_OK 60/60，未 reload 模块（injected=60，consumed_matched=60，missing=0）
- 未经 reload 即自愈；module_exists=true
- 候选补丁 0001 = **NOT_VERIFIED**（仅为反面参考）
- 限制：park 路径可能没有 ANSWER；只有 CREATE/HANGUP*
- 限制：L-03–L-15 未完整跑过；未经生产验证

## L-16a/b/c（FS + TTL，2026-09-25 夜间）
- **L-16a=PASS**，**L-16b=PASS**，**L-16c=PASS**
- 证据路径：`reports/l16-abc-20260925-215439/`
- 模块：lab-freeswitch，`outbox-ttl-ms=120000`（master `37e89154`）
- **L-16a=PASS**（短切断 35 秒，verify_rc=0）：完整送达；注入 60 条，匹配 60 条
- **L-16b=PASS**（切断 150 秒 > TTL）：过期行 `dead/expired_ttl`；注入期间/已老化集合中 expired_ttl dead=30；`expired_still_in_topic=0`；consumed_matched=30 / 60（未过期的已送达）
- **L-16c=PASS**（自愈后拨测，verify_rc=0）：新呼叫 VERIFY_OK 30/30
- 拨号辅助脚本：`lab-mod-event-kafka/dialtest_fast.sh`（bgapi；避免长时间 NO_ANSWER 挂起）
- 候选补丁 0001 = **NOT_VERIFIED**（仅为反面参考）
- 限制：L-03–L-15 未完整跑过；未经生产验证
