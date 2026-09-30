# 故障场景矩阵 — FAULT-SCENARIOS

> **文档性质：** 草案。将 Kafka / 网络 / 容量 / TTL / 进程类故障映射到 `docs/TEST-PLAN.md` 的 L-xx 与验收门。  
> **诚实口径：** 仅当 `docs/STATUS.md` / `docs/TEST-PLAN.md` 明确写为 **PASS** 时，才标为「已 empirically」。其余一律「文档草案 / 未 empirically」。候选补丁 `0001` = **NOT_VERIFIED**（反面参考），不得借本文件宣称已验证。  
> **契约来源：** `docs/DESIGN.md`、`docs/RELIABILITY.md`。具体的 toxiproxy / docker / FS 命令由安装软件大师维护。FS-01、FS-09 已附上断流脚本；FS-11 已 empirically（旧无 outbox + FS 1.10.x lab，R1/R2 PASS；**不是** FS 1.6）；其余场景仍只写概要意图。  
> **实验室拓扑（Phase1）：** 单节点 KRaft + 3 个 toxiproxy 前端（`19092–19094`）均指向同一 broker。禁用全部代理 = 客户端完全断开；真实 3 broker / ISR / 滚动重启仍推迟。

FS-01、FS-09 的「如何注入」已附上实验室断流命令（`toxiproxy_cut_restore.sh`，短断 35s / 超 TTL 断连 150s）。FS-11 已在 FS 1.10.x 实验室用无 outbox 旧模块跑通：`cleanup.policy=compact` 加上缺少 `Channel-Call-UUID` 的事件，得到 broker **+87** `INVALID_RECORD`（R1 PASS）；`cleanup.policy=delete` 的同一路径没有 **+87**（R2 PASS）。这次不是 FS 1.6，也不是断流脚本能复现的症状。步骤见 [DRILL-RUNBOOK.md](DRILL-RUNBOOK.md) §10。其余场景仍只有概要意图，待有真实命令后再补。拓扑见 [KAFKA-DEPLOY.md](KAFKA-DEPLOY.md)。

`docs/STATUS.md` 记录的 L-16 证据目录是 `reports/l16-abc-20260925-215439/`。下文引用的 `l16a/`、`l16b/`、`l16c/` 是该目录下的子目录，与 STATUS 记录的是同一次运行，不另立证据根路径。FS-11 的证据目录是 `reports/fs11-err87-20260930-154534/`（实验室机器上为 `/workspace/mod_event_kafka-fix/reports/fs11-err87-20260930-154534/`；`reports/` 不入库，与 L-16 相同）。断流/断连类场景的验收门使用 `scripts/verify_event_ids.py`。FS-11 的验收门是 broker **+87** `INVALID_RECORD` 加上 topic 配置能解释拒收原因；不要用该脚本的集合相等，也不要用短断日志，代替这一门。

---

## 覆盖图例

| 标记 | 含义 |
|------|------|
| **已 empirically（PASS）** | STATUS/TEST-PLAN 已写 PASS，并有 `reports/…` 证据 |
| **文档草案 / 未 empirically** | TEST-PLAN 为 PLANNED，或 STATUS 写明「L-03–L-15 未完整跑过」 |
| **部分相关已 empirically** | 场景意图与某次 PASS 运行有重叠，但未作为独立的 L-xx 门禁完整跑过 |

**通用验收脚本：** `scripts/verify_event_ids.py reports/<run-id>/`  
产物至少包含 `injected_ids.txt`、`consumed_ids.txt`（消息头 `x-fs-event-id`）；可选 `rejected_ids.txt`、`outbox_snapshot.csv`。  
退出码：`0` VERIFY_OK；`1` 集合不匹配；`2` 同通话顺序失败；`3` 产物损坏。

---

## 场景总表

### FS-01 整集群断开（客户端视角完全断连）

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-01 / 整集群断开（toxiproxy 全部断开） |
| **如何注入** | 概要：toxiproxy 禁用全部前端 `kafka1/kafka2/kafka3`（断连时长建议 > `message-timeout-ms`，短断约 35s）；恢复时全部重新启用。**不** `reload mod_event_kafka`，也不重启 FS。拨测在断连前与断连期间各注入一批事件。实际断流：在仓库根目录执行 `lab/toxiproxy_cut_restore.sh 35`（全禁 `kafka1`/`kafka2`/`kafka3`，约 35 秒后自动恢复）。共享机仍可用 `/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh 35`。断连期间另开终端拨测。超 TTL 的 150 秒全断属于 FS-09，不要拿来作为本场景的通过条件。步骤见 `docs/DRILL-RUNBOOK.md`。 |
| **预期模块行为** | FS 回调仅深拷贝入队，不在网络/磁盘上阻塞；worker 写入 SQLite outbox 后 produce；poll 线程持续调用 `rd_kafka_poll`（调用 `rd_kafka_poll` 期间**不**持有 `rk_mu_`）；未 ACK 的行保持 pending/重试；恢复后 outbox 排空；稳定 `event_id` 经消息头 `x-fs-event-id` 重放；同通话按 `call_uuid`+`created_at_ms` FIFO。 |
| **验收门** | `scripts/verify_event_ids.py`：`set(injected)-set(consumed_dedup)-rejected==∅`（允许已写明原因的 pre-COMMIT 丢失）；排空后 `pending+in_flight==0`；**reload 前**采集的数据即通过（自愈门禁）；报告 `dupes_in_consume`。 |
| **L-xx 覆盖** | **已 empirically：** **L-02=PASS**、**L-07=PASS**（稳定测试架 + FS Phase2）；短断同时覆盖 **L-16a=PASS**。 |
| **证据路径** | `reports/l02-l07-run1790313218/`（稳定测试架）；`reports/l02-l07-fs-20260925-133618/`（FS，VERIFY_OK 60/60）；`reports/l16-abc-20260925-215439/l16a/`（短断 35s，60/60） |

---

### FS-02 单个 bootstrap 入口禁用

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-02 / 单 bootstrap 入口禁用 |
| **如何注入** | 概要：仅禁用 toxiproxy 三个前端之一（另两路仍通）。真正的「单 broker 宕机」需要 3 broker 拓扑（当前 Phase1 三个前端指向同一 broker，禁用一路后通常仍可达）。 |
| **预期模块行为** | 客户端应经剩余 bootstrap 继续 produce；无持续 `rejected_*`；瞬时失败走退避/重试；回调仍不阻塞。 |
| **验收门** | `verify_event_ids.py` 完整性检查；指标上 `produce_ok` 持续增长、无长期积压；对比「禁用一路」与「全部断开」的差异。 |
| **L-xx 覆盖** | **文档草案 / 未 empirically。** 最接近 **L-04**（单 broker 宕机，PLANNED）。Phase1 三个前端指向同一 broker，不能替代真实的 ISR 测试。 |
| **证据路径** | （无独立 PASS 报告） |

---

### FS-03 抖动 / 延迟（latency / jitter）

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-03 / 网络延迟与抖动 |
| **如何注入** | 概要：toxiproxy latency / jitter toxic（可叠加限速）；持续拨测加压。 |
| **预期模块行为** | 瞬时失败保留 outbox 行 + 指数退避（含抖动）；内存队列仅在容量处拒绝（`rejected_mem_full` + WARNING）；**绝不**阻塞媒体/回调线程；线程数与内存保持稳定。 |
| **验收门** | `verify_event_ids.py` 最终集合完整（去重后）；加压期间 FS 不卡死；每次拒绝都须能在 `rejected_ids.txt`/日志中对应上，禁止静默丢弃。 |
| **L-xx 覆盖** | **文档草案 / 未 empirically。** 对应 **L-12**、**L-03**（均为 PLANNED）。 |
| **证据路径** | （无） |

---

### FS-04 TCP RST / reset-peer

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-04 / 连接 RST |
| **如何注入** | 概要：toxiproxy `reset_peer`（或在全部被代理端口上发送 RST）；在 produce/poll 期间注入。 |
| **预期模块行为** | 视为瞬时故障：保留行并退避重试；链路恢复后**无需 reload** 即可排空；可能出现 ACK 窗口重复 → 消费端按 `x-fs-event-id` 去重。 |
| **验收门** | `verify_event_ids.py`；reload 前自愈；报告 `dupes_in_consume`（去重后仍残留重复即判失败）。 |
| **L-xx 覆盖** | **文档草案 / 未 empirically。** 对应 **L-10**、**L-03**（PLANNED）。与 FS-01 的「全部 disable」不同，RST 是连接级重置。 |
| **证据路径** | （无） |

---

### FS-05 黑洞超时（blackhole / 全丢包）

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-05 / 网络黑洞 > `message-timeout-ms` |
| **如何注入** | 概要：toxiproxy timeout / 全丢包 toxic，持续时长 **>** `message-timeout-ms`（默认在 topic 侧为 30s），然后恢复。这与「disable 代理」不同（后者更接近连接被拒绝）。 |
| **预期模块行为** | 行保持 pending/重试（未超 TTL）；恢复后旧到点行排空；**新**事件仍可入队并发送；回调不阻塞。若断连时长 **>** `outbox-ttl-ms`，见 FS-09。 |
| **验收门** | `verify_event_ids.py`（未过期集合完整）；指标 `delivery_fail` 可以上升，`outbox` 终态合理；reload 前自愈。 |
| **L-xx 覆盖** | **部分相关已 empirically：** 全部代理 disable 35s（>30s timeout）已在 **L-02/L-07** 中验证，但正式 **L-11**（blackhole toxic）仍为 **PLANNED / 未 empirically**。 |
| **证据路径** | 相关短断：`reports/l02-l07-fs-20260925-133618/`；L-11 专用：（无） |

---

### FS-06 Broker 重启 / 进程崩溃

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-06 / Broker 重启或 SIGKILL |
| **如何注入** | 概要：停止/SIGKILL Kafka 容器或进程后再拉起；或滚动重启（每次一个 broker，需要真实的多 broker 集群）。客户端 bootstrap 仍只走代理。 |
| **预期模块行为** | 与瞬时故障相同：已 COMMIT 的 outbox 行不会永久丢失；自动重试直至排空；无需 reload；librdkafka 内存队列中的数据可能丢失，但磁盘 outbox 是事实来源。 |
| **验收门** | `verify_event_ids.py`；排空后 pending/in_flight=0；滚动场景下已提交行无永久缺失。 |
| **L-xx 覆盖** | **部分相关已 empirically：** 客户端侧「全断开再恢复」由 **L-02/L-07** 覆盖。正式 **L-08**（滚动）、**L-09**（broker SIGKILL）= **PLANNED / 未 empirically**（真实 3 broker 推迟）。 |
| **证据路径** | 相关：`reports/l02-l07-run1790313218/`、`reports/l02-l07-fs-20260925-133618/`；L-08/L-09：（无） |

---

### FS-07 内存队列满（queue full）

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-07 / `mem-queue-max` 背压 |
| **如何注入** | 概要：将 `mem-queue-max` 调小；在 broker 不可达或 worker 缓慢时高速灌事件，使有界队列溢出。 |
| **预期模块行为** | `try_push` 失败 → `rejected_mem_full` + WARNING；**显式拒绝，不静默丢弃**；**不**阻塞 FS 媒体/回调线程；已入队或已写入 outbox 的行仍按至少一次路径处理。 |
| **验收门** | 每个拒绝 ID 出现在 `rejected_ids.txt`/日志；`verify_event_ids.py`：`missing` 不计入已拒绝的集合；FS 拨测不卡死。 |
| **L-xx 覆盖** | **文档草案 / 未 empirically。** 对应 **L-05**（PLANNED）。单元测试侧有针对有界队列的 U-Q-*，但不能替代本实验室门禁。 |
| **证据路径** | （无） |

---

### FS-08 磁盘 / outbox 满

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-08 / outbox 行数或字节上限 / INSERT 失败 |
| **如何注入** | 概要：压低 `outbox-max-rows` / `outbox-max-bytes`，或使 `outbox-path` 不可写 / 磁盘满，迫使 `insert_pending` 失败。 |
| **预期模块行为** | 拒绝 + `rejected_disk_full`（或打开失败告警）；不崩溃；不静默丢弃；不误 ACK；仍存活的 pending 行不会为腾出空间而被删除（已过期 dead 行的回收规则见 RELIABILITY）。 |
| **验收门** | 拒绝可追溯；进程存活；`verify_event_ids.py` 对**已接受** ID 仍完整；指标/日志可见。 |
| **L-xx 覆盖** | **文档草案 / 未 empirically。** 对应 **L-13**（PLANNED）。单元测试 U-DSK-02/03 为 CODED/PASS，但不属于 FS 实验室验证。 |
| **证据路径** | （无实验室 PASS） |

---

### FS-09 超过 Outbox TTL（beyond TTL）

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-09 / pending 超过 `outbox-ttl-ms` |
| **如何注入** | 概要：配置 `outbox-ttl-ms=120000`（或实验值）；toxiproxy **全断时长 > TTL**（如 150s）；在断连前与断连期间注入；恢复后再拨新呼叫。对照：短断 **≤ TTL** 时应全部送达。实际断流：超 TTL 场景在仓库根目录执行 `lab/toxiproxy_cut_restore.sh 150`（全禁三个入口，150 秒后恢复，大于默认的 `120000`）；短断对照用同一脚本加参数 `35`（即 FS-01）。共享机上的绝对路径为 `/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh`。断连期间另开终端拨测，恢复后再拨新呼叫。步骤见 `docs/DRILL-RUNBOOK.md`。 |
| **预期模块行为** | `now_ms - created_at_ms > outbox-ttl-ms` 的 **pending** → `state=dead`、`last_error=expired_ttl`、计 `outbox_expired`，**不投递**；in-flight 行不会就地过期；恢复后 dead 行不进入 `fetch_due`，新事件优先；短断不触发过期，走自愈排空路径。 |
| **验收门** | 短断：`verify_event_ids.py` 全量 OK。长断：过期 ID **不得**出现在 topic 中（`expired_still_in_topic=0`）；未过期子集应匹配；outbox 可见 `dead/expired_ttl`；自愈后新注入集合 VERIFY_OK。 |
| **L-xx 覆盖** | **已 empirically：** **L-16a=PASS**（短断 35s，60/60）、**L-16b=PASS**（150s>TTL，matched=30/60，expired_ttl，expired_still_in_topic=0）、**L-16c=PASS**（自愈后 30/30）。注：L-16a/b/c 记录在 STATUS 证据中，**尚未作为正式行写入 TEST-PLAN §3.2 矩阵**。 |
| **证据路径** | `reports/l16-abc-20260925-215439/`（子目录 `l16a/`、`l16b/`、`l16c/`）；模块 `outbox-ttl-ms=120000`（master `37e89154`） |

---

### FS-10 进程杀死与恢复（FS / 模块 SIGKILL）

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-10 / FS 或模块进程杀死后恢复 |
| **如何注入** | 概要：中断期间对 FS/模块进程发送 SIGKILL（或 unload）；再拉起 FS / `load mod_event_kafka`；broker 随后恢复。可叠加 outage。 |
| **预期模块行为** | 已 COMMIT 但未 ACK 的行留在磁盘；启动时执行 `requeue_in_flight`；以**同一** `event_id` / `x-fs-event-id` 重放；在 ACK 与本地 DELETE 之间崩溃可能导致重复 → 消费端去重；COMMIT 之前、仅在内存队列中的窗口内可能丢失（见 RELIABILITY 丢失窗口）。关闭顺序：停止入队 → 内存队列写入 outbox → flush/poll → join。 |
| **验收门** | 重启前后 outbox 快照对比；`verify_event_ids.py` 验证已持久化 ID 至少送达一次；重复仅在去重语义下允许。 |
| **L-xx 覆盖** | **文档草案 / 未 empirically。** 对应 **L-14**、**L-06**（均为 PLANNED）。**不得**用 L-02 的「未 reload 自愈」冒充本场景。 |
| **证据路径** | （无） |

---

### FS-11 Err-87 INVALID_RECORD（云 Kafka 升级后 broker 校验拒收）

> **已 empirically（旧无 outbox + FS 1.10.x lab）。** R0/R1/R2 于 2026-09-30 PASS。**不是**「已在 FS 1.6 实证」。未经生产验证。  
> 生产侧症状 `[ERR] mod_event_kafka.cpp:197 Message delivery failed Err-87` 仍是 **broker 错误码 +87** = `INVALID_RECORD`。旧版 librdkafka 若不认识该码，会打成 `Err-87?`。本次实验室的 librdkafka 打出的是同一码的具名文本：`Broker: Broker failed to validate record`，独立冒烟为 `DR_FAIL err=87`。  
> 这与 librdkafka **本地**错误 **-187** `ALL_BROKERS_DOWN` 不是同一个码。**-187** 属于断连/断流一类，FS-01 / FS-09 的 `lab/toxiproxy_cut_restore.sh` 覆盖的是这一类。本次没有用断流脚本，短断证据不得当作 FS-11 通过。

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-11 / Err-87 INVALID_RECORD（云 Kafka 升级后 broker 校验拒收） |
| **如何注入** | **已跑通的是 broker 校验拒收，不是断流。** 禁止用 `lab/toxiproxy_cut_restore.sh`（短断 35s / 超 TTL 断连 150s）代替：那只制造客户端断连，对应 **-187** `ALL_BROKERS_DOWN`。**R0（PASS）：** 镜像 `lab-freeswitch:1.10.12-kafka`，二进制报告 `FreeSWITCH version: 1.10.7-dev+git~20210825T173719Z~dd2411336f~64bit`（git `dd24113`）。Kafka 为实验室 `apache/kafka:3.8.1`（KRaft），bootstrap 仍只走 toxiproxy `127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094`。模块是 upstream **旧模块、无 outbox**，投递失败在 `dr_msg_cb`、`mod_event_kafka.cpp:197`。加载日志是 `KafkaEventPublisher Initialising...`（约第 87 行），**不是**当前 master 的 `Initialising (outbox pipeline)`。**FS 1.6 这次没跑：** 证据 `fs16-blocker.txt`。曾拉取 `praekeltfoundation/freeswitch:1.6`（FreeSWITCH 1.6.20，Debian Jessie）。镜像没有 freeswitch-dev / 头文件，也没有 librdkafka；Jessie 的 apt 已 EOL。实验室现成头文件是 FS 1.10.12（bookworm），与 1.6 ABI 不兼容。因此改用该 1.10.x 镜像 + 旧 `.so`（同一条 cpp:197 null-key 路径）。生产目标仍是 FS 1.6；Err-87 是模块与 broker 的行为。不得写成已在 FS 1.6 实证。**R1（PASS，主路径）：** topic `fs_events_compact`，`cleanup.policy=compact`（partitions=1，RF=1）。`event-filter` 为空，日志为 `Found 0 subscriptions` / `Subscribed to ALL events`。没有 `Channel-Call-UUID` 的事件（HEARTBEAT、RE_SCHEDULE，以及不少 CUSTOM/系统事件）以 null key 调用 `rd_kafka_produce`。观察到 `[ERR] mod_event_kafka.cpp:197  Message delivery failed Broker: Broker failed to validate record`；R1 窗口内 482 行。独立冒烟：`DR_FAIL err=87 (Broker: Broker failed to validate record) topic=fs_events_compact key_len=0`。强制 null key 的实验补丁曾编过，**R1 没有用它**，走的是真实缺头。**R2（PASS，对照）：** 同一旧模块、同一类 null key，topic `fs_events_delete`，`cleanup.policy=delete`。该窗口 delivery-fail = 0，`INVALID_RECORD` = 0。独立对照（`broker-reason.txt`）：delete + NULL key 为 OK；compact + 非空 key 为 OK。**R3（可选，本次未跑）：** 收紧 `message.timestamp.*.max.ms` 不在这次 PASS 里。实际命令在证据目录的 `reproduce.sh`。步骤见 `docs/DRILL-RUNBOOK.md` §10。 |
| **预期模块行为** | 旧模块 `dr_msg_cb` 收到投递失败。本次日志文本是 `Message delivery failed Broker: Broker failed to validate record`，对应 broker **+87** `INVALID_RECORD`（记录未通过 broker 校验）。compact topic 要求非空 key 才能按 key 保留最新值；null key 在校验阶段被拒绝。链路是通的，这不是网络中断。**-187** `ALL_BROKERS_DOWN` 不是本次日志。更旧的 librdkafka 仍可能把同一个 **+87** 印成 `Err-87?`；本次库已经能打出具名文本，并以 `err=87` 核对过。当前 master 的 outbox 流水线（`KafkaPipeline::on_delivery`）**不会**打出 `mod_event_kafka.cpp:197` 这一行；加载时会写 `Initialising (outbox pipeline)`。本次通过的是旧 `.so`。旧模块没有 outbox，也不要假设消息头里有 `x-fs-event-id`。 |
| **验收门** | **已 empirically（旧无 outbox + FS 1.10.x lab）：** R1 PASS，R2 PASS。固定复现是证据目录里的 `reproduce.sh`。R1 重复打出第 197 行，文本为 `Broker: Broker failed to validate record`，独立冒烟 `err=87`，topic 配置为 `cleanup.policy=compact`，原因说明是 compact + null key（缺 `Channel-Call-UUID`）。R2 同一路径在 `cleanup.policy=delete` 上没有 **+87**。路径标明为无 outbox 旧模块，FS 二进制是 1.10.7-dev，**不是** FS 1.6。只有一行 `Err-87?`、没有 topic 配置和 broker 侧原因，仍然不算齐。`lab/toxiproxy_cut_restore.sh` 与任何 **-187** `ALL_BROKERS_DOWN` **不得**作为 FS-11 PASS。不要用 `scripts/verify_event_ids.py` 的集合相等代替本门。R3 未跑，不计入。未经生产验证。 |
| **L-xx 覆盖** | **已 empirically（STATUS，旧无 outbox + FS 1.10.x lab）。** TEST-PLAN 仍没有针对 `INVALID_RECORD`（**+87**）的 L-xx，本文不新编编号。L-15（认证/ACL/永久性 produce 错误，PLANNED）是当前 outbox 路径上的另一项，不是本场景。FS-01 / L-02 / L-07 / L-16 的断流证据对应断连类 **-187**，同样不是本场景。 |
| **证据路径** | `reports/fs11-err87-20260930-154534/`。实验室机器：`/workspace/mod_event_kafka-fix/reports/fs11-err87-20260930-154534/`。目录含 `RESULT.txt`、`broker-reason.txt`、`topic-config.txt`、`reproduce.sh`、`fs16-blocker.txt`、模块说明、R1/R2 日志。`reports/` 被 `.gitignore` 忽略，与 L-16 一样只记路径、不把证据或 `.so` 提交进本仓库。 |

**+87 与 -187 对照（不得混用）：**

| 代码 | 名称 | 谁产生 | 和本场景的关系 |
|------|------|--------|----------------|
| **+87** | `INVALID_RECORD` | Kafka **broker** 校验拒收 | FS-11 本次看到的就是这个码。实验室文本是 `Broker: Broker failed to validate record`（`err=87`）。生产日志里的 `Err-87`、旧库的 `Err-87?` 也是这个 **正** 错误码 |
| **-187** | `ALL_BROKERS_DOWN` | librdkafka **本地**错误（断连/断流） | FS-01、FS-09 短断与超 TTL 断连已覆盖的一类。不是 Err-87。本次运行不是这条，不得拿断流证据当作 FS-11 PASS |

FS-11 的整体思路与复现方法总览见 [FS11-ERR87-REPRO-OVERVIEW.md](FS11-ERR87-REPRO-OVERVIEW.md)。

---

## TEST-PLAN 中已规划但未完整跑过的 L-xx

摘自 master `docs/TEST-PLAN.md` §3.2 与 `docs/STATUS.md`「未宣称：L-03–L-15 未完整跑过」：

| ID | 故障摘要 | TEST-PLAN 状态 |
|----|----------|----------------|
| **L-01** | 正常路径下每个 event_id 各出现一次 | PLANNED |
| **L-03** | toxiproxy reset-peer / 延迟 | PLANNED |
| **L-04** | 单 broker 宕机（ISR 仍正常） | PLANNED |
| **L-05** | `mem-queue-max` 压力 | PLANNED |
| **L-06** | 中断期间卸载/重新加载模块 | PLANNED |
| **L-08** | 滚动重启（需要真实多 broker） | PLANNED |
| **L-09** | Broker SIGKILL | PLANNED |
| **L-10** | 全部代理端口 TCP RST | PLANNED |
| **L-11** | 黑洞 > `message-timeout-ms` | PLANNED |
| **L-12** | 延迟 / 反复抖动 | PLANNED |
| **L-13** | 磁盘满 / outbox INSERT 失败 | PLANNED |
| **L-14** | FS/模块 SIGKILL 后重放同 event_id | PLANNED |
| **L-15** | 认证/ACL 等永久错误 → `dead` | PLANNED |

**已 PASS（勿与上表混淆）：**

| ID | 结论 | 证据 |
|----|------|------|
| **L-02** | PASS（稳定测试架 + FS Phase2） | `reports/l02-l07-run1790313218/`、`reports/l02-l07-fs-20260925-133618/` |
| **L-07** | PASS（与 L-02 同一次运行；reload 前已自愈） | 同上 |
| **L-16a/b/c** | PASS（FS+TTL；STATUS 证据；非正式 TEST-PLAN 矩阵行） | `reports/l16-abc-20260925-215439/` |

**FS-11（无 L-xx，勿与上表编号混淆）：** 已 empirically（旧无 outbox + FS 1.10.x lab）。R1/R2 PASS。证据 `reports/fs11-err87-20260930-154534/`。不是 FS 1.6，也不是 TEST-PLAN 矩阵行。

---

## 场景 ↔ L-xx 速查

| 架构师场景 | 主 L-xx | 覆盖诚实标记 |
|------------|---------|--------------|
| 整集群断开 | L-02 / L-07（+L-16a 短断） | 已 empirically |
| 单 bootstrap 入口禁用 | L-04（近） | 文档草案 / 未 empirically |
| 抖动/延迟 | L-12 / L-03 | 文档草案 / 未 empirically |
| RST | L-10 / L-03 | 文档草案 / 未 empirically |
| 黑洞超时 | L-11（相关：L-02 全断） | L-11 未跑；L-02 部分相关 |
| Broker 重启 | L-08 / L-09（相关：L-02） | L-08/09 未跑；L-02 部分相关 |
| 队列满 | L-05 | 文档草案 / 未 empirically |
| 磁盘/outbox 满 | L-13 | 文档草案 / 未 empirically |
| 超过 TTL | L-16a/b/c | 已 empirically（STATUS） |
| 进程杀死恢复 | L-14 / L-06 | 文档草案 / 未 empirically |
| Err-87 `INVALID_RECORD`（broker **+87** 校验拒收） | （无对应 L-xx） | 已 empirically（旧无 outbox + FS 1.10.x lab；不是 FS 1.6；不是 **-187** `ALL_BROKERS_DOWN`） |

---

## 维护说明

1. 新场景跑通后：更新本文件对应行的「L-xx 覆盖」与「证据路径」，并同步 `docs/STATUS.md` / `docs/TEST-PLAN.md` 状态列。  
2. 断流/断连类通过规则以 **event_id 集合** 为准，禁止仅凭「无错误日志」宣称 PASS。FS-11 以 broker **+87** `INVALID_RECORD` 与 topic 配置为准，见该节。  
3. Phase1 为单 broker + 三个前端指向同一 broker：FS-01 有效；FS-02/L-04/L-08 需在真实多 broker 环境下跑过后方可宣称。  
4. 建议后续把 **L-16a/b/c** 正式写入 TEST-PLAN §3.2，避免只记录在 STATUS 中。  
5. FS-11 的 R1/R2 已在 FS 1.10.x 实验室 + 无 outbox 旧模块上 PASS，证据 `reports/fs11-err87-20260930-154534/`。不得写成已在 FS 1.6 实证（见该目录 `fs16-blocker.txt`）。R3（`message.timestamp.*.max.ms`）未跑。只有一行 `Err-87?` 而没有 topic 配置与 broker 拒收原因，或只有断流产生的 **-187** `ALL_BROKERS_DOWN`，都不算通过。不要把 **+87** 与 **-187** 写成同一个错误。当前 master 的 outbox 路径不会打出第 197 行。
