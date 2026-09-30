# 故障场景矩阵 — FAULT-SCENARIOS

> **文档性质：** 草案。将 Kafka / 网络 / 容量 / TTL / 进程类故障映射到 `docs/TEST-PLAN.md` 的 L-xx 与验收门。  
> **诚实口径：** 仅当 `docs/STATUS.md` / `docs/TEST-PLAN.md` 明确写为 **PASS** 时，才标为「已 empirically」。其余一律「文档草案 / 未 empirically」。候选补丁 `0001` = **NOT_VERIFIED**（反面参考），不得借本文件宣称已验证。  
> **契约来源：** `docs/DESIGN.md`、`docs/RELIABILITY.md`。具体的 toxiproxy / docker / FS 命令由安装软件大师维护。FS-01、FS-09 已附上断流脚本；FS-11 附有实验室草案步骤（**文档草案 / 未 empirically**，R1 尚未跑过）；其余场景仍只写概要意图。  
> **实验室拓扑（Phase1）：** 单节点 KRaft + 3 个 toxiproxy 前端（`19092–19094`）均指向同一 broker。禁用全部代理 = 客户端完全断开；真实 3 broker / ISR / 滚动重启仍推迟。

FS-01、FS-09 的「如何注入」已附上实验室断流命令（`toxiproxy_cut_restore.sh`，短断 35s / 超 TTL 断连 150s）。FS-11 在 [DRILL-RUNBOOK.md](DRILL-RUNBOOK.md) §10 写了草案命令（`cleanup.policy=compact` 的 topic 加上 null key，期望 broker 错误 **+87** `INVALID_RECORD`）。该节 **尚未 empirically**，也 **不是** 断流脚本能复现的症状。其余场景仍只有概要意图，待有真实命令后再补。操作步骤见 [DRILL-RUNBOOK.md](DRILL-RUNBOOK.md)；拓扑见 [KAFKA-DEPLOY.md](KAFKA-DEPLOY.md)。

`docs/STATUS.md` 记录的 L-16 证据目录是 `reports/l16-abc-20260925-215439/`。下文引用的 `l16a/`、`l16b/`、`l16c/` 是该目录下的子目录，与 STATUS 记录的是同一次运行，不另立证据根路径。断流/断连类场景的验收门使用 `scripts/verify_event_ids.py`。FS-11 的验收门是 broker **+87** `INVALID_RECORD` 加上 topic 配置能解释拒收原因；不要用该脚本的集合相等，也不要用短断日志，代替这一门。

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

> **文档草案 / 未 empirically。** R1 尚未在实验室跑过。本文不宣称生产验证。  
> 生产侧看到的 `[ERR] mod_event_kafka.cpp:197 Message delivery failed Err-87` 是 **broker 错误码 +87**，名字是 `INVALID_RECORD`（记录被 broker 校验拒绝）。旧版 librdkafka 若错误表里没有这个码，`rd_kafka_err2str` 会打成 `Err-87?`（问号表示库不认识该码，数值仍是 **+87**）。  
> 这与 librdkafka **本地**错误 **-187** `ALL_BROKERS_DOWN` 不是同一个码。**-187** 属于断连/断流一类，FS-01 / FS-09 的 `lab/toxiproxy_cut_restore.sh` 短断或超 TTL 断连覆盖的是这一类。短断证据不得当作 FS-11 通过。

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-11 / Err-87 INVALID_RECORD（云 Kafka 升级后 broker 校验拒收） |
| **如何注入** | **文档草案。** 必须注入 **broker 校验拒收**，让 broker 返回 **+87** `INVALID_RECORD`。禁止用 `lab/toxiproxy_cut_restore.sh`（短断 35s / 超 TTL 断连 150s）代替：那只制造客户端断连，对应 **-187** `ALL_BROKERS_DOWN`，打不出本场景要的 **+87**。**R0：** FS **1.6** 实验室镜像 + Kafka 3.x/KRaft（与云上较新 broker 同类；可用现有 `apache/kafka:3.8.1`，bootstrap 仍只走实验室 toxiproxy，禁止指向生产/云端地址）。加载 **outbox 之前**、会在 `dr_msg_cb` 打印 `Message delivery failed` 的旧 `mod_event_kafka`（引入 outbox 之前的源码里，该日志在 `mod_event_kafka.cpp` 约第 197 行）。现有 `lab-freeswitch:1.10.12-kafka` 与当前 master 的 outbox 模块 **不是** 这条路径。**R1（主路径）：** 新建 topic，`cleanup.policy=compact`；制造 **null key**（事件没有 `Channel-Call-UUID`，或在旧模块实验室副本里强制把 key 指针设为 NULL）。compact topic 不接受 null key 时，broker 在校验阶段拒收。期望 FS 日志出现 `Message delivery failed`，且错误文本是 **+87** / `INVALID_RECORD`（旧库可为 `Err-87?`）。**R2（对照）：** 同一条发送路径，但 topic 为 `cleanup.policy=delete`，或者 key 始终非空。对照 **不得** 出现 **+87**。**R3（可选）：** 收紧 `message.timestamp.*.max.ms`（新 broker 上常见 `message.timestamp.before.max.ms` 与 `message.timestamp.after.max.ms`），作为第二条 `INVALID_RECORD` 路径；仍是草案，未跑。分步命令见 `docs/DRILL-RUNBOOK.md` §10。 |
| **预期模块行为** | 走旧模块的投递报告回调 `dr_msg_cb`：投递失败，日志为 `Message delivery failed` 后接 `rd_kafka_err2str` 的文本。broker **不接受** 该记录，被拒的消息不应出现在 topic 里。这是校验拒收，不是网络中断：链路仍通，失败原因是记录本身。旧库不认识 **+87** 时，同一条日志里的文本是 `Err-87?`，仍表示 broker **+87** `INVALID_RECORD`。**-187** `ALL_BROKERS_DOWN`（断流/断连，常见文案与 “All broker connections are down” 同类）不是本场景的预期日志。当前 master 使用 outbox 流水线（`KafkaPipeline::on_delivery`），**不会**打出 `mod_event_kafka.cpp:197` 的 `Message delivery failed`。本场景要复现的是旧模块 / FS 1.6 实验室路径上的那一行；在当前 master 上即使 broker 同样拒收，日志形态也不同，不能拿 master 的 outbox 日志冒充这条生产症状。旧模块没有 outbox，也不要假设消息头里有 `x-fs-event-id`。 |
| **验收门** | **文档草案 / 未 empirically**（R1 未跑，不得标 PASS，不宣称生产验证）。将来判通过时须同时满足：固定命令能重复打出旧模块 `Message delivery failed`，且错误是 broker **+87** / `INVALID_RECORD`（旧库文本 `Err-87?` 可以，但必须能对应 **+87**）；证据目录里的 topic 配置 / broker 拒收原因能解释该码（优先 `cleanup.policy=compact` + null key）；R2 对照不出现 **+87**；写明路径是 **无 outbox 的旧模块**。只有一行 `Err-87?`、没有 broker 侧原因说明，不算证据齐。`lab/toxiproxy_cut_restore.sh` 的短断/断连，以及任何 **-187** `ALL_BROKERS_DOWN` 日志，**不得**作为 FS-11 PASS。不要用 `scripts/verify_event_ids.py` 的「注入集合等于消费集合」代替本门：被拒记录本来就不进 topic。 |
| **L-xx 覆盖** | **文档草案 / 未 empirically。** TEST-PLAN 里还没有针对 broker `INVALID_RECORD`（**+87**）的 L-xx，本文不新编一条 PASS。L-15（认证/ACL/永久性 produce 错误，状态 PLANNED）是当前 outbox 路径上的另一项，不是本场景的覆盖。FS-01 / L-02 / L-07 / L-16 的断流证据对应断连类 **-187**，同样不是本场景。 |
| **证据路径** | 占位：`reports/fs11-err87-<ts>/`。目录里至少要有：FS 日志片段（`Message delivery failed`，文本对应 **+87** / `INVALID_RECORD` 或旧库 `Err-87?`）、topic 配置（含 `cleanup.policy`）、实际执行的复现命令、以及一份 broker 侧原因说明（优先写明 compact + null key）。目前没有该目录。仅有 `Err-87?` 一行、缺少 topic 配置与拒收原因的，不能当作本场景证据。 |

**+87 与 -187 对照（不得混用）：**

| 代码 | 名称 | 谁产生 | 和本场景的关系 |
|------|------|--------|----------------|
| **+87** | `INVALID_RECORD` | Kafka **broker** 校验拒收 | FS-11 验收必须看到。用户日志里的 `Err-87` / 旧库 `Err-87?` 指的是这个 **正** 错误码 |
| **-187** | `ALL_BROKERS_DOWN` | librdkafka **本地**错误（断连/断流） | FS-01、FS-09 短断与超 TTL 断连已覆盖的一类。不是 Err-87，不得当作 FS-11 PASS |

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
| Err-87 `INVALID_RECORD`（broker **+87** 校验拒收） | （无对应 L-xx） | 文档草案 / 未 empirically；不是 **-187** `ALL_BROKERS_DOWN` |

---

## 维护说明

1. 新场景跑通后：更新本文件对应行的「L-xx 覆盖」与「证据路径」，并同步 `docs/STATUS.md` / `docs/TEST-PLAN.md` 状态列。  
2. 断流/断连类通过规则以 **event_id 集合** 为准，禁止仅凭「无错误日志」宣称 PASS。FS-11 以 broker **+87** `INVALID_RECORD` 与 topic 配置为准，见该节。  
3. Phase1 为单 broker + 三个前端指向同一 broker：FS-01 有效；FS-02/L-04/L-08 需在真实多 broker 环境下跑过后方可宣称。  
4. 建议后续把 **L-16a/b/c** 正式写入 TEST-PLAN §3.2，避免只记录在 STATUS 中。  
5. FS-11 在 R1 于 FS 1.6 + 无 outbox 旧模块上重复打出 broker **+87** `INVALID_RECORD`，且 `reports/fs11-err87-<ts>/` 含 topic 配置与 broker 拒收原因之前，保持「文档草案 / 未 empirically」。只有 `Err-87?` 一行，或只有断流产生的 **-187** `ALL_BROKERS_DOWN`，都不算通过。不要把 **+87** 与 **-187** 写成同一个错误。
