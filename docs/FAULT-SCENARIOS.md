# 故障场景矩阵 — FAULT-SCENARIOS

> **文档性质：** 草案。将 Kafka / 网络 / 容量 / TTL / 进程类故障映射到 `docs/TEST-PLAN.md` 的 L-xx 与验收门。  
> **诚实口径：** 仅当 `docs/STATUS.md` / `docs/TEST-PLAN.md` 明确写 **PASS** 时标为「已 empirically」。其余一律「文档草案 / 未 empirically」。候选补丁 `0001` = **NOT_VERIFIED**（反面参考），不得借本文件宣称已验证。  
> **契约来源：** `docs/DESIGN.md`、`docs/RELIABILITY.md`。具体 toxiproxy / docker / FS 命令由安装软件大师维护；此处只写高层注入意图。  
> **实验室拓扑（Phase1）：** 单节点 KRaft + 3 个 toxiproxy 前端（`19092–19094`）均指向同一 broker。禁用全部代理 = 客户端完全断开；真·3 broker / ISR / 滚动重启仍推迟。

各场景的「如何注入」保持高层意图。具体命令由安装软件大师补齐后再挂到对应行，本文件先不写 toxiproxy / docker / FS 操作细节。已归档的 FS-01、FS-09 照做步骤见 [DRILL-RUNBOOK.md](DRILL-RUNBOOK.md)；拓扑见 [KAFKA-DEPLOY.md](KAFKA-DEPLOY.md)。

`docs/STATUS.md` 记录的 L-16 证据目录是 `reports/l16-abc-20260925-215439/`。下文在该目录下另列 `l16a/`、`l16b/`、`l16c/`，与 STATUS 同一次运行，不另造证据根路径。验收门一律是 `scripts/verify_event_ids.py`。

---

## 覆盖图例

| 标记 | 含义 |
|------|------|
| **已 empirically（PASS）** | STATUS/TEST-PLAN 已写 PASS，并有 `reports/…` 证据 |
| **文档草案 / 未 empirically** | TEST-PLAN 为 PLANNED，或 STATUS 写明「L-03–L-15 未完整跑过」 |
| **部分相关已 empirically** | 场景意图与某次 PASS 运行重叠，但未作为独立 L-xx 门禁跑完 |

**通用验收脚本：** `scripts/verify_event_ids.py reports/<run-id>/`  
产物至少含 `injected_ids.txt`、`consumed_ids.txt`（头 `x-fs-event-id`）；可选 `rejected_ids.txt`、`outbox_snapshot.csv`。  
退出码：`0` VERIFY_OK；`1` 集合不匹配；`2` 同通话顺序失败；`3` 产物损坏。

---

## 场景总表

### FS-01 整集群断开（客户端视角完全断连）

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-01 / 整集群断开（toxiproxy 全切） |
| **如何注入** | 高层：toxiproxy 禁用全部前端 `kafka1/kafka2/kafka3`（切断时长建议 > `message-timeout-ms`，短切约 35s）；恢复时全部重新启用。**不** `reload mod_event_kafka`、不重启 FS。拨测在切断前与切断中各注入一批事件。 |
| **预期模块行为** | FS 回调仅深拷贝入队，不在网络/磁盘上阻塞；worker 写入 SQLite outbox 后 produce；poll 线程持续 `rd_kafka_poll`（**不**跨 `rd_kafka_poll` 持 `rk_mu_`）；未 ACK 行留 pending/重试；恢复后 outbox 排空；稳定 `event_id` 经头 `x-fs-event-id` 重放；同通话按 `call_uuid`+`created_at_ms` FIFO。 |
| **验收门** | `scripts/verify_event_ids.py`：`set(injected)-set(consumed_dedup)-rejected==∅`（允许写明的 pre-COMMIT 丢失）；排空后 `pending+in_flight==0`；**reload 前**采集即通过（自愈门）；报告 `dupes_in_consume`。 |
| **L-xx 覆盖** | **已 empirically：** **L-02=PASS**、**L-07=PASS**（稳定架 + FS Phase2）；短切亦覆盖 **L-16a=PASS**。 |
| **证据路径** | `reports/l02-l07-run1790313218/`（稳定架）；`reports/l02-l07-fs-20260925-133618/`（FS，VERIFY_OK 60/60）；`reports/l16-abc-20260925-215439/l16a/`（短切 35s，60/60） |

---

### FS-02 单个 bootstrap 入口禁用

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-02 / 单 bootstrap 入口禁用 |
| **如何注入** | 高层：仅禁用 toxiproxy 三前端之一（另两路仍通）。真·「单 broker 宕机」需 3 broker 拓扑（当前 Phase1 三前端同源，禁用一路通常仍可达）。 |
| **预期模块行为** | 客户端应经剩余 bootstrap 继续 produce；无持续 `rejected_*`；瞬时失败走退避/重试；回调仍不阻塞。 |
| **验收门** | `verify_event_ids.py` 完整性；指标上 `produce_ok` 继续、无长期积压；对比「禁用一路 vs 全切」差异。 |
| **L-xx 覆盖** | **文档草案 / 未 empirically。** 最接近 **L-04**（单 broker 宕机，PLANNED）。Phase1 三前端同源，不能替代真 ISR 测试。 |
| **证据路径** | （无独立 PASS 报告） |

---

### FS-03 抖动 / 延迟（latency / jitter）

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-03 / 网络延迟与抖动 |
| **如何注入** | 高层：toxiproxy latency / jitter toxic（可叠加限速）；持续拨测加压。 |
| **预期模块行为** | 瞬时失败保留 outbox 行 + 指数退避（含抖动）；内存队列仅在容量处拒绝（`rejected_mem_full` + WARNING）；**绝不**阻塞媒体/回调线程；线程与内存稳定。 |
| **验收门** | `verify_event_ids.py` 最终集合完整（去重后）；加压期间无 FS 卡死；拒绝须有 `rejected_ids.txt`/日志对应，禁止静默丢。 |
| **L-xx 覆盖** | **文档草案 / 未 empirically。** 对应 **L-12**、**L-03**（均为 PLANNED）。 |
| **证据路径** | （无） |

---

### FS-04 TCP RST / reset-peer

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-04 / 连接 RST |
| **如何注入** | 高层：toxiproxy `reset_peer`（或全部被代理端口上的 RST）；produce/poll 期间注入。 |
| **预期模块行为** | 视为瞬时故障：行保留、退避重试；路径恢复后**无需 reload** 即可排空；可能出现 ACK 窗口重复 → 消费端按 `x-fs-event-id` 去重。 |
| **验收门** | `verify_event_ids.py`；reload 前自愈；报告 `dupes_in_consume`（去重后不得残留重复判定失败）。 |
| **L-xx 覆盖** | **文档草案 / 未 empirically。** 对应 **L-10**、**L-03**（PLANNED）。与 FS-01「全 disable」不同，RST 为连接级重置。 |
| **证据路径** | （无） |

---

### FS-05 黑洞超时（blackhole / 全丢包）

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-05 / 网络黑洞 > `message-timeout-ms` |
| **如何注入** | 高层：toxiproxy timeout / 全丢 toxic，时长 **>** `message-timeout-ms`（默认主题侧 30s）；再恢复。区别于「disable 代理」（后者更接近连接拒绝）。 |
| **预期模块行为** | 行保持 pending/重试（未超 TTL）；恢复后旧到点行排空；**新**事件仍可入队并发送；回调不阻塞。若切断 **>** `outbox-ttl-ms`，见 FS-09。 |
| **验收门** | `verify_event_ids.py`（未过期集合完整）；指标 `delivery_fail` 可升、`outbox` 终态合理；reload 前自愈。 |
| **L-xx 覆盖** | **部分相关已 empirically：** 全代理 disable 35s（>30s timeout）已在 **L-02/L-07** 验证，但正式 **L-11**（blackhole toxic）仍为 **PLANNED / 未 empirically**。 |
| **证据路径** | 相关短切：`reports/l02-l07-fs-20260925-133618/`；L-11 专用：（无） |

---

### FS-06 Broker 重启 / 进程崩溃

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-06 / Broker 重启或 SIGKILL |
| **如何注入** | 高层：停止/SIGKILL Kafka 容器或进程后拉起；或滚动重启（一次一个 broker，需真多 broker）。客户端 bootstrap 仍只走代理。 |
| **预期模块行为** | 与瞬时故障相同：已 COMMIT 的 outbox 不永久丢失；自动重试排空；无需 reload；librdkafka 内存队列可丢，但磁盘 outbox 为事实来源。 |
| **验收门** | `verify_event_ids.py`；排空后 pending/in_flight=0；滚动场景下已提交行无永久缺失。 |
| **L-xx 覆盖** | **部分相关已 empirically：** 客户端侧「全断开再恢复」由 **L-02/L-07** 覆盖。正式 **L-08**（滚动）、**L-09**（broker SIGKILL）= **PLANNED / 未 empirically**（真 3 broker 推迟）。 |
| **证据路径** | 相关：`reports/l02-l07-run1790313218/`、`reports/l02-l07-fs-20260925-133618/`；L-08/L-09：（无） |

---

### FS-07 内存队列满（queue full）

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-07 / `mem-queue-max` 背压 |
| **如何注入** | 高层：将 `mem-queue-max` 调小；在 broker 不可达或 worker 缓慢时高速灌事件，使有界队列溢出。 |
| **预期模块行为** | `try_push` 失败 → `rejected_mem_full` + WARNING；**显式拒绝、不静默丢**；**不**阻塞 FS 媒体/回调线程；已入队/已 outbox 行仍按至少一次路径处理。 |
| **验收门** | 每个拒绝 ID 出现在 `rejected_ids.txt`/日志；`verify_event_ids.py`：`missing` 不计已拒绝集合；FS 拨测无卡死。 |
| **L-xx 覆盖** | **文档草案 / 未 empirically。** 对应 **L-05**（PLANNED）。单元侧有界队列有 U-Q-*，不替代本实验室门。 |
| **证据路径** | （无） |

---

### FS-08 磁盘 / outbox 满

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-08 / outbox 行数或字节上限 / INSERT 失败 |
| **如何注入** | 高层：压低 `outbox-max-rows` / `outbox-max-bytes`，或使 `outbox-path` 不可写 / 磁盘满，迫使 `insert_pending` 失败。 |
| **预期模块行为** | 拒绝 + `rejected_disk_full`（或打开失败告警）；不崩溃；不静默丢；不错误 ACK；pending 存活行不为腾空间被删（过期 dead 回收规则见 RELIABILITY）。 |
| **验收门** | 拒绝可追溯；进程存活；`verify_event_ids.py` 对**已接受** ID 仍完整；指标/日志可见。 |
| **L-xx 覆盖** | **文档草案 / 未 empirically。** 对应 **L-13**（PLANNED）。单元 U-DSK-02/03 有 CODED/PASS，非 FS 实验室。 |
| **证据路径** | （无实验室 PASS） |

---

### FS-09 超过 Outbox TTL（beyond TTL）

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-09 / pending 超过 `outbox-ttl-ms` |
| **如何注入** | 高层：配置 `outbox-ttl-ms=120000`（或实验值）；toxiproxy **全切时长 > TTL**（如 150s）；切断前与切断中注入；恢复后再拨新呼叫。对照：短切 **≤ TTL** 应完整送达。 |
| **预期模块行为** | `now_ms - created_at_ms > outbox-ttl-ms` 的 **pending** → `state=dead`、`last_error=expired_ttl`、计 `outbox_expired`，**不投递**；in-flight 不就地过期；恢复后 dead 不进 `fetch_due`，新事件优先；短切不触发过期，走自愈排空。 |
| **验收门** | 短切：`verify_event_ids.py` 全量 OK。长切：过期 ID **不得**出现在 topic（`expired_still_in_topic=0`）；未过期子集应匹配；outbox 可见 `dead/expired_ttl`；自愈后新注入集合 VERIFY_OK。 |
| **L-xx 覆盖** | **已 empirically：** **L-16a=PASS**（短切 35s，60/60）、**L-16b=PASS**（150s>TTL，matched=30/60，expired_ttl，expired_still_in_topic=0）、**L-16c=PASS**（自愈后 30/30）。注：L-16a/b/c 写在 STATUS 证据中，**尚未作为 TEST-PLAN §3.2 矩阵正式行**。 |
| **证据路径** | `reports/l16-abc-20260925-215439/`（子目录 `l16a/`、`l16b/`、`l16c/`）；模块 `outbox-ttl-ms=120000`（master `37e89154`） |

---

### FS-10 进程杀死与恢复（FS / 模块 SIGKILL）

| 字段 | 内容 |
|------|------|
| **场景 ID / 名称** | FS-10 / FS 或模块进程杀死后恢复 |
| **如何注入** | 高层：中断期间对 FS/模块进程 SIGKILL（或 unload）；再拉起 FS / `load mod_event_kafka`；broker 随后恢复。可叠加 outage。 |
| **预期模块行为** | 已 COMMIT 未 ACK 行留在磁盘；启动 `requeue_in_flight`；以**同一** `event_id` / `x-fs-event-id` 重放；ACK 与本地 DELETE 之间崩溃可导致重复 → 消费端去重；COMMIT 前仅内存队列窗口可能丢（RELIABILITY 丢失窗口）。关闭顺序：停入队 → 内存排入 outbox → flush/poll → join。 |
| **验收门** | 重启前后 outbox 快照对比；`verify_event_ids.py` 对已持久化 ID 至少一次；重复仅允许在去重语义下。 |
| **L-xx 覆盖** | **文档草案 / 未 empirically。** 对应 **L-14**、**L-06**（均为 PLANNED）。**不得**用 L-02「未 reload 自愈」冒充本场景。 |
| **证据路径** | （无） |

---

## TEST-PLAN 中已规划但未完整跑过的 L-xx

摘自 master `docs/TEST-PLAN.md` §3.2 与 `docs/STATUS.md`「未宣称：L-03–L-15 未完整跑过」：

| ID | 故障摘要 | TEST-PLAN 状态 |
|----|----------|----------------|
| **L-01** | 正常路径全量 event_id 各一次 | PLANNED |
| **L-03** | toxiproxy reset-peer / 延迟 | PLANNED |
| **L-04** | 单 broker 宕机（ISR 仍可） | PLANNED |
| **L-05** | `mem-queue-max` 压力 | PLANNED |
| **L-06** | 中断中卸载/重新加载模块 | PLANNED |
| **L-08** | 滚动重启（需真多 broker） | PLANNED |
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
| **L-02** | PASS（稳定架 + FS Phase2） | `reports/l02-l07-run1790313218/`、`reports/l02-l07-fs-20260925-133618/` |
| **L-07** | PASS（与 L-02 同次；reload 前自愈） | 同上 |
| **L-16a/b/c** | PASS（FS+TTL；STATUS 证据；非正式 TEST-PLAN 矩阵行） | `reports/l16-abc-20260925-215439/` |

---

## 场景 ↔ L-xx 速查

| 架构师场景 | 主 L-xx | 覆盖诚实标记 |
|------------|---------|--------------|
| 整集群断开 | L-02 / L-07（+L-16a 短切） | 已 empirically |
| 单 bootstrap 入口禁用 | L-04（近） | 文档草案 / 未 empirically |
| 抖动/延迟 | L-12 / L-03 | 文档草案 / 未 empirically |
| RST | L-10 / L-03 | 文档草案 / 未 empirically |
| 黑洞超时 | L-11（相关：L-02 全切） | L-11 未跑；L-02 部分相关 |
| Broker 重启 | L-08 / L-09（相关：L-02） | L-08/09 未跑；L-02 部分相关 |
| 队列满 | L-05 | 文档草案 / 未 empirically |
| 磁盘/outbox 满 | L-13 | 文档草案 / 未 empirically |
| 超过 TTL | L-16a/b/c | 已 empirically（STATUS） |
| 进程杀死恢复 | L-14 / L-06 | 文档草案 / 未 empirically |

---

## 维护说明

1. 新跑通场景：更新本文件对应行的「L-xx 覆盖」与「证据路径」，并同步 `docs/STATUS.md` / `docs/TEST-PLAN.md` 状态列。  
2. 通过规则以 **event_id 集合** 为准，禁止仅凭「无错误日志」宣称 PASS。  
3. Phase1 单 broker + 三前端同源：FS-01 有效；FS-02/L-04/L-08 需真多 broker 后方可宣称。  
4. 建议后续把 **L-16a/b/c** 正式写入 TEST-PLAN §3.2，避免仅存在于 STATUS。
