# Kafka 故障场景

本页对照 [DESIGN.md](DESIGN.md) 的 outbox 与 `outbox-ttl-ms`，列出实验室里已经跑过的断连，以及尚未 empirically 执行的场景。状态名与 [TEST-PLAN.md](TEST-PLAN.md)、[STATUS.md](STATUS.md) 一致。

**未经生产验证。** 下面凡是写 PASS 的，都是 2026-09-25 实验室记录。候选补丁 `0001-kafka-restart-resilience.patch` 保持 **NOT_VERIFIED**，只作反面参考：它会在重建 producer 时丢掉 librdkafka 内存队列，不能用来给任何一行打 PASS。

实验室客户端看到的「整集群断连」是 toxiproxy 把 `kafka1`/`kafka2`/`kafka3` 全部 `enabled=false`。单个 Kafka 进程可以仍在运行。这和杀掉三个独立 broker 不是同一次操作。Phase1 只有一个 KRaft 进程，见 [KAFKA-DEPLOY.md](KAFKA-DEPLOY.md)。

预期行为的共同规则（DESIGN）：

- 已 `COMMIT` 进 SQLite、且年龄未超过 `outbox-ttl-ms`（默认 `120000`）的行：瞬时失败则保留，指数退避后重试，恢复后排空，无需 `reload mod_event_kafka`。
- `now_ms - created_at_ms > outbox-ttl-ms` 的 **pending** 行：`state=dead`，`last_error=expired_ttl`，计入 `outbox_expired`，不投递。
- in-flight 行不就地过期。投递成功则删除；失败退回 pending 后，若已超过 TTL，下一轮再标 `expired_ttl`。
- 永久失败（认证、主题授权、非法消息）：`state=dead`，`last_error` 留证据，不进入紧重试循环。这和 `expired_ttl` 不是同一类。
- `COMMIT` 之前只在内存里的事件，进程崩溃可以丢失。broker ACK 之后、本地删除之前崩溃，可能重复；消费端按头 `x-fs-event-id` 去重。

## 覆盖一览

| 场景 | 实验室结论 | 用例 | 证据 |
|------|------------|------|------|
| 整集群断连，短断约 35s | 已跑，PASS | L-02、L-07、L-16a | 见下节 |
| 超 TTL 长断约 150s | 已跑，PASS | L-16b | `reports/l16-abc-20260925-215439/` |
| 恢复后的新呼叫 | 已跑，PASS | L-16c | 同上 |
| 超 TTL 过期丢弃 | 已跑，PASS | L-16b | 同上 |
| 单入口 disable | **文档草案 / 未 empirically** | 接近但不等价于 L-04（PLANNED） | 无 |
| 抖动 / 延迟 toxics | **文档草案 / 未 empirically** | L-03、L-12（PLANNED） | 无 |
| TCP RST / reset-peer | **文档草案 / 未 empirically** | L-03、L-10（PLANNED） | 无 |
| 网络黑洞 > `message-timeout-ms` | **文档草案 / 未 empirically** | L-11（PLANNED） | 无 |
| Broker 滚动重启 / SIGKILL | **文档草案 / 未 empirically** | L-08、L-09（PLANNED） | 无 |
| 内存队列打满 `mem-queue-max` | **文档草案 / 未 empirically** | L-05（PLANNED） | 无 |
| 磁盘 / outbox 满 | **文档草案 / 未 empirically** | L-13（PLANNED） | 无 |
| 模块或进程杀掉后恢复 | **文档草案 / 未 empirically** | L-06、L-14（PLANNED） | 无 |
| 永久性 produce 错误 → dead | **文档草案 / 未 empirically** | L-15（PLANNED） | 无 |

L-03 至 L-15 在 STATUS 里记为未完整跑过。单元测试里相关的行（例如队列容量、磁盘字节上限、`dead` 标记）不能代替上表的实验室用例。

---

## 已有实验室记录

### 整集群断连（toxiproxy 全 disable），短断约 35s

**现象。** 三个代理一起禁用约 35 秒，然后重新启用。35 秒大于 `message-timeout-ms` 默认 `30000`，小于 `outbox-ttl-ms` 默认 `120000`。Kafka 进程保持运行。拨测不 reload 模块。

**预期模块行为。** 断连属于瞬时错误：已提交的行留在 outbox，退避重试。恢复后在 TTL 内排空。未 ACK 的行以同一个 event_id 再投递。消费端用 `x-fs-event-id` 去重。不需要 `reload mod_event_kafka`。

**覆盖。**

- FS Phase2：**L-02=PASS**，**L-07=PASS**。拨号 `dialtest_originate.sh`，loopback/park，切断前 ×5、切断期间 ×5。`buffer-size=100000`，主题 `fs_events`，bootstrap 仅 toxiproxy。`verify_event_ids.py` VERIFY_OK，注入 60、匹配 60、missing=0。`module_exists=true`。park 路径可能没有 ANSWER，事件以 CREATE/HANGUP* 为主。
- 同日夜间 TTL 配置下的短断：**L-16a=PASS**（`verify_rc=0`）。模块 `outbox-ttl-ms=120000`。注入 60，匹配 60。拨号脚本是 `dialtest_fast.sh`。
- 另有一条稳定 `KafkaPipeline` 测试架记录（不是 FreeSWITCH `.so`）：主题 `fs_events_l02b`，同样禁用三个代理 35 秒，不停止流水线。reload 前 enqueued=60、produce_ok=90、acked=60、delivery_fail=30、producer_rebuilds=0，outbox pending/in_flight/dead=0，`healed_without_reload=1`。`verify_event_ids.py` 退出码 0。TEST-PLAN 里 L-02/L-07 的 PASS 注记指向这次测试架。

**证据。**

- FS 模块：`reports/l02-l07-fs-20260925-133618/`
- FS + TTL 短断（L-16a）：`reports/l16-abc-20260925-215439/`
- 测试架（非 `.so`）：`reports/l02-l07-run1790313218/`

### 超 TTL 长断约 150s

**现象。** `toxiproxy_cut_restore.sh 150`（默认时长就是 150 秒）把三个代理全部禁用，长于 `outbox-ttl-ms=120000`。恢复后检查 outbox 与主题。

**预期模块行为。** 切断期间已提交、并且到恢复时年龄已超过 TTL 的 pending 行标为 `dead` / `expired_ttl`，不 produce。尚未超过 TTL 的行仍走自愈排空。过期死信不在 `fetch_due` 里，恢复后的新工作优先。

**覆盖。** **L-16b=PASS**。

**证据。** `reports/l16-abc-20260925-215439/`

- 过期行：`dead` / `expired_ttl`。注入期间与已老化集合里 expired_ttl dead=30
- `expired_still_in_topic=0`（这些过期 event_id 不在主题上）
- `consumed_matched=30 / 60`：未过期的那一部分已送达

这次 PASS 不是「60 条全部出现在主题上」。对**整份**注入集合跑 `verify_event_ids.py` 时，过期 id 会算作 missing，除非把它们单独列入拒绝/过期清单。主题侧的验收是过期 id 不在主题上，未过期 id 在主题上。

### 恢复后新呼叫

**现象。** 长断恢复、代理重新 `enabled=true` 之后，再发起新的 loopback 呼叫。

**预期模块行为。** 新事件照常入队、写入 outbox、投递。过期死信不挡住新的 pending。不需要 reload。

**覆盖。** **L-16c=PASS**（`verify_rc=0`）。新呼叫 VERIFY_OK 30/30。

**证据。** `reports/l16-abc-20260925-215439/`（与 L-16a/L-16b 同一目录）。拨号辅助脚本：`/workspace/lab-mod-event-kafka/dialtest_fast.sh`。

### 超 TTL 过期丢弃

**现象。** pending 行的年龄超过 `outbox-ttl-ms` 后仍未投递成功。实验室里对应的触发方式就是上一节的约 150 秒全断，不是单独一条「只改 TTL、不断网络」的用例。

**预期模块行为。** `state=dead`，`last_error=expired_ttl`，指标 `outbox_expired` 增加，该行不进入 produce。`outbox-ttl-ms=0` 时关闭这条规则。in-flight 不在本规则里直接改成过期。这是有意丢掉过期话务状态。永久性 `dead`（认证等）不会为了腾出空间被删掉；`expired_ttl` 行只在会堵住新插入时被回收。

**覆盖。** **L-16b=PASS**（见上）。证据目录 `reports/l16-abc-20260925-215439/`：expired_ttl dead=30，`expired_still_in_topic=0`。

---

## 文档草案 / 未 empirically

下列场景只写设计预期和 TEST-PLAN 里的计划编号。没有实验室 PASS，也没有证据目录。执行前以 [TEST-PLAN.md](TEST-PLAN.md) 的 PLANNED 行为准，跑完再改 STATUS，不要把本页草案写成已验证。

### 单入口 disable（kafka1 / kafka2 / kafka3 之一）

**现象（草案）。** 只把一个代理设为 `enabled=false`，例如 `toxiproxy_cut_restore.sh 35 kafka1`。另外两个端口仍转发到同一个 `kafka-1:9094`。

**预期模块行为。** 客户端仍可通过其余 bootstrap 地址到达唯一的 broker。个别连接失败按瞬时错误退避；已提交的行保留。不应当出现「三个入口一起断开」时的整段停顿。

**覆盖。** **文档草案 / 未 empirically。** TEST-PLAN **L-04**（单个 broker 宕机且 ISR 仍满足，produce 继续）状态是 **PLANNED**。L-04 需要真实多 broker。本实验室禁用一个 toxiproxy 名字时，上游仍是同一个进程，不能当作 L-04 已覆盖。

**证据。** 无。

### 抖动 / 延迟 toxics

**现象（草案）。** 在 toxiproxy 上对 `kafka1`/`kafka2`/`kafka3` 添加 latency 等 toxic，或反复开关。当前仓库里的 `lab/toxiproxy.json` 只有三条启用的代理，没有预置 toxic。

**预期模块行为。** 传输变慢或抖动属于瞬时类：行留在 outbox，退避，最终 ACK。重复只出现在「broker 已 ACK、本地尚未删除」的窗口，消费端按 `x-fs-event-id` 去重。拒绝只发生在 `mem-queue-max` 或 outbox 容量打满时，媒体线程不被网络堵住。超过 TTL 的 pending 仍按 `expired_ttl` 丢弃。

**覆盖。** **文档草案 / 未 empirically。** **L-03**（produce 期间 reset-peer / 延迟）与 **L-12**（延迟 / 反复抖动）均为 **PLANNED**。

**证据。** 无。

### TCP RST / reset-peer

**现象（草案）。** toxiproxy `reset_peer`，或链路上对代理端口发 TCP RST。L-10 的范围是全部被代理的 broker 端口。

**预期模块行为。** 与断连一样保留 outbox 行并退避。路径恢复且行未过期时，无需 reload 即可排空。已超过 TTL 的 pending 改为 `dead` / `expired_ttl`，不投递。

**覆盖。** **文档草案 / 未 empirically。** **L-03**、**L-10** 均为 **PLANNED**。前面的 35 秒用例是代理 `enabled=false`，不是 RST。

**证据。** 无。

### 网络黑洞，时长大于 message-timeout-ms

**现象（草案）。** 数据面丢包、没有 RST（toxiproxy timeout / 黑洞），持续时长大于 `message-timeout-ms`（默认 `30000`）。短于 TTL 时，恢复后旧行和新事件都应还能送出。长于 TTL 时，过期规则叠加生效。

**预期模块行为。** TEST-PLAN L-11：行保持 pending 或重试；恢复后新事件仍入队并发送，到点的旧行排空。DESIGN 在此之上增加 TTL：pending 年龄超过 `outbox-ttl-ms` 则 `expired_ttl`，不再投递。

**覆盖。** **文档草案 / 未 empirically。** **L-11** 为 **PLANNED**。35 秒全 disable 已经长于 `message-timeout-ms`，但那是代理关闭，记录在 L-02/L-07/L-16a，不记成 L-11。

**证据。** 无。

### Broker 滚动重启 / SIGKILL

**现象（草案）。**

- 滚动重启：一次停一个 broker，bootstrap 里的每个地址都经过代理（L-08）。本实验室只有 `lab-kafka-1` 一个进程，做不到「停一个、其余继续服务」。
- SIGKILL：杀掉 broker 进程（L-09）。实验室里若 `docker kill lab-kafka-1`，唯一的上游消失，三个代理的上游一起不可用。

**预期模块行为。** 已提交的行保留在 outbox。进程重新起来且行未过期时自动重试，无需 reload。客户端致命错误（`RD_KAFKA_RESP_ERR__FATAL`）时在锁内重建 producer，outbox 仍是事实来源。超过 TTL 的 pending 标 `expired_ttl`。

**覆盖。** **文档草案 / 未 empirically。** **L-08**、**L-09** 均为 **PLANNED**。35 秒与 150 秒记录都是 toxiproxy 禁用，Kafka 进程保持运行。

**证据。** 无。

### 内存队列打满 mem-queue-max

**现象（草案）。** 入队速度超过 worker 取走速度，内存队列深度达到 `mem-queue-max`（默认 `10000`；实验室示例配置也是 `10000`）。`buffer-size`（实验室 FS 配置为 `100000`）是另一项 librdkafka 缓冲，不要和这个上限混用。

**预期模块行为。** `try` 入队失败时增加 `rejected_mem_full`（以及设计里的 `event_kafka_rejected_total`），打 WARNING，回调返回。媒体线程不等待网络或磁盘。被拒绝的事件没有 outbox 行，必须出现在拒绝清单里，不能只看指标。已经入队并 `COMMIT` 的行不受这次拒绝的影响。

**覆盖。** **文档草案 / 未 empirically。** **L-05** 为 **PLANNED**。有界队列的单元测试（U-Q-01 CODED、U-Q-02 PASS）只覆盖库，不是 FS 拨测。

**证据。** 无。

### 磁盘 / outbox 满

**现象（草案）。** `outbox-path` 所在磁盘写满，或行数达到 `outbox-max-rows`（默认 `100000`），或体积达到 `outbox-max-bytes`（默认 `512MB`）。实验室 outbox 文件路径是 `/usr/local/freeswitch/var/lib/freeswitch/db/event_kafka_outbox.db`。

**预期模块行为。** `insert_pending` 失败，走 `rejected_disk_full`，告警，不静默丢弃，进程不崩。已经在库里的行保持原样，不能被标成已 ACK。`outbox-path` 本身打不开时，打开失败要显式返回错误（U-DSK-01 仍是 PLANNED）。

**覆盖。** **文档草案 / 未 empirically。** **L-13** 为 **PLANNED**。单元侧 U-DSK-02（行数，CODED）和 U-DSK-03（字节上限，PASS）不是 FS 实验室记录。

**证据。** 无。

### 模块 / 进程杀掉后恢复（同一 event_id 重放）

**现象（草案）。** 中断过程中 `SIGKILL` FreeSWITCH 或模块进程，再启动；或者中断期间 `unload`/`load`（L-06）。

**预期模块行为。** 已经 `COMMIT` 的未 ACK 行留在 SQLite 里。下次加载时 `requeue_in_flight` 把 in-flight 改回 pending，用**同一个** event_id 再投递，头仍是 `x-fs-event-id`。`COMMIT` 之前的内存事件可以丢失。若 broker 已经 ACK 而本地删除没提交，消费端会看到重复，按该头去重。关闭顺序见 DESIGN：停止入队 → 内存排入 outbox → flush/poll → join → 销毁 producer。

**覆盖。** **文档草案 / 未 empirically。** **L-14**、**L-06** 均为 **PLANNED**。已有 PASS 的短断与长断都明确没有 reload，也没有杀进程。单元 U-REC-01/02（CODED）和 U-UNL-01/02（PASS）不是这条实验室用例。

**证据。** 无。

### 永久性 produce 错误 → dead

**现象（草案）。** 认证失败、ACL/主题授权失败或非法消息，librdkafka 返回永久性错误。与网络断开、超时不同。

**预期模块行为。** `state=dead`，`last_error` 留下错误文本，告警计数增加，不再紧循环重试。该行可以留在库里作证据。这不是 `expired_ttl`：过期行的 `last_error` 固定为 `expired_ttl`，由年龄触发，已由 L-16b 覆盖。

**覆盖。** **文档草案 / 未 empirically。** **L-15** 为 **PLANNED**。单元 U-RTY-03 对 `dead` 标记是 CODED，错误分类映射仍是 PLANNED，且没有 FS 拨测证据。

**证据。** 无。
