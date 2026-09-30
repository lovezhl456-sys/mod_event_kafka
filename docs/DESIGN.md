# mod_event_kafka Outbox 可靠性设计

上游：voiceip/mod_event_kafka@master  
目标：云端 Kafka 重启或抖动之后，已持久化的事件能自动重发，无需重启 FreeSWITCH，也无需 `reload mod_event_kafka`。

## 为什么候选补丁不够

草案 `0001-kafka-restart-resilience.patch` 增加了后台 poll 与生产者重建，并去掉了分离的重试线程。这有助于重连后继续推进，但**重建会丢弃 librdkafka 的内存队列**，因此中断期间已接收的事件仍会丢失。此外，它在背压下仍从 FreeSWITCH 回调路径投递。该补丁仅作**反面参考**，不会原样合入。

## 架构

```
FS event_handler
    │  （除有界入队外，绝不阻塞在网络 / 磁盘同步上）
    ▼
有界内存队列（深拷贝的 payload + key + event_id）
    │
    ▼
固定数量的 worker 线程（一个或多个）
    │  1) BEGIN; INSERT outbox (pending); COMMIT   ← 持久化
    │  1b) 若 outbox-ttl-ms > 0，把超过 TTL 的 pending 行置为过期（dead / expired_ttl），不投递
    │  2) rd_kafka_produce（拷贝 / 自有缓冲区）
    │  3) 标记 state=in_flight
    ▼
后台 poll 线程  →  dr_msg_cb
    │  成功 → BEGIN; DELETE/ACK outbox; COMMIT
    │  可重试失败 → state=pending，退避
    │  致命错误 → 安全重建 producer，保留 outbox 行
```

### 线程

| 线程 | 职责 |
|--------|------|
| FS 回调 | 序列化为 JSON，深拷贝 key，分配稳定的 `event_id`，尝试入队；队列满时递增拒绝计数并打 WARNING 日志（绝不静默丢弃） |
| worker | 把内存队列取空 → 写入 SQLite outbox → produce；同时扫描已到点（due）的 pending/重试行 |
| poll | 持续调用 `rd_kafka_poll`，驱动投递报告与重连 |

### 所有权

- 入队项持有 JSON 正文与 key 的堆上副本，直到 produce 成功为止；其间的所有权转移**不**采用发后即忘式的 detach。
- outbox 插入完成后，优先使用带显式拷贝标志（`RD_KAFKA_MSG_F_COPY`）的 `rd_kafka_producev`，使 outbox 始终作为事实来源，并避免 `MSG_F_FREE` 引起的释放后使用（UAF）。
- **禁止**使用分离的重试线程。

### Outbox 表结构（SQLite）

```sql
CREATE TABLE outbox (
  event_id TEXT PRIMARY KEY,
  topic TEXT NOT NULL,
  msg_key TEXT,
  payload BLOB NOT NULL,
  state TEXT NOT NULL,           -- pending | in_flight | dead
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at_ms INTEGER NOT NULL,
  last_error TEXT,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  call_uuid TEXT                  -- 可选，用于保证同一通话内的顺序
);
CREATE INDEX idx_outbox_due ON outbox(state, next_attempt_at_ms);
CREATE INDEX idx_outbox_call ON outbox(call_uuid, created_at_ms);
```

只有在投递成功**且**本地 ACK 事务提交之后，才删除该行。

### 错误分类

| 类别 | 示例 | 处理 |
|-------|----------|--------|
| 瞬时 | 断连、QUEUE_FULL、传输超时 | 保留该行，指数退避（加抖动），记录指标 |
| 客户端致命 | `RD_KAFKA_RESP_ERR__FATAL` | 在互斥锁保护下销毁并重建 producer+topic；保留 outbox 行 |
| 永久 | 认证失败、topic 授权失败、非法消息 | `state`=`dead`，证据留在 `last_error`，记告警指标；不陷入紧循环 |
| 过期 | pending 行年龄 `> outbox-ttl-ms` | `state`=`dead`，`last_error=expired_ttl`，指标 `outbox_expired`；不投递。这是有意丢弃已过时的话务状态。`0` 表示关闭 |

### 配置兼容性

原有键名保持不变：`bootstrap-servers`、`topic`、`topic-prefix`、`username`、`password`、`buffer-size`、`compression`、`event-filter`。

新增项（默认值均安全）：

| 参数 | 默认值 | 含义 |
|-------|---------|---------|
| `outbox-path` | `/var/lib/freeswitch/event_kafka_outbox.db` | SQLite 路径 |
| `mem-queue-max` | `10000` | 有界内存队列深度 |
| `outbox-max-rows` | `100000` | 磁盘容量；写满时拒绝并告警 |
| `outbox-max-bytes` | `512MB` | 磁盘软预算 |
| `worker-poll-ms` | `50` | worker 空闲时的休眠间隔 |
| `rdkafka-poll-ms` | `100` | poll 线程间隔 |
| `message-timeout-ms` | `30000` | **必须**通过 topic 配置才能生效（上游缺陷） |
| `enable-idempotence` | `true` | `enable.idempotence=true` |
| `security-protocol` | 自动；设置了用户名时为 `SASL_PLAINTEXT` | 可设为 `SASL_SSL`/`SSL` |
| `ssl-ca-location` | 空 | TLS |
| `acks` | `all` | 持久性 |
| `outbox-ttl-ms` | `120000` | 满足 `now_ms - created_at_ms > outbox-ttl-ms` 的 pending 行标为 `dead`（`expired_ttl`），不投递。`0` 表示关闭。中断时长 **≤ TTL** 时仍会自愈并排空，无需 reload |

消息 **key** 仍在存在 `Channel-Call-UUID` 时取该值（行为不变）。  
稳定的 **event_id** 是入队时生成的 UUIDv4，存入 outbox，并通过 Kafka 头 `x-fs-event-id` 随消息发送（为保持兼容，正文 JSON 格式不变）。消费端按该头去重。

### 可靠性边界（明确声明）

1. **outbox COMMIT 之前崩溃**：事件可能丢失（此时仅存于内存）。窗口 = 入队 → COMMIT。
2. **produce 结果不明确 / broker ACK 之后、本地 DELETE 之前崩溃**：重启后可能**重复**；消费端必须按 `x-fs-event-id` 去重。
3. **同一通话内的顺序**：有 key 时，worker 对每个 `call_uuid` 按 `created_at_ms` 做 FIFO 发送；跨通话不保证顺序。语义为至少一次 + 按头去重。
4. **容量耗尽**：拒绝新的入队，递增 `event_kafka_rejected_total`，以 WARNING/CRIT 记录；绝不静默丢弃，也绝不无限期阻塞媒体线程。
5. **关闭**：解绑事件 → 停止接受入队 → worker 把内存队列中的事件写入 outbox → flush/poll 直到空闲或超时 → join 线程 → 销毁 producer。未 ACK 的行留在磁盘上，供下次加载时处理。
6. **Outbox TTL**：过期只作用于超过 `outbox-ttl-ms` 的 `pending` 行。in-flight 行照常 ACK（删除）或重试；重试退回 pending 时若已超过 TTL，则在下一轮被置为过期，不再投递。未过期的行仍遵循同一条持久化 ACK 规则。已过期的死信不在到点扫描（fetch_due）范围内，因此新的 pending 工作优先投递；若它们会占满 outbox，则将其删除，以免新插入被拒绝。永久性 `dead` 行与仍存活的行不会为腾出空间而被删除。

### 指标（日志 + 可选的 JSON 文件计数）

- `enqueued`、`rejected_mem_full`、`rejected_disk_full`
- `outbox_pending`、`outbox_in_flight`、`outbox_dead`
- `produce_ok`、`produce_fail`、`delivery_fail`、`producer_rebuilds`
- `outbox_expired`（被标为 `expired_ttl` 的 pending 行）
- `oldest_pending_age_ms`

## 测试计划摘要

见 `docs/TEST-PLAN.md`。本机必须跑单元测试以及 ASan/UBSan；完整的 FS + 3 broker + 故障代理实验室需要 Docker/Podman（本机尚未安装），或使用安装软件大师提供的外部实验室主机。

## 交付物布局

```
mod_event_kafka.cpp   FS 回调粘合层（仅做深拷贝入队）
include/              有界队列、outbox、流水线头文件
src/                  kafka_outbox.cpp、kafka_pipeline.cpp
docs/DESIGN.md        本文件
docs/TEST-PLAN.md
docs/RELIABILITY.md   丢失/重复/顺序边界
docs/DEPLOY-ROLLBACK.md
tests/                单元测试（不依赖 FS）；CMakeLists.txt 位于仓库根目录
lab/                  compose + toxiproxy + 拨测示例配置
scripts/              verify_event_ids.py
reports/              真实运行后填写；未测项必须显式标出
```
