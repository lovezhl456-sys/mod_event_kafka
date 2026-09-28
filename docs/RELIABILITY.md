# 可靠性边界

## 保证（目标设计）
- 对已成功提交到 SQLite outbox、且未被 `outbox-ttl-ms` 过期的事件，提供**至少一次**投递。
- 稳定的 **event_id**（UUIDv4）在入队时分配；重试 produce 复用同一 id（流水线附加头时为 `x-fs-event-id`）。
- 同一通话的顺序：outbox 的 `fetch_due` 先按 `call_uuid`、再按 `created_at_ms` 排序。

## 丢失窗口
- FS 回调返回**之后**、outbox `INSERT` 提交**之前**崩溃 → 事件可能丢失（只在内存队列中）。
- 容量耗尽（内存或磁盘）→ **显式拒绝** + 指标/日志；绝不静默丢弃。

## 过期事件（outbox TTL）
- `outbox-ttl-ms` 默认 `120000`（2 分钟）。`0` 关闭过期，并保持原先行为（不按年龄丢弃）。
- worker 投递之前，满足 `now_ms - created_at_ms > outbox-ttl-ms` 的 **pending** 行被设为 `state=dead`、`last_error=expired_ttl`，并计入 `outbox_expired`。该行不会被投递。
- 这一丢弃是有意的：长时间中断之后，负载已是过期的话务状态，而不是必须在数小时后仍要送达的记录。
- **≤ TTL** 的短时中断不会使行过期。这些行留在既有的自愈并排空路径上，无需 `reload mod_event_kafka` 即可完整送达。
- in-flight 行不会就地过期。投递成功仍删除该行（ACK）。失败投递在 TTL 之后把行退回 `pending` 时，下一轮将其过期，不再投递。
- 恢复之后，dead/已过期行不属于到点扫描（fetch_due），因此新入队的事件会先于该死信被投递。已过期行仅在会阻塞新插入时被回收。pending 与 in-flight 行绝不会为腾出空间而被删除。

## 重复
- broker ACK 之后、本地 `DELETE` 之前崩溃 → 重启后重复。消费端**必须**按 `event_id` 去重。
- 生产者重建，或在结果不明确的失败之后重试，也可能重复。

## 关闭
1. 停止接受入队  
2. 将内存队列排入 outbox  
3. `rd_kafka_flush` + poll，直到空闲或超时  
4. Join worker/poll 线程  
5. 销毁 producer  
未 ACK 的行留在磁盘上，供下次加载（启动时 `requeue_in_flight`）。
