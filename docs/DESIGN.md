# 设计与配置

当前 master 使用 SQLite outbox。目标是让可重试故障后的已持久化、未过期事件继续投递；实际保证受 [可靠性边界](RELIABILITY.md) 和 [验证范围](FAULT-SCENARIOS.md) 限制。

## 数据流

```text
FS 事件 → JSON/key 深拷贝 → 有界内存队列
                              ↓ worker
                         SQLite pending
                              ↓ 标记 in_flight，再 producev(COPY)
                         Kafka delivery callback
                     成功删除 / 失败重试或 dead
```

- FS 回调只序列化和入队，不等待 Kafka 或 SQLite COMMIT。
- 一个 worker 负责落库、到期扫描、TTL 和发送；一个 poll 线程分发 delivery callback。
- 稳定 event_id 在入队时生成，通过 `x-fs-event-id` 消息头传递；重试沿用该 ID。
- key 来自 `Channel-Call-UUID`；缺字段时是 null key。compact topic 必须另行处理这一约束。
- outbox 表定义与事务实现以 [kafka_outbox.cpp](../src/kafka_outbox.cpp) 为准。

## 错误与终态

| 路径 | 当前处理 / 边界 |
|---|---|
| 普通 produce / delivery 失败 | 同一策略：未到 `max_attempts_before_dead`（默认 50）则 pending + 按尝试次数退避；到达上限则 dead |
| topic/cluster 授权、INVALID_MSG、消息过大 | `is_permanent_error` 中列为 dead |
| +87 INVALID_RECORD | 当前未列为永久错误；不能承诺自动修正非法记录 |
| fatal | 触发 producer 重建；安全性仍需专门并发测试，普通断连通过不代表此路径通过 |
| 已可投递且年龄超过 TTL 的 pending 头部或无分组行 | dead / `expired_ttl`。还排在头部后面的后续事件不过期。0 只关闭按年龄过期 |
| outbox INSERT 失败 | 递增 `rejected_disk_full`；当前还缺按事件 ID 的持久拒绝记录，不能宣称完整拒绝闭环 |

## FS XML 配置

原有键保留：`bootstrap-servers`、`topic`、`topic-prefix`、`username`、`password`、`buffer-size`、`compression`、`event-filter`。

| 新增键 | 默认值 | 含义 |
|---|---|---|
| outbox-path | `/var/lib/freeswitch/event_kafka_outbox.db` | FS 用户可写的 SQLite 文件 |
| mem-queue-max | 10000 | 内存队列条数 |
| outbox-max-rows | 100000 | outbox 行数上限 |
| outbox-ttl-ms | 120000 | 已可投递头部或无分组 pending 行的最大年龄；0 关闭按年龄过期，不关闭尝试次数上限 |
| message-timeout-ms | 30000 | 一次 librdkafka 投递生命周期上限，已传入 topic 配置 |
| enable-idempotence | 1 | producer 幂等开关，不等于跨重建/业务端恰好一次 |
| security-protocol | 自动 | 有用户名则 SASL_PLAINTEXT，否则 PLAINTEXT |
| ssl-ca-location | 空 | CA 路径 |

`outbox_max_bytes`、worker/poll 间隔目前只在 C++ PipelineConfig 中，**没有对应 XML 透传**；`acks=all` 由 core 设置。`api.version.*` 和 librdkafka `debug` 也没有 XML 透传。配置在构造 producer 时应用，仅 `reloadxml` 不会更新已创建的实例。

core 的 buffer_size 默认 100000；FS XML 解析默认 buffer-size 为 10，示例配置另有显式值。应记录运行时配置，不要混用不同层级的默认值。
