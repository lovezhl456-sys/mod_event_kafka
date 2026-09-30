# +87：已知复现与生产缺口

**结论：compact + null key 可以触发 +87，但不能单独解释云 Kafka 发布后几十分钟自动恢复。** 原始生产现象来自旧无 outbox 模块；当前 master 是不同实现。

## 错误码不要混用

| 数值 | 含义 | 可说明什么 |
|---|---|---|
| +87 `INVALID_RECORD` | broker 校验记录失败 | 需要 broker 具体拒收原因，不能唯一确定 compact |
| -187 `ALL_BROKERS_DOWN` | 客户端认为所有 broker 不可用 | 连接/可达性故障 |
| -184 `QUEUE_FULL` | 本地队列满 | 消息尚未成功入队 |
| +32 `INVALID_TIMESTAMP` | 时间戳不被接受 | Kafka 3.8.1 的标准时间戳范围校验路径 |

旧 librdkafka 不认识 +87 时可能打印 `Err-87?`，连字符不是该码的负号。收紧时间戳范围的原 R3 未执行；不能把它当作已知 +87 注入方法。

## 已报告的实验

旧模块用 `Channel-Call-UUID` 作 key；订阅 ALL 时，部分系统事件没有该字段，因此发送 null key。

| 条件 | 报告结果 |
|---|---|
| FS 1.6.20 + 旧模块 + compact + null key | R1：670 行校验失败 |
| 同一旧模块 + delete + null key | R2：0 次 INVALID_RECORD |
| 较早 FS 1.10.x 对照 | compact/null 失败；delete/null 和 compact/key 成功 |

这里的要求是 **key 非 null**，不是 key 长度必须大于零。原始报告不在 Git 中；版本、hash 和证据目录统一见 [STATUS](STATUS.md)。

## 现在可运行的对照

```bash
# 先按 Kafka 实验室文档启动三节点栈，并构建 record_probe
python3 lab/run_record_matrix.py
```

这是独立 librdkafka 探针：记录运行时版本、有效协议默认值、压缩方式和数值错误码。八组对照检查策略、key 和压缩三个维度。它不加载旧 FS `.so`，不能替代原始模块复现。

原 FS 1.6.20 重跑仍需实验室上的工具链 `/workspace/lab-mod-event-kafka/fs16/`，以及主证据目录中的 `reproduce.sh`、`REBUILD-SO.md` 和旧 `.so`。这些文件未随仓库交付；只读本文不足以从零重建那次实验。

## 针对云发布还缺什么

1. 生产实际加载的 librdkafka、云端升级前后版本、topic 有效配置。
2. 同一 broker/partition 的 API 协商、ProduceRequest 版本、leader 变化及完整拒收原因。
3. 同一 FS 启动身份和模块实例下，记录基线 → 发布期间连续错误 → 恢复后实际消费。
4. 若怀疑协议回退：使用同版旧客户端，定向中断 ApiVersions 协商。0.9.3 默认关闭协商，0.11.6 默认回退间隔 20 分钟，1.9.2 默认间隔 0；不能统一套用“20 分钟”。
5. 若怀疑压缩：相同版本/负载下比较 snappy 与 none。一次改善不足以证明压缩缺陷。

当前 master 仍可能发送 null key，且 `is_permanent_error` 未列入 +87；不要把换成 outbox 当作记录格式问题已经解决。TTL 会限制旧行继续投递，但过期不是发送成功。

源码依据：[Kafka 3.8.1 错误码](https://github.com/apache/kafka/blob/3.8.1/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java)、[校验实现](https://github.com/apache/kafka/blob/3.8.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java)。
