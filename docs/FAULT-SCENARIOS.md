# 故障覆盖矩阵

现有方式不能验证所有情况。本表区分“能运行的工具”与“通过的结果”；具体结果见 [STATUS](STATUS.md)。

## 可运行场景

| 场景 | 对应原编号 | 工具 / 命令入口 | 能验证什么 | 不能据此证明什么 |
|---|---|---|---|---|
| FS-01 全入口断连 | L-02/L-07 | `run_recovery_drill.py --fault cut` | 候选 core 在同一进程中恢复；消费集合与 outbox 终态 | broker 进程真的重启、旧 FS 模块自愈 |
| FS-02 单入口禁用 | L-04 的部分前置 | `fault_proxy.py cut --proxies kafka1` | 该代理路径断开 | 单 broker 宕机；单节点栈的三个入口不是三副本 |
| FS-03 延迟 / 抖动 | L-03/L-12 | `run_recovery_drill.py --fault latency` | 下行延迟时的恢复、重复和积压终态 | 全部网络抖动分布、生产媒体线程性能 |
| FS-05 下行黑洞 | L-11 | `run_recovery_drill.py --fault blackhole` | broker 响应被阻塞超过消息超时后的恢复 | 真实双向丢包、所有 TCP 状态 |
| FS-09 超 TTL | L-16a/b/c | `--fault cut --seconds 150 --ttl-ms 120000` | 有证据的过期终态、恢复后新消息 | 所有旧消息必须补回；超龄 in-flight 也被禁止送达 |
| FS-12 三 broker 滚动重启 | L-08，部分 L-04/L-09 | `--fault rolling` | 三个独立 broker；每次停一个，ISR 恢复后再停下一个 | 跨版本升级、云厂商网关/路由发布 |
| FS-11 记录校验对照 | 无原 L 编号 | `run_record_matrix.py` | compact/delete × null/key × snappy/none，实际检查返回码 | 原始旧模块复现、发布后几十分钟自愈 |

`run_recovery_drill.py` 使用真实 Kafka + 当前 `KafkaPipeline`，范围是 **candidate-core**。`run_record_matrix.py` 是独立 librdkafka **record-probe**，不是 FS 模块。完整命令见 [演练手册](DRILL-RUNBOOK.md)。

## 仍需补齐的情况

| 场景 | 缺口 / 下一次应如何验证 |
|---|---|
| FS-04 TCP RST（L-10） | 当前代理版本的 reset-peer 能力未验证；普通 disable 不能标成精确 RST 注入 |
| FS-06 broker SIGKILL（L-09） | 当前滚动工具做正常 stop/start；SIGKILL 与磁盘恢复需单独跑 |
| FS-07 队列满（L-05） | 单元覆盖有界队列；仍缺真实 FS 回调负载、线程数、拒绝 ID 闭环。旧模块的悬空 key 要用旧源码 + ASan 独立验证 |
| FS-08 磁盘/SQLite 失败（L-13） | 行数/字节容量单元测试不等于 ENOSPC、只读文件系统或事务 I/O 失败 |
| FS-10 FS SIGKILL、卸载（L-06/L-14） | 单元重开 DB 不等于真实 FS 中断；须保存 COMMIT 前后集合和未 ACK 行 |
| FS-13 API 协商回退 | 必须使用生产实际 librdkafka，定向打断 ApiVersions 协商，并记录降级与恢复；全断代理不能确定命中协商窗口 |
| 跨版本滚动升级 | 当前集群固定 Kafka 3.8.1；必须按厂商支持路径固定升级前后镜像、协议/元数据版本和回滚条件 |
| 认证/ACL/证书（L-15） | 需要独立 SASL/TLS/ACL 栈；当前 PLAINTEXT 栈不能验证 |
| 致命错误与 producer 重建 | 需要确定性错误注入和并发内存检查；普通断连不能证明 fatal 重建安全 |
| 严格通话顺序、ACK 不确定性（L-01 等） | 开启 `--check-order`；排序查询不保证重试/多分区下的最终消费顺序；还需 ACK 后本地提交前崩溃注入 |
| 云端发布窗口 | 对齐版本、broker/partition、服务端拒收原因、FS 启动身份与恢复后实际消费；实验室通过不能替代 |

## 为什么不能用 +87 实验代替云发布演练

compact + null key 是持续存在的拒收条件。若 key、策略和流量不变，它不能单独解释“发布期间持续错误、几十分钟后 FS 不重启就恢复”。需要补齐“触发 → 持续 → 无重启恢复”的完整时间线，见 [+87 专题](FS11-ERR87-REPRO-OVERVIEW.md)。
