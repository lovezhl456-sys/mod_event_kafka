# 真实 FreeSWITCH 联调

候选 core 演练不加载 FS 模块。要验证 FS 回调、卸载和旧模块缺陷，需要额外提供与目标 ABI 一致的 FS、头文件和 `.so`；本仓库没有交付可直接拉取的 FS 1.6.20 构建镜像。

## 前置证据

- 实际 FS 版本、实际加载的 librdkafka 版本和模块 hash；不要只记录镜像标签。
- 模块是否为旧无 outbox 实现，或当前 outbox 实现。
- topic 配置、事件过滤器、压缩、容量、TTL，以及实验室 bootstrap。
- FS 进程 PID + 启动身份，模块初始化次数；不把 reload 后正常称为自动恢复。

历史路径、镜像名与证据目录见 [STATUS](../docs/STATUS.md)，不代表当前环境已安装。

## 执行顺序

1. 在专用 FS 实验室加载模块，确认基线事件被 Kafka 消费。
2. 使用 [演练手册](../docs/DRILL-RUNBOOK.md) 的代理工具注入故障，持续产生事件。
3. 故障恢复后继续产生新事件，保持 FS 和模块实例不变。
4. 保存独立注入集合、原始消费记录及最终 outbox，按 [验收格式](../docs/TEST-PLAN.md) 核对。

当前 FS glue 没有持续导出成功入队的 event_id 清单，完整自动验收还需要测试观测点或独立事件映射。**不能从已消费列表或最终 outbox 反推出 injected_ids，再宣称无丢失。** core harness 在 enqueue 返回时记录 ID，因此不依赖消费结果构造期望集合。

`dialtest_originate.sh` / `dialtest_fast.sh` 是历史 loopback/park 辅助脚本，使用前核对其中的容器与 fs_cli 路径。park 不保证产生 ANSWER 事件。仅用于专用实验室，不能直接对生产容器执行。
