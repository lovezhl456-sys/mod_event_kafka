# 实验室入口

- [部署两套 Kafka 拓扑](../docs/KAFKA-DEPLOY.md)：原单节点 19092–19094；真正三节点 29092–29094。
- [运行自动演练](../docs/DRILL-RUNBOOK.md)：断连、下行黑洞、延迟、滚动重启、TTL 和 +87 对照。
- [验收证据格式](../docs/TEST-PLAN.md)：事件集合、终态、同进程恢复与可选顺序。
- [真实 FS 实验室参考](README-PHASE2.md)：需要另行提供匹配 ABI 的 FS 和模块；候选 core 工具不等于 FS 联调。

脚本只操作专用实验室。不要连接生产，也不要全局修改防火墙或清理其他 Docker 资源。每轮 reports 与独立 topic 保留供复核。
