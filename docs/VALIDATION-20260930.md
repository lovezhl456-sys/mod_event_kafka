# 本次验证记录（2026-09-30）

范围：本 PR 的演练工具、候选 core 和文档。基础源码 commit 为 `d740c30`，core 生产实现未改；各轮 `run.json` 保存执行时工作树状态及代码 SHA-256。最终验收器再次核对了六轮原始证据。

环境：Linux arm64 测试容器（Debian 12、GCC 12、librdkafka 2.0.2、SQLite 3.40.1），Kafka 3.8.1 × 3，toxiproxy 2.1.4。没有加载 FreeSWITCH `.so`，没有连接生产。

## 结果

| 检查 | 结果 | 证据 / 参数 |
|---|---|---|
| C++ core + 测试构建 | 通过 | ASan/UBSan 同时应用 core 与测试；不是仅对测试程序插桩 |
| CTest | 2/2 通过 | outbox 套件 + 21 项 Python 工具测试 |
| 三 broker 元数据 | 通过 | 每轮独立 topic，3 分区、RF=3、min ISR=2；前后检查完整 ISR |
| 短断，snappy | 恢复通过 | 82 注入/82 消费；0 重复；`reports/review-cut-v2/` |
| 短断，none | 恢复通过 | 82/82；1 次重复；`reports/review-cut-none/` |
| 下行黑洞 | 恢复通过 | 78/78；5 次重复；`reports/review-blackhole/` |
| 下行延迟 | 恢复通过 | 77/77；0 重复；`reports/review-latency/` |
| 超 TTL | 终态与恢复通过 | 107 注入、47 消费、60 合法过期；`reports/review-ttl/` |
| 三 broker 滚动重启 | 恢复通过 | 469/469；0 重复；每次只停一个且等完整 ISR 后继续；`reports/review-rolling/` |
| 记录校验矩阵 | 8/8 通过 | 每组 5 个 delivery callback；compact/null 得到 +87，其余 ACK；`reports/review-record-matrix/` |
| SIGTERM 清理 | 通过 | 故障脚本退出 1，三个代理均恢复 enabled；`reports/review-signal-cleanup/` |
| 同通话严格顺序 | **失败，保留问题** | 对短断证据另加 `--check-order`，返回 2；见下文 |

六轮恢复结果均为同一发送进程身份、无 reload、无拒绝、无 pending/in_flight 残留。重复计数保留，未预先去重后伪装为零。恢复通过不包含严格顺序通过。

短断/黑洞/延迟注入 3 秒，message timeout=1000 ms，TTL=120000 ms；超 TTL 注入 6 秒、TTL=2000 ms；滚动每个 broker 停 1 秒、逐个等待 ISR。所有轮次恢复后继续发送 5 秒，再等待消费和 outbox 终态。这是缩短时间参数的机制验证，不能代替默认 35s/150s、真实 FS 和云发布窗口。

## 新发现：积压恢复会乱序

同一个 `drill-call`、同一 Kafka partition=2：消费者先看到注入 sequence=81（offset=6），再看到 sequence=16（offset=7）。82 条均到达，最终 outbox 为空，但首见消费顺序不符合注入顺序。

这不是跨分区合并引起的顺序差异。当前 fetch_due 排序不能阻止新消息越过处于重试退避的旧消息。PR 修正原文档的保证，并提供严格验收开关；**没有修改生产排序策略或宣称该问题已解决**。后续如要保证顺序，需设计每通话头部阻塞、稳定序号及重试/重建测试。

## 执行中修正的问题

- 新卷起初不属于 Kafka 的 uid=1000，broker 格式化失败；加入仅针对本实验室卷的初始化服务后，三节点均健康。
- 首轮短断在宿主机读取容器 SQLite 时导出失败，该轮保留为 ERROR（`reports/review-cut/`）。改由发送进程停止流水线后直接导出最终快照，重新运行通过；没有将失败轮追记成 PASS。
- 空跑、缺最终快照、意外 ID、未排空、无依据过期、拒绝/消费冲突、错误顺序、重启以及消费时间不在故障两侧，均有反例测试。

## 证据与复跑

完整产物保留在本次执行环境的 `reports/`，不提交 DB/原始日志。当前推送凭据没有 GitHub workflow 写入权限；[CI 模板](../lab/ci-drills.yml.example) 作为普通文件提供，需维护者放入 `.github/workflows/drills.yml` 后启用。模板会独立重跑并上传其 reports；本表不是远程 CI 通过记录。目标与 fork 的旧 workflow 仅有注释语言差异；本 PR 保留 fork 既有内容，解析后的 YAML 对象与目标相同，未改变执行逻辑。

按 [演练手册](DRILL-RUNBOOK.md) 构建并启动集群后：

```bash
python3 lab/run_recovery_drill.py --fault cut --seconds 3 --message-timeout-ms 1000 --post-seconds 5
python3 lab/run_recovery_drill.py --fault cut --seconds 6 --ttl-ms 2000 --message-timeout-ms 1000 --post-seconds 5
python3 lab/run_recovery_drill.py --fault rolling --seconds 1 --message-timeout-ms 1000 --post-seconds 5
python3 lab/run_record_matrix.py
# 对指定 run 的既有产物独立检查严格顺序
python3 scripts/verify_event_ids.py reports/RUN --require-recovery --ttl-ms 120000 --check-order
```

未验证：生产旧 librdkafka、真实 FS 事件生命周期、跨版本升级、定向 ApiVersions 回退、fatal 竞态、SQLite 实际 I/O 故障与生产媒体性能。完整缺口见 [覆盖矩阵](FAULT-SCENARIOS.md)。
