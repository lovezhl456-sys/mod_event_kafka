# 测试与验收

## 运行级别

| 级别 | 命令/对象 | 覆盖与限制 |
|---|---|---|
| 单元 | `ctest --test-dir build --output-on-failure` | 队列深拷贝/并发、outbox 容量与状态、TTL、停止、验收工具反例；不代表 FS 或云环境通过 |
| 候选 core 集成 | `lab/run_recovery_drill.py` | 真实 Kafka、发送进程不重启、独立消费、最终 outbox |
| 记录协议探针 | `lab/run_record_matrix.py` | 记录校验返回码和策略/key/压缩对照；没有 FS/outbox |
| FS 联调 / 云发布 | [覆盖矩阵](FAULT-SCENARIOS.md) 的待验证项 | 要求对应二进制、环境、故障及恢复时间线 |

CMake 的 `ENABLE_SANITIZERS=ON` 同时编译 core 和测试为 ASan/UBSan。原先只给测试可执行文件加标志不足以覆盖 core。ASan/UBSan 也不能代替 TSan 竞态检查。

Linux Docker 构建（无需 FS 头文件）：

```bash
docker build -f lab/Dockerfile.tests -t event-kafka-tests .
docker run --rm -v "$PWD:/src" event-kafka-tests
```

不要与其他平台共用同一个 `build/`；切换平台使用新的 CMake 构建目录。

## 验收器输入

```bash
python3 scripts/verify_event_ids.py reports/RUN --require-recovery
# 本轮允许过期时，明确给出运行配置中的 TTL
python3 scripts/verify_event_ids.py reports/RUN --ttl-ms 120000 --require-recovery
# 要验证通话顺序时额外加 --check-order
```

| 文件 | 格式与用途 |
|---|---|
| `injected_ids.txt` | 必需，非空、每行唯一 ID；记录每次尝试，包括明确拒绝的事件 |
| `consumed_ids.txt` | 必需，每次消费一行，保留重复，不能预先去重 |
| `outbox_final.csv` | 必需，最终单次快照；列 `event_id,state,last_error,created_at_ms,updated_at_ms`；排空时保留表头 |
| `rejected_ids.txt` | 可选；只有显式 `--allow-rejected` 才接受；不得与已消费/死信重叠 |
| `expected_dead_ids.txt` | 可选；故障计划指定的永久死信 ID，必须与实际 dead 集合完全相等 |
| `injected_events.csv` | 顺序/自愈检查必需；列 `event_id,call_uuid,sequence,phase`；phase 为 baseline/during/recovery；sequence 为每通话唯一注入序号 |
| `consumed_records.csv` | 自愈检查必需；列 `event_id,partition,offset,observed_at_ms`；保留每次消费，必须与原始消费 ID 多重集合一致 |
| `recovery.json` | 自愈检查必需，格式如下；来自控制器和发送进程的实际记录 |

```json
{
  "sender_before": {"pid": 123, "start_id": "unique-process-start-id"},
  "sender_after": {"pid": 123, "start_id": "unique-process-start-id"},
  "reload_count": 0,
  "fault_start_ms": 1000,
  "fault_end_ms": 36000,
  "fault_exit_code": 0
}
```

- 注入集合必须在生产/入队侧独立记录，不能从消费结果或最终 DB 反推。
- 注入集合必须等于互不重叠的消费、拒绝、合法过期、预期永久死信集合之和；出现意外 ID、未解释缺失、pending/in_flight 都失败。
- `dead/expired_ttl` 必须有 `updated_at_ms-created_at_ms > --ttl-ms > 0` 的证据；不能仅把缺失 ID 写进免责列表。已回收的过期行需提前保留独立终态证据并扩展验证器，当前缺证会失败。
- 顺序依据独立注入序号，不用毫秒时间戳加 event_id 排序，也不用最终已排空的快照推断顺序。
- 自愈还要求故障前/中/后均有注入，基线 ID 在故障开始前已消费，恢复阶段新 ID 在故障结束后实际消费，发送进程身份不变、reload=0、故障工具成功恢复。
- 消费重复是至少一次语义的可观察结果，报告数量，不自动失败。ACK 不明确后“消费且过期”的交集会失败，需保留证据分析，不能静默豁免。
- 未请求的顺序/自愈检查会显示 `NOT_CHECKED`。`VERIFY_OK` 不意味着覆盖整个故障矩阵。

退出码：0 已请求的检查通过；1 集合/终态/自愈失败；2 仅顺序失败；3 产物缺失或损坏。

## CI 接入

[ci-drills.yml.example](../lab/ci-drills.yml.example) 是不自动执行的模板，包含单元和六组真实 Kafka 演练。具备 workflow 写权限的维护者可审阅后放入 `.github/workflows/drills.yml`；当前 PR 没有启用新的远程 CI。旧 workflow 保留当前 fork 的既有注释，已比较解析后的 YAML，执行逻辑与目标仓库完全一致。

## 仍缺少的验证

SQLite 真 I/O 故障、fatal 重建竞态、真实 FS 卸载/SIGKILL、ACK 崩溃窗口、媒体线程延迟和旧库兼容性，不能由当前单元测试推断通过。详细编号统一在 [覆盖矩阵](FAULT-SCENARIOS.md)，结果统一在 [STATUS](STATUS.md)。

PR 可以提交工具或文档改进，但描述必须列明未跑项；发布可靠性结论必须有对应层级的真实证据。历史候选补丁 `0001` 一直是 NOT_VERIFIED，不作为验收依据。
