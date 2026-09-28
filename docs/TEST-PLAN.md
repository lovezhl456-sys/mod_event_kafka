# TEST-PLAN — mod_event_kafka Outbox 可靠性

状态图例：`PLANNED` | `CODED` | `PASS` | `FAIL` | `BLOCKED` | `N/A`  
**候选补丁 `0001-kafka-restart-resilience.patch` 仅为反面参考——绝不得据此把任何一行标为 PASS。**

契约来源：`docs/DESIGN.md`。实验室 FS + 3 broker + 故障代理依赖 Docker/Podman（不在本机）或安装软件大师提供的主机。

---

## 0. 优先补齐的单元缺口（已提出）

| ID | 结果 | 备注 |
|----|--------|-------|
| U-OWN-01 队列深拷贝 | PASS | `try_push` 之后改调用方；弹出内容不变 |
| U-OWN-02 outbox 深拷贝 | PASS | `insert_pending` 之后改记录；取出内容不变 |
| U-DSK-03 字节上限 | PASS | `max_bytes` 拒绝；pending 完好 |
| U-Q-02 并发入队 | PASS | 8 线程 × 40 进入容量 64；pushed+rejected 精确 |
| U-UNL-01/02 安全卸载 | PASS | `notify_stop` 解除阻塞；`close`/`stop` 可重复；停止后入队被拒绝；outbox 保留行 |

命令：`cmake --build build && ctest --test-dir build --output-on-failure`（二进制以 ASan+UBSan 构建）。

## 1. 单元测试矩阵（无 FreeSWITCH）

目标二进制：`build/test_outbox`（以及将来的 `tests/test_*.cpp`）。在 ASan+UBSan 下运行（见 §2）。

| ID | 范围 | 场景 | 期望 | 状态 |
|----|------|----------|--------|--------|
| U-OWN-01 | 所有权 | `QueuedEvent` 持有深拷贝的 payload+key+event_id；`try_push` 之后改调用方缓冲区 | 队列项不变 | PASS |
| U-OWN-02 | 所有权 | Produce 路径使用 `RD_KAFKA_MSG_F_COPY`（或等价物）；outbox 行在 ACK 之前仍是事实来源（SoT） | 无 MSG_F_FREE UAF；in_flight 期间 DB 中 payload 仍可读 | PASS |
| U-OWN-03 | 所有权 | worker/API 表面不存在分离的重试线程 | 静态检查/grep：produce/重试路径无 `std::thread(...).detach` | PLANNED |
| U-Q-01 | 队列上限 | `BoundedQueue(N)` 接受 N 条，拒绝第 N+1 条 | `try_push` 为 false；`rejected()==1` | CODED |
| U-Q-02 | 队列上限 | 在容量附近并发 push/pop | 计数不丢；仅在真正溢出时 rejected 递增 | PASS |
| U-Q-03 | 队列上限 | 空队列上 `pop_wait_for_ms` + `notify_stop` | 返回 nullopt；不挂起 | PLANNED |
| U-RTY-01 | 重试 | `mark_retry` 设为 `pending`，递增 `attempts`，`next_attempt_at_ms` 在将来 | `fetch_due(now)` 为空；过了 next_ms 之后到点 | CODED |
| U-RTY-02 | 重试 | 指数退避（+抖动）的调度辅助 | 间隔增长；抖动落在文档范围内 | PLANNED |
| U-RTY-03 | 重试 | 瞬时类保留该行；永久类 → `dead` | state/`last_error` 正确；dead 上无紧循环 | CODED（dead）/ PLANNED（错误分类映射） |
| U-ACK-01 | ACK | `mark_in_flight` → `mark_acked` 删除该行 | stats 的 in_flight/pending 下降；行消失 | CODED |
| U-ACK-02 | ACK | ACK 是事务性的：在提交完成前重新打开，模拟 DELETE 中途崩溃 | 至少一次：行仍在或已消失；绝不出现半状态 | PLANNED |
| U-ACK-03 | ACK | ACK 之后、崩溃窗口内的重复投递 | 消费端去重键 = 头 `x-fs-event-id`；正文 JSON 不变 | PLANNED（文档 + 辅助逻辑） |
| U-DSK-01 | 磁盘失败 | `outbox-path` 不可写 / 打开失败 | `open` 为 false 并带 err；入队路径以指标/日志拒绝（不静默） | PLANNED |
| U-DSK-02 | 磁盘失败 | `max_rows` 耗尽 | `insert_pending` 为 false；走 `rejected_disk_full` 路径 | CODED（行数） |
| U-DSK-03 | 磁盘失败 | `max_bytes` 耗尽 | 插入被拒绝；已有行完好 | PASS |
| U-DSK-04 | 磁盘失败 | INSERT/UPDATE 上的 SQLite I/O 错误 | 错误被暴露；不崩溃；行不会被错误地 ACK | PLANNED |
| U-REC-01 | 恢复 | 重新打开后仍持久 | dead/pending 在进程重启后仍在 | CODED |
| U-REC-02 | 恢复 | 启动/重建时 `requeue_in_flight` | in_flight → pending；无孤立 in_flight | CODED |
| U-REC-03 | 恢复 | 同一 `call_uuid` 按 `created_at_ms` FIFO | fetch_due 顺序与该通话内的入队顺序一致 | CODED |
| U-REC-04 | 恢复 | 允许跨通话无序 | 两个 call_uuid 交错即可 | PLANNED |
| U-UNL-01 | 卸载 | 关闭顺序：停止入队 → 内存排入 outbox → flush/poll 超时 → join | join 不挂起；未 ACK 行留在磁盘 | PASS |
| U-UNL-02 | 卸载 | 未 open 时重复 close / destroy | 不崩溃（ASan 干净） | PASS |
| U-ID-01 | event_id | `make_event_id` 的 UUIDv4 形态，以及 N≥10k 上的唯一性 | 正则 + 集合大小 == N | PLANNED |

### 相对当前 `tests/test_outbox.cpp` 的缺口

已覆盖：U-Q-01、U-RTY-01、U-ACK-01、U-DSK-02（行数）、U-REC-01/02/03、dead 标记。  
**在宣称单元测试完成之前仍缺：** U-OWN-*、U-Q-02/03、U-RTY-02、U-ACK-02/03、U-DSK-01/03/04、U-REC-04、U-UNL-*、U-ID-01、字节上限、并发。

---

## 2. ASan / UBSan 检查清单

构建（`CMakeLists.txt` 中 `test_outbox` 已接好）：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

| 检查 | 方法 | 通过标准 | 状态 |
|-------|-----|---------------|--------|
| A-01 单元套件上的 AddressSanitizer | 编译并链接 `-fsanitize=address,undefined` | 退出码 0；无 ASan 报告 | CODED（编译标志）/ 运行结果见 reports |
| A-02 produce/ACK 上的释放后使用 | 持有 payload 视图时加压 insert→in_flight→ack | 无 heap-use-after-free | PLANNED |
| A-03 双重释放 / MSG_F_FREE 回归 | grep + 否定测试：COPY 路径拥有缓冲区 | rdkafka 不释放 outbox 拥有的 blob | PLANNED |
| A-04 线程生命周期 | 停止 worker+poll；join；销毁 Outbox | 无 stack-use-after-return / 线程泄漏 | PLANNED |
| A-05 UBSan 整数/空指针 | 全套件 | 无运行时错误 | 与 A-01 一并 |
| A-06 LeakSanitizer（可选 ASAN_OPTIONS=detect_leaks=1） | 套件 + 重新打开循环 | outbox/队列无确定泄漏 | PLANNED |

CI/本机环境：

```bash
export ASAN_OPTIONS=detect_leaks=1:halt_on_error=1:abort_on_error=1
export UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
```

**FreeSWITCH 下的模块 `.so`** 消毒器构建仅用于实验室（需要匹配的 FS/库）。在实验室主机实际跑过之前，不得宣称已加载模块的 ASan PASS。

---

## 3. 验收实验室 — FS + 3 节点 Kafka + 故障代理

实验室 compose 位于 `/workspace/lab-mod-event-kafka/`（有 Docker）。FreeSWITCH 主机仍可选 / Phase-2。运行时负责人：安装软件大师。

### 3.1 拓扑（可用时）

- FreeSWITCH + 已打补丁的 `mod_event_kafka`（outbox 设计，不是候选补丁）
- Kafka KRaft/ZK 3 个 broker
- bootstrap 前面的 Toxiproxy（或等价物）
- 记录 Kafka 头 `x-fs-event-id`（以及可选 key=`Channel-Call-UUID`）的消费者

### 3.2 场景（高层）

| ID | 故障 | 期望 | 状态 |
|----|-------|--------|--------|
| L-01 | 正常路径 | 注入的全部 event_id 在主题中各出现一次 | PLANNED |
| L-02 | 流量中途杀掉全部 broker 30 秒；重启；**不** `reload mod_event_kafka` | pending outbox 排空；已发送集合 ⊆ 已注入；缺失仅限 COMMIT 前窗口（DESIGN §可靠性边界 1） | **PASS**（2026-09-25，稳定测试架 `reports/l02-l07-run1790313218`，不是 FS `.so`） |
| L-03 | produce 期间 Toxiproxy reset-peer / 延迟 | 重试；最终 ACK；重复只出现在 ACK-崩溃窗口 → 按 event_id 去重 | PLANNED |
| L-04 | 单个 broker 宕机（ISR 仍正常） | 无持续拒绝；produce_ok 继续 | PLANNED |
| L-05 | `mem-queue-max` 压力 | `rejected_mem_full` + WARNING；媒体线程不被阻塞 | PLANNED |
| L-06 | 中断期间卸载/重新加载模块 | 磁盘 outbox 保留；reload 且 broker 起来之后，排空完成 | PLANNED |
| L-07 | 自愈门禁 | L-02 之后比较 **reload 之前** 与 **reload 之后**——仅当无需 reload 即自愈时 PASS | **PASS**（与 L-02 同一次运行；reload 前 acked=60，verify 退出码 0） |

| L-08 | 滚动重启（一次一个 broker，全部 bootstrap 地址都经过代理） | 已提交的 outbox 行无永久性丢失；积压排空 | PLANNED |
| L-09 | Broker SIGKILL / 进程崩溃 | 与瞬时故障相同；outbox 保留；自动重试 | PLANNED |
| L-10 | 所有被代理的 broker 端口上 TCP RST | 保留 + 退避；路径恢复后无需 reload 即可排空 | PLANNED |
| L-11 | 网络黑洞（全部丢弃）> `message-timeout-ms` | 行保持 pending/重试；恢复后，**新**事件仍入队并发送；旧的到点行排空 | PLANNED |
| L-12 | 延迟 / 反复抖动（toxiproxy toxics） | 线程/内存稳定；仅在容量处拒绝；不阻塞媒体线程 | PLANNED |
| L-13 | 磁盘满 / outbox INSERT 失败 | 拒绝 + 告警指标；不静默丢弃；不崩溃 | PLANNED |
| L-14 | 中断期间 FS / 模块进程 SIGKILL；重启模块/FS | 未 ACK 的 outbox 行以**相同** event_id 重放 | PLANNED |
| L-15 | 认证/acl/永久性 produce 错误 | `state`=`dead` + 证据；无紧重试循环 | PLANNED |

**通过规则：** 比较 event_id 集合（injected / rejected / consumed_dedup / outbox 终态）。绝不能仅凭“没有错误日志”通过。


### 3.3 Event ID 集合核对方法

稳定 ID 是入队时创建的 UUIDv4，存入 outbox，并作为 Kafka 头 **`x-fs-event-id`** 附带（正文 JSON 不变）。

**产物（实验室运行写在 `reports/<run-id>/` 下）：**

| 文件 | 内容 |
|------|---------|
| `injected_ids.txt` | 每行一个 event_id，顺序 = 注入顺序 |
| `outbox_snapshot.csv` | 检查点上的 `event_id,state,attempts,call_uuid,created_at_ms`（T0 注入完成 / T1 broker 宕机 / T2 broker 起来并排空 / T3 卸载） |
| `consumed_ids.txt` | 来自消费者的 event_id（头 `x-fs-event-id`），每行一个，若有重复可包含重复 |
| `consumed_ids_dedup.txt` | 唯一集合 |
| `metrics.json` | DESIGN 指标列表中的计数器 |

**检查（脚本：`scripts/verify_event_ids.py`）：**

1. **完整性（自愈后的至少一次）：**  
   `set(injected) - set(consumed_dedup) == ∅`  
   *例外：* 从未到达 outbox COMMIT 的 ID（任何有意的提交前丢失都要写明条数 + 原因）。
2. **容量下无静默丢弃：** 若拒绝数 > 0，每个被拒绝的 ID 必须出现在 `rejected_ids.txt` 日志中，不能只出现在指标里。
3. **去重：** 报告 `len(consumed) - len(consumed_dedup)`；若按 `x-fs-event-id` 做消费端去重之后仍有重复，则 FAIL。
4. **顺序（同一通话）：** 对每个 `call_uuid`，`consumed_dedup` 中 event_id 的相对顺序与 outbox 快照中的 `created_at_ms` 顺序一致（跨通话：不断言）。
5. **Outbox 终态：** 排空之后，成功 ID 的 `pending+in_flight == 0`；`dead` 仅用于注入了永久性错误的情形。
6. **自愈：** L-07 要求在 **reload 之前** 的消费者采集上通过检查 1–5。

退出码：`0` 全部断言通过；`1` 集合不匹配；`2` 顺序失败；`3` 产物损坏。

---

## 4. 本机现在能跑什么，以及被阻塞的部分

| 工作 | 位置 | 状态 |
|------|-------|--------|
| Outbox/队列单元测试 + ASan/UBSan 标志 | 本机 | 可运行（`ctest`）；扩展矩阵仍为 PLANNED |
| Kafka×3 + toxiproxy 实验室 | `/workspace/lab-mod-event-kafka/` | READY（compose）；FS 可选，属 Phase-2 |
| 完整 FS+Kafka+代理 | 实验室 + FS 主机 | 等待安装软件大师提供 FS 二进制/路径，PENDING |
| 把候选 poll/重建补丁当作已验证 | — | **禁止**（DESIGN：反面参考） |

---

## 5. “可以提 PR”的退出标准

1. OWN/Q/RTY/ACK/DSK/REC/UNL 的单元矩阵行要么 PASS，要么经架构师签字明确豁免。  
2. 单元套件上 A-01 干净。  
3. 实验室上 L-02 + L-07 PASS，且 `verify_event_ids.py` 退出码为 0（产物在 `reports/` 下）。  
4. 报告把每个跳过的场景标为 `BLOCKED`/`N/A`，不得沉默跳过。
