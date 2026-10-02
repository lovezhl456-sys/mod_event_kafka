# 演练手册

目标：持续发送，在故障前/中/后独立消费，并核对消息终态。无日志不代表成功；工具返回 PASS 也只覆盖 `result.json` 标明的范围。

## 1. 准备

1. 按 [Kafka 实验室](KAFKA-DEPLOY.md) 启动 **三 broker** 栈。
2. 安装 C++17、CMake、pkg-config、SQLite3、librdkafka 开发包和 Python 3.9+。
3. 从仓库根目录构建：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j2
ctest --test-dir build --output-on-failure
```

CMake 默认给 core 与测试程序启用 ASan/UBSan；`test_pipeline_drill` 不需要 FS 头文件。Docker 构建替代方式见 [测试计划](TEST-PLAN.md)。

## 2. 自动恢复演练

每次运行创建独立 topic、outbox 和 `reports/<run-id>/`。发送进程先消费到 5 条基线消息才允许注入故障；故障期间继续发送，恢复后再发送新事件。工具不 reload 或重启发送进程。

```bash
# 短断：超过 message timeout，但小于 TTL
python3 lab/run_recovery_drill.py --fault cut --seconds 35

# 超 TTL：允许有证据的 expired_ttl，不把它当成补发成功
python3 lab/run_recovery_drill.py --fault cut --seconds 150 --ttl-ms 120000

# 延迟、下行黑洞
python3 lab/run_recovery_drill.py --fault latency --seconds 35
python3 lab/run_recovery_drill.py --fault blackhole --seconds 35

# 真正的滚动重启：每个 broker 停 5 秒，等待所有分区 ISR 恢复
python3 lab/run_recovery_drill.py --fault rolling --seconds 5

# 压缩对照：其他参数保持相同，分开运行并比较证据
python3 lab/run_recovery_drill.py --fault cut --seconds 35 --compression none
```

`--check-order` 额外验证同一通话的首次消费顺序；未开启时明确输出 `order=NOT_CHECKED`，不能宣称顺序通过。上述程序验证的是候选 core，不是 FS 回调、媒体线程或旧模块。

## 3. +87 对照

```bash
python3 lab/run_record_matrix.py
```

八组：compact/delete × null/key × snappy/none。只有 compact + null key 预期返回数值 +87，其余预期 ACK；每组必须收到 5 个匹配的 delivery callback。超时或错误码不符即失败。原始协议日志保存在各组 `.log`；它不是云升级自愈演练。

## 4. 只注入代理故障（用于真实 FS 实验室）

```bash
# 原单 broker 栈：默认端口 8474，保留旧命令入口
bash lab/toxiproxy_cut_restore.sh 35

# 新三 broker 栈：管理端口 8475
python3 lab/fault_proxy.py cut --api http://127.0.0.1:8475 --seconds 35
python3 lab/fault_proxy.py latency --api http://127.0.0.1:8475 --seconds 35
python3 lab/fault_proxy.py blackhole --api http://127.0.0.1:8475 --seconds 35
```

工具在异常、SIGINT、SIGTERM 后尝试恢复本次改动，保留其他 toxics。`RESTORE_FAILED`/`RESTORE_UNKNOWN` 必须人工检查后才能开始下一轮；SIGKILL/掉电无法执行清理。远程实验室用 SSH 隧道访问 localhost API。

## 5. 验收与取证

核心工具自动调用 [集合验收器](TEST-PLAN.md)。手工 FS 演练也必须生成相同证据，并额外保存 FS 版本、实际加载的 librdkafka 版本、模块 hash、模块初始化次数、通话事件 ID 与进程启动身份。只有 PID 相同不够。

- 故障注入脚本退出 0：只代表故障已注入并恢复。
- `VERIFY_OK`：只代表明确列出的集合/终态/可选顺序和自愈检查通过。
- 实验室通过：不能据此标记其他版本、旧模块或云发布通过。
- 保留每轮 broker 日志、topic 配置、leader/ISR、客户端日志及原始消费记录。topic 与 reports 默认保留，避免清理掉失败证据。
