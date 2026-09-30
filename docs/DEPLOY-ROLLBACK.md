# 部署 / 回滚

## 部署
1. 安装 `librdkafka`、`libsqlite3`。
2. 构建 `mod_event_kafka.so`（见仓库根目录的 `Makefile`；它会以 `-Iinclude` 编译 `src/kafka_*.cpp`）。
3. 安装配置文件；将 `bootstrap-servers` 设为 **toxiproxy/实验室 bootstrap 或生产 bootstrap**（二者切勿混用）。
4. 确保 `outbox-path` 所在目录对 FreeSWITCH 运行用户可写。
5. `load mod_event_kafka`。流水线健康时，Kafka 重启后**无需** reload。

## 回滚
1. `unload mod_event_kafka`
2. 恢复旧版 `.so`，以及不含 outbox 参数的配置（旧配置依然有效；新参数均为可选）。
3. Outbox 数据库可以归档；若日后重新加载新构建，残留行可以安全保留。

## 本环境未验证
- 生产环境的云端 Kafka
- 在本机完整加载新 `.so` 的 FreeSWITCH（Phase1 阶段本机没有 FS 头文件）
