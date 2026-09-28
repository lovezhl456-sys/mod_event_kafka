# 部署 / 回滚

## 部署
1. 安装 `librdkafka`、`libsqlite3`。
2. 构建 `mod_event_kafka.so`（见仓库根目录 `Makefile`；它以 `-Iinclude` 编译 `src/kafka_*.cpp`）。
3. 安装配置；将 `bootstrap-servers` 设为 **toxiproxy/实验室或生产 bootstrap**（切勿混用）。
4. 确保 `outbox-path` 所在目录对 FreeSWITCH 用户可写。
5. `load mod_event_kafka`——流水线健康时，Kafka 重启后**不**需要 reload。

## 回滚
1. `unload mod_event_kafka`
2. 恢复先前的 `.so` 与不含 outbox 参数的配置（仍然有效；新参数可选）。
3. Outbox 数据库可以归档；若日后重新加载新构建，残留行可以安全保留。

## 本环境未验证
- 生产云端 Kafka
- 在本机完整加载新 `.so` 的 FreeSWITCH（Phase1 本机没有 FS 头文件）
