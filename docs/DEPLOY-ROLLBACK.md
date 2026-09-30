# 部署与回滚

先在与生产一致的 FS ABI、librdkafka 版本和配置下联调；core 测试通过不等于 `.so` 可加载。演练结果范围见 [STATUS](STATUS.md)。

## 安装检查

1. 安装构建所需的 FS 头文件、librdkafka、SQLite3、zlib、OpenSSL 开发包。
2. `make` 构建模块；保存源码 commit、`.so` hash、FS 和运行时 librdkafka 版本。
3. 备份现有模块和配置；明确使用实验室或生产 bootstrap，避免混用。
4. 配置 FS 用户可写的 outbox 路径，明确 TTL、容量和压缩设置。
5. 在已安排的加载窗口安装并加载模块，确认实际消费到带 `x-fs-event-id` 的事件。

正常故障恢复演练期间不 reload；是否需要部署新二进制是另一件事。

## 回滚

1. 停止新事件进入模块并卸载，保留日志、DB、WAL/SHM 的一致备份。
2. 恢复匹配的旧 `.so` 与配置后加载。
3. 旧无 outbox 模块不会自动重放新模块的 SQLite 数据。保留数据库，另行决定重放、过期或归档，防止重复和旧状态回灌。

不要为了“恢复”直接删除 outbox，也不要把单次重载成功写成无需重载自愈。
