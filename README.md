# mod_event_kafka
FreeSWITCH Kafka 插件


本插件用于将 FreeSWITCH 事件发布到 Kafka。事件回调只负责把事件深拷贝进有界内存队列；worker 线程先把每条事件写入持久化的 SQLite outbox，再进行投递。Kafka 重启或短暂不可达之后，流水线会重试已持久化且未过期的行；无需重启 FS 的恢复能力应在对应环境通过演练确认。

文档从 [docs/README.md](docs/README.md) 开始。排查故障先看 [覆盖矩阵](docs/FAULT-SCENARIOS.md)，运行演练看 [演练手册](docs/DRILL-RUNBOOK.md)，已跑结果与历史证据只在 [STATUS](docs/STATUS.md) 维护。

当前测试不能穷尽故障，也不代表已在云 Kafka 发布期间验证。默认 2 分钟 TTL 会使已经可投递的旧头部或无分组 pending 行过期；排在未完成头部后面的后续事件不过期。过期不是补发成功。

配置文件为 `event_kafka.conf.xml`，原有键名保持不变。新增的可选参数：`outbox-path`、`mem-queue-max`、`outbox-max-rows`、`message-timeout-ms`、`enable-idempotence`、`security-protocol`、`ssl-ca-location`。实验室拨测示例见 `lab/event_kafka.fs.conf.xml`。

```xml
<configuration name="event_kafka.conf" description="Kafka Event Configuration">
	<settings>
		<param name="bootstrap-servers" value="localhost:9092"/>
		<param name="topic" value="kafa-topic-name" /> 
		<param name="username" value="" />
		<param name="password" value="" />
		<param name="buffer-size" value="256" /> 
		<param name="compression" value="snappy"/>
		<param name="event-filter" value=""/> 
	</settings>
 </configuration>
```
然后在 `modules.conf.xml` 中加入以下条目，让模块随 FreeSWITCH 自动加载：

```xml
 <load module="mod_event_kafka"/>
```



# 构建

开发容器中的 Debian 9 配置是历史环境，不作为当前依赖锁定。无 FS 头文件时可先构建 core 测试；依赖与 Docker 替代构建见 [测试计划](docs/TEST-PLAN.md)。

## 手动构建

### 安装依赖
```bash
sudo apt-get install libfreeswitch-dev
sudo apt-get install build-essential pkg-config 
sudo apt-get install librdkafka-dev libsqlite3-dev libz-dev libssl-dev
```

### 构建

```
make
make install
```

`librdkafka` 必须支持 `rd_kafka_producev` 与消息头（建议使用发行版支持的 1.x/2.x 并核对所用 API；Debian 9 自带的 0.9.3 过旧）。

单元测试（不依赖 FreeSWITCH 头文件；core 与测试程序均启用 ASan/UBSan）：

```
cmake -S . -B build && cmake --build build && ctest --test-dir build --output-on-failure
```

