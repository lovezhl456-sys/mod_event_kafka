# mod_event_kafka
FreeSWITCH Kafka 插件

[![Build Status](https://github.com/voiceip/mod_event_kafka/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/voiceip/mod_event_kafka/actions/workflows/main.yml)

安装此插件可将 FreeSWITCH 事件发布到 Kafka。事件回调只把事件深拷贝到一个有界内存队列。工作线程将每条事件写入持久化的 SQLite outbox，然后再投递。Kafka 重启或短暂不可达之后，已提交的行会自动重发——无需重启 FreeSWITCH，也无需 `reload mod_event_kafka`。

丢失、重复与顺序的边界见 [docs/RELIABILITY.md](docs/RELIABILITY.md)。设计、测试、部署与实验室状态见 [docs/DESIGN.md](docs/DESIGN.md)、[docs/TEST-PLAN.md](docs/TEST-PLAN.md)、[docs/DEPLOY-ROLLBACK.md](docs/DEPLOY-ROLLBACK.md)、[docs/STATUS.md](docs/STATUS.md)。拨测 compose 与示例配置见 [lab/](lab/)。

配置 `event_kafka.conf.xml`。既有键名不变。可选的增量参数：`outbox-path`、`mem-queue-max`、`outbox-max-rows`、`message-timeout-ms`、`enable-idempotence`、`security-protocol`、`ssl-ca-location`。实验室拨测示例为 `lab/event_kafka.fs.conf.xml`。

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
并在 `modules.conf.xml` 中加入以下条目，以启用该模块的自动加载

```xml
 <load module="mod_event_kafka"/>
```



# 构建

## 基于 IDE 的构建

我们使用 vscode + docker，借助 `Visual Studio Code` 的 [remote-container](https://code.visualstudio.com/docs/remote/containers#_getting-started) 功能，在任意平台上简化构建。若初次接触，请遵循[入门指南](https://code.visualstudio.com/docs/remote/containers#_getting-started)。

在 `Visual Studio Code` 中打开项目，直接运行任务 `Release`。


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

`librdkafka` 必须提供 `rd_kafka_producev` 与消息头（1.x 或更新版本；Debian 9 自带的 0.9.3 过旧）。

单元测试（不依赖 FreeSWITCH 头文件；`test_outbox` 启用 ASan/UBSan）：

```
cmake -S . -B build && cmake --build build && ctest --test-dir build --output-on-failure
```

