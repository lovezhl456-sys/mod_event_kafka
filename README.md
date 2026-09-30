# mod_event_kafka
FreeSWITCH Kafka 插件

[![Build Status](https://github.com/voiceip/mod_event_kafka/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/voiceip/mod_event_kafka/actions/workflows/main.yml)

本插件用于将 FreeSWITCH 事件发布到 Kafka。事件回调只负责把事件深拷贝进有界内存队列；worker 线程先把每条事件写入持久化的 SQLite outbox，再进行投递。Kafka 重启或短暂不可达之后，已提交的行会自动重发，既无需重启 FreeSWITCH，也无需 `reload mod_event_kafka`。

丢失、重复与顺序的边界见 [docs/RELIABILITY.md](docs/RELIABILITY.md)。设计、测试、部署与实验室状态分别见 [docs/DESIGN.md](docs/DESIGN.md)、[docs/TEST-PLAN.md](docs/TEST-PLAN.md)、[docs/DEPLOY-ROLLBACK.md](docs/DEPLOY-ROLLBACK.md)、[docs/STATUS.md](docs/STATUS.md)。拨测用的 compose 与示例配置见 [lab/](lab/)。

## 文档导航

表中最后三篇运维文档只描述实验室栈与已归档的证据，不代表已在生产环境验证。

| 文档 | 内容 |
|------|------|
| [docs/DESIGN.md](docs/DESIGN.md) | outbox、`outbox-ttl-ms` 与错误分类 |
| [docs/RELIABILITY.md](docs/RELIABILITY.md) | 丢失、重复、顺序与过期边界 |
| [docs/TEST-PLAN.md](docs/TEST-PLAN.md) | 单元与实验室用例（含 L-03…L-15） |
| [docs/STATUS.md](docs/STATUS.md) | 已跑结果与证据路径 |
| [docs/DEPLOY-ROLLBACK.md](docs/DEPLOY-ROLLBACK.md) | 模块安装与回滚 |
| [docs/KAFKA-DEPLOY.md](docs/KAFKA-DEPLOY.md) | 实验室 Kafka / toxiproxy 部署（仓库内使用 `lab/` 相对路径；共享机上的绝对路径仍可用） |
| [docs/FAULT-SCENARIOS.md](docs/FAULT-SCENARIOS.md) | FS-01…FS-11 与 L-xx 的映射；FS-01、FS-09 已附断流命令；FS-11 为 broker **+87** `INVALID_RECORD` 草案（未 empirically，不是 **-187**） |
| [docs/DRILL-RUNBOOK.md](docs/DRILL-RUNBOOK.md) | 短断 35s、超 TTL 断连 150s、恢复后新呼叫的分步演练；§10 为 FS-11 草案（未 empirically） |

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

## 基于 IDE 的构建

我们使用 VS Code + Docker：借助 `Visual Studio Code` 的 [remote-container](https://code.visualstudio.com/docs/remote/containers#_getting-started) 功能，可以在任意平台上便捷地构建。初次使用请参阅[入门指南](https://code.visualstudio.com/docs/remote/containers#_getting-started)。

在 `Visual Studio Code` 中打开项目后，直接运行 `Release` 任务即可。


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

`librdkafka` 必须支持 `rd_kafka_producev` 与消息头（需 1.x 或更高版本；Debian 9 自带的 0.9.3 过旧）。

单元测试（不依赖 FreeSWITCH 头文件；`test_outbox` 启用了 ASan/UBSan）：

```
cmake -S . -B build && cmake --build build && ctest --test-dir build --output-on-failure
```

