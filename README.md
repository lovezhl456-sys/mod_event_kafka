# mod_event_kafka
Freeswitch Kafka Plugin 

[![Build Status](https://github.com/voiceip/mod_event_kafka/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/voiceip/mod_event_kafka/actions/workflows/main.yml)

Install this plugin to publish FreeSWITCH events to Kafka. The event callback only deep-copies the event onto a bounded memory queue. A worker thread inserts each event into a durable SQLite outbox and then produces it. After Kafka restarts or is briefly unreachable, committed rows are resent automatically — no FreeSWITCH restart and no `reload mod_event_kafka`.

Loss, duplicate, and ordering boundaries are in [docs/RELIABILITY.md](docs/RELIABILITY.md). Design, tests, deploy, and lab status: [docs/DESIGN.md](docs/DESIGN.md), [docs/TEST-PLAN.md](docs/TEST-PLAN.md), [docs/DEPLOY-ROLLBACK.md](docs/DEPLOY-ROLLBACK.md), [docs/STATUS.md](docs/STATUS.md). Dialtest compose and sample conf: [lab/](lab/).

Configure `event_kafka.conf.xml`. Existing keys are unchanged. Optional additive knobs: `outbox-path`, `mem-queue-max`, `outbox-max-rows`, `message-timeout-ms`, `enable-idempotence`, `security-protocol`, `ssl-ca-location`. The lab dialtest sample is `lab/event_kafka.fs.conf.xml`. 

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
and enable autoloading of the module by adding the following entry in `modules.conf.xml`

```xml
 <load module="mod_event_kafka"/>
```



# Building

## IDE Based Build

We use vscode + docker, to enable easy building on any platform with the use of [remote-container](https://code.visualstudio.com/docs/remote/containers#_getting-started) feature of `Visual Studio Code`. If you are new to this, follow the [getting started guide](https://code.visualstudio.com/docs/remote/containers#_getting-started) 

Open the project in `Visual Studio Code` and just Run Task `Release`.


## Manually Building

### Install Dependencies
```bash
sudo apt-get install libfreeswitch-dev
sudo apt-get install build-essential pkg-config 
sudo apt-get install librdkafka-dev libsqlite3-dev libz-dev libssl-dev
```

### Build

```
make
make install
```

`librdkafka` must provide `rd_kafka_producev` and message headers (1.x or newer; Debian 9's 0.9.3 is too old).

Unit tests (no FreeSWITCH headers; ASan/UBSan on `test_outbox`):

```
cmake -S . -B build && cmake --build build && ctest --test-dir build --output-on-failure
```


