# 实验室：Kafka Phase1 — 单节点 KRaft + toxiproxy（mod_event_kafka）

**警告：仅限实验室 — 不是生产环境。** 隔离在本 Grok Bot 机器上。不要把
FreeSWITCH / 生产 / 云端 Kafka 接到这套栈上。FreeSWITCH 属于 Phase 2（已跳过）。

## Phase1 布局（当前）

| 容器       | 角色                                      | 备注                                      |
|-----------------|-------------------------------------------|--------------------------------------------|
| `lab-kafka-1`   | 单个 KRaft broker+controller            | 仅内部（不向宿主机发布端口）            |
| `lab-toxiproxy` | 3 个前端 → 同一 broker 的 EXTERNAL:9094   | 宿主机端口 19092–19094 + 管理端口 8474        |

**3 broker 集群推迟。** Phase1 使用一个 broker，以便本机上栈保持健康；
三个 toxiproxy 入口端口仍都代理到 `kafka-1:9094`，以便 QA 禁用
`kafka1`/`kafka2`/`kafka3` 来模拟客户端完全断开。

客户端路径（唯一支持的 bootstrap）：

```
127.0.0.1:19092  →  toxiproxy(kafka1) → kafka-1:9094  （对外通告 EXTERNAL 127.0.0.1:19092）
127.0.0.1:19093  →  toxiproxy(kafka2) → kafka-1:9094  （同一 broker）
127.0.0.1:19094  →  toxiproxy(kafka3) → kafka-1:9094  （同一 broker）
```

**Bootstrap servers 字符串：**

```
127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094
```

Toxiproxy 管理 API：`http://127.0.0.1:8474`

测试主题：`fs_events`（partitions=3，replication-factor=1）

镜像：`apache/kafka:3.8.1`、`shopify/toxiproxy:2.1.4`  
堆：`-Xmx384M -Xms256M`

## 启动 / 停止

```bash
# 仓库根目录进入 lab/；共享机上该目录为 /workspace/lab-mod-event-kafka
cd lab
# 若容器间流量失败（UNRECORDED votes / connection refused）：
sudo iptables-legacy -P FORWARD ACCEPT
sg docker -c 'docker compose up -d'
sg docker -c 'docker compose ps'
sg docker -c 'docker compose down -v --remove-orphans'
```

## 健康检查 / 冒烟（仅经 toxiproxy）

优先在 compose 网络上用一次性客户端（broker 容器把
`127.0.0.1` 解析成自己——不要在 `lab-kafka-1` 内部对
宿主机映射的代理端口做冒烟）。

因为 `EXTERNAL` 通告的是 `127.0.0.1:19092`，一次性客户端必须使用
`--network host`（或直接在宿主机上运行）。Bridge + `host.docker.internal` 能连上
toxiproxy 拿到 bootstrap 元数据，随后重连到通告的 `127.0.0.1` 会失败。

```bash
BOOT=127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094
IMG=apache/kafka:3.8.1

# 创建主题（管理操作用 INTERNAL 即可）
sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic fs_events --partitions 3 --replication-factor 1"

# 经 toxiproxy bootstrap 生产 / 消费（宿主机网络）
echo 'hello-fs-events' | sg docker -c "docker run --rm -i --network host $IMG \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $BOOT --topic fs_events"

sg docker -c "docker run --rm --network host $IMG \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BOOT \
  --topic fs_events --from-beginning --timeout-ms 15000"
```

## 切流与拨测脚本

在仓库根目录执行。共享实验室机器上，同名脚本在 `/workspace/lab-mod-event-kafka/`（无 `lab/` 前缀）。

| 脚本 | 作用 |
|------|------|
| `lab/toxiproxy_cut_restore.sh` | 切断全部 toxiproxy 入口若干秒后自动恢复（默认 150s；短切示例 `35`） |
| `lab/dialtest_fast.sh` | 快拨测（bgapi，避免 park 长时间 `NO_ANSWER`） |
| `lab/dialtest_originate.sh` | loopback/park 拨测，产生 CHANNEL_CREATE / ANSWER / HANGUP* |

```bash
lab/toxiproxy_cut_restore.sh 35
lab/dialtest_fast.sh 5
# 或：lab/dialtest_originate.sh 2
```

## 模拟完全断开（toxiproxy）

优先用上一节的 `lab/toxiproxy_cut_restore.sh`。手工禁用全部三个代理（客户端看到断开；单个 Kafka 进程保持运行）：

```bash
for p in kafka1 kafka2 kafka3; do
  curl -s -X POST "http://127.0.0.1:8474/proxies/$p" \
    -H 'Content-Type: application/json' -d '{"enabled":false}'
  echo
done
```

恢复：

```bash
for p in kafka1 kafka2 kafka3; do
  curl -s -X POST "http://127.0.0.1:8474/proxies/$p" \
    -H 'Content-Type: application/json' -d '{"enabled":true}'
  echo
done
```

查看：`curl -s http://127.0.0.1:8474/proxies | python3 -m json.tool`

## 资源说明

- 存储驱动是 `vfs`——日志留在容器内的 `/tmp`。
- 一个 broker × 约 384M 堆 + toxiproxy；3 broker、RF=2 的布局推迟。
