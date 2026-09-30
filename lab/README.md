# 实验室：Kafka Phase1 — 单节点 KRaft + toxiproxy（mod_event_kafka）

**警告：仅限实验室，不是生产环境。** 本栈隔离在这台 Grok Bot 机器上。不要把
FreeSWITCH / 生产 / 云端 Kafka 接到这套栈上。FreeSWITCH 属于 Phase 2（Phase1 中跳过）。

## Phase1 布局（当前）

| 容器       | 角色                                      | 备注                                      |
|-----------------|-------------------------------------------|--------------------------------------------|
| `lab-kafka-1`   | 单个 KRaft broker+controller            | 仅内部访问（不向宿主机发布端口）            |
| `lab-toxiproxy` | 3 个前端 → 同一 broker 的 EXTERNAL:9094   | 宿主机端口 19092–19094 + 管理端口 8474        |

**3 broker 集群推迟。** Phase1 只用一个 broker，以保证本机上的栈稳定健康；
三个 toxiproxy 入口端口仍都代理到 `kafka-1:9094`，这样 QA 可以禁用
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

测试 topic：`fs_events`（partitions=3，replication-factor=1）

镜像：`apache/kafka:3.8.1`、`shopify/toxiproxy:2.1.4`  
堆：`-Xmx384M -Xms256M`

## 启动 / 停止

```bash
# 从仓库根目录进入 lab/；共享机上对应目录为 /workspace/lab-mod-event-kafka
cd lab
# 若容器间通信失败（UNRECORDED votes / connection refused）：
sudo iptables-legacy -P FORWARD ACCEPT
sg docker -c 'docker compose up -d'
sg docker -c 'docker compose ps'
sg docker -c 'docker compose down -v --remove-orphans'
```

## 健康检查 / 冒烟（仅经 toxiproxy）

优先在 compose 网络上使用一次性客户端（在 broker 容器内，
`127.0.0.1` 指向容器自身，因此不要在 `lab-kafka-1` 内部对
宿主机映射的代理端口做冒烟）。

由于 `EXTERNAL` 通告的是 `127.0.0.1:19092`，一次性客户端必须使用
`--network host`（或直接在宿主机上运行）。使用 bridge + `host.docker.internal` 虽能连上
toxiproxy 并拿到 bootstrap 元数据，但随后重连到通告地址 `127.0.0.1` 时会失败。

```bash
BOOT=127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094
IMG=apache/kafka:3.8.1

# 创建 topic（管理操作走 INTERNAL 即可）
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

## 断流与拨测脚本

以下命令在仓库根目录执行。在共享实验室机器上，同名脚本位于 `/workspace/lab-mod-event-kafka/`（不带 `lab/` 前缀）。

| 脚本 | 作用 |
|------|------|
| `lab/toxiproxy_cut_restore.sh` | 断开全部 toxiproxy 入口，若干秒后自动恢复（默认 150s；短断示例 `35`） |
| `lab/dialtest_fast.sh` | 快拨测（bgapi，避免 park 路径长时间 `NO_ANSWER`） |
| `lab/dialtest_originate.sh` | loopback/park 拨测，产生 CHANNEL_CREATE / ANSWER / HANGUP* |

```bash
lab/toxiproxy_cut_restore.sh 35
lab/dialtest_fast.sh 5
# 或：lab/dialtest_originate.sh 2
```

## 模拟完全断开（toxiproxy）

优先使用上一节的 `lab/toxiproxy_cut_restore.sh`。也可手工禁用全部三个代理（客户端看到的是断开，而唯一的 Kafka 进程仍在运行）：

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

- 存储驱动是 `vfs`，日志留在容器内的 `/tmp`。
- 一个 broker（约 384M 堆）+ toxiproxy；3 broker、RF=2 的布局推迟。
