# Kafka 实验室

两套拓扑可以并存，端口和 Compose project 相互独立。仅用于隔离测试，不连接生产 FS/Kafka。

| 拓扑 | 启动文件 | 客户端 bootstrap | API | 可验证范围 |
|---|---|---|---|---|
| 原 Phase1 | `lab/docker-compose.yml` | `127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094` | 8474 | 一个 broker 的客户端断连 |
| 三 broker | `lab/cluster-compose.yml` | `127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094` | 8475 | 独立节点、RF=3、min ISR=2、滚动重启 |

## 三 broker 栈

每个代理指向不同 broker；所有 EXTERNAL 通告地址都经过对应代理。INTERNAL 用于 broker 复制和管理。Kafka 固定为 3.8.1；这套栈没有进行跨版本升级。

```bash
# 在仓库根目录执行
 docker compose -p event-kafka-cluster -f lab/cluster-compose.yml config --quiet
 docker compose -p event-kafka-cluster -f lab/cluster-compose.yml up -d
 docker compose -p event-kafka-cluster -f lab/cluster-compose.yml ps
```

三节点均健康后运行 [演练手册](DRILL-RUNBOOK.md)。runner 每轮新建 3 分区、RF=3、`min.insync.replicas=2` 的 topic，并检查每个分区的 leader 和完整 ISR。三个端口不是三个节点的证据，要核对 topic 元数据。

```bash
# 保存 broker 日志（reports 目录应先创建）
docker compose -p event-kafka-cluster -f lab/cluster-compose.yml logs --no-color > reports/brokers.log

# 停止本套实验室，保留数据卷和历史 topic
docker compose -p event-kafka-cluster -f lab/cluster-compose.yml down
```

不要自动执行 `down -v` 或全局 Docker 清理。数据卷含故障证据，需明确选择后再删除。

## 原单 broker 栈

```bash
docker compose -f lab/docker-compose.yml up -d
docker compose -f lab/docker-compose.yml ps
```

其三个 toxiproxy 前端均通向 `kafka-1:9094`，返回元数据时通常只通告 `127.0.0.1:19092`。只禁用 19093 或 19094 可能根本没有切断正在使用的 broker 链路。全断要覆盖所有实际通告路径。

## 网络与容量

- 客户端在宿主机运行，使用上表的回环地址；容器中的 `127.0.0.1` 是容器自己。Linux 可用 host network，其他平台优先使用宿主机原生构建。
- 新三节点栈预留至少约 3 GB 内存；每个 broker Java 堆最大 384 MB，进程还需要堆外内存。
- 健康失败时检查 Docker 资源、端口冲突和该 Compose 网络。不要为了演练全局修改主机 FORWARD 策略。
- SASL/TLS、云厂商代理、DNS 切换、真实丢包及跨版本升级不在本栈中。
