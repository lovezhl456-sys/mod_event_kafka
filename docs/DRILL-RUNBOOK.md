# Kafka / mod_event_kafka 演练手册（一步步）

> 目标：在本机 lab 上按固定步骤复现「短断自愈 / 超 TTL 过期 / 恢复后新呼叫」。  
> 命令均来自本机已跑通路径。参数名、`x-fs-event-id`、状态字面量保持英文。  
> 实验室绝对路径与仓库 `lab/` 的对照见 [KAFKA-DEPLOY.md](KAFKA-DEPLOY.md) §2.1。本文步骤使用实验室绝对路径。

对应故障场景：短断是 [FAULT-SCENARIOS.md](FAULT-SCENARIOS.md) 的 FS-01；超 TTL 与恢复后新呼叫是 FS-09。

## 0. 前置检查

```bash
# Docker 可用
sg docker -c 'docker info' >/dev/null

# 桥接互通（本机曾需要）
sudo iptables-legacy -P FORWARD ACCEPT

# 镜像是否在本地（FS 需事先 build/commit）
sg docker -c 'docker images | rg "apache/kafka|toxiproxy|lab-freeswitch"'
```

确认 conf 含 `outbox-ttl-ms=120000`，且加载日志可见该值（超 TTL 演练硬前置）。

## 1. 起栈

```bash
cd /workspace/lab-mod-event-kafka
sg docker -c 'docker compose up -d'
sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic fs_events --partitions 3 --replication-factor 1"

sg docker -c 'docker rm -f lab-freeswitch' || true
sg docker -c 'docker run -d --name lab-freeswitch --network host lab-freeswitch:1.10.12-kafka \
  bash -c "export LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib; \
    /usr/local/freeswitch/bin/freeswitch -nonat -nf -nc -nosql -rp"'

# 等待 FS 就绪
sleep 5
sg docker -c 'docker exec lab-freeswitch env LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib \
  /usr/local/freeswitch/bin/fs_cli -x "module_exists mod_event_kafka"'
# 期望：true
```

可选：Kafka 经 toxiproxy 产消冒烟（见 `KAFKA-DEPLOY.md` §3.1）。

## 2. 基线拨测

优先快拨测（避免 park 路径长时间 `NO_ANSWER` 阻塞）：

```bash
/workspace/lab-mod-event-kafka/dialtest_fast.sh 5
# 或：/workspace/lab-mod-event-kafka/dialtest_originate.sh 2
```

验收要点：

- `module_exists=true`
- FS 日志出现 `enqueued event_id=...`
- 经 toxiproxy 消费可见 header `x-fs-event-id:...`

## 3. 短断演练（约 35s，小于 TTL）

对应 L-02 / L-07 / L-16a 思路：断连期间事件进 outbox，恢复后**无需 reload 模块**即可排空。

```bash
# 终端 A：切断全部入口 35 秒后自动恢复
/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh 35

# 终端 B：切断期间拨测（与 A 重叠）
/workspace/lab-mod-event-kafka/dialtest_fast.sh 10
```

恢复后等待若干秒让 worker 排空，再：

```bash
python3 /workspace/mod_event_kafka-fix/scripts/verify_event_ids.py \
  --injected <注入 id 列表文件> \
  --consumed <消费到的 x-fs-event-id 列表文件>
```

期望：注入集合与消费集合匹配（历史证据：`reports/l02-l07-fs-20260925-133618/`、`reports/l16-abc-20260925-215439/l16a`）。STATUS 记录的 L-16 根目录是 `reports/l16-abc-20260925-215439/`。

本仓库的 `scripts/verify_event_ids.py` 只接受一个目录参数：`scripts/verify_event_ids.py reports/<run-id>/`，目录里要有 `injected_ids.txt` 与 `consumed_ids.txt`。上面的 `--injected` / `--consumed` 是实验室机器 `/workspace/mod_event_kafka-fix/scripts/verify_event_ids.py` 的用法。调用哪一份，就按那一份的参数来；退出码 `0` 仍表示 VERIFY_OK。

一键脚本（本机已有）：

```bash
# 内含短断 + 超 TTL + 恢复后新呼叫；会写 reports/l16-abc-<时间戳>/
bash /workspace/mod_event_kafka-fix/scripts/run_l16_abc.sh
```

## 4. 超 TTL 全断演练（约 150s，大于默认 120s）

对应 L-16b：pending 行超时 → `state=dead`，`last_error=expired_ttl`，**不上 topic**。

```bash
/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh 150
# 切断期间拨测注入一批 id
/workspace/lab-mod-event-kafka/dialtest_fast.sh 10
```

恢复后：

1. 检查 outbox / 指标：过期计数 `outbox_expired`；死信为 `expired_ttl`
2. 消费 topic：这批过期 id **不应**出现在 `x-fs-event-id` 中（`expired_still_in_topic=0`）

## 5. 恢复后新呼叫（L-16c）

在 §4 恢复且代理已 `enabled=true` 后：

```bash
/workspace/lab-mod-event-kafka/dialtest_fast.sh 10
# verify_event_ids.py → 新一批应 VERIFY_OK（历史：30/30）
```

期望：TTL 丢弃过期后，新事件仍可正常投递；仍**无需**为 Kafka 恢复而 reload 模块。

## 6. 常用辅助命令

```bash
# 查代理状态
curl -fsS http://127.0.0.1:8474/proxies

# 手动切/开单个入口
curl -fsS -X POST http://127.0.0.1:8474/proxies/kafka1 \
  -H 'Content-Type: application/json' -d '{"enabled":false}'

# 带 header 消费
sg docker -c 'docker run --rm --network host apache/kafka:3.8.1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 127.0.0.1:19092 --topic fs_events \
  --from-beginning --timeout-ms 20000 \
  --property print.headers=true --property print.value=false'
```

## 7. 收尾拆栈

```bash
sg docker -c 'docker rm -f lab-freeswitch lab-toxiproxy lab-kafka-1'
# 镜像与 /workspace/lab-mod-event-kafka、模块源码保留，下次回归再起
```

## 8. 证据与对照

| 步骤 | 用例 | 本机证据目录（示例） |
|------|------|----------------------|
| 短断自愈 | L-02 / L-07 / L-16a | `reports/l02-l07-fs-20260925-133618/`、`reports/l16-abc-20260925-215439/l16a` |
| 超 TTL 死信 | L-16b | `reports/l16-abc-20260925-215439/l16b` |
| 恢复后新呼叫 | L-16c | `reports/l16-abc-20260925-215439/l16c` |

未在本手册逐步展开、但可在 `FAULT-SCENARIOS.md` 标注「文档草案」的项：单入口 disable、延迟/带宽 toxic、broker 进程杀、磁盘打满等——需要时再扩脚本，勿与已 PASS 的 L-16 结论混淆。

## 9. 安全提醒

- 演练 bootstrap **只能**是 toxiproxy 三端口。  
- 禁止把 lab conf 改成生产/云 Kafka 地址。  
- 本文不宣称生产验证。
