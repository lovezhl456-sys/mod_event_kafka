# 演练手册：短断、超 TTL 长断、恢复后新呼叫

按顺序做。全部命令面向实验室机器上的目录 `/workspace/lab-mod-event-kafka/`。本仓库同步后的说明和 compose 在 `lab/`（见 [KAFKA-DEPLOY.md](KAFKA-DEPLOY.md) 的路径表）。`dialtest_fast.sh` 与 `toxiproxy_cut_restore.sh` 以实验室绝对路径为准。

这是实验室演练，**不是生产验证**。不要把 bootstrap 指到生产或云端 Kafka。候选补丁 0001 的状态是 **NOT_VERIFIED**，演练使用当前 outbox 模块，不加载该补丁。

已归档结果（2026-09-25，数字属于那几次运行，新跑一轮会随拨号次数变化）：

| 步骤 | 用例 | 归档结果 | 证据目录 |
|------|------|----------|----------|
| 短断 35s | L-02/L-07、L-16a | FS：VERIFY_OK 60/60。L-16a：注入 60、匹配 60，`verify_rc=0` | `reports/l02-l07-fs-20260925-133618/`、`reports/l16-abc-20260925-215439/` |
| 长断 150s | L-16b | `dead`/`expired_ttl`，expired_ttl dead=30，`expired_still_in_topic=0`，consumed_matched=30/60 | `reports/l16-abc-20260925-215439/` |
| 恢复后新呼叫 | L-16c | VERIFY_OK 30/30，`verify_rc=0` | 同上 |

这些 `reports/` 路径记录在 [STATUS.md](STATUS.md)。它们是归档目录。新的演练另外写 `reports/<本次运行>/`，不要把旧目录当成刚才生成的文件。

park 的 loopback 可能没有 ANSWER，主题上多见 CREATE 与 HANGUP*。

## 0. 准备

两个终端都在实验室机器上。FreeSWITCH 使用宿主机网络，Kafka 只通过 toxiproxy 暴露给宿主机。

| 脚本 | 路径 |
|------|------|
| compose 与 Phase1 说明 | `/workspace/lab-mod-event-kafka/docker-compose.yml`、`README.md` |
| Phase2 说明 | `/workspace/lab-mod-event-kafka/README-PHASE2.md` |
| 拨测 | `/workspace/lab-mod-event-kafka/dialtest_originate.sh` |
| 快速拨测（L-16 用过，避免 park 长时间 NO_ANSWER） | `/workspace/lab-mod-event-kafka/dialtest_fast.sh` |
| 切断后按秒数恢复 | `/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh` |
| 核对 | 本仓库 `scripts/verify_event_ids.py` |

仓库内对应文件：`lab/docker-compose.yml`、`lab/README.md`、`lab/README-PHASE2.md`、`lab/dialtest_originate.sh`。

## 1. 起栈

```bash
cd /workspace/lab-mod-event-kafka
sudo iptables-legacy -P FORWARD ACCEPT
sg docker -c 'docker compose up -d'
sg docker -c 'docker compose ps'
```

`FORWARD` 那一行来自实验室 README：该机器上 iptables-legacy 的 FORWARD 若为 DROP，容器之间会 connection refused，KRaft 投票会出现 UNRECORDED。compose 只启动 `lab-kafka-1` 与 `lab-toxiproxy`。

等待 `lab-kafka-1` 健康后再创建主题（已存在则跳过）：

```bash
sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic fs_events --partitions 3 --replication-factor 1"
```

按 [lab/README-PHASE2.md](../lab/README-PHASE2.md) 启动 FreeSWITCH（compose 不包含它）：

```bash
sg docker -c 'docker rm -f lab-freeswitch'
sg docker -c 'docker run -d --name lab-freeswitch --network host lab-freeswitch:1.10.12-kafka \
  bash -c "export LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib; \
    /usr/local/freeswitch/bin/freeswitch -nonat -nf -nc -nosql -rp"'
```

## 2. 确认 bootstrap、主题、outbox-ttl-ms

代理应全部 `enabled=true`，监听 19092、19093、19094：

```bash
curl -s http://127.0.0.1:8474/proxies | python3 -m json.tool
```

主题 `fs_events`：

```bash
sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic fs_events"
```

模块已加载，且配置与 [lab/event_kafka.fs.conf.xml](../lab/event_kafka.fs.conf.xml) 一致。关注这几项：

- `bootstrap-servers` = `127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094`
- `topic` = `fs_events`
- `outbox-ttl-ms` = `120000`
- `buffer-size` = `100000`
- `outbox-path` = `/usr/local/freeswitch/var/lib/freeswitch/db/event_kafka_outbox.db`

```bash
sg docker -c "docker exec lab-freeswitch env LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib \
  /usr/local/freeswitch/bin/fs_cli -x 'module_exists mod_event_kafka'"

sg docker -c "docker exec lab-freeswitch grep -E 'name=\"(bootstrap-servers|topic|buffer-size|outbox-ttl-ms|outbox-path)\"' \
  /usr/local/freeswitch/etc/freeswitch/autoload_configs/event_kafka.conf.xml"
```

`module_exists` 的输出应为 true。`outbox-ttl-ms` 不是 `0`。随后的短断要短于 120 秒，长断要长于 120 秒。

## 3. 拨测

先打通一条，确认 fs_cli 可用：

```bash
/workspace/lab-mod-event-kafka/dialtest_originate.sh 2
```

脚本里的 originate 是：

```text
originate {ignore_early_media=true,origination_caller_id_number=dialtest}loopback/park/default &park()
```

L-16 归档使用快速拨测（`bgapi`，`originate_timeout=2`，然后 `hupall`）：

```bash
/workspace/lab-mod-event-kafka/dialtest_fast.sh 2
```

两条都会打印 `module_exists=`。正式切断前，用你这次要计数的方式再打一组「切断前」呼叫（归档的 FS L-02 是切断前 ×5）。例如：

```bash
/workspace/lab-mod-event-kafka/dialtest_originate.sh 5
```

把本次注入的 event_id 记到 `reports/<本次运行>/injected_ids.txt`（每行一个）。消费端从主题 `fs_events` 读取头 `x-fs-event-id`，写入同目录的 `consumed_ids.txt`。字段说明见 [TEST-PLAN.md](TEST-PLAN.md) §3.3。

## 4. 短断 35 秒

`toxiproxy_cut_restore.sh` 会禁用代理、睡眠、再启用，期间终端被占住。切断期间的拨号放在另一个终端。

终端 A：

```bash
/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh 35
```

看到 `== CUT` 之后、`== RESTORE` 之前，在终端 B 拨号（归档为切断期间 ×5）：

```bash
/workspace/lab-mod-event-kafka/dialtest_originate.sh 5
```

脚本等价于对 `kafka1`、`kafka2`、`kafka3` 发 `{"enabled":false}`，睡 35 秒，再发 `{"enabled":true}`。管理地址默认 `http://127.0.0.1:8474`。不要在这段时间里 `reload mod_event_kafka`。

RESTORE 之后等 outbox 排空（以分钟计，短于 TTL 的行应被投递）。然后：

```bash
scripts/verify_event_ids.py reports/<本次运行>/
```

短断的通过标准：退出码 0，打印 `VERIFY_OK`。注入集合与消费去重集合一致（缺失只允许发生在从未 `COMMIT` 的窗口，并要单独写明）。归档对照是 FS `reports/l02-l07-fs-20260925-133618/` 的 60/60，以及 L-16a 在 `reports/l16-abc-20260925-215439/` 中的 60/60。

若只复看归档、不新打呼叫：

```bash
scripts/verify_event_ids.py reports/l02-l07-fs-20260925-133618/
```

该目录在产生证据的实验室记录树下，不保证已经提交进 git。

## 5. 超 TTL：全断 150 秒

确认第 2 步里 `outbox-ttl-ms` 仍是 `120000`。150 秒大于该值。默认参数就是 150：

```bash
/workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh 150
```

和短断一样，睡眠期间用另一个终端拨号，这样才会有「切断过程中变老」的 pending 行。不要 reload。

RESTORE 之后检查两类结果，不要用「60 条全部在主题上」当作通过：

1. **过期行。** outbox（`outbox-path` 那一个 sqlite 文件）里，年龄已超过 TTL 的 pending 应为 `state=dead` 且 `last_error=expired_ttl`。这些 event_id 不应出现在主题 `fs_events` 上。归档 L-16b：expired_ttl dead=30，`expired_still_in_topic=0`。
2. **未过期行。** 恢复时年龄仍小于 TTL 的行应被投递，头为 `x-fs-event-id`。归档 L-16b：`consumed_matched=30 / 60`。

对**未过期**的注入子集运行：

```bash
scripts/verify_event_ids.py reports/<本次运行-未过期子集>/
```

退出码 0 且 `VERIFY_OK`。若把过期 id 和未过期 id 放进同一个 `injected_ids.txt`，又不把过期 id 写入 `rejected_ids.txt`，脚本会把过期 id 报成 missing（退出码 1）。那表示核对范围包含了有意不投递的行，需要和 outbox 里的 `expired_ttl` 对照，而不是把模块判失败。

容器里若有 `sqlite3`，可以按 DESIGN 的列看分布（表名 `outbox`，列 `state`、`last_error`）：

```bash
sg docker -c "docker exec lab-freeswitch sqlite3 \
  /usr/local/freeswitch/var/lib/freeswitch/db/event_kafka_outbox.db \
  \"SELECT state, last_error, COUNT(*) FROM outbox GROUP BY state, last_error;\""
```

没有 `sqlite3` 时，以本次 `reports/` 里的 outbox 快照和消费清单为准。归档结论以 `reports/l16-abc-20260925-215439/` 为准。

## 6. 恢复后新呼叫

第 5 步的脚本在结束时会把三个代理设回 `enabled=true`。再确认一次：

```bash
curl -s http://127.0.0.1:8474/proxies | python3 -m json.tool
```

然后只打新呼叫，单独记一份注入/消费清单（不要和长断那一份混在同一个文件里）：

```bash
/workspace/lab-mod-event-kafka/dialtest_fast.sh 5
scripts/verify_event_ids.py reports/<本次运行-恢复后>/
```

通过标准：退出码 0，`VERIFY_OK`。新事件被投递。过期死信留在 `dead`/`expired_ttl`（或按 DESIGN，仅在会堵住新插入时被回收），不要求它们出现在主题上。归档 L-16c：VERIFY_OK 30/30，`verify_rc=0`，证据仍是 `reports/l16-abc-20260925-215439/`。

## 7. 收尾拆栈

```bash
sg docker -c 'docker rm -f lab-freeswitch'
cd /workspace/lab-mod-event-kafka
sg docker -c 'docker compose down -v --remove-orphans'
```

`down -v` 会删掉本次 compose 卷。需要保留 outbox 或报告时，先从 `lab-freeswitch` 里拷出 `outbox-path` 指向的数据库，并确认 `reports/<本次运行>/` 已经写在容器外面。

拆完之后如要再演练，从第 1 步重新起栈。生产集群不使用本页的 `down -v` 和 toxiproxy 切断命令。
