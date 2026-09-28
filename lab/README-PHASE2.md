# Phase2 — FreeSWITCH + mod_event_kafka（实验室）

## 运行时
- 镜像：`lab-freeswitch:1.10.12-kafka`（在 bookworm 上从源码构建的 FS 1.10.12-release）
- 容器：`lab-freeswitch`（`--network host`）
- Kafka bootstrap（仅 toxiproxy）：`127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094`
- 主题：`fs_events`
- 模块：`/usr/local/freeswitch/lib/freeswitch/mod/mod_event_kafka.so`
- 配置：`/usr/local/freeswitch/etc/freeswitch/autoload_configs/event_kafka.conf.xml`
- buffer-size：`100000`（XML 键 `buffer-size`）
- Kafka 记录头：`x-fs-event-id`

## 重启 FS
```bash
sg docker -c 'docker rm -f lab-freeswitch'
sg docker -c 'docker run -d --name lab-freeswitch --network host lab-freeswitch:1.10.12-kafka \
  bash -c "export LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib; \
    /usr/local/freeswitch/bin/freeswitch -nonat -nf -nc -nosql -rp"'
```

## 拨测
```bash
# 仓库根目录；共享机：/workspace/lab-mod-event-kafka/dialtest_originate.sh 2
lab/dialtest_originate.sh 2
# 实际使用的 originate：
# originate {ignore_early_media=true,origination_caller_id_number=dialtest}loopback/park/default &park()
```

## 说明
- 本机没有 dsh/nvm；直接使用 apt+docker。
- 不要把 bootstrap 指向生产/云端 Kafka。
