# Phase2 — FreeSWITCH + mod_event_kafka (lab)

## Runtime
- Image: `lab-freeswitch:1.10.12-kafka` (FS 1.10.12-release built from source on bookworm)
- Container: `lab-freeswitch` (`--network host`)
- Kafka bootstrap (toxiproxy ONLY): `127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094`
- Topic: `fs_events`
- Module: `/usr/local/freeswitch/lib/freeswitch/mod/mod_event_kafka.so`
- Conf: `/usr/local/freeswitch/etc/freeswitch/autoload_configs/event_kafka.conf.xml`
- buffer-size: `100000` (XML key `buffer-size`)
- Kafka record header: `x-fs-event-id`

## Restart FS
```bash
sg docker -c 'docker rm -f lab-freeswitch'
sg docker -c 'docker run -d --name lab-freeswitch --network host lab-freeswitch:1.10.12-kafka \
  bash -c "export LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib; \
    /usr/local/freeswitch/bin/freeswitch -nonat -nf -nc -nosql -rp"'
```

## Dialtest
```bash
/workspace/lab-mod-event-kafka/dialtest_originate.sh 2
# exact originate:
# originate {ignore_early_media=true,origination_caller_id_number=dialtest}loopback/park/default &park()
```

## Notes
- dsh/nvm not available on this box; used apt+docker directly.
- Do NOT point bootstrap at production/cloud Kafka.
