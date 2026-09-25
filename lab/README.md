# Lab: Kafka Phase1 — single-node KRaft + toxiproxy (mod_event_kafka)

**WARNING: LAB ONLY — not production.** Isolated on this Grok Bot box. Do not attach
FreeSWITCH / production / cloud Kafka to this stack. FreeSWITCH is Phase 2 (skipped).

## Phase1 layout (current)

| Container       | Role                                      | Notes                                      |
|-----------------|-------------------------------------------|--------------------------------------------|
| `lab-kafka-1`   | Single KRaft broker+controller            | Internal only (no host publish)            |
| `lab-toxiproxy` | 3 frontends → same broker EXTERNAL:9094   | Host ports 19092–19094 + admin 8474        |

**3-broker cluster is deferred.** Phase1 uses one broker so the stack stays healthy
on this box; all three toxiproxy entry ports still proxy to `kafka-1:9094` so QA can
disable `kafka1`/`kafka2`/`kafka3` to simulate a full client disconnect.

Client path (only supported bootstrap):

```
127.0.0.1:19092  →  toxiproxy(kafka1) → kafka-1:9094  (advertised EXTERNAL 127.0.0.1:19092)
127.0.0.1:19093  →  toxiproxy(kafka2) → kafka-1:9094  (same broker)
127.0.0.1:19094  →  toxiproxy(kafka3) → kafka-1:9094  (same broker)
```

**Bootstrap servers string:**

```
127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094
```

Toxiproxy admin API: `http://127.0.0.1:8474`

Test topic: `fs_events` (partitions=3, replication-factor=1)

Images: `apache/kafka:3.8.1`, `shopify/toxiproxy:2.1.4`  
Heap: `-Xmx384M -Xms256M`

## Start / stop

```bash
cd /workspace/lab-mod-event-kafka
# If inter-container traffic fails (UNRECORDED votes / connection refused):
sudo iptables-legacy -P FORWARD ACCEPT
sg docker -c 'docker compose up -d'
sg docker -c 'docker compose ps'
sg docker -c 'docker compose down -v --remove-orphans'
```

## Health / smoke (via toxiproxy only)

Prefer a one-shot client on the compose network (broker containers resolve
`127.0.0.1` to themselves — do not smoke from inside `lab-kafka-1` against
host-mapped proxy ports).

Because `EXTERNAL` advertises `127.0.0.1:19092`, one-shot clients must use
`--network host` (or run on the box host). Bridge + `host.docker.internal` reaches
toxiproxy for bootstrap metadata, then fails reconnecting to advertised `127.0.0.1`.

```bash
BOOT=127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094
IMG=apache/kafka:3.8.1

# Create topic (INTERNAL is fine for admin)
sg docker -c "docker exec lab-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic fs_events --partitions 3 --replication-factor 1"

# Produce / consume via toxiproxy bootstrap (host network)
echo 'hello-fs-events' | sg docker -c "docker run --rm -i --network host $IMG \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $BOOT --topic fs_events"

sg docker -c "docker run --rm --network host $IMG \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BOOT \
  --topic fs_events --from-beginning --timeout-ms 15000"
```

## Simulate full disconnect (toxiproxy)

Disable all three proxies (clients see disconnect; the single Kafka process stays up):

```bash
for p in kafka1 kafka2 kafka3; do
  curl -s -X POST "http://127.0.0.1:8474/proxies/$p" \
    -H 'Content-Type: application/json' -d '{"enabled":false}'
  echo
done
```

Restore:

```bash
for p in kafka1 kafka2 kafka3; do
  curl -s -X POST "http://127.0.0.1:8474/proxies/$p" \
    -H 'Content-Type: application/json' -d '{"enabled":true}'
  echo
done
```

Inspect: `curl -s http://127.0.0.1:8474/proxies | python3 -m json.tool`

## Resource notes

- Storage driver is `vfs` — logs stay in-container under `/tmp`.
- One broker × ~384M heap + toxiproxy; 3-broker RF=2 layout postponed.
