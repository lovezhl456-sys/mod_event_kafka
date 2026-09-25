# mod_event_kafka Outbox Resilience Design

Upstream: voiceip/mod_event_kafka@master  
Goal: After cloud Kafka restart/flap, already-persisted events are automatically resent without FreeSWITCH restart or `reload mod_event_kafka`.

## Why the candidate patch is insufficient

The draft `0001-kafka-restart-resilience.patch` adds background poll and producer recreate, and removes detached retries. That helps reconnect progress, but **recreate drops librdkafka in-memory queue**, so events accepted during the outage are still lost. It also still produces from the FreeSWITCH callback path under backpressure. It is treated as a **negative reference only**, not merged as-is.

## Architecture

```
FS event_handler
    │  (never blocks on network / disk sync beyond bounded enqueue)
    ▼
Bounded memory queue (deep-copied payload + key + event_id)
    │
    ▼
Fixed worker thread(s)
    │  1) BEGIN; INSERT outbox (pending); COMMIT   ← durable
    │  2) rd_kafka_produce (copy / owned buffer)
    │  3) mark state=in_flight
    ▼
Background poll thread  →  dr_msg_cb
    │  success → BEGIN; DELETE/ACK outbox; COMMIT
    │  retryable fail → state=pending, backoff
    │  fatal → safe rebuild producer, keep outbox rows
```

### Threads

| Thread | Duty |
|--------|------|
| FS callback | Serialize JSON, deep-copy key, allocate stable `event_id`, try enqueue; on full → increment reject metric + log WARNING (never silent) |
| worker | Drain memory queue → SQLite outbox → produce; also scan pending/retry due rows |
| poll | `rd_kafka_poll` continuously; drive delivery reports and reconnect |

### Ownership

- Enqueue owns heap copies of JSON body and key until produce succeeds with a transfer strategy that does **not** use fire-and-forget detach.
- Prefer `rd_kafka_producev` with explicit copy (`RD_KAFKA_MSG_F_COPY`) after outbox insert, so outbox remains source of truth and free of MSG_F_FREE UAF.
- Detached retry threads are **forbidden**.

### Outbox schema (SQLite)

```sql
CREATE TABLE outbox (
  event_id TEXT PRIMARY KEY,
  topic TEXT NOT NULL,
  msg_key TEXT,
  payload BLOB NOT NULL,
  state TEXT NOT NULL,           -- pending | in_flight | dead
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at_ms INTEGER NOT NULL,
  last_error TEXT,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  call_uuid TEXT                  -- optional, for same-call ordering
);
CREATE INDEX idx_outbox_due ON outbox(state, next_attempt_at_ms);
CREATE INDEX idx_outbox_call ON outbox(call_uuid, created_at_ms);
```

Delete row only after delivery success **and** local ACK transaction commits.

### Error classes

| Class | Examples | Action |
|-------|----------|--------|
| Transient | disconnect, QUEUE_FULL, transport timeout | keep row, exponential backoff (+jitter), metrics |
| Fatal client | `RD_KAFKA_RESP_ERR__FATAL` | destroy/recreate producer+topic under mutex; outbox retained |
| Permanent | auth fail, topic auth, invalid msg | state=`dead`, keep evidence in `last_error`, alert metric; no tight loop |

### Config compatibility

Existing keys unchanged: `bootstrap-servers`, `topic`, `topic-prefix`, `username`, `password`, `buffer-size`, `compression`, `event-filter`.

Additive (defaults safe):

| Param | Default | Meaning |
|-------|---------|---------|
| `outbox-path` | `/var/lib/freeswitch/event_kafka_outbox.db` | SQLite path |
| `mem-queue-max` | `10000` | bounded memory queue depth |
| `outbox-max-rows` | `100000` | disk capacity; reject + alert when full |
| `outbox-max-bytes` | `512MB` | soft disk budget |
| `worker-poll-ms` | `50` | worker idle sleep |
| `rdkafka-poll-ms` | `100` | poll thread |
| `message-timeout-ms` | `30000` | **must** be applied via topic conf (upstream bug) |
| `enable-idempotence` | `true` | `enable.idempotence=true` |
| `security-protocol` | auto / `SASL_PLAINTEXT` when user set | allow `SASL_SSL`/`SSL` |
| `ssl-ca-location` | empty | TLS |
| `acks` | `all` | durability |

Message **key** remains `Channel-Call-UUID` when present (unchanged).  
Stable **event_id** is a UUIDv4 created at enqueue, stored in outbox, and attached as Kafka header `x-fs-event-id` (body JSON unchanged for format compatibility). Consumers dedupe on that header.

### Reliability boundaries (explicit)

1. **Crash before outbox COMMIT**: event can be lost (in-memory only). Window = enqueue → COMMIT.
2. **Produce success path unclear / broker ACK then crash before local DELETE**: possible **duplicate** after restart; consumers must dedupe on `x-fs-event-id`.
3. **Same-call ordering**: worker sends per-`call_uuid` FIFO by `created_at_ms` when key present; cross-call unordered. At-least-once + header dedupe.
4. **Capacity exhausted**: reject new enqueue, increment `event_kafka_rejected_total`, log at WARNING/CRIT — never silent drop, never block media threads indefinitely.
5. **Shutdown**: unbind events → stop accepting enqueue → worker drains memory into outbox → flush/poll until idle or timeout → join threads → destroy producer. Un-ACKed rows remain on disk for next load.

### Metrics (log + optional JSON file counters)

- `enqueued`, `rejected_mem_full`, `rejected_disk_full`
- `outbox_pending`, `outbox_in_flight`, `outbox_dead`
- `produce_ok`, `produce_fail`, `delivery_fail`, `producer_rebuilds`
- `oldest_pending_age_ms`

## Test plan summary

See `docs/TEST-PLAN.md`. Unit + ASan/UBSan required on this box; full FS+3-broker+fault-proxy lab requires Docker/Podman (not yet installed here) or an external lab host from 安装软件大师.

## Delivery layout

```
mod_event_kafka.cpp   FS callback glue (deep-copy enqueue only)
include/              bounded queue, outbox, pipeline headers
src/                  kafka_outbox.cpp, kafka_pipeline.cpp
docs/DESIGN.md        this file
docs/TEST-PLAN.md
docs/RELIABILITY.md   loss/dup/order boundaries
docs/DEPLOY-ROLLBACK.md
tests/                unit tests (no FS); CMakeLists.txt at repo root
lab/                  compose + toxiproxy + dialtest sample conf
scripts/              verify_event_ids.py
reports/              filled after real runs; mark untested explicitly
```
