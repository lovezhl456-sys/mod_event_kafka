# Reliability boundaries

## Guarantees (target design)
- **At-least-once** delivery for events that successfully commit to the SQLite outbox and are not expired by `outbox-ttl-ms`.
- Stable **event_id** (UUIDv4) assigned at enqueue; retried produces reuse the same id (header `x-fs-event-id` when pipeline attaches it).
- Same-call ordering: outbox `fetch_due` orders by `call_uuid`, then `created_at_ms`.

## Loss window
- Crash **after** FS callback returns but **before** outbox `INSERT` commit → event can be lost (only in memory queue).
- Capacity exhaustion (mem or disk) → **explicit reject** + metrics/log; never silent drop.

## Stale events (outbox TTL)
- `outbox-ttl-ms` defaults to `120000` (2 minutes). `0` disables expiry and keeps the previous behavior (no age-based drop).
- Before the worker produces, a **pending** row with `now_ms - created_at_ms > outbox-ttl-ms` is set to `state=dead`, `last_error=expired_ttl`, and counted in `outbox_expired`. It is not produced.
- That drop is intentional: the payload is stale telephony state after a long outage, not a record that must be delivered hours later.
- Short outages **≤ TTL** do not expire rows. Those rows stay on the existing heal-and-drain path and are fully delivered without `reload mod_event_kafka`.
- In-flight rows are not expired in place. Delivery success still deletes the row (ACK). A failed delivery that returns the row to `pending` after the TTL is expired on the next pass and is not produced again.
- After recovery, dead/expired rows are not part of the due scan, so newly enqueued events are produced ahead of that dead letter. Expired rows are reclaimed only when they would otherwise block a new insert. Pending and in-flight rows are never deleted to make room.

## Duplicates
- Crash after broker ACK but before local `DELETE` → duplicate on restart. Consumers **must** dedupe on `event_id`.
- Producer rebuild / retry after ambiguous failure may also duplicate.

## Shutdown
1. Stop accepting enqueue  
2. Drain memory queue into outbox  
3. `rd_kafka_flush` + poll until idle or timeout  
4. Join worker/poll threads  
5. Destroy producer  
Un-ACKed rows remain on disk for next load (`requeue_in_flight` on start).
