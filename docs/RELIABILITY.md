# Reliability boundaries

## Guarantees (target design)
- **At-least-once** delivery for events that successfully commit to the SQLite outbox.
- Stable **event_id** (UUIDv4) assigned at enqueue; retried produces reuse the same id (header `x-fs-event-id` when pipeline attaches it).
- Same-call ordering: outbox `fetch_due` orders by `call_uuid`, then `created_at_ms`.

## Loss window
- Crash **after** FS callback returns but **before** outbox `INSERT` commit → event can be lost (only in memory queue).
- Capacity exhaustion (mem or disk) → **explicit reject** + metrics/log; never silent drop.

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
