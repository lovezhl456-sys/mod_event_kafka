# Durable per-call ordering

This change fixes application retry overtaking. It does **not** infer a missing source order from
UUIDs, timestamps, Kafka offsets, or the order in which concurrent callbacks acquire a lock.

## Contract

The ordered stream is `(topic, Channel-Call-UUID)`. Its source owns a persistent, contiguous,
zero-based sequence for **the selected events in that stream**. A UUID must identify a fresh call
and must not be reused. Retries preserve the original payload, event ID, source sequence and key.
For a fixed Kafka partition count, a stable nonempty key, one call owner, and acknowledged broker
writes that survive the broker failure model, first occurrences in Kafka preserve that sequence.
Different calls may be in flight concurrently, including calls sharing one partition.

`KafkaPipeline::enqueue(..., source_sequence)` commits the event and ordering metadata to SQLite
before returning success. A future sequence may arrive before its predecessor; it waits for the
missing sequence, even if no predecessor row exists yet. Source sequence is included in the
`x-fs-call-sequence` Kafka header; `x-fs-event-id` remains the retry/deduplication identity.
The original payload is never rewritten to make a validation sequence look ordered.

Compatibility calls omitting the optional sequence allocate a durable **admission** ordinal in the
same SQLite transaction. They guarantee accepted admission order, not upstream business order.
A stream cannot change between admission/source modes or change its Kafka key. Repeated source
sequences, already acknowledged sequences, and conflicting keys are rejected; this submission API
is not an idempotent upstream RPC. A caller must retain a failed admission and reconcile ambiguous
admissions; skipping a rejected sequence intentionally blocks the stream.

The FreeSWITCH adapter defaults `require-source-sequence=1`. Call events must carry
`Kafka-Call-Sequence`; missing or malformed sequence is rejected and logged. `CHANNEL_*` events
without a call identity are also rejected. Native `Event-Sequence` is global, has per-call gaps,
and is **not** a substitute. Filtering must occur before the upstream per-call sequence is assigned.
Setting the option to `0` explicitly chooses the weaker compatibility admission-order contract;
this must not be described as source-order validation.

**Rollout prerequisite:** integrate a real source owner that emits this header and persists its
sequence across its own restart. This repository cannot recover an earlier event that has not yet
arrived from an unconstrained concurrent FreeSWITCH dispatcher. The cloud tests exercise the core
and compile the module, but do not load the module or validate that upstream integration.
Do not deploy the default strict configuration to an existing unmodified FreeSWITCH source: its
call events will be rejected. Non-call events without a call UUID remain ungrouped.

## Durable state machine

`call_streams.next_seq` is the only sendable sequence. `fetch_due` intersects that cursor with a
pending, due row; it does not discard the cursor when an older row is in flight, in backoff, dead,
or absent. `mark_in_flight` rechecks the head. An ACK atomically advances the cursor and deletes
that same in-flight row. A failure returns that row to pending without changing its sequence.

Synchronous permanent produce errors and permanent delivery errors mark the original row dead.
TTL also marks rows dead. For a call, these rows are **barriers**, not capacity-reclaim candidates.
Later rows cannot be sent past them. Ungrouped TTL rows retain their previous reclaim behavior.
An operator can stop the pipeline and call `Outbox::retry_dead(id)` to retry the **same** event after
fixing its cause. For TTL, first explicitly choose a suitable TTL policy; retrying does not falsify
`created_at_ms`. An irreparable poison event quarantines the entire call. No automatic skip/delete,
renumbering, or gap-ack API is provided. Releasing a poison call by discarding a source event would
require an explicitly approved business gap/compensation protocol outside this change.

A single worker owns produce, poll and producer lifecycle operations. Delivery callbacks request
rebuild; they never destroy their own producer. Rebuild purges and services the old producer's
callbacks, destroys it, returns remaining in-flight rows to pending, and creates a new producer.
Startup performs the same durable requeue, with cursors unchanged. A sidecar advisory lock excludes
another cooperating instance using the same outbox path; it is not distributed call ownership and
cannot constrain legacy binaries that ignore the lock or another host with a different database.
Errors in durable state transitions stop admission and sending rather than pretending an ACK was
committed. They increment `state_errors` and log the error.

Delivery is still **at least once**: broker ACK loss or a crash between broker append and the SQLite
ACK transaction can cause repeated event IDs. No exactly-once claim is made. Consumers must dedupe
stable event IDs (and source positions) atomically with business side effects, and must not skip a
gap. The integration verifier retains every raw duplicate and separately checks first-occurrence
order, raw nondecreasing order, per-call partition, payload sequence and header sequence.

## Upgrade, capacity and rollback

Schema version 2 adds `outbox.call_seq`, a unique `(topic,call_uuid,call_seq)` index, and durable
`call_streams` cursors/key/mode. Fresh and **empty** legacy outboxes upgrade transactionally.
A nonempty legacy outbox fails closed before schema migration; its source order cannot be proved.
Back it up, preserve its WAL consistently, reconcile it against original source/consumer evidence,
and explicitly drain/quarantine it before upgrade. Never fabricate source sequence during migration.
Unknown future schema versions and missing ordered cursors also fail closed.

Do not point an old binary at a v2 outbox: it does not know the cursor rules. Rollback needs a stopped
publisher and a separately reconciled backup, not an automatic downgrade. No production migration
has been executed by this work.

SQLite uses WAL and `synchronous=FULL`. Admission now performs a durable disk transaction on the
calling thread. The old `mem-queue-max` setting/metric is retained for source compatibility but is
not used by this path. Outbox row and payload-byte limits remain hard admission limits, and retained
stream metadata is separately limited to `outbox-max-rows` streams. The payload-byte limit is **not**
a cap on SQLite pages, indexes, WAL, keys or total filesystem use. Monitor and reserve disk space.

Completed cursors deliberately remain, so a restart cannot restart an old call at sequence zero.
They are not silently GC'd: after the stream limit, new calls are explicitly refused. Production
needs a completed-call retention/retirement procedure based on source-confirmed closure, no remaining
outbox rows and no possible late/replayed source submissions; this candidate does not implement an
automatic retirement service. Raising the limit trades storage for a longer retention horizon.

Per-call throughput is bounded by one successful head at a time, broker ACK latency, polling and
SQLite commits. Other calls can progress; head selection uses least-recently-served call time before
age to avoid continuously selecting only the oldest busy calls. There is no promise of a precise
latency/fairness bound under overload. Retained poison calls can eventually consume the shared disk
quota and cause visible global admission rejection. SQLite FULL fsync and large backlogs require
real storage/FreeSWITCH-media-load qualification before rollout.

Fixed partition count/topic identity is a precondition: this patch does not migrate an active call
between partition counts, topics or independent producers. Do not change them during a call.

## Tests and reproduction

`test_ordering` covers head-only scheduling, backoff/in-flight/dead blocking, source gaps,
concurrent admission, exact cursor recovery, ownership, TTL barriers, empty/blocked migration and
durable admission. The existing outbox TTL tests now isolate independent streams where their purpose
is a TTL boundary; the new tests explicitly assert that expiry cannot unblock a same-call successor.

CMake builds `test_ordering_drill` and `order_readback`. The cloud also directly compiled both core
sources and unit tests with ASan/UBSan, and built the actual module with the saved SDK. The broker
experiment uses a local TCP cut/ACK-loss proxy and a fresh 3-partition topic per scenario. This is
one broker/RF1, not a production durability or replication failover certification.

The evidence bundle contains pinned lab startup/proxy scripts, baseline driver, all commands,
source plan, admission sequence/time/latency, per-process metrics, DB snapshots before SIGKILL,
raw independent-consumer offsets/payloads/IDs, duplicates and final cursors. To reuse its workspace:

```bash
export ORDER_WORKSPACE=/workspace/kafka-order-fix-evidence
export ORDER_DRIVER=/absolute/build/test_ordering_drill
export ORDER_READER=/absolute/build/order_readback
export ORDER_CONTAINER=kafka-order-fix-20261002
python3 lab/run_ordering_validation.py new-normal normal 4 100 10
python3 lab/run_ordering_validation.py new-concurrent concurrent 4 100 0
python3 lab/run_ordering_validation.py new-cut cut 4 120 50
python3 lab/run_ordering_validation.py new-rebuild rebuild 4 120 50
python3 lab/run_ordering_validation.py new-restart restart 4 120 20
python3 lab/run_ordering_validation.py new-poison poison 2 8 0
python3 lab/run_ordering_validation.py new-ttl ttl 2 50 30
python3 lab/run_ordering_validation.py new-ackloss blackhole 4 120 50
```

Run scenarios sequentially; they share the local proxy fault files. Poison/TTL passes mean the
expected durable block occurred, **not** that all events were delivered. Rebuild tests exercise the
public rebuild request while in flight; they do not inject a genuine broker/client fatal error.
SIGKILL tests exercise process death after durable admissions, not power-loss correctness of the
underlying storage. Neither these tests nor the ordering fix establish the production Alibaba Cloud
ERR87 incident's root cause.
