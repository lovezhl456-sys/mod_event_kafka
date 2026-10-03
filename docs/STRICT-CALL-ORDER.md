# Durable per-call ordering

This change fixes application retry overtaking. It does **not** infer a missing source order from
UUIDs, timestamps, Kafka offsets, or the order in which concurrent callbacks acquire a lock.

## Module-only contract (no FreeSWITCH changes)

For each `(topic, Channel-Call-UUID)`, successfully accepted events are ordered by the SQLite
transaction commit that persists both the payload and its next admission ordinal. The pipeline
lifecycle mutex serializes concurrent admissions, and the outbox transaction is the linearization
point. Callback entry, event creation, log printing and enqueue return timing are not the ordering
reference. A failed transaction allocates no ordinal; acknowledged cursors survive process restart.
Retries preserve payload, event ID, ordinal and Kafka key. The ordinal is also returned to diagnostics
and emitted as `x-fs-call-sequence`; its meaning in this module is **admission**, not source creation.

The FS adapter always uses this internal ordinal. It neither requires nor interprets a custom
`Kafka-Call-Sequence` header. That header, if present, remains ordinary payload. The old
`require-source-sequence` option is accepted for config compatibility but ignored and warned about.
The standalone core retains its explicit-sequence API for historical tests/other verified producers;
that optional API is not used by this FS module and is not an FS integration prerequisite.

This is a restricted guarantee, requiring user acceptance because the original requirement may
mean earlier FS source order. No claim is made that native FS creation/fire order is restored.
`Event-Sequence` is global, has legitimate gaps from other calls, filters and never-fired events,
and resets across source epochs. Timestamps can tie, regress, or describe different event phases.
`Core-UUID` distinguishes epochs but supplies no contiguous per-call order. Concurrent asynchronous
FS dispatch may deliver a lower creation ordinal later. Waiting for consecutive global numbers
can wait forever; a bounded sort window cannot prove completeness or causality.

The existing exact Channel-Call-UUID grouping/key is preserved. No new A/B-leg association is
inferred, no distinct IDs are merged by timestamp/related headers, and no Core-UUID key change is
silently introduced. If upstream already assigns the same ID to multiple legs, this module provides
one combined **admission** order for that ID, not a cross-leg causal order. Missing IDs remain
ungrouped, with no per-call guarantee. Separate module instances/databases do not coordinate order.
For a fixed Kafka partition count/topic identity, stable key, single module owner and acknowledged
broker writes surviving the failure model, Kafka first occurrences preserve the admission ordinal.

An event lost before durable admission (including upstream ignoring admission rejection) is outside
this contract. Admission is synchronous SQLite WAL/FULL and may block the FS callback on storage.
The tests compile the FS module but do not load it or run real calls; this is not FS end-to-end proof.

## Durable state machine

`call_streams.next_seq` is the only sendable sequence. `fetch_due` intersects that cursor with a
pending, due row; it does not discard the cursor when an older row is in flight, in backoff, dead,
or absent. `mark_in_flight` rechecks the head. An ACK atomically advances the cursor and deletes
that same in-flight row. A failure returns that row to pending without changing its sequence.

Synchronous produce failures and delivery-report failures use one policy. A permanent broker error,
or a durable attempt count that has reached `max_attempts_before_dead` (default 50), marks that
in-flight row dead. Every other failure returns the same row to pending with the same attempt-based
backoff. The delivery-report path does not ignore the attempt limit. The module does not expose a
separate FreeSWITCH XML key for the limit; the pipeline default applies.

`outbox-ttl-ms` (default 120000) expires only a pending row that is already deliverable: an ungrouped
row, or the ordered row whose `call_seq` is `next_seq`. Age is `(now - eligible_at_ms)`, and that
timestamp is set when the row becomes deliverable — at admission if it is already the head, or when
the previous head is acknowledged. Time spent waiting behind an earlier event does not expire a
successor, including while the head is in flight or in backoff. An in-flight row is not expired in
place. Expiry uses a strict greater-than comparison. A dead head remains a barrier. Its successors
stay pending and become deliverable only after that same head is acknowledged, so `next_seq` is not
left on a dead row that never became the head. Set `outbox-ttl-ms` to **0** to disable age expiry
during an outage longer than the head's deliverable window. Zero does not disable the attempt limit.
Retrying a TTL-dead head does not reset `eligible_at_ms` or `created_at_ms`; raise the TTL or set 0
before that retry, or the next scan expires the head again.

For a call, dead and TTL rows are **barriers**, not capacity-reclaim candidates. Later rows cannot be
sent past them. Ungrouped TTL rows retain their previous reclaim behavior. An operator can stop the
pipeline and call `Outbox::retry_dead(id)` to retry the **same** event after fixing its cause. An
irreparable poison event quarantines the entire call. No automatic skip/delete, renumbering, or
gap-ack API is provided. Releasing a poison call by discarding a source event would require an
explicitly approved business gap/compensation protocol outside this change.

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

Schema version 5 adds `outbox.eligible_at_ms`, the time a row became deliverable. Upgrade stamps
`created_at_ms` onto that column only for a row that is already the current head or ungrouped.
Successors stay unstamped until the cursor reaches them. No call sequence is created or rewritten.
Version 4 adds permanent explicit-retirement fences. Version 3 introduced the durable scheduler turn. Version 2 added `outbox.call_seq`, a unique `(topic,call_uuid,call_seq)` index, and durable
`call_streams` cursors/key/mode. Fresh and **empty** legacy outboxes upgrade transactionally.
A v2 outbox upgrades transactionally with existing cursors preserved; the scheduler starts above its retained service timestamps. A nonempty pre-v2 legacy outbox fails closed before schema migration; its source order cannot be proved.
Back it up, preserve its WAL consistently, reconcile it against original source/consumer evidence,
and explicitly drain/quarantine it before upgrade. Never fabricate source sequence during migration.
Unknown future schema versions and missing ordered cursors also fail closed.

Do not point an old binary at a v4 or v5 outbox: it does not know the cursor or deliverable-age rules. Rollback needs a stopped
publisher and a separately reconciled backup, not an automatic downgrade. No production migration
has been executed by this work.

SQLite uses WAL and `synchronous=FULL`. Admission now performs a durable disk transaction on the
calling thread. The old `mem-queue-max` setting/metric is retained for source compatibility but is
not used by this path. Outbox row and payload-byte limits remain hard admission limits, and retained
stream metadata is separately limited to `outbox-max-rows` streams. The payload-byte limit is **not**
a cap on SQLite pages, indexes, WAL, keys or total filesystem use. Monitor and reserve disk space.

Completed cursors remain by default: no timeout or leg event triggers deletion. Explicit offline
retirement is now available through `scripts/outbox_ops.py`, producing a new DB copy only. It requires
an empty stream with next_seq=admit_seq, an operator reason, and explicit acceptance that this exact
(topic,call ID) must **never** be admitted again. It moves the active cursor to a permanent
`retired_calls` fence, releasing one active slot without permitting sequence reset. Any late/reused ID
is visibly rejected, including after restart. The tool refuses pending/in-flight/dead/TTL rows and
unresolved gaps. This is an operator policy, not inferred business closure; without closure knowledge,
use monitoring/capacity increases instead. Retirement fences also have a tool-enforced configured
row limit; they are never evicted. Each active/fence limit uses the supplied `outbox-max-rows`, so the
two tables may together contain twice that many identities. Audit history, keys and SQLite/WAL bytes
need separate filesystem monitoring. See [operations runbook](OUTBOX-OPERATIONS.md).

Per-call throughput is bounded by one successful head at a time, broker ACK latency, polling and
SQLite commits. Other calls can progress; head selection uses least-recently-served durable call turn before
age to avoid continuously selecting only the oldest busy calls. There is no promise of a precise
latency/fairness bound under overload. Retained poison calls can eventually consume the shared disk
quota and cause visible global admission rejection. SQLite FULL fsync and large backlogs require
real storage/FreeSWITCH-media-load qualification before rollout.

Fixed partition count/topic identity is a precondition: this patch does not migrate an active call
between partition counts, topics or independent producers. Do not change them during a call.

## Tests and reproduction

`test_ordering` covers head-only scheduling, backoff/in-flight/dead blocking, source gaps,
concurrent admission, exact cursor recovery, ownership, TTL barriers, empty/blocked migration and
durable admission. Expiry of an in-flight or backoff head does not kill same-call successors, and
after that head is acknowledged the next row is deliverable instead of leaving `next_seq` on a dead
non-head. A v4 database gains `eligible_at_ms` without a new sequence. Delivery-report failures use
the same attempt limit as synchronous produce failures: a non-permanent delivery failure can reach
dead, the successor stays pending, and acknowledging the dead head releases it. `ctest` also runs
`ops_interop`. These tests do not load FreeSWITCH or place real calls.

CMake builds `test_ordering_drill` and `order_readback`. The cloud also directly compiled both core
sources and unit tests with ASan/UBSan, and built the actual module with the saved SDK. The broker
experiment uses a local TCP cut/ACK-loss proxy and a fresh 3-partition topic per scenario. This is
one broker/RF1, not a production durability or replication failover certification.

The evidence bundle contains pinned lab startup/proxy scripts, baseline driver, all commands,
source plan, admission sequence/time/latency, per-process metrics, DB snapshots before SIGKILL,
raw independent-consumer offsets/payloads/IDs, duplicates and final cursors. To reuse its workspace:

```bash
export ORDER_ADMISSION=1 # module admission contract, no supplied source sequence
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

## Nonempty legacy database procedure

No sequence is inferred from rowid, created_at_ms, Event-Sequence or current retry state. An old
backlog may already have been partially delivered and reordered; a module upgrade cannot undo it.
For a nonempty pre-v2 DB, open fails before altering the schema. Keep the original binary and DB
backup. In an isolated copy, inventory pending/in_flight/dead IDs and reconcile broker offsets with
independent receipts. Choose explicitly between draining under the old weaker contract (which does
not repair old order) or quarantining the backlog and starting a separately identified new stream.
Neither choice may be silently labeled continuous strict order. Take the backup using SQLite's
consistent backup mechanism or after a clean stopped/checkpointed writer; copying the .db alone while
WAL is active is not sufficient. Production execution is outside this patch.

A nonempty historical candidate DB in explicit `source` mode also cannot be relabeled `admission`:
the mode/key guard rejects that conversion. V2 admission-mode DBs upgrade through v5, preserving every
cursor and without inventing sequence. Schema versions above 5 are refused. Never run an original pre-ordering binary against the
new DB or delete cursor rows to make an upgrade succeed. The unit migration tests verify the legacy
nonempty refusal and cursor-preserving ordered upgrade; no production backlog is claimed migrated.
