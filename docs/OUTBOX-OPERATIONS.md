# Module admission order: startup, migration and capacity operations

User-approved boundary: successful module admission, not earlier FS creation/fire order. No FS core
change or custom header is required. An event and its per-call ordinal linearize at the same SQLite
WAL/FULL transaction commit under serialized admission. Failed transactions allocate no ordinal.
Only the current head can send; ACK advances/deletes atomically. Retry/stop/restart preserve identity,
key and ordinal. Unknown ACKs can cause duplicates; consumers must dedupe stable event IDs.
Single owner/database, stable exact Channel-Call-UUID grouping/key and fixed topic partitions are
preconditions. Missing IDs are ungrouped. Distinct legs/IDs are not silently merged.

## First enablement

1. Build/test the module against the target FS ABI and actual librdkafka. Cloud compilation against
   saved FS1.10.12 is not a successful FS load or real-call test. Do not run install on production from
   this diagnostic workspace. Record binary hash and configuration.
2. Quiesce the old module/writers and explicitly account for admission downtime. A live FS can keep
   producing events while its module is unloaded; this patch cannot guarantee those events. Choose
   a maintenance/new-call boundary or upstream buffering that already exists. No FS modification is
   requested, and this runbook does not assume an unavailable reliable buffer.
3. Take a consistent SQLite backup (backup API or stopped writer/checkpoint), including WAL semantics;
   archive old module/config. Do not copy only a live .db file. Inspect states and independent broker
   receipts. Never point both old/new modules at the same DB, nor run both copies as same-call owners.
4. Fresh/empty legacy DB automatically upgrades through v5. Ordered v2/v3/v4 admission DB preserves
   cursors. v5 only adds deliverable-age stamps; it does not invent sequence. Explicit-source candidate
   streams cannot be relabeled admission. Nonempty pre-v2 uses the guarded choices below. The offline
   tool still emits an ordered v4 copy for a legacy migrate and does not write `eligible_at_ms`; the
   module stamps it on the next open, only for the current head and ungrouped rows. Set
   `outbox-ttl-ms` to 0 during a deliberate backlog recovery or any outage longer than the head's
   deliverable window if age expiry is not wanted. That 0 disables age expiry only. The default can
   still turn an already-deliverable old head into a barrier; it does not expire successors that have
   not become the head. No tool silently changes TTL.
5. Set outbox path/capacity, preserve key/topic/partition count, start candidate at the agreed boundary.
   Confirm admission-order warning and no state/capacity errors. Validate injected IDs against actual
   consumed partition/offset and assigned ordinal; an empty queue or arrival ratio alone is insufficient.

## Nonempty legacy choices

There is no historical acceptance sequence in the old DB. Already delivered order cannot be undone.
Either (A) retain the old binary and explicitly drain/reconcile under its old weaker guarantee, then
switch at an empty boundary, or (B) import **all** remaining records into a new copy using a declared
cutover ordering. Neither path repairs the past. Do not silently select between these in production.
For B, operator explicitly accepts `created_at_ms,event_id` ordering of the remaining backlog per
(topic,call_uuid); this is a deterministic policy, NOT recovered acceptance time. Future admissions
append behind that backlog. In-flight rows retain their state in the copy; normal startup requeues
with the same ID and new durable ordinal. Dead/TTL rows stay dead barriers. Ambiguous prior delivery
can duplicate, requiring receipt reconciliation/deduplication.

```bash
python3 scripts/outbox_ops.py inspect /isolated/old.db --max-rows 100000
python3 scripts/outbox_ops.py migrate /isolated/old.db \
  --output /isolated/candidate.db --offline-confirm \
  --accept-unknown-legacy-order --max-rows 100000 > migration-audit.json
```

The tool only reads a consistent source snapshot and creates a NEW output. It never switches config,
modifies source rows, overwrites output, renumbers existing ordered data, or drops records. It refuses
unknown states, differing/missing keys, insufficient row capacity and unsupported schemas. Original
fields (including byte payload, IDs, state, attempts, timestamps) have a matching SHA256 fingerprint;
per-event ordinal mapping is stored in the output's operations_audit transaction and printed as JSON.
Inspect counts, fingerprints and integrity before choosing to switch; payload-byte/disk capacity must
also be configured explicitly. `--max-rows` must match the planned module config.

The cooperating .lock prevents a new module running concurrently. Legacy binaries do not honor it:
`--offline-confirm` means the operator has actually stopped every writer. A snapshot alone is not a
safe cutover if the old writer resumes afterward. The tool cannot detect remote/uncooperative writers.

Copy publication uses a committed/fsynced staging DB, atomic no-clobber hard link, then parent fsync.
Before publication a crash leaves no final path; rerun from the unchanged source. After publication,
the final DB is complete and contains its audit; retries refuse overwrite. Inspect it, do not force
replace. An interruption may leave `.OUTPUT.stage-*`/journal files: retain until the final/source
copies are reconciled, then remove only known operation staging files while offline. Operations on
filesystems lacking SQLite/fsync/hardlink semantics are not qualified by these tests.

## Capacity, alerts and explicit terminal retirement

`outbox-max-rows` limits queued records and active cursors independently; the operations tool applies
the same configured limit to retired-ID fences. `outbox-max-bytes` counts payload, NOT total SQLite,
indexes, keys, audit, WAL or filesystem bytes. Reserve disk and monitor actual usage.

`inspect` prints state counts, payload bytes, active/fence counts and warnings at >=80% row capacity.
Schedule/run it using existing operations monitoring; this patch does not install a monitoring service.
Also alert on admission rejection, state_errors, oldest pending age, dead count and fsync latency.
Estimate active cursor headroom from distinct IDs/day times retention horizon. Raising row limits
releases capacity without changing cursor semantics, at the cost of storage and larger scans. A full
queue or unknown closure is not permission to drop events. A full fence table requires capacity growth
or an explicitly separate stream epoch/archival policy; there is no fence-delete command.

If an operator knows an entire call ID is permanently closed, it can be explicitly terminated:

```bash
python3 scripts/outbox_ops.py retire /isolated/stopped-v4.db \
  --output /isolated/retired-v4.db --offline-confirm --max-rows 100000 \
  --topic events --call-id EXACT_ID --reason 'ticket: verified whole-call closure' \
  --reject-future-call-id > retirement-audit.json
```

Any pending/in-flight/dead/TTL row or unresolved cursor gap blocks retirement. The new copy keeps a
permanent fence with last cursor/key/reason and releases one active slot. All future arrivals with
that topic/ID are rejected, including intentional ID reuse; they do not start at zero. This is a
terminal policy, not a way to permit reuse. Do not retire based only on one leg's HANGUP/DESTROY or
idle time. If IDs can legitimately be reused, retain their active cursors (sequence continues), raise
capacity, or choose a separately scoped explicit epoch; do not pretend to infer business identity.
No existing accepted message is removed. Before choosing the retired copy, reconcile the audit and
source snapshot and keep the old writer stopped. The current module does not hot-reload DB paths.

## Rollback and cost

Stop/quiesce admission before rollback. After new admissions/ACKs, an old backup is stale: directly
restoring it can lose new accepted events or replay old ones out of order. Preserve both databases and
reconcile IDs/offsets under the new ordering contract, or continue forward with the new core. Original
legacy binaries ignore cursor/fence semantics; never point them at v4. Before any new admissions,
restoring the untouched old DB/config is possible only under its old documented guarantee.

One outstanding head per call limits throughput by ACK/poll/SQLite latency; other calls may progress.
WAL/FULL adds synchronous admission latency. Shared storage/row exhaustion affects multiple calls.
Dead/TTL barriers intentionally retain backlog. Production disk, FS media load, lifecycle callbacks,
real fatal client errors and multiple-owner coordination still require separate qualification.
