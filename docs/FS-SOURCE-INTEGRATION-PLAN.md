> HISTORICAL DESIGN ONLY. Superseded by the user constraint “不改 FS”. No FS core patch, source bridge, or new header is requested or implemented. See STRICT-CALL-ORDER.md for the current module-only scope.

# FreeSWITCH source integration — open design, not production acceptance

The core retry fix is not the completion of the business source-order task. The previous proposal
requiring a new header does not, by itself, provide that header to an existing FreeSWITCH producer.

## Verified existing source

The saved SDK is FreeSWITCH 1.10.12, commit `a88d069d6ffb74df797bcaf001f7e63181c07a09`.
In that source, `switch_event_create_subclass_detailed` calls `switch_event_prep_for_delivery_detailed`
(approximately lines 748–778 of `src/switch_event.c`). The latter assigns `Event-Sequence` under
`EVENT_QUEUE_MUTEX` (approximately 1972–2001). This is an event **creation** ordinal. The event's call
headers have not necessarily been populated at that point, and an allocated event can be abandoned.
`switch_channel_event_set_data` subsequently derives `Channel-Call-UUID` from the channel's
`call_uuid` variable or session UUID (`src/switch_channel.c`, approximately 2703–2707).

`switch_event_fire_detailed` (approximately 2006–2037) receives the prepared event and chooses an
asynchronous dispatcher queue or thread pool. Multiple dispatcher callbacks can run concurrently.
The module binds a normal delivery callback; its `PublishEvent` runs after this asynchronous handoff.
The inspected public event API does not expose a module callback at event creation / pre-dispatch
publication. Core-UUID identifies a FreeSWITCH instance epoch, not a contiguous per-call cursor.
Timestamps and state numbers likewise cannot establish the missing total order.

Therefore a callback-only module cannot guarantee the earlier creation ordinal with complete
coverage just by sorting, assigning a new counter, or waiting for a guessed time window. Global
Event-Sequence has legitimate per-call gaps and gaps from events never fired. A mutex in PublishEvent
only linearizes module admission, which is the explicit compatibility guarantee now implemented.

## Implementable source path, contingent on deployment constraints

For a source-order solution, define the authoritative boundary as **publish-ready admission before
async dispatch**. At that boundary, event headers and the intended call identity must be available.
Add a narrowly scoped FreeSWITCH source bridge, or use an existing upstream event producer that owns
that boundary, to submit selected events directly to a durable ingress. This needs actual source-side
support; it cannot be implemented by inventing `Kafka-Call-Sequence` in the downstream callback.

The bridge must:

1. Apply exactly the configured event selection before assigning a contiguous per-call cursor.
2. Identify the source instance epoch and a non-reused call incarnation. Preserve the business call
   key separately from stream identity; shared A/B legs and transferred calls need an explicit owner.
3. In one durable transaction record the source event, its stable identity and source sequence before
   acknowledging ingress. The existing persistent admission transaction can serve this purpose only
   if invoked at this newly defined **source** boundary, before the asynchronous race. It cannot be
   retroactively described as honoring the earlier event-creation Event-Sequence.
4. Preserve payload/ID/sequence across retries, reload and FS restart. Prevent the ordinary async
   module subscriber from emitting the same event a second time. Do not persist only an in-memory
   sequence and then hand a possibly lost event to the old asynchronous queue.
5. Register/unregister with a lifecycle protocol that drains existing calls without deadlocking FS
   event locks. Do not hold EVENT_QUEUE_MUTEX or the event subscription RWLOCK during SQLite fsync.
   Bound any per-call ingress lock wait and specify what source code does when durable admission
   fails; returning a status that upstream ignores does not provide all-event delivery.
6. Install before new calls, or resume a proven durable source registry. Hot-loading halfway through
   an already active call does not reconstruct missing history. A capture discontinuity must be
   explicit and must not be converted into a fresh sequence zero for the same incarnation.

This is a design for an FS source patch/bridge, not a claim that such a patch was deployed or loaded.
If the required order is specifically the earlier Event-Sequence creation order, the source must
also publish explicit cancellation/skip knowledge for created-but-never-fired events, with call
identity established there. That is a larger source contract than ordering publish-ready events.

## Compatibility and state policies

* **Existing configuration:** default `require-source-sequence=0` retains durable module admission
  order and logs a startup warning that FS source order is not guaranteed. Explicit mode `1` remains
  available for verified source producers. Pipeline start/migration failure now fails module load;
  it must not appear successfully loaded while dropping every incoming event.
* **Legacy nonempty DB:** no automatic guessed sequence. Stop admission, take a consistent backup
  including WAL, inventory every event and state, and obtain source sequence and acknowledged-cursor
  evidence. Migrate a copy with an auditable mapping. If that evidence does not exist, retain the
  legacy stream under a separately declared weaker contract or quarantine it; do not mix it into
  a strict source stream and call the result repaired. No such production mapping has been assumed.
* **Existing ordered v2 DB:** v3 preserves rows/cursors/mode and transactionally introduces a durable
  scheduling turn. Seeding above prior last-served timestamps prevents wall-clock rollback from
  starving calls. An older v2 binary rejects v3 by version, rather than silently using new metadata.
* **Completed calls:** retain cursor state until the source confirms closure of the whole call
  incarnation, all selected events are ACKed, no unresolved gap/dead row remains, and replay/late
  ingress has been fenced. Only then archive heavy metadata and retain a compact closed-incarnation
  fence. Do not treat one leg's CHANNEL_HANGUP/DESTROY or a wall-clock timeout as proof of group closure.
  Compact fences can be removed only after a persisted source-epoch/replay floor rejects older
  submissions. This source registry/retirement service is still unimplemented; current metadata is
  hard-bounded and admission fails visibly at the limit, not unbounded or silently GC'd.
* **Call-ID reuse:** before retirement, repeated old sequence zero is rejected by the persisted
  cursor. After retirement, protection needs the incarnation/epoch fence above. Bare reusable
  Channel-Call-UUID plus a new counter is insufficient. Do not delete current cursors as a shortcut.
* **Poison message:** keep original ID/payload/sequence and all successors. Operator retry after the
  cause is corrected is supported. An irreparable record quarantines the call. A business-approved
  gap/tombstone/compensation protocol would be a separate explicit state transition, not DELETE.
* **TTL:** TTL is an operational dead/blocked transition of the current deliverable head (or an
  ungrouped row), not business success, cursor advancement, call closure or permission to free the
  stream. Successors waiting behind that head are not expired. Retry does not falsify original
  creation time or deliverable age; set TTL to 0 or raise it before retrying an expired head.

## Independent review and next test gates

The existing core has a single producer owner, an outbox mutex, and admission/lifecycle mutex.
The worker never acquires the lifecycle mutex, so stop may join it while holding that mutex without
forming that lock cycle. New regression tests race stop with concurrent durable admissions and verify
all successful admissions remain in the database. The source bridge needs its own FS-lock/lifecycle
review; the core test is not a proof for an unimplemented hook.

New scheduler regression seeds a migrated call's prior last-served value far ahead of wall time,
then verifies 96 ready calls each receive one turn before any receives a second. Ordinary poisoned
call blocking is already tested with another call sharing its Kafka partition. Shared SQLite fsync,
quota exhaustion and finite scheduling batches still affect cross-call latency; they are not claimed
to have a hard realtime fairness bound. Review and load-test storage separately.

Before source implementation, obtain only the information that changes this design:

1. Actual deployed FS version/source commit, and whether a core/source bridge is possible or only
   replacement of mod_event_kafka.so is allowed.
2. Whether strict order means publish-ready `fire` admission or earlier event-creation Event-Sequence.
3. The selected event set/filter and whether Channel-Call-UUID groups multiple legs/transfers or can
   be explicitly reused. This determines the source owner and what constitutes complete call closure.

No production access is needed to answer these. Until resolved, the source-order task remains open.
