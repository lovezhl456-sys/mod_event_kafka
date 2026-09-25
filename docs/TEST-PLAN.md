# TEST-PLAN — mod_event_kafka Outbox Resilience

Status legend: `PLANNED` | `CODED` | `PASS` | `FAIL` | `BLOCKED` | `N/A`  
**Candidate patch `0001-kafka-restart-resilience.patch` is a negative reference only — never mark any row PASS based on it.**

Contract source: `docs/DESIGN.md`. Lab FS+3-broker+fault-proxy depends on Docker/Podman (not on this box) or host from 安装软件大师.

---

## 0. Priority unit gaps (requested)

| ID | Result | Notes |
|----|--------|-------|
| U-OWN-01 queue deep copy | PASS | mutate caller after `try_push`; pop unchanged |
| U-OWN-02 outbox deep copy | PASS | mutate record after `insert_pending`; fetch unchanged |
| U-DSK-03 byte cap | PASS | `max_bytes` rejects; pending intact |
| U-Q-02 concurrent enqueue | PASS | 8 threads × 40 into cap 64; pushed+rejected exact |
| U-UNL-01/02 safe unload | PASS | `notify_stop` unblocks; double `close`/`stop`; post-stop enqueue rejected; outbox retains rows |

Command: `cmake --build build && ctest --test-dir build --output-on-failure` (binary built with ASan+UBSan).

## 1. Unit test matrix (no FreeSWITCH)

Target binary: `build/test_outbox` (+ future `tests/test_*.cpp`). Run under ASan+UBSan (see §2).

| ID | Area | Scenario | Expect | Status |
|----|------|----------|--------|--------|
| U-OWN-01 | Ownership | `QueuedEvent` holds deep-copied payload+key+event_id; mutate caller buffers after `try_push` | queue item unchanged | PASS |
| U-OWN-02 | Ownership | Produce path uses `RD_KAFKA_MSG_F_COPY` (or equivalent); outbox row remains SoT until ACK | no MSG_F_FREE UAF; payload still readable in DB while in_flight | PASS |
| U-OWN-03 | Ownership | Detached retry threads absent from worker/API surface | static/grep: no `std::thread(...).detach` in produce/retry path | PLANNED |
| U-Q-01 | Queue limit | `BoundedQueue(N)` accepts N, rejects N+1 | `try_push` false; `rejected()==1` | CODED |
| U-Q-02 | Queue limit | Concurrent push/pop around capacity | no lost count; rejected increments only on true overflow | PASS |
| U-Q-03 | Queue limit | `pop_wait_for_ms` + `notify_stop` with empty queue | returns nullopt; no hang | PLANNED |
| U-RTY-01 | Retry | `mark_retry` sets `pending`, bumps `attempts`, `next_attempt_at_ms` future | `fetch_due(now)` empty; due after next_ms | CODED |
| U-RTY-02 | Retry | Exponential backoff (+jitter) schedule helper | intervals grow; jitter within documented bound | PLANNED |
| U-RTY-03 | Retry | Transient class keeps row; permanent → `dead` | state/`last_error` correct; no tight loop on dead | CODED (dead) / PLANNED (class map) |
| U-ACK-01 | ACK | `mark_in_flight` → `mark_acked` deletes row | stats in_flight/pending drop; row gone | CODED |
| U-ACK-02 | ACK | ACK is transactional: crash mid-DELETE simulated by reopen before commit complete | at-least-once: row still present OR gone; never half-state | PLANNED |
| U-ACK-03 | ACK | Duplicate delivery after ACK-then-crash window | consumer dedupe key = header `x-fs-event-id`; body JSON unchanged | PLANNED (doc+helper) |
| U-DSK-01 | Disk fail | `outbox-path` unwritable / open fail | `open` false + err; enqueue path rejects with metric/log (no silent) | PLANNED |
| U-DSK-02 | Disk fail | `max_rows` exhausted | `insert_pending` false; `rejected_disk_full` path | CODED (rows) |
| U-DSK-03 | Disk fail | `max_bytes` exhausted | insert rejected; existing rows intact | PASS |
| U-DSK-04 | Disk fail | SQLite I/O error on INSERT/UPDATE | error surfaced; no crash; row not falsely acked | PLANNED |
| U-REC-01 | Recovery | Persist across reopen | dead/pending survive process restart | CODED |
| U-REC-02 | Recovery | `requeue_in_flight` on startup/rebuild | in_flight → pending; no orphan in_flight | CODED |
| U-REC-03 | Recovery | Same `call_uuid` FIFO by `created_at_ms` | fetch_due order matches enqueue order within call | CODED |
| U-REC-04 | Recovery | Cross-call unordered allowed | two call_uuids interleaved OK | PLANNED |
| U-UNL-01 | Unload | Shutdown sequence: stop enqueue → drain mem→outbox → flush/poll timeout → join | no join hang; unacked rows remain on disk | PASS |
| U-UNL-02 | Unload | Double close / destroy without open | no crash (ASan clean) | PASS |
| U-ID-01 | event_id | `make_event_id` UUIDv4 shape + uniqueness over N≥10k | regex + set size == N | PLANNED |

### Gaps vs current `tests/test_outbox.cpp`

Already covers: U-Q-01, U-RTY-01, U-ACK-01, U-DSK-02 (rows), U-REC-01/02/03, dead mark.  
**Missing before claiming unit complete:** U-OWN-*, U-Q-02/03, U-RTY-02, U-ACK-02/03, U-DSK-01/03/04, U-REC-04, U-UNL-*, U-ID-01, byte-cap, concurrency.

---

## 2. ASan / UBSan checklist

Build (already wired on `test_outbox` in `CMakeLists.txt`):

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

| Check | How | Pass criteria | Status |
|-------|-----|---------------|--------|
| A-01 AddressSanitizer on unit suite | `-fsanitize=address,undefined` link+compile | exit 0; no ASan report | CODED (flags) / run = see reports |
| A-02 Use-after-free on produce/ACK | stress insert→in_flight→ack while holding payload views | no heap-use-after-free | PLANNED |
| A-03 Double-free / MSG_F_FREE regression | grep + negative test that COPY path owns buffer | no free of outbox-owned blob by rdkafka | PLANNED |
| A-04 Thread lifetime | stop worker+poll; join; destroy Outbox | no stack-use-after-return / thread leak | PLANNED |
| A-05 UBSan integer/null | full suite | no runtime error | with A-01 |
| A-06 LeakSanitizer (optional ASAN_OPTIONS=detect_leaks=1) | suite + reopen loop | no definite leaks in outbox/queue | PLANNED |

Env for CI/local:

```bash
export ASAN_OPTIONS=detect_leaks=1:halt_on_error=1:abort_on_error=1
export UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
```

**Module `.so` under FreeSWITCH** sanitizer builds are lab-only (needs matching FS/libs). Do not claim ASan PASS for the loaded module until lab host runs it.

---

## 3. Acceptance lab — FS + 3-node Kafka + fault proxy

Lab compose lives at `/workspace/lab-mod-event-kafka/` (Docker available). FreeSWITCH host still optional/Phase-2. Owner for runtime: 安装软件大师.

### 3.1 Topology (when available)

- FreeSWITCH + patched `mod_event_kafka` (outbox design, not candidate patch)
- Kafka KRaft/ZK 3 brokers
- Toxiproxy (or equivalent) in front of bootstrap
- Consumer that records Kafka header `x-fs-event-id` (+ optional key=`Channel-Call-UUID`)

### 3.2 Scenarios (high level)

| ID | Fault | Expect | Status |
|----|-------|--------|--------|
| L-01 | Happy path | all injected event_ids appear once in topic | PLANNED |
| L-02 | Kill all brokers 30s mid-traffic; restart; **no** `reload mod_event_kafka` | pending outbox drains; sent set ⊆ injected; missing only pre-COMMIT window (DESIGN §Reliability 1) | **PASS** (2026-09-25, stable harness `reports/l02-l07-run1790313218`, not FS `.so`) |
| L-03 | Toxiproxy reset-peer / latency during produce | retries; eventually ACK; duplicates only in ACK-crash window → deduped by event_id | PLANNED |
| L-04 | Single broker down (ISR still ok) | no sustained reject; produce_ok continues | PLANNED |
| L-05 | `mem-queue-max` pressure | `rejected_mem_full` + WARNING; media thread not blocked | PLANNED |
| L-06 | Module unload/reload mid outage | disk outbox retained; after reload+broker up, drain completes | PLANNED |
| L-07 | Self-heal gate | after L-02, compare **before reload** vs **after reload** — PASS only if heal without reload | **PASS** (same run as L-02; pre-reload acked=60, verify exit 0) |

| L-08 | Rolling restart (one broker at a time, all bootstrap addrs covered by proxy) | no permanent loss of committed outbox rows; backlog drains | PLANNED |
| L-09 | Broker SIGKILL / process crash | same as transient; outbox retained; auto retry | PLANNED |
| L-10 | TCP RST on all proxied broker ports | retain + backoff; after path heal, drain without reload | PLANNED |
| L-11 | Network blackhole (drop all) > `message-timeout-ms` | rows stay pending/retry; after heal, **new** events still enqueue+send; old due rows drain | PLANNED |
| L-12 | Latency / repeated flap (toxiproxy toxics) | thread/memory stable; rejects only at capacity; no media-thread block | PLANNED |
| L-13 | Disk full / outbox INSERT fail | reject + alert metrics; no silent drop; no crash | PLANNED |
| L-14 | FS / module process SIGKILL mid outage; restart module/FS | unacked outbox rows replay with **same** event_id | PLANNED |
| L-15 | Auth/acl/permanent produce error | state=`dead` + evidence; no tight retry loop | PLANNED |

**Pass rule:** compare event_id sets (injected / rejected / consumed_dedup / outbox terminal). Never pass on “no error logs” alone.


### 3.3 Event ID set verification method

Stable ID is UUIDv4 created at enqueue, stored in outbox, attached as Kafka header **`x-fs-event-id`** (body JSON unchanged).

**Artifacts (lab run writes under `reports/<run-id>/`):**

| File | Content |
|------|---------|
| `injected_ids.txt` | one event_id per line, order = injection order |
| `outbox_snapshot.csv` | `event_id,state,attempts,call_uuid,created_at_ms` at checkpoints (T0 inject done / T1 broker down / T2 broker up+drain / T3 unload) |
| `consumed_ids.txt` | event_ids from consumer (header `x-fs-event-id`), one per line, may contain dupes if any |
| `consumed_ids_dedup.txt` | unique set |
| `metrics.json` | counters from DESIGN metrics list |

**Checks (script: `scripts/verify_event_ids.py`):**

1. **Completeness (at-least-once after heal):**  
   `set(injected) - set(consumed_dedup) == ∅`  
   *Except* IDs never reaching outbox COMMIT (document any intentional pre-commit loss with count + reason).
2. **No silent drop under capacity:** if rejects > 0, every rejected ID must appear in a `rejected_ids.txt` log, not only in metrics.
3. **Dedupe:** `len(consumed) - len(consumed_dedup)` reported; FAIL if dupes after consumer-side dedupe by `x-fs-event-id`.
4. **Ordering (same call):** for each `call_uuid`, relative order of event_ids in `consumed_dedup` matches `created_at_ms` order in outbox snapshot (cross-call: no assert).
5. **Outbox terminal state:** after drain, `pending+in_flight == 0` for successful IDs; `dead` only for permanent-error injected cases.
6. **Self-heal:** L-07 requires checks 1–5 on the **pre-reload** consumer capture.

Exit codes: `0` all assert pass; `1` set mismatch; `2` order fail; `3` bad artifacts.

---

## 4. What this box can run now vs blocked

| Work | Where | Status |
|------|-------|--------|
| Outbox/queue unit + ASan/UBSan flags | this box | runnable (`ctest`); expand matrix still PLANNED |
| Kafka×3+toxiproxy lab | `/workspace/lab-mod-event-kafka/` | READY (compose); FS optional Phase-2 |
| Full FS+Kafka+proxy | lab + FS host | PENDING FS binary/path from 安装软件大师 |
| Treat candidate poll/rebuild patch as verified | — | **Forbidden** (DESIGN: negative reference) |

---

## 5. Exit criteria for “ready to PR”

1. Unit matrix rows for OWN/Q/RTY/ACK/DSK/REC/UNL either PASS or explicitly waived with architect sign-off.  
2. A-01 clean on unit suite.  
3. L-02 + L-07 PASS on lab with `verify_event_ids.py` exit 0 (artifacts under `reports/`).  
4. Reports mark every skipped scenario `BLOCKED`/`N/A`, never silent.
