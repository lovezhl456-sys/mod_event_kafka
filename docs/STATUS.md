# Status — 2026-09-25 (Asia/Shanghai)

Lab sources are integrated at this repository root (`include/`, `src/`, `mod_event_kafka.cpp`). Notes below that mention `work/` describe the lab-box tree that produced the evidence; they are not a second copy in this PR.

## Summary
- FS Phase2 拨测 **L-02/L-07=PASS**
- Evidence path: `reports/l02-l07-fs-20260925-133618/`
- `verify_event_ids.py` VERIFY_OK 60/60, no module reload
- Candidate 0001 = **NOT_VERIFIED**

## Done
- `outbox-ttl-ms` is implemented (default `120000`; `0` disables). Pending rows older than the TTL are marked `dead` / `last_error=expired_ttl` and are not produced. L-TTL lab case not yet run.
- Design locked: SQLite outbox + bounded queue + worker/poll (candidate 0001 NOT accepted as final).
- Outbox + bounded queue library built; ASan unit tests PASS (`test_outbox`).
- FS module glue is wired: event callback deep-copies and enqueues only; worker inserts the SQLite outbox row, then produces; poll thread ACKs after delivery.
- FS Phase2 拨测 **L-02/L-07=PASS**. Evidence path: `reports/l02-l07-fs-20260925-133618/`. `verify_event_ids.py` VERIFY_OK 60/60, no module reload.
- TEST-PLAN authored by 测试质检 under `docs/TEST-PLAN.md`.
- Lab compose authored by 安装软件大师 under `/workspace/lab-mod-event-kafka/` (Kafka×3 + toxiproxy).
- Phase1 lab READY (single KRaft + 3 toxiproxy frontends). Phase2 FS dialtest has since passed (see below); a true 3-broker cluster is still not what Phase1 ran.

## Not claimed
- L-03–L-15 not fully run.
- Not production-verified.
- Candidate patch 0001 = **NOT_VERIFIED** (negative reference only). It must not be marked production-verified.

## Client bootstrap (when lab healthy)
`127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094` via toxiproxy only.

## Lab smoke (stable pipeline)
- 2026-09-25T13:08:26+08:00: `stable/build/test_pipeline_smoke 127.0.0.1:19092 fs_events` → **SMOKE_PASS** (enqueued=produce_ok=acked=20). Root cause fixed: poll_loop no longer holds `rk_mu_` across `rd_kafka_poll`.
- Candidate 0001 = **NOT_VERIFIED** (negative reference only). FS Phase2 拨测 later **L-02/L-07=PASS**; evidence path `reports/l02-l07-fs-20260925-133618/`.

## Phase1 lab READY
- Bootstrap: `127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094` via toxiproxy; topic `fs_events`.
- 2026-09-25T13:08:45+08:00: stable smoke reconfirmed against full bootstrap.
- L-02/L-07 + `scripts/verify_event_ids.py` were unblocked for QA and have since passed on both the stable harness and the FS Phase2 dialtest.

## L-02 / L-07 (stable pipeline harness, Phase1 lab)
- Run: `reports/l02-l07-run1790313218/` on topic `fs_events_l02b`
- Cut: toxiproxy disable kafka1/2/3 for **35s** (> `message_timeout_ms=30s`); re-enable; **no** pipeline stop/reload
- Metrics pre-reload: enqueued=60 produce_ok=90 acked=60 delivery_fail=30 producer_rebuilds=0 outbox drained (pending/in_flight/dead=0) healed_without_reload=1
- `verify_event_ids.py`: exit 0 (injected=consumed_unique=60, missing=0)
- Result: **L-02=PASS**, **L-07=PASS**
- Scope note: this row is the stable `KafkaPipeline` harness (not the FreeSWITCH `.so`). The FS Phase2 dialtest that owns the module evidence is the next section. Phase1 = single KRaft + 3 toxiproxy frontends
- Candidate **0001 = NOT_VERIFIED** (negative reference only)
- Harness: `stable/build/test_lab_l02_l07`; header `x-fs-event-id` required for verify

## L-02 / L-07 (FS Phase2 dialtest, 2026-09-25)
This section owns the FS Phase2 evidence.

- FS Phase2 拨测 **L-02/L-07=PASS**
- Evidence path: `reports/l02-l07-fs-20260925-133618/`
- FS 1.10.12 + `work/` `mod_event_kafka.so`; conf buffer-size=100000; bootstrap toxiproxy only; topic `fs_events`
- Dial: `dialtest_originate.sh` loopback/park ×5 before + ×5 during cut (35s toxiproxy disable kafka1/2/3); no module reload
- `verify_event_ids.py` VERIFY_OK 60/60, no module reload (injected=60, consumed_matched=60, missing=0)
- Healed without reload; module_exists=true
- Candidate 0001 = **NOT_VERIFIED** (negative reference only)
- Caveat: park path may lack ANSWER; CREATE/HANGUP* only
- Caveat: L-03–L-15 not fully run; not production-verified
