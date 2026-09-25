# Deploy / rollback

## Deploy
1. Install `librdkafka`, `libsqlite3`.
2. Build `mod_event_kafka.so` (see the repository root `Makefile`; it compiles `src/kafka_*.cpp` with `-Iinclude`).
3. Install conf; set `bootstrap-servers` to **toxiproxy/lab or production bootstrap** (never mix).
4. Ensure `outbox-path` directory is writable by the FreeSWITCH user.
5. `load mod_event_kafka` — do **not** require reload after Kafka restart if pipeline is healthy.

## Rollback
1. `unload mod_event_kafka`
2. Restore previous `.so` + conf without outbox params (still valid; new params optional).
3. Outbox DB can be archived; leftover rows are safe to keep if reloading new build later.

## Not verified in this environment
- Production cloud Kafka
- Full FreeSWITCH load of the new `.so` (no FS headers on this box in Phase1)
