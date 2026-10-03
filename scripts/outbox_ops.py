#!/usr/bin/env python3
"""Offline, copy-only outbox operations. Never switches or overwrites a live database."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS call_streams(topic TEXT NOT NULL,call_uuid TEXT NOT NULL,
 msg_key TEXT NOT NULL,mode TEXT NOT NULL,next_seq INTEGER NOT NULL DEFAULT 0,
 admit_seq INTEGER NOT NULL DEFAULT 0,last_served_ms INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(topic,call_uuid));
CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_seq ON outbox(topic,call_uuid,call_seq);
CREATE INDEX IF NOT EXISTS idx_outbox_due ON outbox(state,next_attempt_at_ms);
CREATE INDEX IF NOT EXISTS idx_outbox_ttl ON outbox(state,created_at_ms);
CREATE TABLE IF NOT EXISTS order_scheduler(id INTEGER PRIMARY KEY CHECK(id=1),turn INTEGER NOT NULL);
INSERT OR IGNORE INTO order_scheduler SELECT 1,COALESCE(MAX(last_served_ms),0) FROM call_streams;
CREATE TABLE IF NOT EXISTS retired_calls(topic TEXT NOT NULL,call_uuid TEXT NOT NULL,
 msg_key TEXT NOT NULL,next_seq INTEGER NOT NULL,retired_at_ms INTEGER NOT NULL,
 reason TEXT NOT NULL,PRIMARY KEY(topic,call_uuid));
CREATE TABLE IF NOT EXISTS operations_audit(operation_id TEXT PRIMARY KEY,body TEXT NOT NULL);
PRAGMA user_version=4;
"""


def connect_read(path):
    return sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=1)


def inventory(db, max_rows):
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    states = dict(db.execute('SELECT state,count(*) FROM outbox GROUP BY state'))
    counts = {t: db.execute('SELECT count(*) FROM ' + t).fetchone()[0]
              if t in tables else 0 for t in ['call_streams', 'retired_calls']}
    payload = db.execute('SELECT coalesce(sum(length(payload)),0) FROM outbox').fetchone()[0]
    warnings = [name + ' >= 80% of configured row limit' for name, value in
                dict(outbox=sum(states.values()), **counts).items() if value >= max_rows * .8]
    return dict(schema=db.execute('PRAGMA user_version').fetchone()[0], states=states,
                payload_bytes=payload, **counts, max_rows=max_rows, warnings=warnings)


def fingerprint(db):
    # Preserve every legacy field, including payload bytes, IDs, retry state and timestamps.
    names = [r[1] for r in db.execute('PRAGMA table_info(outbox)') if r[1] != 'call_seq']
    rows = db.execute('SELECT ' + ','.join('"' + n.replace('"', '""') + '"' for n in names)
                      + ' FROM outbox ORDER BY event_id')
    h = hashlib.sha256()
    for row in rows:
        encoded = json.dumps([{'bytes': v.hex()} if isinstance(v, bytes) else v for v in row],
                             ensure_ascii=True, separators=(',', ':')).encode()
        h.update(len(encoded).to_bytes(8, 'big')); h.update(encoded)
    return h.hexdigest()


def run(args):
    src = Path(args.source)
    if not src.is_file():
        raise ValueError('source database must exist')
    if args.command == 'inspect':
        with connect_read(src) as db:
            db.execute('BEGIN')
            return inventory(db, args.max_rows)
    if not args.offline_confirm:
        raise ValueError('stop all writers first; --offline-confirm is required (old binaries ignore flock)')
    dst = Path(args.output)
    if dst.exists() or dst.resolve() == src.resolve():
        raise ValueError('output must be a new separate path; never overwrite')
    if args.command == 'migrate' and not args.accept_unknown_legacy_order:
        raise ValueError('--accept-unknown-legacy-order is required; this is a declared cutover order, not recovered history')
    if args.command == 'retire' and not args.reject_future_call_id:
        raise ValueError('--reject-future-call-id is required; retirement permanently rejects this topic/call ID')
    lock = open(str(src) + '.lock', 'a+b')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    fd, stage = tempfile.mkstemp(prefix='.' + dst.name + '.stage-', dir=dst.parent)
    os.close(fd)
    try:
        with connect_read(src) as source, sqlite3.connect(stage) as db:
            source.backup(db)  # Consistent snapshot, including committed WAL.
            db.execute('PRAGMA journal_mode=DELETE'); db.execute('PRAGMA synchronous=FULL')
            before = fingerprint(db)
            version = db.execute('PRAGMA user_version').fetchone()[0]
            columns = {r[1] for r in db.execute('PRAGMA table_info(outbox)')}
            if version > 4 or not columns:
                raise ValueError('unsupported schema')
            if args.command == 'migrate':
                if 'call_seq' in columns:
                    raise ValueError('already ordered; use unchanged DB with automatic supported schema upgrade')
                states = {r[0] for r in db.execute('SELECT DISTINCT state FROM outbox')}
                if states - {'pending', 'in_flight', 'dead'}:
                    raise ValueError('unknown legacy state; manual reconciliation required')
                db.execute('ALTER TABLE outbox ADD COLUMN call_seq INTEGER')
            elif version not in (3, 4) or 'call_seq' not in columns:
                raise ValueError('retirement requires ordered v3/v4 database')
            db.commit()
            # executescript commits first; everything after BEGIN below is one transaction.
            db.executescript('BEGIN IMMEDIATE;\n' + SCHEMA)
            audit = dict(operation=args.command, at_ms=time.time_ns() // 1000000,
                         input_schema=version, input_legacy_fields_sha256=before)
            if args.command == 'migrate':
                mapping = []
                groups = db.execute("SELECT DISTINCT topic,call_uuid FROM outbox WHERE coalesce(call_uuid,'')<>'' ORDER BY topic,call_uuid").fetchall()
                for topic, call in groups:
                    rows = db.execute('SELECT event_id,msg_key FROM outbox WHERE topic=? AND call_uuid=? ORDER BY created_at_ms,event_id', (topic, call)).fetchall()
                    keys = {r[1] for r in rows}
                    if len(keys) != 1 or not next(iter(keys)):
                        raise ValueError('inconsistent/missing legacy key; no rewrite allowed')
                    db.execute('INSERT INTO call_streams(topic,call_uuid,msg_key,mode,admit_seq) VALUES(?,?,?,\'admission\',?)', (topic, call, next(iter(keys)), len(rows)))
                    for seq, (event, _) in enumerate(rows):
                        db.execute('UPDATE outbox SET call_seq=? WHERE event_id=?', (seq, event))
                        mapping.append(dict(event_id=event, topic=topic, call_uuid=call, admission_sequence=seq))
                audit.update(declared_backlog_order='created_at_ms,event_id; NOT recovered historical acceptance', mapping=mapping)
                # Preserve in_flight as well: normal startup requeues it, retaining identity/ordinal.
            else:
                if not args.reason.strip():
                    raise ValueError('auditable retirement reason required')
                row = db.execute('SELECT msg_key,next_seq,admit_seq FROM call_streams WHERE topic=? AND call_uuid=?', (args.topic, args.call_id)).fetchone()
                if not row or row[1] != row[2]:
                    raise ValueError('missing cursor or unresolved sequence gap')
                if db.execute('SELECT count(*) FROM outbox WHERE topic=? AND call_uuid=?', (args.topic, args.call_id)).fetchone()[0]:
                    raise ValueError('cannot retire any pending/in_flight/dead/TTL record')
                if db.execute('SELECT count(*) FROM retired_calls').fetchone()[0] >= args.max_rows:
                    raise ValueError('retirement fence capacity exceeded; no fence eviction')
                db.execute('INSERT INTO retired_calls VALUES(?,?,?,?,?,?)', (args.topic, args.call_id, row[0], row[1], audit['at_ms'], args.reason))
                db.execute('DELETE FROM call_streams WHERE topic=? AND call_uuid=?', (args.topic, args.call_id))
                audit.update(topic=args.topic, call_uuid=args.call_id, next_seq=row[1], reason=args.reason, permanently_reject_future=True)
            if fingerprint(db) != before:
                raise ValueError('original event fields changed; refusing output')
            stats = inventory(db, args.max_rows)
            if stats['call_streams'] > args.max_rows or sum(stats['states'].values()) > args.max_rows:
                raise ValueError('configured capacity insufficient for copied data; raise explicitly, never discard')
            audit['output_inventory'] = stats
            ident = hashlib.sha256(json.dumps(audit, sort_keys=True).encode()).hexdigest()
            db.execute('INSERT INTO operations_audit VALUES(?,?)', (ident, json.dumps(audit, sort_keys=True)))
            if os.environ.get('OUTBOX_OPS_TEST_INTERRUPT') == 'before_commit':
                os._exit(75)  # isolated test failpoint; never a partial published output
            db.commit()
            if db.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                raise ValueError('integrity check failed')
        if os.environ.get('OUTBOX_OPS_TEST_INTERRUPT') == 'after_commit':
            os._exit(75)
        with open(stage, 'rb') as f:
            os.fsync(f.fileno())
        # Same directory/filesystem; atomic no-clobber publication. Never switch module config.
        os.link(stage, dst)
        parent_fd = os.open(dst.parent, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(parent_fd)
        finally: os.close(parent_fd)
        if os.environ.get('OUTBOX_OPS_TEST_INTERRUPT') == 'after_publish':
            os._exit(75)
        return dict(output=str(dst), operation_id=ident, audit=audit,
                    output_sha256=hashlib.sha256(dst.read_bytes()).hexdigest())
    finally:
        Path(stage).unlink(missing_ok=True)
        Path(stage + '-journal').unlink(missing_ok=True)
        lock.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('command', choices=['inspect', 'migrate', 'retire'])
    p.add_argument('source')
    p.add_argument('--output')
    p.add_argument('--max-rows', type=int, default=100000)
    p.add_argument('--offline-confirm', action='store_true')
    p.add_argument('--accept-unknown-legacy-order', action='store_true')
    p.add_argument('--reject-future-call-id', action='store_true')
    p.add_argument('--topic'); p.add_argument('--call-id'); p.add_argument('--reason', default='')
    args = p.parse_args()
    if args.max_rows <= 0 or (args.command != 'inspect' and not args.output):
        p.error('positive --max-rows and a new --output for copy operations required')
    if args.command == 'retire' and (not args.topic or not args.call_id):
        p.error('--topic and --call-id required')
    try:
        print(json.dumps(run(args), ensure_ascii=False, indent=2))
    except (ValueError, OSError, sqlite3.Error) as e:
        p.exit(2, str(e) + '\n')


if __name__ == '__main__':
    main()
