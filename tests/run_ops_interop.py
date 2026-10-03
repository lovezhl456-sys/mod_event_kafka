#!/usr/bin/env python3
"""CTest entry point: exercise the real C++ core against offline-tool outputs."""
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from test_outbox_ops import DDL

binary = Path(sys.argv[1]).resolve()
tool = Path(__file__).resolve().parents[1] / 'scripts/outbox_ops.py'
with tempfile.TemporaryDirectory(prefix='ops-interop-') as directory:
    root = Path(directory)
    source = root / 'legacy.db'
    with sqlite3.connect(source) as db:
        db.execute(DDL)
        for event, state, order in [('a', 'in_flight', 3), ('b', 'dead', 1), ('c', 'pending', 2)]:
            db.execute('INSERT INTO outbox VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       (event, 't', 'k', b'payload\0' + event.encode(), state, 2, 1, 'test', order, order, 'call'))
    def operation(*args):
        result = subprocess.run([sys.executable, str(tool), *map(str, args)],
                                check=True, capture_output=True, text=True)
        return json.loads(result.stdout)
    migrated = root / 'migrated.db'
    operation('migrate', source, '--output', migrated, '--offline-confirm', '--accept-unknown-legacy-order')
    subprocess.run([str(binary), 'migrated', str(migrated)], check=True)
    with sqlite3.connect(migrated) as db:
        assert db.execute('SELECT next_seq,admit_seq FROM call_streams').fetchone() == (3, 4)
        assert db.execute('SELECT event_id,call_seq FROM outbox').fetchall() == [('new', 3)]
    seed, retired = root / 'seed.db', root / 'retired.db'
    subprocess.run([str(binary), 'seed', str(seed)], check=True)
    operation('retire', seed, '--output', retired, '--offline-confirm', '--topic', 't',
              '--call-id', 'closed', '--reason', 'isolated verified ACKs', '--reject-future-call-id')
    subprocess.run([str(binary), 'retired', str(retired)], check=True)
print('actual offline-tool / core interoperability PASS')
