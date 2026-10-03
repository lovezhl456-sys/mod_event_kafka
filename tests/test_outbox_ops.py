import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

TOOL = Path(__file__).resolve().parents[1] / 'scripts/outbox_ops.py'
DDL = '''CREATE TABLE outbox(event_id TEXT PRIMARY KEY,topic TEXT NOT NULL,msg_key TEXT,
payload BLOB NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL,next_attempt_at_ms INTEGER NOT NULL,
last_error TEXT,created_at_ms INTEGER NOT NULL,updated_at_ms INTEGER NOT NULL,call_uuid TEXT)'''


class Operations(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.src = self.root / 'legacy.db'
        with sqlite3.connect(self.src) as db:
            db.execute(DDL)

    def tearDown(self):
        self.tmp.cleanup()

    def call(self, command, source=None, output='candidate.db', flags=(), ok=True, env=None):
        cmd = [sys.executable, str(TOOL), command, str(source or self.src), '--max-rows', '4']
        if command != 'inspect':
            cmd += ['--output', str(self.root / output), '--offline-confirm']
        if command == 'migrate':
            cmd += ['--accept-unknown-legacy-order']
        p = subprocess.run(cmd + list(flags), env=env, capture_output=True, text=True)
        if ok:
            self.assertEqual(p.returncode, 0, p.stderr)
            return json.loads(p.stdout)
        self.assertNotEqual(p.returncode, 0)
        return p

    def populate(self):
        with sqlite3.connect(self.src) as db:
            for event, state, created in [('a','in_flight',3),('b','dead',1),('c','pending',2)]:
                db.execute('INSERT INTO outbox VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                           (event,'t','k',b'\x00raw\xff'+event.encode(),state,7,100,'reason',created,5,'call'))

    def test_empty_and_nonempty_preserve(self):
        empty = self.call('migrate', output='empty.db')
        self.assertEqual(empty['audit']['mapping'], [])
        self.populate()
        before = self.src.read_bytes()
        result = self.call('migrate')
        self.assertEqual(self.src.read_bytes(), before)
        self.assertEqual([r['event_id'] for r in result['audit']['mapping']], ['b','c','a'])
        with sqlite3.connect(self.root/'candidate.db') as db, sqlite3.connect(self.src) as old:
            self.assertEqual(db.execute('SELECT * FROM outbox ORDER BY event_id').fetchall(),
                             [tuple(row) + (dict(b=0,c=1,a=2)[row[0]],)
                              for row in old.execute('SELECT * FROM outbox ORDER BY event_id')])
            self.assertEqual(db.execute('SELECT next_seq,admit_seq FROM call_streams').fetchone(), (0,3))
        self.call('migrate', ok=False)  # never overwrite a completed output

    def test_interrupted_migration_repeat(self):
        self.populate(); before = self.src.read_bytes()
        p = self.call('migrate', ok=False, env=dict(os.environ, OUTBOX_OPS_TEST_INTERRUPT='before_commit'))
        self.assertEqual(p.returncode, 75)
        self.assertFalse((self.root/'candidate.db').exists())
        self.assertEqual(self.src.read_bytes(), before)
        self.call('migrate')
        self.assertTrue((self.root/'candidate.db').exists())

    def test_committed_stage_and_published_recovery(self):
        self.populate()
        self.call('migrate', ok=False, env=dict(os.environ, OUTBOX_OPS_TEST_INTERRUPT='after_commit'))
        self.assertFalse((self.root/'candidate.db').exists())
        self.call('migrate', ok=False, env=dict(os.environ, OUTBOX_OPS_TEST_INTERRUPT='after_publish'))
        self.assertTrue((self.root/'candidate.db').exists())
        with sqlite3.connect(self.root/'candidate.db') as db:
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
            self.assertEqual(db.execute('SELECT count(*) FROM outbox').fetchone()[0], 3)
            self.assertEqual(db.execute('SELECT count(*) FROM operations_audit').fetchone()[0], 1)
        self.call('migrate', ok=False) # existing valid output is never overwritten on retry

    def test_missing_explicit_acceptance_and_bad_key(self):
        p = subprocess.run([sys.executable,str(TOOL),'migrate',str(self.src),'--output',str(self.root/'bad.db'),'--offline-confirm'], capture_output=True)
        self.assertNotEqual(p.returncode,0);self.assertFalse((self.root/'bad.db').exists())
        self.populate()
        with sqlite3.connect(self.src) as db:db.execute("UPDATE outbox SET msg_key='other' WHERE event_id='a'")
        self.call('migrate', ok=False)
        self.assertFalse((self.root/'candidate.db').exists())

    def test_live_cooperating_writer_lock(self):
        import fcntl
        with open(str(self.src)+'.lock','w') as f:
            fcntl.flock(f,fcntl.LOCK_EX)
            self.call('migrate',ok=False)

    def test_retirement_barriers_and_fence_capacity(self):
        self.populate();self.call('migrate')
        src = self.root/'candidate.db'
        flags=['--topic','t','--call-id','call','--reason','operator verified closure','--reject-future-call-id']
        # Includes dead and in_flight: never release any of them.
        self.call('retire',source=src,output='retired.db',flags=flags,ok=False)
        with sqlite3.connect(src) as db:
            db.execute('DELETE FROM outbox') # test fixture emulates all ACKs, never production tool
            db.execute('UPDATE call_streams SET next_seq=admit_seq')
        # No explicit irreversible-policy choice is rejected.
        self.call('retire',source=src,output='retired.db',flags=flags[:-1],ok=False)
        # Interrupted copy does not release original cursor.
        self.call('retire',source=src,output='retired.db',flags=flags,ok=False,
                  env=dict(os.environ, OUTBOX_OPS_TEST_INTERRUPT='before_commit'))
        with sqlite3.connect(src) as db:self.assertEqual(db.execute('SELECT count(*) FROM call_streams').fetchone()[0],1)
        self.call('retire',source=src,output='retired.db',flags=flags)
        retired=self.root/'retired.db'
        with sqlite3.connect(retired) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM call_streams').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT next_seq FROM retired_calls').fetchone()[0],3)
            for i in range(3):db.execute('INSERT INTO retired_calls VALUES(?,?,?,?,?,?)',('t',str(i),'k',0,1,'test'))
            db.execute("INSERT INTO call_streams(topic,call_uuid,msg_key,mode) VALUES('t','new','k','admission')")
        self.call('retire',source=retired,output='full.db',flags=['--topic','t','--call-id','new','--reason','test','--reject-future-call-id'],ok=False)
        info=self.call('inspect',source=retired)
        self.assertEqual(info['retired_calls'],4);self.assertTrue(info['warnings'])

    def test_gap_blocks_retirement_and_capacity_blocks_migration(self):
        self.call('migrate')
        src=self.root/'candidate.db'
        with sqlite3.connect(src) as db:
            db.execute("INSERT INTO call_streams(topic,call_uuid,msg_key,mode,admit_seq) VALUES('t','gap','k','source',5)")
        self.call('retire',source=src,output='bad.db',flags=['--topic','t','--call-id','gap','--reason','test','--reject-future-call-id'],ok=False)
        with sqlite3.connect(self.src) as db:
            for n in range(5):db.execute('INSERT INTO outbox VALUES(?,?,?,?,?,?,?,?,?,?,?)',(str(n),'t','k','p','pending',0,1,'',1,1,str(n)))
        self.call('migrate',output='overflow.db',ok=False)

    def test_cpp_core_opens_tool_databases(self):
        binary = os.environ.get('OPS_INTEROP_BIN')
        if not binary:
            self.skipTest('OPS_INTEROP_BIN is not set')
        self.populate()
        self.call('migrate', output='migrated.db')
        migrated = subprocess.run([binary, 'migrated', str(self.root / 'migrated.db')],
                                  capture_output=True, text=True)
        self.assertEqual(migrated.returncode, 0, migrated.stdout + migrated.stderr)
        seed = self.root / 'seed.db'
        seeded = subprocess.run([binary, 'seed', str(seed)], capture_output=True, text=True)
        self.assertEqual(seeded.returncode, 0, seeded.stdout + seeded.stderr)
        flags = ['--topic', 't', '--call-id', 'closed', '--reason', 'operator verified closure',
                 '--reject-future-call-id']
        self.call('retire', source=seed, output='retired.db', flags=flags)
        retired = subprocess.run([binary, 'retired', str(self.root / 'retired.db')],
                                 capture_output=True, text=True)
        self.assertEqual(retired.returncode, 0, retired.stdout + retired.stderr)


if __name__ == '__main__':unittest.main()
