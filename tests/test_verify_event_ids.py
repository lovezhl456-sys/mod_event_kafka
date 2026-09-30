import contextlib
import csv
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location('verify', Path(__file__).parents[1] / 'scripts/verify_event_ids.py')
verify = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify)


class VerifyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.write('injected_ids.txt', 'a\nb\nc\n')
        self.write('consumed_ids.txt', 'a\nb\nc\n')
        self.final([])

    def write(self, name, text):
        (self.root / name).write_text(text)

    def final(self, data):
        self.write('outbox_final.csv', 'event_id,state,last_error,created_at_ms,updated_at_ms\n' + '\n'.join(data))

    def run_check(self, *flags):
        with contextlib.redirect_stdout(io.StringIO()):
            return verify.main([str(self.root), *flags])

    def recovery(self):
        self.write('consumed_records.csv', 'event_id,partition,offset,observed_at_ms\na,0,0,900\nb,0,1,1500\nc,0,2,2100\n')
        self.write('injected_events.csv', 'event_id,call_uuid,sequence,phase\na,call,0,baseline\nb,call,1,during\nc,call,2,recovery\n')
        self.write('recovery.json', json.dumps(dict(sender_before=dict(pid=1, start_id='boot-1'),
            sender_after=dict(pid=1, start_id='boot-1'), reload_count=0,
            fault_start_ms=1000, fault_end_ms=2000, fault_exit_code=0)))

    def test_complete_and_duplicate_delivery_allowed(self):
        self.write('consumed_ids.txt', 'a\nb\na\nc\n')
        self.assertEqual(self.run_check(), 0)

    def test_empty_or_duplicate_injection_is_invalid(self):
        for content in ('', 'a\na\n'):
            self.write('injected_ids.txt', content)
            self.assertEqual(self.run_check(), 3)

    def test_missing_final_snapshot_is_not_pass(self):
        (self.root / 'outbox_final.csv').unlink()
        self.assertEqual(self.run_check(), 3)

    def test_missing_and_unexpected_consumption(self):
        for content in ('a\nb\n', 'a\nb\nc\nx\n'):
            self.write('consumed_ids.txt', content)
            self.assertEqual(self.run_check(), 1)

    def test_pending_even_after_consumption_is_not_drained(self):
        self.final(['a,in_flight,,100,200'])
        self.assertEqual(self.run_check(), 1)

    def test_rejected_must_be_explicit_and_disjoint(self):
        self.write('rejected_ids.txt', 'c\n')
        self.assertEqual(self.run_check('--allow-rejected'), 1)
        self.write('consumed_ids.txt', 'a\nb\n')
        self.assertEqual(self.run_check(), 1)
        self.assertEqual(self.run_check('--allow-rejected'), 0)
        self.write('rejected_ids.txt', 'c\nx\n')
        self.assertEqual(self.run_check('--allow-rejected'), 1)

    def test_expiry_requires_age_evidence_and_policy(self):
        self.write('consumed_ids.txt', 'a\nb\n')
        self.final(['c,dead,expired_ttl,100,1201'])
        self.assertEqual(self.run_check(), 1)
        self.assertEqual(self.run_check('--ttl-ms', '1000'), 0)
        self.final(['c,dead,expired_ttl,100,1100'])
        self.assertEqual(self.run_check('--ttl-ms', '1000'), 1)

    def test_expired_but_consumed_conflicts(self):
        self.final(['c,dead,expired_ttl,100,1201'])
        self.assertEqual(self.run_check('--ttl-ms', '1000'), 1)

    def test_permanent_dead_needs_expected_id(self):
        self.write('consumed_ids.txt', 'a\nb\n')
        self.final(['c,dead,authorization failed,100,200'])
        self.assertEqual(self.run_check(), 1)
        self.write('expected_dead_ids.txt', 'c\n')
        self.assertEqual(self.run_check(), 0)

    def test_bad_snapshot_fails_closed(self):
        for data in (['c,wrong,,100,200'], ['c,dead,,100,200'],
                     ['c,dead,expired_ttl,broken,200'], ['c,dead,x,300,200'],
                     ['c,dead,x,100,200', 'c,dead,x,100,200']):
            self.final(data)
            self.assertEqual(self.run_check(), 3)

    def test_order_has_independent_sequence_not_final_snapshot(self):
        self.recovery()
        self.assertEqual(self.run_check('--check-order'), 0)
        self.write('consumed_ids.txt', 'b\na\nc\n')
        self.assertEqual(self.run_check('--check-order'), 2)
        self.write('injected_events.csv', 'event_id,call_uuid,sequence,phase\na,call,0,baseline\n')
        self.assertEqual(self.run_check('--check-order'), 3)

    def test_recovery_needs_all_phases_and_no_restart(self):
        self.recovery()
        self.assertEqual(self.run_check('--require-recovery'), 0)
        meta = json.loads((self.root / 'recovery.json').read_text())
        for field, value in [('reload_count', 1), ('fault_exit_code', 1),
                             ('sender_after', dict(pid=1, start_id='boot-2'))]:
            changed = dict(meta, **{field: value})
            self.write('recovery.json', json.dumps(changed))
            self.assertEqual(self.run_check('--require-recovery'), 1)
        self.write('recovery.json', json.dumps(meta))
        self.write('injected_events.csv', 'event_id,call_uuid,sequence,phase\na,call,0,baseline\nb,call,1,during\nc,call,2,during\n')
        self.assertEqual(self.run_check('--require-recovery'), 1)

    def test_recovery_requires_consumption_on_correct_side_of_fault(self):
        self.recovery()
        for first, last in [(1100, 2100), (900, 1900)]:
            self.write('consumed_records.csv', f'event_id,partition,offset,observed_at_ms\na,0,0,{first}\nb,0,1,1500\nc,0,2,{last}\n')
            self.assertEqual(self.run_check('--require-recovery'), 1)

    def test_receipts_must_match_raw_consumption(self):
        self.recovery()
        self.write('consumed_ids.txt', 'a\nb\nc\nc\n')
        self.assertEqual(self.run_check('--require-recovery'), 3)

    def test_malformed_recovery_identity_and_counts(self):
        self.recovery()
        meta = json.loads((self.root / 'recovery.json').read_text())
        for field, value in [('reload_count', False), ('fault_start_ms', 1.5),
                             ('sender_before', dict(pid='1', start_id='boot-1'))]:
            self.write('recovery.json', json.dumps(dict(meta, **{field: value})))
            self.assertEqual(self.run_check('--require-recovery'), 3)

    def test_recovery_without_metadata_fails(self):
        self.assertEqual(self.run_check('--require-recovery'), 3)


if __name__ == '__main__':
    unittest.main()
