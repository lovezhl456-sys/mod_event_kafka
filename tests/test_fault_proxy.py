import contextlib
import importlib.util
import io
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('fault', Path(__file__).parents[1] / 'lab/fault_proxy.py')
fault = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fault)


class FaultTests(unittest.TestCase):
    def run_fault(self, kind='cut', fail=False, interrupt=False, restore_fail=False):
        calls = []
        def api(base, method, path, body=None):
            calls.append((method, path, body))
            if method == 'GET':
                return {n: {'enabled': True} for n in ('kafka1', 'kafka2')}
            if fail and path == '/proxies/kafka2' and body == {'enabled': False}:
                raise RuntimeError('HTTP response lost')
            if restore_fail and body == {'enabled': True}:
                raise RuntimeError('restore unavailable')
        def sleep(seconds):
            if interrupt:
                raise InterruptedError('interrupted')
        with contextlib.redirect_stdout(io.StringIO()):
            if fail or interrupt or restore_fail:
                with self.assertRaises((RuntimeError, InterruptedError)):
                    fault.inject('http://localhost', ['kafka1', 'kafka2'], kind, 1, call=api, sleep=sleep)
            else:
                fault.inject('http://localhost', ['kafka1', 'kafka2'], kind, 1, call=api, sleep=sleep)
        return calls

    def test_partial_cut_and_interruption_restore_all_touched(self):
        for options in ({'fail': True}, {'interrupt': True}, {}):
            calls = self.run_fault(**options)
            restored = {path for method, path, body in calls if body == {'enabled': True}}
            self.assertEqual(restored, {'/proxies/kafka1', '/proxies/kafka2'})

    def test_toxic_cleanup_only_deletes_own_toxic(self):
        for kind in ('latency', 'blackhole'):
            calls = self.run_fault(kind=kind, interrupt=True)
            created = [body['name'] for method, path, body in calls if path.endswith('/toxics')]
            deleted = [path.rsplit('/', 1)[1] for method, path, body in calls if method == 'DELETE']
            self.assertEqual(created, deleted)
            self.assertTrue(all(name.startswith('drill-') for name in deleted))

    def test_failed_restore_is_not_success(self):
        self.run_fault(restore_fail=True)

    def test_bad_preflight_makes_no_mutation(self):
        calls = []
        def api(base, method, path, body=None):
            calls.append(method)
            return {'kafka1': {'enabled': False}}
        with self.assertRaises(ValueError):
            fault.inject('http://localhost', ['kafka1'], 'cut', 1, call=api)
        self.assertEqual(calls, ['GET'])


if __name__ == '__main__':
    unittest.main()
