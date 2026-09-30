import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('recovery', Path(__file__).parents[1] / 'lab/run_recovery_drill.py')
recovery = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recovery)


class RecoveryToolsTests(unittest.TestCase):
    def test_isr_gate_requires_three_distinct_replicas_on_every_partition(self):
        good = '\n'.join(f'Topic: t Partition: {p} Leader: 1 Replicas: 1,2,3 Isr: 2,1,3' for p in range(3))
        self.assertTrue(recovery.full_isr(good))
        for broken in ('', good.replace('Isr: 2,1,3', 'Isr: 1,2', 1),
                       good.replace('Leader: 1', 'Leader: -1', 1),
                       good.replace('Replicas: 1,2,3', 'Replicas: 1', 1),
                       good.splitlines()[0], '\n'.join([good.splitlines()[0]] * 3)):
            self.assertFalse(recovery.full_isr(broken))


if __name__ == '__main__':
    unittest.main()
