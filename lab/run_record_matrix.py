#!/usr/bin/env python3
"""Eight isolated key/policy/compression controls. No claim of cloud-upgrade reproduction."""
import argparse
import json
from pathlib import Path
import subprocess
import uuid
from run_recovery_drill import ROOT, BOOTSTRAP, admin, wait_isr


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, default=ROOT / 'build/record_probe')
    p.add_argument('--output', type=Path)
    a = p.parse_args()
    if not a.binary.is_file():
        p.error('build record_probe first')
    run_id = uuid.uuid4().hex
    root = (a.output or ROOT / 'reports' / ('records-' + run_id)).resolve()
    root.mkdir(parents=True, exist_ok=False)
    results = []
    status = 'ERROR'
    try:
        for policy in ('compact', 'delete'):
            topic = 'records_' + policy + '_' + run_id
            admin(topic, '--create', '--partitions', '3', '--replication-factor', '3',
                  '--config', 'min.insync.replicas=2', '--config', 'cleanup.policy=' + policy)
            wait_isr(topic, root / (policy + '-topic.txt'))
            for codec in ('snappy', 'none'):
                for key in ('null', 'key'):
                    expected = 87 if policy == 'compact' and key == 'null' else 0
                    name = f'{policy}-{codec}-{key}'
                    result = subprocess.run([str(a.binary.resolve()), BOOTSTRAP, topic, key, codec, str(expected)],
                                            capture_output=True, text=True, timeout=30)
                    (root / (name + '.log')).write_text(result.stdout + result.stderr)
                    results.append(dict(case=name, expected_error=expected, exit_code=result.returncode))
                    print(name, 'PASS' if result.returncode == 0 else 'FAIL')
        ok = len(results) == 8 and all(r['exit_code'] == 0 for r in results)
        status = 'PASS' if ok else 'FAIL'
        return 0 if ok else 1
    finally:
        (root / 'result.json').write_text(json.dumps(dict(status=status, scope='record-probe', cases=results), indent=2))
        print('Evidence:', root)


if __name__ == '__main__':
    raise SystemExit(main())
