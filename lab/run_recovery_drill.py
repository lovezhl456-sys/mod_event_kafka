#!/usr/bin/env python3
"""Exercise the candidate C++ pipeline against the isolated 3-broker lab."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ['docker', 'compose', '-p', 'event-kafka-cluster', '-f', str(ROOT / 'lab/cluster-compose.yml')]
BOOTSTRAP = '127.0.0.1:29092,127.0.0.1:29093,127.0.0.1:29094'


def command(args, timeout=60):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f'{args[0]} exit={result.returncode}: {result.stderr[-2000:]}')
    return result.stdout


def admin(topic, *args, broker='kafka1'):
    return command(COMPOSE + ['exec', '-T', broker, '/opt/kafka/bin/kafka-topics.sh',
                   '--bootstrap-server', 'localhost:9092', '--topic', topic, *args])


def full_isr(description):
    partitions = []
    for line in description.splitlines():
        if not re.search(r'\bPartition:\s*\d+', line):
            continue
        match = re.search(r'Leader:\s*(-?\d+).*Replicas:\s*([\d,]+).*Isr:\s*([\d,]+)', line)
        if not match:
            return False
        leader, replicas, isr = match.groups()
        partition_id = int(re.search(r'\bPartition:\s*(\d+)', line).group(1))
        partitions.append((partition_id, len(set(replicas.split(','))) == 3 and
                          set(replicas.split(',')) == set(isr.split(',')) and leader in isr.split(',')))
    return len(partitions) == 3 and {p for p, _ in partitions} == {0, 1, 2} and all(ok for _, ok in partitions)


def wait_isr(topic, path, broker='kafka1'):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            description = admin(topic, '--describe', broker=broker)
            path.write_text(description)
            if full_isr(description):
                return
        except RuntimeError:
            pass
        time.sleep(2)
    raise RuntimeError('three replicas/ISR did not recover; see ' + str(path))


def phase(root, value):
    temp = root / 'phase.tmp'
    temp.write_text(value + '\n')
    temp.replace(root / 'phase')


def roll(topic, root, hold):
    for broker in ('kafka1', 'kafka2', 'kafka3'):
        observer = 'kafka2' if broker == 'kafka1' else 'kafka1'
        wait_isr(topic, root / (broker + '-before.txt'), observer)
        try:
            command(COMPOSE + ['stop', '-t', '10', broker])
            (root / (broker + '-stopped.txt')).write_text(admin(topic, '--describe', broker=observer))
            time.sleep(hold)
        finally:
            # Even an interrupted stop must attempt to restore this lab service.
            command(COMPOSE + ['start', broker])
        wait_isr(topic, root / (broker + '-after.txt'), observer)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--fault', choices=('cut', 'latency', 'blackhole', 'rolling'), default='cut')
    p.add_argument('--seconds', type=float, default=35, help='fault duration; rolling: hold per broker')
    p.add_argument('--ttl-ms', type=int, default=120000)
    p.add_argument('--message-timeout-ms', type=int, default=30000)
    p.add_argument('--compression', choices=('snappy', 'none'), default='snappy')
    p.add_argument('--post-seconds', type=float, default=10)
    p.add_argument('--binary', type=Path, default=ROOT / 'build/test_pipeline_drill')
    p.add_argument('--output', type=Path)
    p.add_argument('--check-order', action='store_true', help='also test stricter per-call first-observation order')
    a = p.parse_args()
    if not 0 < a.seconds <= 600 or not 0 < a.post_seconds <= 600 or a.ttl_ms < 0 or a.message_timeout_ms < 100:
        p.error('invalid time bounds')
    if not a.binary.is_file():
        p.error('build test_pipeline_drill first')
    run_id = time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:8]
    root = (a.output or ROOT / 'reports' / run_id).resolve()
    root.mkdir(parents=True, exist_ok=False)
    topic = 'drill_' + uuid.uuid4().hex
    sender = fault = None
    fault_rc = -1
    try:
        metadata = dict(scope='candidate-core', scenario=a.fault, topic=topic,
                        ttl_ms=a.ttl_ms, message_timeout_ms=a.message_timeout_ms,
                        compression=a.compression, source_commit=command(['git', '-C', str(ROOT), 'rev-parse', 'HEAD']).strip())
        names = command(['git', '-C', str(ROOT), 'ls-files', '--cached', '--others', '--exclude-standard']).splitlines()
        digest = hashlib.sha256()
        for name in sorted(names):
            if name.startswith(('src/', 'include/', 'tests/', 'scripts/', 'lab/')) or name == 'CMakeLists.txt':
                digest.update(name.encode() + b'\0' + (ROOT / name).read_bytes())
        metadata['code_sha256'] = digest.hexdigest()
        metadata['source_dirty'] = bool(command(['git', '-C', str(ROOT), 'status', '--porcelain']).strip())
        (root / 'run.json').write_text(json.dumps(metadata, indent=2))
        admin(topic, '--create', '--partitions', '3', '--replication-factor', '3',
              '--config', 'min.insync.replicas=2', '--config', 'cleanup.policy=delete')
        wait_isr(topic, root / 'topic-before.txt')
        phase(root, 'baseline')
        with (root / 'sender.log').open('w') as sender_log, (root / 'fault.log').open('w') as fault_log:
            sender = subprocess.Popen([str(a.binary.resolve()), str(root), BOOTSTRAP, topic,
                       str(a.ttl_ms), a.compression, str(a.message_timeout_ms)], stdout=sender_log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 60
            while not (root / 'ready').exists():
                if sender.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError('baseline did not consume 5 events; inspect sender.log')
                time.sleep(0.2)
            phase(root, 'during')
            start_ms = int(time.time() * 1000)
            if a.fault == 'rolling':
                roll(topic, root, a.seconds)
                fault_rc = 0
            else:
                fault = subprocess.Popen([sys.executable, str(ROOT / 'lab/fault_proxy.py'), a.fault,
                           '--api', 'http://127.0.0.1:8475', '--seconds', str(a.seconds)],
                           stdout=fault_log, stderr=subprocess.STDOUT)
                fault_rc = fault.wait(timeout=a.seconds + 90)
            end_ms = int(time.time() * 1000)
            if fault_rc:
                raise RuntimeError('fault/restore failed; inspect fault.log')
            phase(root, 'recovery')
            time.sleep(a.post_seconds)
            (root / 'stop').touch()
            if sender.wait(timeout=60):
                raise RuntimeError('sender did not complete; inspect sender.log')
        wait_isr(topic, root / 'topic-after.txt')
        recovery = dict(sender_before=json.loads((root / 'sender_before.json').read_text()),
                        sender_after=json.loads((root / 'sender_after.json').read_text()),
                        reload_count=0, fault_start_ms=start_ms, fault_end_ms=end_ms, fault_exit_code=fault_rc)
        (root / 'recovery.json').write_text(json.dumps(recovery, indent=2))
        args = [sys.executable, str(ROOT / 'scripts/verify_event_ids.py'), str(root), '--require-recovery', '--ttl-ms', str(a.ttl_ms)]
        if a.check_order:
            args.append('--check-order')
        check = subprocess.run(args, capture_output=True, text=True)
        (root / 'verification.txt').write_text(check.stdout + check.stderr)
        print(check.stdout, end='')
        result = dict(status='PASS' if check.returncode == 0 else 'FAIL', scope='candidate-core',
                      verifier_exit_code=check.returncode, order_checked=a.check_order)
        (root / 'result.json').write_text(json.dumps(result, indent=2))
        print('Evidence:', root)
        return check.returncode
    except (Exception, KeyboardInterrupt) as exc:
        (root / 'result.json').write_text(json.dumps(dict(status='ERROR', reason=str(exc))))
        print(f'DRILL_ERROR {exc}; evidence: {root}', file=sys.stderr)
        return 3
    finally:
        if fault and fault.poll() is None:
            fault.terminate()
            try:
                fault.wait(timeout=30)  # allow its finally to restore proxies
            except subprocess.TimeoutExpired:
                print('RESTORE_UNKNOWN: inspect toxiproxy before another run', file=sys.stderr)
        if sender and sender.poll() is None:
            sender.terminate()
            try:
                sender.wait(timeout=10)
            except subprocess.TimeoutExpired:
                sender.kill()
                sender.wait()


if __name__ == '__main__':
    def interrupted(signum, frame):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        raise KeyboardInterrupt(f'signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    raise SystemExit(main())
