#!/usr/bin/env python3
"""Bounded localhost toxiproxy faults; restore only this run's changes."""
import argparse
import json
import signal
import sys
import time
import uuid
from urllib.parse import urlsplit, quote
from urllib.request import Request, urlopen


def request(api, method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = Request(api + path, data=data, method=method,
                  headers={'Content-Type': 'application/json'})
    with urlopen(req, timeout=5) as response:
        payload = response.read()
        return json.loads(payload) if payload else None


def inject(api, names, kind, seconds, latency_ms=500, jitter_ms=100, call=request, sleep=time.sleep):
    proxies = call(api, 'GET', '/proxies')
    for name in names:
        if name not in proxies or not proxies[name].get('enabled'):
            raise ValueError(f'proxy {name!r} missing or already disabled; no changes made')
    changed = []
    toxic_name = 'drill-' + uuid.uuid4().hex
    try:
        for name in names:
            path = '/proxies/' + quote(name, safe='')
            # Register cleanup before mutation: a lost HTTP response is ambiguous.
            changed.append(path)
            if kind == 'cut':
                call(api, 'POST', path, {'enabled': False})
            else:
                attributes = {'latency': latency_ms, 'jitter': jitter_ms} if kind == 'latency' else {'timeout': 0}
                call(api, 'POST', path + '/toxics', dict(name=toxic_name,
                     type='latency' if kind == 'latency' else 'timeout',
                     stream='downstream', toxicity=1.0, attributes=attributes))
        print(f'FAULT_ACTIVE kind={kind} time_ms={int(time.time()*1000)}', flush=True)
        sleep(seconds)
    finally:
        errors = []
        for path in reversed(changed):
            try:
                if kind == 'cut':
                    call(api, 'POST', path, {'enabled': True})
                else:
                    try:
                        call(api, 'DELETE', path + '/toxics/' + toxic_name)
                    except Exception as exc:
                        if getattr(exc, 'code', None) != 404:
                            raise
            except Exception as exc:
                errors.append(f'{path}: {exc}')
        if errors:
            raise RuntimeError('RESTORE_FAILED ' + '; '.join(errors))
        print(f'FAULT_RESTORED time_ms={int(time.time()*1000)}', flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('kind', choices=('cut', 'latency', 'blackhole'))
    p.add_argument('--seconds', type=float, default=35)
    p.add_argument('--api', default='http://127.0.0.1:8474')
    p.add_argument('--proxies', default='kafka1,kafka2,kafka3')
    p.add_argument('--latency-ms', type=int, default=500)
    p.add_argument('--jitter-ms', type=int, default=100)
    a = p.parse_args()
    url = urlsplit(a.api)
    if url.scheme != 'http' or url.hostname not in ('127.0.0.1', 'localhost', '::1') or url.path not in ('', '/') or url.query or url.fragment or url.username:
        p.error('use a localhost HTTP toxiproxy API (SSH tunnel for a remote lab)')
    names = a.proxies.split(',')
    if not 0 < a.seconds <= 3600 or not all(names) or len(names) != len(set(names)) or a.latency_ms < 0 or a.jitter_ms < 0:
        p.error('invalid duration, latency or proxy names')
    def interrupted(signum, frame):
        # A second signal must not interrupt restoration.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        raise InterruptedError(f'signal {signum}')
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        inject(a.api.rstrip('/'), names, a.kind, a.seconds, a.latency_ms, a.jitter_ms)
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
