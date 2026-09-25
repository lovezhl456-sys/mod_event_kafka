#!/usr/bin/env python3
"""Verify injected vs consumed x-fs-event-id sets for lab acceptance.

Usage:
  scripts/verify_event_ids.py reports/<run-id>/

Expected files in that directory (see docs/TEST-PLAN.md §3.3):
  injected_ids.txt
  consumed_ids.txt
  optional: rejected_ids.txt, outbox_snapshot.csv

Exit: 0 ok | 1 set mismatch | 2 order fail | 3 bad artifacts
"""
from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path


def read_lines(path: Path) -> list[str]:
    if not path.is_file():
        return []
    return [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: verify_event_ids.py reports/<run-id>/", file=sys.stderr)
        return 3
    root = Path(sys.argv[1])
    inj_path = root / "injected_ids.txt"
    con_path = root / "consumed_ids.txt"
    if not inj_path.is_file() or not con_path.is_file():
        print(f"missing injected_ids.txt or consumed_ids.txt under {root}", file=sys.stderr)
        return 3

    injected = read_lines(inj_path)
    consumed = read_lines(con_path)
    rejected = set(read_lines(root / "rejected_ids.txt"))

    inj_set = set(injected)
    con_set = set(consumed)
    if len(injected) != len(inj_set):
        print(f"WARN: duplicate ids in injected_ids.txt count={len(injected)-len(inj_set)}")

    missing = inj_set - con_set - rejected
    unexpected = con_set - inj_set
    dupes = len(consumed) - len(con_set)

    print(f"injected={len(inj_set)} consumed_unique={len(con_set)} "
          f"consumed_raw={len(consumed)} rejected={len(rejected)} "
          f"dupes_in_consume={dupes}")
    print(f"missing={len(missing)} unexpected={len(unexpected)}")

    rc = 0
    if missing or unexpected:
        for x in sorted(missing)[:20]:
            print(f"MISSING {x}")
        for x in sorted(unexpected)[:20]:
            print(f"UNEXPECTED {x}")
        rc = 1

    snap = root / "outbox_snapshot.csv"
    if snap.is_file():
        by_call: dict[str, list[tuple[int, str]]] = defaultdict(list)
        with snap.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cu = (row.get("call_uuid") or "").strip()
                if not cu:
                    continue
                try:
                    created = int(row["created_at_ms"])
                except (KeyError, ValueError):
                    continue
                by_call[cu].append((created, row["event_id"]))
        # first occurrence order in consumed_dedup
        first_pos = {}
        for i, eid in enumerate(consumed):
            if eid not in first_pos:
                first_pos[eid] = i
        for cu, items in by_call.items():
            items.sort()
            ids = [eid for _, eid in items if eid in first_pos]
            positions = [first_pos[eid] for eid in ids]
            if positions != sorted(positions):
                print(f"ORDER_FAIL call_uuid={cu}")
                rc = 2 if rc == 0 else rc

    if rc == 0:
        print("VERIFY_OK")
    return rc


if __name__ == "__main__":
    sys.exit(main())
