#!/usr/bin/env python3
"""Verify a non-empty run using consumed IDs and a final outbox snapshot.

See docs/TEST-PLAN.md for the evidence contract. Exit: 0 pass, 1 outcome
mismatch, 2 order mismatch, 3 missing/malformed evidence. Never edits evidence.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import json
from pathlib import Path


class EvidenceError(ValueError):
    pass


def ids(root, name, required=False):
    path = root / name
    if not path.is_file():
        if required:
            raise EvidenceError(f"missing {name}")
        return []
    return [s.strip() for s in path.read_text(encoding="utf-8").splitlines() if s.strip()]


def unique(values, name):
    if len(values) != len(set(values)):
        raise EvidenceError(f"duplicate IDs in {name}")
    return set(values)


def rows(root, name, columns, unique_ids=True):
    with (root / name).open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, strict=True)
        if len(reader.fieldnames or []) != len(set(reader.fieldnames or [])):
            raise EvidenceError(f"{name}: duplicate CSV column")
        if not set(columns).issubset(reader.fieldnames or []):
            raise EvidenceError(f"{name}: required columns {','.join(columns)}")
        result = list(reader)
    if any(None in row or any(row.get(c) is None for c in columns) for row in result):
        raise EvidenceError(f"{name}: malformed CSV row")
    if any(not row["event_id"].strip() for row in result):
        raise EvidenceError(f"{name}: empty event_id")
    if unique_ids:
        unique([r["event_id"] for r in result], name)
    return result


def number(value, field):
    if type(value) not in (int, str):
        raise EvidenceError(f"invalid integer type: {field}")
    try:
        result = int(value)
    except (ValueError, TypeError):
        raise EvidenceError(f"invalid integer: {field}")
    if result < 0:
        raise EvidenceError(f"negative {field}")
    return result


def verify(args):
    root = args.run_dir
    injected = unique(ids(root, "injected_ids.txt", True), "injected_ids.txt")
    if not injected:
        raise EvidenceError("empty run: no injected events")
    consumed_list = ids(root, "consumed_ids.txt", True)
    consumed = set(consumed_list)
    rejected = unique(ids(root, "rejected_ids.txt"), "rejected_ids.txt")
    expected_dead = unique(ids(root, "expected_dead_ids.txt"), "expected_dead_ids.txt")
    final = rows(root, "outbox_final.csv", ("event_id", "state", "last_error",
                                             "created_at_ms", "updated_at_ms"))
    failures = []
    expired, dead, live = set(), set(), set()
    for row in final:
        eid, state = row["event_id"], row["state"]
        created = number(row["created_at_ms"], "created_at_ms")
        updated = number(row["updated_at_ms"], "updated_at_ms")
        if updated < created:
            raise EvidenceError(f"{eid}: updated_at_ms precedes created_at_ms")
        if state in ("pending", "in_flight"):
            live.add(eid)
        elif state == "dead":
            if row["last_error"] == "expired_ttl":
                expired.add(eid)
                if args.ttl_ms <= 0 or updated - created <= args.ttl_ms:
                    failures.append(f"invalid/unauthorized TTL expiry: {eid}")
            elif row["last_error"].strip():
                dead.add(eid)
            else:
                raise EvidenceError(f"{eid}: dead row without last_error")
        else:
            raise EvidenceError(f"{eid}: unknown state {state!r}")
    if live:
        failures.append(f"outbox not drained: {len(live)} pending/in_flight")
    if rejected and not args.allow_rejected:
        failures.append("rejected events require --allow-rejected")
    if dead != expected_dead:
        failures.append("permanent dead IDs differ from expected_dead_ids.txt")
    outcomes = {"consumed": consumed, "rejected": rejected, "expired": expired, "dead": dead}
    covered = set().union(*outcomes.values())
    for name, values in outcomes.items():
        if values - injected:
            failures.append(f"unexpected {name} IDs: {sorted(values - injected)[:10]}")
    if live - injected:
        failures.append("unexpected IDs in final outbox")
    for i, (name, values) in enumerate(outcomes.items()):
        for other, others in list(outcomes.items())[i + 1:]:
            if values & others:
                failures.append(f"conflicting outcomes {name}/{other}: {sorted(values & others)[:10]}")
    if injected - covered:
        failures.append(f"missing IDs: {sorted(injected - covered)[:20]}")

    ordered = None
    if args.check_order or args.require_recovery:
        ordered = rows(root, "injected_events.csv", ("event_id", "call_uuid", "sequence", "phase"))
        if {r["event_id"] for r in ordered} != injected:
            raise EvidenceError("injected_events.csv does not cover exactly injected_ids.txt")
        if any(r["phase"] not in ("baseline", "during", "recovery") for r in ordered):
            raise EvidenceError("invalid phase in injected_events.csv")
    order_failed = False
    if args.check_order:
        first_position = {}
        for i, eid in enumerate(consumed_list):
            first_position.setdefault(eid, i)
        calls = {}
        for row in ordered:
            sequence = number(row["sequence"], "sequence")
            if row["call_uuid"]:
                calls.setdefault(row["call_uuid"], []).append((sequence, row["event_id"]))
        if not calls:
            raise EvidenceError("order requested but no call_uuid evidence")
        for call, events in calls.items():
            if len({n for n, _ in events}) != len(events):
                raise EvidenceError(f"duplicate sequence for call {call}")
            positions = [first_position[eid] for _, eid in sorted(events) if eid in first_position]
            if positions != sorted(positions):
                failures.append(f"ORDER_FAIL call_uuid={call}")
                order_failed = True
    if args.require_recovery:
        meta = json.loads((root / "recovery.json").read_text(encoding="utf-8"))
        # Identities must include process start identity, not just a reusable PID.
        before, after = meta["sender_before"], meta["sender_after"]
        for identity in (before, after):
            if (not isinstance(identity, dict) or type(identity.get("pid")) is not int
                    or identity["pid"] <= 0 or not isinstance(identity.get("start_id"), str)
                    or not identity["start_id"].strip()):
                raise EvidenceError("sender identity needs pid and start_id")
        if type(meta["reload_count"]) is not int or type(meta["fault_exit_code"]) is not int:
            raise EvidenceError("reload_count/fault_exit_code must be integers")
        if before != after or meta["reload_count"] != 0:
            failures.append("sender restarted or module reloaded")
        start = number(meta["fault_start_ms"], "fault_start_ms")
        end = number(meta["fault_end_ms"], "fault_end_ms")
        if end <= start or meta["fault_exit_code"] != 0:
            failures.append("fault did not complete successfully")
        receipts = rows(root, "consumed_records.csv", ("event_id", "partition", "offset", "observed_at_ms"), unique_ids=False)
        if Counter(r["event_id"] for r in receipts) != Counter(consumed_list):
            raise EvidenceError("consumed_records.csv differs from consumed_ids.txt")
        observed = {}
        for row in receipts:
            number(row["partition"], "partition")
            number(row["offset"], "offset")
            observed.setdefault(row["event_id"], []).append(number(row["observed_at_ms"], "observed_at_ms"))
        for phase in ("baseline", "during", "recovery"):
            phase_ids = {r["event_id"] for r in ordered if r["phase"] == phase}
            if not phase_ids:
                failures.append(f"no injected events in {phase}")
            if phase == "baseline" and not any(t <= start for eid in phase_ids for t in observed.get(eid, [])):
                failures.append("no baseline consumption before fault")
            if phase == "recovery" and not any(t > end for eid in phase_ids for t in observed.get(eid, [])):
                failures.append("no new recovery consumption after fault")

    print(f"injected={len(injected)} consumed_unique={len(consumed)} "
          f"duplicates={len(consumed_list)-len(consumed)} rejected={len(rejected)} "
          f"expired={len(expired)} dead={len(dead)} live={len(live)}")
    print(f"order={'checked' if args.check_order else 'NOT_CHECKED'} "
          f"recovery={'checked' if args.require_recovery else 'NOT_CHECKED'}")
    for message in failures:
        print(f"FAIL {message}")
    if failures:
        return 2 if order_failed and len(failures) == 1 else 1
    print("VERIFY_OK (only the checks explicitly listed above)")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--allow-rejected", action="store_true")
    parser.add_argument("--ttl-ms", type=int, default=0,
                        help="allow evidenced expired_ttl outcomes under this TTL; default forbids expiry")
    parser.add_argument("--check-order", action="store_true")
    parser.add_argument("--require-recovery", action="store_true")
    args = parser.parse_args(argv)
    if args.ttl_ms < 0:
        parser.error("--ttl-ms must be non-negative")
    try:
        return verify(args)
    except (OSError, UnicodeError, ValueError, KeyError, TypeError, csv.Error) as exc:
        print(f"EVIDENCE_ERROR {exc}")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
