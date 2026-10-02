#!/usr/bin/env bash
# Compatibility entry point. Cleanup on errors/signals is handled by fault_proxy.py.
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$SCRIPT_DIR/fault_proxy.py" cut \
  --seconds "${1:-150}" \
  --api "${TOXIPROXY_API:-http://127.0.0.1:8474}" \
  --proxies "${2:-${PROXIES:-kafka1,kafka2,kafka3}}"
