#!/usr/bin/env bash
# 实验室辅助：切断全部 toxiproxy Kafka 前端 DURATION 秒，然后恢复。
# outbox TTL 测试：DURATION 须大于 outbox-ttl-ms（默认 120000 → 切断 >120s）。
# 短断自愈回归（L-02）：约 35s（小于 TTL）。
#
# 用法（仓库根目录；也可在脚本所在目录用 ./toxiproxy_cut_restore.sh）：
#   lab/toxiproxy_cut_restore.sh                 # 默认 150s（>120s TTL）
#   lab/toxiproxy_cut_restore.sh 35              # 短切
#   lab/toxiproxy_cut_restore.sh 150 kafka1,kafka2,kafka3
# 共享机绝对路径仍可用：
#   /workspace/lab-mod-event-kafka/toxiproxy_cut_restore.sh
#
# 环境变量：
#   TOXIPROXY_API  默认 http://127.0.0.1:8474
#   PROXIES        默认 kafka1,kafka2,kafka3
set -euo pipefail

DURATION_SEC="${1:-150}"
TOXIPROXY_API="${TOXIPROXY_API:-http://127.0.0.1:8474}"
PROXIES_CSV="${2:-${PROXIES:-kafka1,kafka2,kafka3}}"
IFS=',' read -r -a PROXIES <<< "$PROXIES_CSV"

api() {
  local method=$1 path=$2 body=${3:-}
  if [[ -n "$body" ]]; then
    curl -fsS -X "$method" -H 'Content-Type: application/json' -d "$body" "${TOXIPROXY_API}${path}"
  else
    curl -fsS -X "$method" "${TOXIPROXY_API}${path}"
  fi
}

set_enabled() {
  local name=$1 enabled=$2
  api POST "/proxies/${name}" "{\"enabled\":${enabled}}" >/dev/null
  echo "proxy ${name} enabled=${enabled}"
}

echo "toxiproxy_cut_restore: api=${TOXIPROXY_API} duration=${DURATION_SEC}s proxies=${PROXIES_CSV}"
api GET /proxies | head -c 200 >/dev/null || {
  echo "ERROR: toxiproxy API not reachable at ${TOXIPROXY_API}" >&2
  exit 1
}

echo "== CUT $(date -Is) =="
for p in "${PROXIES[@]}"; do
  set_enabled "$p" false
done

echo "sleeping ${DURATION_SEC}s (simulate Kafka outage)..."
sleep "$DURATION_SEC"

echo "== RESTORE $(date -Is) =="
for p in "${PROXIES[@]}"; do
  set_enabled "$p" true
done

echo "done. proxies:"
curl -fsS "${TOXIPROXY_API}/proxies" | python3 -m json.tool 2>/dev/null || curl -fsS "${TOXIPROXY_API}/proxies"
echo
