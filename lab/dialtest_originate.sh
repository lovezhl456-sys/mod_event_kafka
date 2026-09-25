#!/usr/bin/env bash
# Phase2 lab dialtest — generate CHANNEL_CREATE / ANSWER / HANGUP* via loopback.
# Requires: docker container lab-freeswitch (host network), toxiproxy Kafka up.
#
# Example:
#   /workspace/lab-mod-event-kafka/dialtest_originate.sh 2
#
# Exact fs_cli command used:
#   originate {ignore_early_media=true,origination_caller_id_number=dialtest}loopback/park/default &park()
set -euo pipefail

COUNT="${1:-1}"
FS_CONTAINER="${FS_CONTAINER:-lab-freeswitch}"

fsx() {
  local cmd=$1
  sg docker -c "docker exec ${FS_CONTAINER} env LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib \
    /usr/local/freeswitch/bin/fs_cli -x $(printf %q "$cmd")"
}

echo "dialtest: module_exists=$(fsx 'module_exists mod_event_kafka' | tr -d '\r')"
echo "dialtest: originating ${COUNT} loopback call(s)"

for i in $(seq 1 "$COUNT"); do
  CMD='originate {ignore_early_media=true,origination_caller_id_number=dialtest}loopback/park/default &park()'
  echo ">> $CMD"
  RESULT=$(fsx "$CMD" || true)
  echo "originate result: $RESULT"
  sleep 1
  UUID=$(echo "$RESULT" | grep -Eo '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1 || true)
  if [[ -n "${UUID:-}" ]]; then
    fsx "uuid_kill $UUID" || true
  else
    fsx "hupall normal_clearing" || true
  fi
  sleep 0.3
done
echo "dialtest done"
