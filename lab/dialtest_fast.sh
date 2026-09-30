#!/usr/bin/env bash
# 快拨测：bgapi + 短等待（避免 park 路径因长时间 NO_ANSWER 而阻塞）。
# 在仓库根目录执行：lab/dialtest_fast.sh [次数]
# 共享机上的绝对路径仍可用：/workspace/lab-mod-event-kafka/dialtest_fast.sh
set -euo pipefail
COUNT="${1:-1}"
FS_CONTAINER="${FS_CONTAINER:-lab-freeswitch}"
fsx() {
  sg docker -c "docker exec ${FS_CONTAINER} env LD_LIBRARY_PATH=/usr/local/freeswitch/lib:/usr/local/lib \
    /usr/local/freeswitch/bin/fs_cli -x $(printf %q "$1")"
}
echo "dialtest_fast: module_exists=$(fsx 'module_exists mod_event_kafka' | tr -d '\r')"
for i in $(seq 1 "$COUNT"); do
  CMD='bgapi originate {ignore_early_media=true,originate_timeout=2,origination_caller_id_number=dialtest}loopback/park/default &park()'
  echo ">> $CMD"
  fsx "$CMD" || true
  sleep 0.8
  fsx 'hupall normal_clearing' || true
  sleep 0.2
done
echo "dialtest_fast done"
