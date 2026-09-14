#!/usr/bin/env bash
#
# Print the memory breakdown of a cgroup-limited server, plus its process RSS.
#
# Run this during a load run. It answers the question a throughput number
# cannot: is the memory cap actually doing anything?
#
#   memory.high breaches = 0   the cap is above the working set, nothing changed
#   breaches in the thousands  the kernel is reclaiming constantly
#
# Under buffered IO the interesting split is anon vs file. The process RSS can
# read ~200MB while the cgroup holds 20GB, because the data is in the kernel
# page cache rather than the process. RSS alone will mislead you.
#
# Usage: UNIT=fluxkv-bench bench/cgroup_stats.sh

set -euo pipefail

UNIT=${UNIT:-fluxkv-bench}
PORT=${PORT:-12210}
CG=/sys/fs/cgroup/system.slice/${UNIT}.service

if [[ ! -d "${CG}" ]]; then
    echo "no cgroup at ${CG} - is the unit running?" >&2
    exit 1
fi

field() {
    grep -w "^$1" "${CG}/memory.stat" 2>/dev/null | awk '{print $2}'
}

gib() {
    awk -v b="${1:-0}" 'BEGIN { printf "%.1f", b / 1073741824 }'
}

current=$(cat "${CG}/memory.current" 2>/dev/null || echo 0)
peak=$(cat "${CG}/memory.peak" 2>/dev/null || echo 0)
anon=$(field anon)
file=$(field file)
breaches=$(grep -w high "${CG}/memory.events" 2>/dev/null | awk '{print $2}')
pgscan=$(field pgscan)

echo "cgroup:        ${UNIT}"
echo "  memory.max:  $(cat "${CG}/memory.max" 2>/dev/null)"
echo "  memory.high: $(cat "${CG}/memory.high" 2>/dev/null)"
echo "  current:     $(gib "${current}") GiB   (peak $(gib "${peak}") GiB)"
echo "  anon:        $(gib "${anon}") GiB   (process memory)"
echo "  file:        $(gib "${file}") GiB   (kernel page cache)"
echo "  high breaches: ${breaches:-0}   pgscan: ${pgscan:-0}"

pid=$(pgrep -f "fluxkv_server --port ${PORT}" | head -1 || true)
if [[ -n "${pid}" ]]; then
    rss=$(awk '/VmRSS/{print $2}' "/proc/${pid}/status" 2>/dev/null || echo 0)
    hwm=$(awk '/VmHWM/{print $2}' "/proc/${pid}/status" 2>/dev/null || echo 0)
    echo "process ${pid}:"
    echo "  VmRSS:       $(gib $((rss * 1024))) GiB"
    echo "  VmHWM:       $(gib $((hwm * 1024))) GiB   (peak RSS)"
fi
