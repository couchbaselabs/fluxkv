#!/usr/bin/env bash
#
# Sample what the server is doing during a load run: CPU, disk, network and
# memory. Run it on the server while the client drives load from another node.
#
# Throughput alone does not say what is limiting you. These four together do:
#
#   CPU near 100% busy      the server is the limit
#   NIC near line rate      the wire is the limit
#   disk r/s flat           the device is the limit
#   RSS far below the quota the cache is not the limit, so raising it is futile
#
# Report RSS every time. It is the difference between "needs 100GB" and
# "asked for 100GB and used 17".
#
# Usage: PORT=12210 NIC=eno1np0 bench/server_metrics.sh [sample-seconds]

set -euo pipefail

PORT=${PORT:-12210}
NIC=${NIC:-$(ip -o -4 route show to default 2>/dev/null | awk '{print $5}' | head -1)}
DEV=${DEV:-md0}
SECS=${1:-4}

pid=$(pgrep -f "fluxkv_server --port ${PORT}" | head -1 || true)
if [[ -z "${pid}" ]]; then
    echo "no fluxkv_server on port ${PORT}" >&2
    exit 1
fi

# CPU: idle is the number that matters. Anything under ~10% idle means the
# server, not the disk or the wire, is setting the ceiling.
cpu=$(top -bn2 -d"${SECS}" | awk '/^%Cpu/{l=$0} END{print l}')

# Disk: reads per second against ops per second gives read amplification.
disk=$(iostat -x "${SECS}" 2 2>/dev/null \
       | awk -v d="^${DEV}" '$0 ~ d {r=$2; mb=$3/1024} END{printf "%.0f r/s, %.2f GB/s", r, mb/1024}')

# Network: compare against the link speed to see how much headroom is left.
speed=$(cat "/sys/class/net/${NIC}/speed" 2>/dev/null || echo "?")
a=$(cat "/sys/class/net/${NIC}/statistics/tx_bytes")
sleep "${SECS}"
b=$(cat "/sys/class/net/${NIC}/statistics/tx_bytes")
tx=$(awk -v a="$a" -v b="$b" -v s="${SECS}" 'BEGIN{printf "%.2f", (b-a)/s*8/1e9}')

rss=$(awk '/VmRSS/{printf "%.2f", $2/1048576}' "/proc/${pid}/status")
hwm=$(awk '/VmHWM/{printf "%.2f", $2/1048576}' "/proc/${pid}/status")
quota=$(tr '\0' ' ' < "/proc/${pid}/cmdline" \
        | grep -oE 'mem-quota [0-9]+' | awk '{printf "%.0f", $2/1073741824}')

echo "  CPU:     ${cpu}"
echo "  disk:    ${disk} (${DEV})"
echo "  network: ${tx} Gbit/s of ${speed} Mb/s link (${NIC})"
echo "  memory:  RSS ${rss} GB, peak ${hwm} GB, quota ${quota} GB"
