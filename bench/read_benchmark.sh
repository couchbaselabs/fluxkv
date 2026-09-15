#!/usr/bin/env bash
#
# The read benchmark, as actually run. Starts the server, verifies the dataset
# is readable, warms the cache, then measures while sampling the server.
#
# Measured with this setup - 200M x 1KB incompressible keys (233G on disk),
# client on a separate node over 10GbE:
#
#   throughput          1,028,000 ops/s, zero errors
#   network             9.62 Gbit/s - 96% of the 10GbE link
#   disk                1,084,799 reads/s, 4.80 GB/s
#   read amplification  1.055 disk reads per GET
#   CPU                 98% busy
#   RSS                 17.39 GB of a 100 GB quota
#   latency             p50 31.8ms, p99 35.0ms
#
# All three resources are at their limit together, which is what a saturated
# system looks like.
#
# What the settings below are worth, measured on the same dataset:
#
#   quota 64G -> 100G        691K -> 1,005K ops/s. The cache holds the index,
#                            not the data: amplification fell 1.57 -> 1.06.
#                            RSS settles at ~17GB, so past ~20G buys nothing.
#   64 conns x 512 pipeline  1,012K -> 1,028K vs 256x128 at the same in-flight
#                            depth. Fewer sockets, fewer syscalls - 43.8% of
#                            CPU is syscall overhead.
#   io_uring                 1,004K. No different from libaio here.
#   io-threads 16            476K. Starves this many connections; the "lean is
#                            better" comment in main.cc is for a lighter load.
#   --no-hot-stats           545K. Halves throughput for reasons not yet
#                            understood - the counters it skips are only
#                            reported, never used for decisions. Leave it off.
#
# Usage:
#   CLIENT_HOST=root@10.0.0.2 DATA_DIR=/data/fluxkv-1k KEYS=200000000 \
#     bench/read_benchmark.sh

set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

CLIENT_HOST=${CLIENT_HOST:?set CLIENT_HOST, e.g. root@10.0.0.2}
CLIENT_BIN=${CLIENT_BIN:-/tmp/echobench-rv/echobench-rv}
DATA_DIR=${DATA_DIR:?set DATA_DIR to the dataset directory}
KEYS=${KEYS:?set KEYS to the dataset key count}

SERVER_IP=${SERVER_IP:-$(hostname -I | awk '{print $1}')}
PORT=${PORT:-12210}
VBUCKETS=${VBUCKETS:-256}

CONNS=${CONNS:-64}
PIPELINE=${PIPELINE:-512}
WARMUP=${WARMUP:-60s}
RUNTIME=${RUNTIME:-45s}
SSH=${SSH:-ssh}

client() {
    # shellcheck disable=SC2029
    ${SSH} "${CLIENT_HOST}" "${CLIENT_BIN} \
        -host ${SERVER_IP}:${PORT} \
        -mode get \
        -keys ${KEYS} \
        -vbuckets ${VBUCKETS} \
        -conns ${CONNS} \
        -pipeline ${PIPELINE} \
        -runtime $1 ${2:-}"
}

echo "### starting server"
DATA_DIR="${DATA_DIR}" PORT="${PORT}" HOSTNAME_ARG="${SERVER_IP}" \
SHARDS=${SHARDS:-32} VBUCKETS="${VBUCKETS}" \
READERS=${READERS:-64} WRITERS=${WRITERS:-32} IO_THREADS=${IO_THREADS:-32} \
IO_QUEUE_DEPTH=${IO_QUEUE_DEPTH:-32} MAX_READ_BATCH=${MAX_READ_BATCH:-1024} \
MEM_QUOTA=${MEM_QUOTA:-$((100 * 1024 * 1024 * 1024))} \
EXTRA_ARGS="${EXTRA_ARGS:---no-compression --write-queue-mem 8589934592}" \
    "${HERE}/run_server.sh"

# Check the dataset before measuring it. A GET for a key that was never stored
# is answered with a 24-byte KeyNotFound and no value, so an incomplete dataset
# reports a *higher* rate than a correct one.
echo "### verifying dataset"
probe=$(client 10s)
errs=$(sed -n 's/.*errs=\([0-9]*\).*/\1/p' <<<"${probe}" | tail -1)
if [[ "${errs}" != "0" ]]; then
    echo "FAIL: ${errs} operations failed - the dataset is incomplete." >&2
    echo "      Load it with bench/load.sh, which verifies as it goes." >&2
    exit 1
fi
echo "  dataset reads clean"

# RSS plateaus within about 25s; this is generous.
echo "### warming (${WARMUP})"
client "${WARMUP}" > /dev/null

echo "### measuring (${RUNTIME})"
client "${RUNTIME}" -histN\ 100 > /tmp/fluxkv-read-result.txt 2>&1 &
client_pid=$!

sleep 12
echo "### server during the run"
PORT="${PORT}" "${HERE}/server_metrics.sh" || true

wait "${client_pid}" || true
echo "### result"
grep -E '^DONE|^LAT' /tmp/fluxkv-read-result.txt
