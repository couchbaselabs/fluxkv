#!/usr/bin/env bash
#
# Start fluxkv_server for a benchmark run, and wait until it is listening.
#
# The IO path is selected by environment variables read by magma:
#   MAGMA_ASYNC_IO=uring   io_uring instead of the libaio default
#   MAGMA_DIRECT_IO=0      buffered IO instead of the O_DIRECT default
#
# Everything else is set by the variables below. Defaults match the read
# benchmarks: a large dataset, a lean IO thread count and 64 readers.
#
# Usage:
#   bench/run_server.sh
#   MAGMA_ASYNC_IO=uring MAGMA_DIRECT_IO=0 bench/run_server.sh

set -euo pipefail

SERVER_BIN=${SERVER_BIN:-./build/fluxkv_server}
DATA_DIR=${DATA_DIR:?set DATA_DIR to the dataset directory}
PORT=${PORT:-12210}
HOSTNAME_ARG=${HOSTNAME_ARG:-127.0.0.1}
BUCKET=${BUCKET:-default}

SHARDS=${SHARDS:-32}
VBUCKETS=${VBUCKETS:-256}
READERS=${READERS:-64}
WRITERS=${WRITERS:-32}
IO_THREADS=${IO_THREADS:-32}
IO_QUEUE_DEPTH=${IO_QUEUE_DEPTH:-32}
MAX_READ_BATCH=${MAX_READ_BATCH:-1024}
MEM_QUOTA=${MEM_QUOTA:-$((64 * 1024 * 1024 * 1024))}
LOG=${LOG:-/tmp/fluxkv-server.log}

EXTRA_ARGS=${EXTRA_ARGS:-}
DROP_CACHES=${DROP_CACHES:-1}

# Ask the server to stop and give it time to flush before forcing it.
#
# SIGKILL here loses whatever magma has not yet written out. After a 200M key
# load that cost 29% of the dataset: reads of the earliest keys all succeeded
# while 97% of the most recently written keys were gone. The data looked
# present - 177G on disk - so the loss only showed up as a miss rate.
stop_server() {
    pkill -TERM -f "fluxkv_server --port ${PORT}" 2>/dev/null || true

    for _ in $(seq 1 "${SHUTDOWN_TIMEOUT:-120}"); do
        pgrep -f "fluxkv_server --port ${PORT}" > /dev/null || return 0
        sleep 1
    done

    echo "WARN: server did not exit within ${SHUTDOWN_TIMEOUT:-120}s;" \
         "forcing. Recently written data may be lost." >&2
    pkill -9 -f "fluxkv_server --port ${PORT}" 2>/dev/null || true
    sleep 2
}

stop_server

# Start from a cold page cache so runs are comparable. Without this, a
# buffered-IO run inherits whatever the previous run left cached.
if [[ "${DROP_CACHES}" == "1" ]]; then
    sync
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
fi

# shellcheck disable=SC2086
setsid "${SERVER_BIN}" \
    --port "${PORT}" \
    --data-dir "${DATA_DIR}" \
    --bucket "${BUCKET}" \
    --hostname "${HOSTNAME_ARG}" \
    --shards "${SHARDS}" \
    --vbuckets "${VBUCKETS}" \
    --readers "${READERS}" \
    --writers "${WRITERS}" \
    --io-threads "${IO_THREADS}" \
    --io-queue-depth "${IO_QUEUE_DEPTH}" \
    --max-read-batch "${MAX_READ_BATCH}" \
    --mem-quota "${MEM_QUOTA}" \
    ${EXTRA_ARGS} \
    > "${LOG}" 2>&1 < /dev/null &

# Wait on the log line, not a fixed sleep: magma has to open every kvstore
# first, and how long that takes depends on the dataset size.
for _ in $(seq 1 300); do
    if grep -q "listening on" "${LOG}" 2>/dev/null; then
        break
    fi
    sleep 1
done

if ! grep -q "listening on" "${LOG}" 2>/dev/null; then
    echo "FAIL: server did not start listening" >&2
    tail -20 "${LOG}" >&2
    exit 1
fi

pid=$(pgrep -f "fluxkv_server --port ${PORT}" | head -1)
echo "UP pid=${pid} port=${PORT} quota=${MEM_QUOTA}" \
     "async_io=${MAGMA_ASYNC_IO:-libaio} direct_io=${MAGMA_DIRECT_IO:-1}"
