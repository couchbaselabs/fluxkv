#!/usr/bin/env bash
#
# Run the server under a systemd memory cgroup, capping its total memory.
#
# This exists to test whether buffered IO earns its advantage or simply
# borrows free RAM. Buffered reads go through the kernel page cache, which is
# charged to the cgroup but does not show up in the process RSS. Capping total
# memory takes that page cache away.
#
# Measured on a 500M x 1KB dataset, io_uring + buffered, moderate load:
#
#   no cap        887K/s @ p50 443us
#   64G cap       900K/s @ p50 433us   cap never bit: the hot set is only ~40G
#   24G cap       489K/s @ p50 948us   page cache squeezed, collapses
#
# At 24G the cgroup reported 33,782 limit breaches and 33.5M pages reclaimed,
# and throughput fell below DirectIO's 540K. Buffered double-buffers - the
# same block sits in the page cache and in magma's cache - so when memory is
# scarce it is worse than DirectIO, not better.
#
# Note the process RSS stays tiny under buffered IO (peak ~1.5G), because the
# memory lives in the page cache. RSS is misleading here; read the cgroup.
#
# Usage:
#   DATA_DIR=/data/dataset MEM_MAX=24G MEM_HIGH=20G \
#     MEM_QUOTA=$((16*1024*1024*1024)) bench/cgroup_run.sh

set -euo pipefail

SERVER_BIN=${SERVER_BIN:-./build/fluxkv_server}
DATA_DIR=${DATA_DIR:?set DATA_DIR to the dataset directory}
UNIT=${UNIT:-fluxkv-bench}
PORT=${PORT:-12210}
BUCKET=${BUCKET:-default}

MEM_MAX=${MEM_MAX:-68G}
MEM_HIGH=${MEM_HIGH:-64G}
MEM_QUOTA=${MEM_QUOTA:-$((64 * 1024 * 1024 * 1024))}

SHARDS=${SHARDS:-32}
VBUCKETS=${VBUCKETS:-256}
READERS=${READERS:-64}
WRITERS=${WRITERS:-32}
IO_THREADS=${IO_THREADS:-32}
LOG=${LOG:-/tmp/fluxkv-cgroup.log}

systemctl stop "${UNIT}" 2>/dev/null || true
systemctl reset-failed "${UNIT}" 2>/dev/null || true
pkill -9 -f "fluxkv_server --port ${PORT}" 2>/dev/null || true
for _ in $(seq 1 30); do
    ss -ltn | grep -q ":${PORT}" || break
    sleep 1
done

sync
echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true

# LimitNOFILE is not optional. A transient systemd unit gets a low default
# file-descriptor limit, while a login shell does not. magma opens a file per
# sstable, so on a large dataset the server dies during recovery with
# "Too many open files" - which looks like corruption in the log, not a limit.
systemd-run \
    --unit="${UNIT}" \
    --property=MemoryMax="${MEM_MAX}" \
    --property=MemoryHigh="${MEM_HIGH}" \
    --property=LimitNOFILE=1048576 \
    --setenv=MAGMA_ASYNC_IO="${MAGMA_ASYNC_IO:-libaio}" \
    --setenv=MAGMA_DIRECT_IO="${MAGMA_DIRECT_IO:-1}" \
    bash -c "exec ${SERVER_BIN} \
        --port ${PORT} \
        --data-dir ${DATA_DIR} \
        --bucket ${BUCKET} \
        --shards ${SHARDS} \
        --vbuckets ${VBUCKETS} \
        --readers ${READERS} \
        --writers ${WRITERS} \
        --io-threads ${IO_THREADS} \
        --mem-quota ${MEM_QUOTA} \
        > ${LOG} 2>&1" > /dev/null

for _ in $(seq 1 300); do
    grep -q "listening on" "${LOG}" 2>/dev/null && break
    sleep 1
done

if ! grep -q "listening on" "${LOG}" 2>/dev/null; then
    echo "FAIL: server did not start under cgroup" >&2
    systemctl status "${UNIT}" --no-pager 2>&1 | head -10 >&2
    tail -20 "${LOG}" >&2
    exit 1
fi

CG=/sys/fs/cgroup/system.slice/${UNIT}.service
echo "UP unit=${UNIT} memory.max=$(cat "${CG}/memory.max" 2>/dev/null)" \
     "memory.high=$(cat "${CG}/memory.high" 2>/dev/null)"

# Call bench/cgroup_stats.sh during the run to see whether the cap actually
# bites; a cap above the working set changes nothing.
