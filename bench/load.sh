#!/usr/bin/env bash
#
# Load a dataset, then verify every write actually landed.
#
# The verification is the point. A SET that the server cannot queue is
# answered with TMPFAIL, and that key is simply never stored. Loading 200M
# keys at ~1GB/s against the default 256MB write queue had 95.7M writes
# (48%) rejected this way. The dataset looked fine - hundreds of GB on disk,
# server healthy - and the shortfall only appeared later as a read miss rate
# that climbed with key order.
#
# So: raise the write queue, keep the load within what the engine can absorb,
# and refuse to call the dataset good unless cmd_set_resp matches the key
# count and tmp_fails is zero.
#
# Usage:
#   DATA_DIR=/data/fluxkv-1k KEYS=200000000 bench/load.sh
#
# Expects a server already running (see run_server.sh) with a write queue
# large enough - pass EXTRA_ARGS="--write-queue-mem 8589934592" there.

set -euo pipefail

CLIENT_HOST=${CLIENT_HOST:?set CLIENT_HOST to the load generator, e.g. root@10.0.0.2}
CLIENT_BIN=${CLIENT_BIN:-/tmp/echobench-rv/echobench-rv}
SERVER_HOST=${SERVER_HOST:-127.0.0.1}
PORT=${PORT:-12210}
STATS_URL=${STATS_URL:-http://127.0.0.1:80/}

KEYS=${KEYS:?set KEYS to the number of keys to load}
VBUCKETS=${VBUCKETS:-256}
VALSIZE=${VALSIZE:-1024}

# Deliberately moderate. Pushing harder does not load faster - it just gets
# writes rejected. 16x4 sustained ~470K/s with zero rejections where 32x32
# managed ~800K/s and shed 14% of the dataset.
CONNS=${CONNS:-16}
PIPELINE=${PIPELINE:-4}
RUNTIME=${RUNTIME:-3600s}

SSH=${SSH:-ssh}

stat_value() {
    curl -s --max-time 5 "${STATS_URL}" \
        | grep -oE "\"$1\": [0-9]+" | grep -oE '[0-9]+' | head -1
}

before_set=$(stat_value cmd_set || echo 0)
before_tmpfail=$(stat_value tmp_fails || echo 0)

echo "# loading ${KEYS} keys x ${VALSIZE}B at ${CONNS}x${PIPELINE}"

# shellcheck disable=SC2029
${SSH} "${CLIENT_HOST}" "${CLIENT_BIN} \
    -host ${SERVER_HOST}:${PORT} \
    -mode set \
    -keys ${KEYS} \
    -vbuckets ${VBUCKETS} \
    -valsize ${VALSIZE} \
    -randvals \
    -conns ${CONNS} \
    -pipeline ${PIPELINE} \
    -runtime ${RUNTIME}" || true

after_set=$(stat_value cmd_set || echo 0)
after_resp=$(stat_value cmd_set_resp || echo 0)
after_tmpfail=$(stat_value tmp_fails || echo 0)

tmpfails=$((after_tmpfail - before_tmpfail))
received=$((after_set - before_set))

echo "# requests received: ${received}"
echo "# writes persisted:  ${after_resp}"
echo "# tmpfail rejected:  ${tmpfails}"

fail=0

if (( tmpfails > 0 )); then
    echo "FAIL: ${tmpfails} writes were rejected with TMPFAIL and are not" \
         "stored." >&2
    echo "      The load outran the write queue. Raise the server's" >&2
    echo "      --write-queue-mem, or lower CONNS/PIPELINE, and load again." >&2
    fail=1
fi

if (( after_resp < KEYS )); then
    echo "FAIL: only ${after_resp} of ${KEYS} keys were persisted." >&2
    fail=1
fi

if (( fail )); then
    echo "Dataset is incomplete. Reads against it will report misses that" >&2
    echo "look like data loss." >&2
    exit 1
fi

echo "PASS: ${after_resp} keys persisted, no rejections"
