#!/usr/bin/env bash
#
# Shutdown durability: data acknowledged before a restart must still be there
# after it.
#
# A SET is acknowledged once it is queued, before it reaches magma. Shutdown
# used to discard that queue, so a clean SIGTERM lost writes the client had
# been told were stored - and nothing in the log or the on-disk size showed it.
#
# The test reads the keyspace before and after a SIGTERM restart. Both passes
# must report zero errors. Reading twice matters: a single post-restart read
# cannot tell data loss apart from keys that were never written.
#
# Usage: durability_test.sh <server-binary> <client-binary>

set -euo pipefail

SERVER_BIN=${1:?usage: durability_test.sh <server-binary> <client-binary>}
CLIENT_BIN=${2:?usage: durability_test.sh <server-binary> <client-binary>}

PORT=${FLUXKV_TEST_PORT:-12398}
VBUCKETS=64
KEYS=100000
DATA_DIR=$(mktemp -d "${TMPDIR:-/tmp}/fluxkv-dur-XXXXXX")
SERVER_PID=""

cleanup() {
    if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
        kill -9 "${SERVER_PID}" 2>/dev/null || true
    fi
    rm -rf "${DATA_DIR}"
}
trap cleanup EXIT

start_server() {
    local log=$1
    "${SERVER_BIN}" \
        --port "${PORT}" \
        --data-dir "${DATA_DIR}" \
        --bucket default \
        --shards 4 \
        --vbuckets "${VBUCKETS}" \
        --readers 8 \
        --writers 4 \
        --io-threads 4 \
        --mem-quota 8589934592 \
        > "${log}" 2>&1 &
    SERVER_PID=$!

    for _ in $(seq 1 90); do
        grep -q "listening on" "${log}" 2>/dev/null && return 0
        kill -0 "${SERVER_PID}" 2>/dev/null || break
        sleep 1
    done
    echo "FAIL: server did not start" >&2
    tail -20 "${log}" >&2
    exit 1
}

client() {
    local mode=$1 extra=${2:-}
    # shellcheck disable=SC2086
    "${CLIENT_BIN}" \
        -host "127.0.0.1:${PORT}" \
        -mode "${mode}" \
        -keys "${KEYS}" \
        -vbuckets "${VBUCKETS}" \
        -conns 2 \
        -pipeline 2 \
        -runtime "${3:-8s}" \
        ${extra}
}

require_clean() {
    local phase=$1 output=$2 ops errs
    ops=$(sed -n 's/.*ops=\([0-9]*\).*/\1/p' <<<"${output}" | head -1)
    errs=$(sed -n 's/.*errs=\([0-9]*\).*/\1/p' <<<"${output}" | head -1)

    if [[ -z "${ops}" || "${ops}" == "0" ]]; then
        echo "FAIL: ${phase}: no operations completed" >&2
        echo "${output}" >&2
        exit 1
    fi
    if [[ "${errs}" != "0" ]]; then
        echo "FAIL: ${phase}: ${errs} of ${ops} operations failed" >&2
        echo "${output}" >&2
        exit 1
    fi
    echo "# ${phase}: ops=${ops} errs=0"
}

start_server "${DATA_DIR}/server1.log"

# Light load on purpose. Heavy writes hit magma's backpressure (TmpFail), and
# rejected writes would look like data loss on the read pass.
require_clean "write" "$(client set '-valsize 1024' 12s)"
require_clean "read before restart" "$(client get)"

# Graceful stop. This is the path that used to drop the queue.
kill -TERM "${SERVER_PID}"
for _ in $(seq 1 120); do
    kill -0 "${SERVER_PID}" 2>/dev/null || break
    sleep 1
done
if kill -0 "${SERVER_PID}" 2>/dev/null; then
    echo "FAIL: server did not exit on SIGTERM within 120s" >&2
    exit 1
fi
SERVER_PID=""
echo "# server stopped cleanly"

start_server "${DATA_DIR}/server2.log"
require_clean "read after restart" "$(client get)"

echo "PASS"
