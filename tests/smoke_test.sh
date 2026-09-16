#!/usr/bin/env bash
#
# End-to-end smoke test: start a fluxkv server on a scratch directory, write
# keys, read them back, and require that every operation succeeded.
#
# A GET for a key that was never stored comes back as KeyNotFound, which
# fluxbench counts as an error. So "zero errors on the read pass" is the real
# assertion here: it proves reads returned what writes stored.
#
# Usage: smoke_test.sh <server-binary> <client-binary>

set -euo pipefail

SERVER_BIN=${1:?usage: smoke_test.sh <server-binary> <client-binary>}
CLIENT_BIN=${2:?usage: smoke_test.sh <server-binary> <client-binary>}

PORT=${FLUXKV_TEST_PORT:-12399}
STATS_PORT=${FLUXKV_TEST_STATS_PORT:-12398}
VBUCKETS=64
# Non-zero runs the same test through the document cache and additionally
# requires the read pass to have been served from it.
CACHE_SIZE=${FLUXKV_TEST_CACHE_SIZE:-0}
# Non-zero starts the server with --auto-tune and runs the read pass with
# enough connections, for long enough, that the tuner resizes the pools and
# moves connections between IO threads while requests are in flight. The
# assertions are then: no errors, no connection dropped, and the tuner did
# act.
AUTO_TUNE=${FLUXKV_TEST_AUTO_TUNE:-0}

# The load here is deliberately light: one connection, one request in flight.
# This is a correctness check, not a benchmark. Heavy concurrent writes make
# magma apply backpressure (TmpFail) once the disk falls behind, which is
# correct engine behaviour but would make this test fail on slower storage.
KEYS=1000
CONNS=1
PIPELINE=1
RUNTIME=3s
EXTRA_SERVER_ARGS=()
if (( AUTO_TUNE > 0 )); then
    EXTRA_SERVER_ARGS+=(--auto-tune)
fi
DATA_DIR=$(mktemp -d "${TMPDIR:-/tmp}/fluxkv-smoke-XXXXXX")
LOG="${DATA_DIR}/server.log"
SERVER_PID=""

cleanup() {
    if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
        kill -9 "${SERVER_PID}" 2>/dev/null || true
    fi
    rm -rf "${DATA_DIR}"
}
trap cleanup EXIT

echo "# data dir: ${DATA_DIR}"

"${SERVER_BIN}" \
    --port "${PORT}" \
    --data-dir "${DATA_DIR}" \
    --bucket default \
    --shards 2 \
    --vbuckets "${VBUCKETS}" \
    --readers 8 \
    --writers 4 \
    --io-threads 4 \
    --mem-quota 268435456 \
    --stats-port "${STATS_PORT}" \
    --cache-size "${CACHE_SIZE}" \
    "${EXTRA_SERVER_ARGS[@]}" \
    > "${LOG}" 2>&1 &
SERVER_PID=$!

# Wait for the listener rather than sleeping a fixed amount; magma has to
# create its kvstores first.
for _ in $(seq 1 60); do
    if grep -q "listening on" "${LOG}" 2>/dev/null; then
        break
    fi
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
        echo "FAIL: server exited during startup" >&2
        tail -20 "${LOG}" >&2
        exit 1
    fi
    sleep 1
done

if ! grep -q "listening on" "${LOG}"; then
    echo "FAIL: server did not start listening within 60s" >&2
    tail -20 "${LOG}" >&2
    exit 1
fi
echo "# server up on port ${PORT}"

run_phase() {
    local mode=$1 extra=${2:-}
    # shellcheck disable=SC2086
    "${CLIENT_BIN}" \
        -host "127.0.0.1:${PORT}" \
        -mode "${mode}" \
        -keys "${KEYS}" \
        -vbuckets "${VBUCKETS}" \
        -conns "${CONNS}" \
        -pipeline "${PIPELINE}" \
        -runtime "${RUNTIME}" \
        ${extra}
}

check() {
    local phase=$1 output=$2
    local ops errs
    ops=$(sed -n 's/.*ops=\([0-9]*\).*/\1/p' <<<"${output}" | head -1)
    errs=$(sed -n 's/.*errs=\([0-9]*\).*/\1/p' <<<"${output}" | head -1)

    if [[ -z "${ops}" || -z "${errs}" ]]; then
        echo "FAIL: ${phase}: could not parse client output" >&2
        echo "${output}" >&2
        exit 1
    fi
    if (( ops == 0 )); then
        echo "FAIL: ${phase}: no operations completed" >&2
        exit 1
    fi
    if (( errs != 0 )); then
        echo "FAIL: ${phase}: ${errs} errors" >&2
        echo "${output}" >&2
        exit 1
    fi
    echo "# ${phase}: ops=${ops} errs=${errs}"
}

set_out=$(run_phase set "-valsize 1024")
check "set" "${set_out}"

get_out=$(run_phase get)
check "get" "${get_out}"

if (( CACHE_SIZE > 0 )); then
    # Every key was written through the cache and the cache is larger than
    # the dataset, so the read pass must have been answered entirely from it.
    stats=$(curl -s --max-time 5 "http://127.0.0.1:${STATS_PORT}/stats/dispatcher")
    hits=$(sed -n 's/.*"cache_hits": \([0-9]*\).*/\1/p' <<<"${stats}" | head -1)
    misses=$(sed -n 's/.*"cache_misses": \([0-9]*\).*/\1/p' <<<"${stats}" | head -1)
    pending=$(sed -n 's/.*"cache_pending_items": \([0-9]*\).*/\1/p' <<<"${stats}" | head -1)
    if [[ -z "${hits}" || "${hits}" == 0 ]]; then
        echo "FAIL: cache: no hits recorded" >&2
        echo "${stats}" >&2
        exit 1
    fi
    if [[ "${misses}" != 0 ]]; then
        echo "FAIL: cache: ${misses} misses on a fully cached dataset" >&2
        echo "${stats}" >&2
        exit 1
    fi
    if [[ "${pending}" != 0 ]]; then
        echo "FAIL: cache: ${pending} items still pinned after writes drained" >&2
        echo "${stats}" >&2
        exit 1
    fi
    echo "# cache: hits=${hits} misses=${misses} pending=${pending}"
fi

if (( AUTO_TUNE > 0 )); then
    # Light load on many connections: the pools start oversized for it, so
    # the tuner shrinks them, and every shrink of the IO pool moves live,
    # pipelined connections between loops.
    CONNS=16
    PIPELINE=8
    RUNTIME=30s
    tune_out=$(run_phase get)
    check "get under auto-tune" "${tune_out}"

    stats=$(curl -s --max-time 5 "http://127.0.0.1:${STATS_PORT}/stats/dispatcher")
    closes=$(sed -n 's/.*"connect_close": \([0-9]*\).*/\1/p' <<<"${stats}" | head -1)
    tuner=$(curl -s --max-time 5 "http://127.0.0.1:${STATS_PORT}/stats/tuner")
    changes=$(sed -n 's/.*"changes": \([0-9]*\).*/\1/p' <<<"${tuner}" | paste -sd+ | bc)
    io_size=$(python3 -c 'import json,sys; d=json.load(sys.stdin); print([p["size"] for p in d["pools"] if p["name"]=="io"][0])' <<<"${tuner}")
    rd_size=$(python3 -c 'import json,sys; d=json.load(sys.stdin); print([p["size"] for p in d["pools"] if p["name"]=="readers"][0])' <<<"${tuner}")
    # Connections close only when the client finishes; each earlier phase
    # opened one. A higher count means a connection broke mid-run.
    expected_closes=$(( 2 + 16 ))
    if (( closes > expected_closes )); then
        echo "FAIL: auto-tune: ${closes} connections closed, expected ${expected_closes}" >&2
        echo "${tuner}" >&2
        exit 1
    fi
    if (( changes == 0 )); then
        echo "FAIL: auto-tune: tuner made no changes in ${RUNTIME}" >&2
        echo "${tuner}" >&2
        exit 1
    fi
    echo "# auto-tune: changes=${changes} io=${io_size} readers=${rd_size} closes=${closes}"
fi

echo "PASS"
