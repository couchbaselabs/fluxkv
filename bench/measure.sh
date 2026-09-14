#!/usr/bin/env bash
#
# Measure a running fluxkv server at two load points and print one line each.
#
#   peak      many connections, deep pipeline - finds the throughput ceiling
#   moderate  few connections, shallow pipeline - shows latency under normal load
#
# Both numbers matter. Peak alone hides the effect of cache and IO changes:
# in the io_uring matrix every configuration peaked near 910K, while the
# moderate point separated them by 1.6x.
#
# Warm-up runs are discarded. A cold block cache makes the first pass
# unrepresentative, and buffered IO needs time to populate the page cache.
#
# Usage: LABEL=libaio+directio bench/measure.sh

set -euo pipefail

CLIENT_BIN=${CLIENT_BIN:-./build/fluxbench}
HOST=${HOST:-127.0.0.1:12210}
KEYS=${KEYS:?set KEYS to the dataset key count}
VBUCKETS=${VBUCKETS:-256}
VALSIZE=${VALSIZE:-1024}
MODE=${MODE:-get}
RANDVALS=${RANDVALS:-1}
LABEL=${LABEL:-run}

PEAK_CONNS=${PEAK_CONNS:-256}
PEAK_PIPELINE=${PEAK_PIPELINE:-128}
MOD_CONNS=${MOD_CONNS:-64}
MOD_PIPELINE=${MOD_PIPELINE:-8}

WARMUPS=${WARMUPS:-2}
WARMUP_RUNTIME=${WARMUP_RUNTIME:-60s}
PEAK_RUNTIME=${PEAK_RUNTIME:-30s}
MOD_RUNTIME=${MOD_RUNTIME:-25s}

randflag=""
if [[ "${RANDVALS}" == "1" ]]; then
    randflag="-randvals"
fi

run_client() {
    local conns=$1 pipeline=$2 runtime=$3
    # shellcheck disable=SC2086
    "${CLIENT_BIN}" \
        -host "${HOST}" \
        -mode "${MODE}" \
        -keys "${KEYS}" \
        -vbuckets "${VBUCKETS}" \
        -valsize "${VALSIZE}" \
        -conns "${conns}" \
        -pipeline "${pipeline}" \
        -runtime "${runtime}" \
        ${randflag} 2>&1
}

summarise() {
    local name=$1 output=$2
    local rate lat status
    rate=$(grep -oE 'rate=[0-9]+' <<<"${output}" | tail -1)
    lat=$(grep -oE 'p50=[0-9]+us p90=[0-9]+us p99=[0-9]+us p999=[0-9]+us' <<<"${output}")
    status=$(grep -E '^STATUS' <<<"${output}" || true)
    echo "[${LABEL}] ${name}: ${rate} ${lat} ${status}"
}

for _ in $(seq 1 "${WARMUPS}"); do
    run_client "${PEAK_CONNS}" "${PEAK_PIPELINE}" "${WARMUP_RUNTIME}" > /dev/null
done

summarise "PEAK c${PEAK_CONNS}p${PEAK_PIPELINE}" \
          "$(run_client "${PEAK_CONNS}" "${PEAK_PIPELINE}" "${PEAK_RUNTIME}")"
summarise "MOD  c${MOD_CONNS}p${MOD_PIPELINE}" \
          "$(run_client "${MOD_CONNS}" "${MOD_PIPELINE}" "${MOD_RUNTIME}")"
