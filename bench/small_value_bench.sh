#!/bin/bash
#
# Small-value GET benchmark: sweeps IO threads, sessions, batch and pipeline
# against a fully cached dataset and reports throughput, server cores and CPU
# per op. At 8-byte values the protocol is the payload, so this measures the
# request path rather than the store.
#
# usage: small_value_bench.sh <io-threads> <sessions> <pipeline> <batch>
#   e.g. small_value_bench.sh 64 192 512 64
#
# Env:
#   SERVER_ADDR  host:port the client connects to (default 127.0.0.1:14001)
#   CLIENT       ssh prefix for the client host; empty means run it here
#   CLIENT_BIN   fluxbench path (default /root/fluxbench)
#   DATA_DIR     dataset to open (default /data/flux-k8-25m)
#   KEYS         key space (default 25000000)
#   CACHE_SIZE   document cache bytes (default 8 GiB)
#   RECIPE       server start script honouring DATA_DIR/IO_THREADS/EXTRA
#
# Run the client on the server itself to take the NIC out of the picture: at
# these rates a 10 GbE link saturates long before the server does. Loopback
# costs the client's CPU on the same box, so read server cores, not box load.
#
# Load the dataset first with a stride SET so every key exists and the GET
# pass can be required to report errs=0:
#   fluxbench -host H:P -keys N -keylen 8 -valsize 8 -randvals -mode set \
#             -conns 32 -pipeline 32 -runtime 1200s
set -u

IO=${1:?usage: small_value_bench.sh <io-threads> <sessions> <pipeline> <batch>}
CONNS=${2:?}
PIPE=${3:?}
BATCH=${4:?}

SERVER_ADDR=${SERVER_ADDR:-127.0.0.1:14001}
CLIENT=${CLIENT:-}
CLIENT_BIN=${CLIENT_BIN:-/root/fluxbench}
DATA_DIR=${DATA_DIR:-/data/flux-k8-25m}
KEYS=${KEYS:-25000000}
CACHE_SIZE=${CACHE_SIZE:-8589934592}
RECIPE=${RECIPE:-/tmp/recipe2.sh}
RUNTIME=${RUNTIME:-20s}
HZ=$(getconf CLK_TCK)

pkill -TERM -f "fluxkv_server --port 14001" 2>/dev/null
for _ in $(seq 1 120); do
    pgrep -f "fluxkv_server --port 14001" >/dev/null || break
    sleep 1
done
pkill -9 -f "fluxkv_server --port 14001" 2>/dev/null
sleep 1

setsid env DATA_DIR="$DATA_DIR" IO_THREADS="$IO" BLOCK=1024 \
    EXTRA="--cache-size $CACHE_SIZE" bash "$RECIPE" \
    > /tmp/recipe.out 2>&1 < /dev/null &
for _ in $(seq 1 60); do
    up=$(tail -1 /tmp/recipe.out 2>/dev/null)
    [[ "$up" == UP* || "$up" == FAIL* ]] && break
    sleep 5
done
if [[ "${up:-}" != UP* ]]; then
    echo "FAIL: server did not start: ${up:-no output}" >&2
    exit 1
fi

# Warm: the cache is filled by the load, but the first pass after a restart
# also settles the allocator and the connection buffers.
# shellcheck disable=SC2086
$CLIENT "$CLIENT_BIN" -host "$SERVER_ADDR" -keys "$KEYS" -vbuckets 256 \
    -keylen 8 -conns "$CONNS" -pipeline "$PIPE" -batch "$BATCH" \
    -pregen 64 -runtime 10s > /dev/null 2>&1

# shellcheck disable=SC2086
$CLIENT "$CLIENT_BIN" -host "$SERVER_ADDR" -keys "$KEYS" -vbuckets 256 \
    -keylen 8 -conns "$CONNS" -pipeline "$PIPE" -batch "$BATCH" \
    -pregen 64 -runtime "$RUNTIME" > /tmp/svb_out.txt 2>&1 &
CLIENT_PID=$!

PID=$(pgrep -f "fluxkv_server --port 14001" | head -1)
sleep 5
T0=$(date +%s.%N)
read -r _ _ _ _ _ _ _ _ _ _ _ _ _ U1 S1 _ < "/proc/$PID/stat"
sleep 10
T1=$(date +%s.%N)
read -r _ _ _ _ _ _ _ _ _ _ _ _ _ U2 S2 _ < "/proc/$PID/stat"
wait $CLIENT_PID 2>/dev/null

RATE=$(grep -oE 'rate=[0-9]+' /tmp/svb_out.txt | tail -1 | cut -d= -f2)
ERRS=$(grep -oE 'errs=[0-9]+' /tmp/svb_out.txt | tail -1 | cut -d= -f2)

# A GET for a key that was never stored is a cheap 24-byte KeyNotFound, so a
# run against an incomplete dataset reports a HIGHER rate than a correct one.
# Refuse to print a throughput number unless every operation succeeded.
if [[ -z "${RATE:-}" || -z "${ERRS:-}" ]]; then
    echo "FAIL: could not parse client output" >&2
    tail -5 /tmp/svb_out.txt >&2
    exit 1
fi
if (( ERRS != 0 )); then
    echo "FAIL: ${ERRS} errors - dataset is incomplete, reload it" >&2
    exit 1
fi

awk -v io="$IO" -v cn="$CONNS" -v pl="$PIPE" -v bt="$BATCH" -v r="$RATE" \
    -v hz="$HZ" -v du=$((U2-U1)) -v ds=$((S2-S1)) -v t0="$T0" -v t1="$T1" \
    'BEGIN {
        el = t1 - t0; c = (du + ds) / hz / el;
        printf "io=%-3s sessions=%-4s pipeline=%-5s batch=%-5s rate=%-11s srv %5.1f cores  %.2f us/op  %.2fM ops/core\n",
               io, cn, pl, bt, r, c, (r > 0 ? c * 1e6 / r : 0), (c > 0 ? r / c / 1e6 : 0);
    }'
