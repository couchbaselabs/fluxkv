#!/bin/bash
#
# Watch the thread tuner converge under a fixed client load. Restarts the
# server through the recipe with --auto-tune, runs the client for RUNTIME and
# prints one line per SAMPLE seconds: server throughput (from its own
# counters), server cores, and each pool's size and busy fraction. The last
# line is the client's own rate and error count.
#
# usage: autotune_bench.sh "<client command>"
#   e.g. autotune_bench.sh "/root/fluxbench -host 127.0.0.1:14001 -keys 25000000 \
#          -vbuckets 256 -keylen 8 -conns 192 -pipeline 512 -batch 64 -pregen 64"
#   The script appends -runtime itself.
#
# Env:
#   RECIPE      server start script honouring DATA_DIR/IO_THREADS/READERS/EXTRA
#   DATA_DIR, IO_THREADS, READERS, BLOCK   passed to the recipe (starting sizes)
#   EXTRA       further server flags; --auto-tune is added here
#   CLIENT      ssh prefix for the client host; empty runs it locally
#   RUNTIME     client run time in seconds (default 240)
#   SAMPLE      seconds between report lines (default 10)
#   STATS       stats URL (default http://127.0.0.1:18091)
set -u

CLIENT_CMD=${1:?usage: autotune_bench.sh "<client command without -runtime>"}
RECIPE=${RECIPE:-/tmp/recipe2.sh}
CLIENT=${CLIENT:-}
RUNTIME=${RUNTIME:-240}
SAMPLE=${SAMPLE:-10}
STATS=${STATS:-http://127.0.0.1:18091}
HZ=$(getconf CLK_TCK)

setsid env DATA_DIR="${DATA_DIR:-/data/flux-k8-25m}" \
    IO_THREADS="${IO_THREADS:-6}" READERS="${READERS:-40}" \
    BLOCK="${BLOCK:-1024}" EXTRA="${EXTRA:-} --auto-tune" bash "$RECIPE" \
    > /tmp/recipe.out 2>&1 < /dev/null &
for _ in $(seq 1 90); do
    up=$(tail -1 /tmp/recipe.out 2>/dev/null)
    [[ "$up" == UP* || "$up" == FAIL* ]] && break
    sleep 5
done
if [[ "${up:-}" != UP* ]]; then
    echo "FAIL: server did not start: ${up:-no output}" >&2
    exit 1
fi
PID=$(pgrep -f "fluxkv_server --port 14001" | head -1)

# shellcheck disable=SC2086
$CLIENT $CLIENT_CMD -runtime "${RUNTIME}s" > /tmp/autotune_client.txt 2>&1 &
CLIENT_PID=$!

ops() {
    curl -s --max-time 2 "$STATS/stats/dispatcher" |
        python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["cmd_get"]+d["cmd_set"]+d["cmd_delete"])'
}
pools() {
    curl -s --max-time 2 "$STATS/stats/tuner" |
        python3 -c 'import json,sys
d=json.load(sys.stdin)
out=[]
for p in d["pools"]:
    s="%s=%d(%.2f)" % (p["name"], p["size"], p["busy_mean"])
    if "thread_load" in p and p["thread_load"]:
        s+=" conns/thread %d-%d" % (min(p["thread_load"]), max(p["thread_load"]))
    if "thread_busy" in p and p["thread_busy"]:
        s+=" busy %.2f-%.2f" % (min(p["thread_busy"]), max(p["thread_busy"]))
    out.append(s)
print(" ".join(out))'
}
cpu() {
    read -r _ _ _ _ _ _ _ _ _ _ _ _ _ u s _ < "/proc/$PID/stat"
    echo $((u + s))
}

printf "%6s %12s %8s  %s\n" "t(s)" "ops/s" "cores" "pools size(busy)"
T0=$(date +%s.%N)
O0=$(ops)
C0=$(cpu)
START=$T0
while kill -0 "$CLIENT_PID" 2>/dev/null; do
    sleep "$SAMPLE"
    T1=$(date +%s.%N)
    O1=$(ops)
    C1=$(cpu)
    [[ -z "$O1" || -z "$O0" ]] && { O0=$O1; C0=$C1; T0=$T1; continue; }
    awk -v t0="$T0" -v t1="$T1" -v o0="$O0" -v o1="$O1" -v c0="$C0" -v c1="$C1" \
        -v hz="$HZ" -v st="$START" -v p="$(pools)" \
        'BEGIN { el = t1 - t0;
                 printf "%6.0f %12.0f %8.1f  %s\n", t1 - st, (o1 - o0) / el,
                        (c1 - c0) / hz / el, p }'
    T0=$T1; O0=$O1; C0=$C1
done
wait "$CLIENT_PID" 2>/dev/null
echo "client: $(grep -oE 'rate=[0-9]+' /tmp/autotune_client.txt | tail -1) $(grep -oE 'errs=[0-9]+' /tmp/autotune_client.txt | tail -1)"
echo "tuner log:"
grep "tuner:" /tmp/recipe.log | sed 's/.*\[INFO\] //'
