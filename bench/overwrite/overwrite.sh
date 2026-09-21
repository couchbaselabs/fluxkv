#!/usr/bin/env bash
#
# Steady-state overwrite benchmark: load a dataset, then overwrite it at full
# offered load and measure each window. Reports client throughput, ingest,
# device read and write bandwidth (from /proc/diskstats, not magma's
# counters), on-disk size (du, not magma's ActiveDiskUsage), space and write
# amplification, plus a per-component write attribution from attr.py.
#
# Every arm gets a fresh data directory and, if FSTRIM=1, an fstrim first:
# LSM state dominates and an arm that inherits a tree is not comparable.
#
# Usage:
#   CONFIG=tuned DATA_DIR=/data/fluxkv-ow bench/overwrite/overwrite.sh
#   CONFIG=baseline VS=256 RUN=1800 WIN=600 bench/overwrite/overwrite.sh
#
# Non-blind writes only: the server looks up each key's previous version so
# a delta is written for it. With --blind-writes nothing is ever marked dead
# and every space figure is meaningless.

set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
CONFIG=${CONFIG:-baseline}
# shellcheck source=/dev/null
source "$HERE/configs/$CONFIG.env"

SERVER_BIN=${SERVER_BIN:-$HERE/../../build/fluxkv_server}
CLIENT_BIN=${CLIENT_BIN:-$HERE/../../build/fluxbench}
DATA_DIR=${DATA_DIR:?set DATA_DIR to an empty directory on the array under test}
PORT=${PORT:-14002}
STATS_PORT=${STATS_PORT:-18092}
STATS_URL="http://127.0.0.1:$STATS_PORT/stats/magma"
# Block devices behind DATA_DIR, as named in /proc/diskstats. Summed.
DEVICES=${DEVICES:-$(awk '$3 ~ /^nvme[0-9]+n1$/ {print $3}' /proc/diskstats | tr '\n' ' ')}
FSTRIM=${FSTRIM:-1}

VS=${VS:-1024}                 # value bytes
KEYLEN=12
LIVE_GIB=${LIVE_GIB:-38.5}     # live dataset; keys derived so every VS is comparable
KEYS=${KEYS:-$(awk -v l=$LIVE_GIB -v v=$VS -v k=$KEYLEN 'BEGIN{printf "%d", l*1024*1024*1024/(v+8)}')}
LOAD=${LOAD:-150}              # seconds allowed for the initial load
RUN=${RUN:-1200}               # overwrite seconds
WIN=${WIN:-600}                # measurement window
CONNS=${CONNS:-112}
PIPE=${PIPE:-128}
TAG=${TAG:-$CONFIG-$VS}
OUT=${OUT:-/tmp/overwrite-$TAG}
mkdir -p "$OUT"

g() { curl -s -m 8 "$STATS_URL" | tr ',' '\n' | grep -m1 "\"$1\"" | grep -oE '[0-9.]+$'; }
dev() { awk -v devs=" $DEVICES " -v col="$1" 'index(devs, " "$3" ") {s+=$col} END{print s*512}' /proc/diskstats; }
duG() { du -sm "$DATA_DIR" | awk '{printf "%.2f", $1/1024}'; }

echo "=== $TAG: config=$CONFIG vb=$VB vs=$VS keys=$KEYS live=${LIVE_GIB}GiB client=${CONNS}x${PIPE} devices=[$DEVICES]"
for p in $(pgrep -f "port $PORT "); do kill "$p" 2>/dev/null; done
for _ in $(seq 1 60); do pgrep -f "port $PORT " >/dev/null || break; sleep 1; done
rm -rf "$DATA_DIR"; mkdir -p "$DATA_DIR"
if [ "$FSTRIM" = 1 ]; then echo "  fstrim: $(fstrim -v "$(df --output=target "$DATA_DIR" | tail -1)" 2>&1)"; sync; sleep 10; fi

# shellcheck disable=SC2086
env $SERVER_ENV setsid bash -c "exec $SERVER_BIN --port $PORT --data-dir $DATA_DIR --hostname 127.0.0.1 \
  --bucket default --vbuckets $VB --stats-port $STATS_PORT $SERVER_FLAGS ${EXTRA:-}" > "$OUT/server.log" 2>&1 &
for _ in $(seq 1 240); do grep -q "listening on" "$OUT/server.log" && break; sleep 1; done
grep -q "listening on" "$OUT/server.log" || { echo "server did not start; see $OUT/server.log"; exit 1; }

"$CLIENT_BIN" -host 127.0.0.1:$PORT -vbuckets "$VB" -mode set -keys "$KEYS" -keylen $KEYLEN -valsize "$VS" \
    -conns "$CONNS" -pipeline "$PIPE" -runtime ${LOAD}s > "$OUT/load.txt" 2>&1
LOADED=$(grep -oE "ops=[0-9]+" "$OUT/load.txt" | head -1 | cut -d= -f2)
sleep 30
echo "  load: ${LOADED:-0}/$KEYS keys; du $(duG) GiB; spaceAmp(du) $(awk -v d=$(duG) -v u=$LIVE_GIB 'BEGIN{printf "%.2f", d/u}')"
[ "${LOADED:-0}" -ge $((KEYS - KEYS/1000)) ] || echo "  WARNING: load is short; raise LOAD"

echo "=== overwrite, uniform keys, ${RUN}s in ${WIN}s windows"
echo "     t   ops/s  ingestMB/s  devWrMB/s  devRdMB/s  du_GiB  spaceAmp  devWA  compWA"
"$CLIENT_BIN" -host 127.0.0.1:$PORT -vbuckets "$VB" -mode set -keys "$KEYS" -keylen $KEYLEN -valsize "$VS" \
    -zipf 0.01 -conns "$CONNS" -pipeline "$PIPE" -runtime $((RUN+30))s > "$OUT/overwrite.txt" 2>&1 &
BP=$!
for w in $(seq 1 $((RUN/WIN))); do
  b0=$(g BytesIncoming); c0=$(g NWriteBytesCompact); w0=$(dev 10); r0=$(dev 6)
  STATS_URL=$STATS_URL python3 "$HERE/attr.py" "$WIN" > "$OUT/attr-$w.txt" 2>&1
  b1=$(g BytesIncoming); c1=$(g NWriteBytesCompact); w1=$(dev 10); r1=$(dev 6)
  awk -v t=$((w*WIN)) -v bi=$((${b1%.*}-${b0%.*})) -v nc=$((${c1%.*}-${c0%.*})) -v dw=$((w1-w0)) -v dr=$((r1-r0)) \
      -v du="$(duG)" -v u=$LIVE_GIB -v sec=$WIN -v vs=$VS '
    BEGIN{ printf "  %5d %7.0f %11.0f %10.0f %10.0f %7.2f %9.2f %6.2f %7.2f\n", t, bi/sec/(vs+8),
           bi/sec/1e6, dw/sec/1e6, dr/sec/1e6, du, du/u, (bi>0?dw/bi:0), (bi>0?nc/bi:0) }'
  sed 's/^/        /' "$OUT/attr-$w.txt"
done
kill $BP 2>/dev/null; wait $BP 2>/dev/null
sleep 20
r0=$(dev 6)
ROPS=$("$CLIENT_BIN" -host 127.0.0.1:$PORT -vbuckets "$VB" -mode get -keys "$KEYS" -keylen $KEYLEN -valsize "$VS" \
       -conns 32 -pipeline 1 -runtime 45s 2>&1 | grep -oE 'rate=[0-9]+' | cut -d= -f2)
r1=$(dev 6)
echo "  read check: ${ROPS:-0} get/s, $(awk -v r=$((r1-r0)) -v o=${ROPS:-1} 'BEGIN{printf "%.2f", r/(o*45)/1024}') KB read per get; final du $(duG) GiB"
curl -s -m 5 "http://127.0.0.1:$STATS_PORT/stats/tuner" > "$OUT/tuner.json" 2>/dev/null
for p in $(pgrep -f "port $PORT "); do kill "$p" 2>/dev/null; done; sleep 3
echo "done $TAG (details in $OUT)"
