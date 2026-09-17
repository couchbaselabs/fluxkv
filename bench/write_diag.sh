#!/bin/bash
# fluxkv SET-only throughput on loopback with everything measured: server and
# client cores, whole-box idle, disk bytes, per-thread CPU by role, write batch
# size, accepted SET/s from the server counters (written + deduplicated),
# magma stats and a profile. Runs on the server host.
# Env: WRITERS FLUSHERS MEMQ VALSIZE CONNS PIPE SRVX SWF TAG PF JECONF
#      ZIPF=0.99 for a skewed keyspace; unset for uniform random keys
#      NOTUNE=" " (fixed thread counts instead of --auto-tune)
#      DWARF=1 (add a DWARF call graph of six writer threads)
set -u
KV=/root/fluxkv/build/fluxkv_server
FB=/root/fluxbench
DATA=/data/flux-write
TAG=${TAG:-diag2}
HZ=$(getconf CLK_TCK)
cpu() { read -r _ _ _ _ _ _ _ _ _ _ _ _ _ u s _ < "/proc/$1/stat"; echo $((u+s)); }
wb()  { grep ^write_bytes /proc/$1/io | awk '{print $2}'; }
# per-thread cpu: "tid comm ticks"
tcpu() { for t in /proc/$1/task/*; do read -r _ c _ _ _ _ _ _ _ _ _ _ _ u s _ < "$t/stat"; echo "${t##*/} $c $((u+s))"; done; }

pkill -f "fluxkv_server --port 14001" 2>/dev/null
for i in $(seq 1 120); do pgrep -f "fluxkv_server --port 14001" >/dev/null || break; sleep 1; done
pkill -9 -f "fluxkv_server --port 14001" 2>/dev/null; sleep 2
rm -rf $DATA; mkdir -p $DATA
cd /root/fluxkv
# shellcheck disable=SC2086
setsid env ${JECONF:+JE_MALLOC_CONF=$JECONF} ${PRELOAD:+LD_PRELOAD=$PRELOAD} FLUXKV_NO_CRASH_HANDLER=1 MAGMA_FLUSH_DELAY_US=100 nohup $KV --port 14001 \
  --data-dir $DATA --hostname 127.0.0.1 --bucket default \
  --shards 8 --vbuckets 256 --mem-quota ${MEMQ:-8589934592} \
  --writers ${WRITERS:-64} --flushers ${FLUSHERS:-16} --write-queue-mem 4294967296 \
  --no-compression --index-compression-lz4 --data-block-size 1024 \
  --io-queue-depth 16 --stats-port 18091 ${NOTUNE:---auto-tune} \
  --shared-wal --shared-wal-flushers ${SWF:-4} ${SRVX:-} \
  > /tmp/flux-$TAG.log 2>&1 </dev/null &
for i in $(seq 1 180); do grep -q "listening on" /tmp/flux-$TAG.log 2>/dev/null && break; sleep 1; done
grep -q "listening on" /tmp/flux-$TAG.log || { echo FAIL; tail /tmp/flux-$TAG.log; exit 1; }
PID=$(pgrep -f "fluxkv_server --port 14001" | head -1)

$FB -host 127.0.0.1:14001 -mode set -keys 256000000 -vbuckets 256 -keylen 8 \
    -valsize ${VALSIZE:-8} ${ZIPF:+-zipf $ZIPF} -conns ${CONNS:-64} -pipeline ${PIPE:-64} -batch 64 -pregen 64 \
    -runtime 70s > /tmp/fb_$TAG.txt 2>&1 &
FBP=$!
sleep 15
CPID=$(pgrep -f "fluxbench -host" | head -1)
D0=$(curl -s --max-time 5 http://127.0.0.1:18091/stats/dispatcher)
read -r _ u0 n0 s0 i0 w0 _ < /proc/stat
T0=$(date +%s.%N); G0=$(cpu $PID); C0=$(cpu ${CPID:-$$}); W0=$(wb $PID)
tcpu $PID > /tmp/tc0_$TAG
if [ -n "${DWARF:-}" ]; then
  TIDS=$(grep -l "fx:writer" /proc/$PID/task/*/comm | head -6 | awk -F/ '{print $5}' | paste -sd,)
  perf record --call-graph dwarf,32768 -F 250 -t $TIDS -o /tmp/flux_$TAG.dw.perf -- sleep 6 >/dev/null 2>&1
  perf record -F ${PF:-499} -g -p $PID -o /tmp/flux_$TAG.perf -- sleep 14 >/dev/null 2>&1
else
  perf record -F ${PF:-499} -g -p $PID -o /tmp/flux_$TAG.perf -- sleep 20 >/dev/null 2>&1
fi
tcpu $PID > /tmp/tc1_$TAG
T1=$(date +%s.%N); G1=$(cpu $PID); C1=$(cpu ${CPID:-$$}); W1=$(wb $PID)
read -r _ u1 n1 s1 i1 w1 _ < /proc/stat
D1=$(curl -s --max-time 5 http://127.0.0.1:18091/stats/dispatcher)
curl -s --max-time 5 http://127.0.0.1:18091/stats/magma > /tmp/magma_$TAG.json 2>/dev/null
wait $FBP 2>/dev/null
R=$(grep -oE 'rate=[0-9]+' /tmp/fb_$TAG.txt | tail -1 | cut -d= -f2)
E=$(grep -oE 'errs=[0-9]+' /tmp/fb_$TAG.txt | tail -1 | cut -d= -f2)
awk -v r="${R:-0}" -v e="${E:-?}" -v g=$((G1-G0)) -v c=$((C1-C0)) -v hz="$HZ" \
    -v t0="$T0" -v t1="$T1" -v w=$((W1-W0)) -v di=$((i1-i0)) -v du=$((u1-u0)) -v ds=$((s1-s0)) \
 'BEGIN{ el=t1-t0; tot=du+ds+di;
   printf "rate=%.0f errs=%s  srv %.1f cores  client %.1f cores  box busy %.1f/80  idle %.0f%%  disk %.2f GB/s (%.0f B/op)\n",
     r, e, g/hz/el, c/hz/el, (du+ds)/hz/el, 100.0*di/tot, w/el/1e9, (r>0? w/el/r:0) }'
echo "--- write batches:"
b0=$(echo "$D0" | grep -oE '"write_batches": [0-9]+' | grep -oE '[0-9]+'); i0=$(echo "$D0" | grep -oE '"write_batch_items": [0-9]+' | grep -oE '[0-9]+')
d0=$(echo "$D0" | grep -oE '"write_dedups": [0-9]+' | grep -oE "[0-9]+"); d1=$(echo "$D1" | grep -oE '"write_dedups": [0-9]+' | grep -oE "[0-9]+")
b1=$(echo "$D1" | grep -oE '"write_batches": [0-9]+' | grep -oE '[0-9]+'); i1=$(echo "$D1" | grep -oE '"write_batch_items": [0-9]+' | grep -oE '[0-9]+')
echo "batches=$((b1-b0)) items=$((i1-i0)) avg=$(( (i1-i0) / ( (b1-b0) > 0 ? (b1-b0) : 1) ))  ACCEPTED=$(echo "($i1-$i0+$d1-$d0)/($T1-$T0)" | bc) SET/s in window (dedup $(echo "($d1-$d0)/($T1-$T0)" | bc), written $(echo "($i1-$i0)/($T1-$T0)" | bc))"
echo "$D1" | grep -oE '"tmp_fails": [0-9]+'
echo "--- per-thread cores by comm:"
join <(sort /tmp/tc0_$TAG) <(sort /tmp/tc1_$TAG) | awk -v hz=$HZ -v el=$(echo "$T1 - $T0" | bc) \
  '{ gsub(/:[0-9]+\)/,")",$2); d=$5-$3; c[$2]+=d; n[$2]++ } END { for (k in c) printf "%-18s threads=%-4d cores=%.1f\n", k, n[k], c[k]/hz/el }' | sort -k3 -t= -rn | head -15
echo "--- top self symbols:"
perf report -i /tmp/flux_$TAG.perf --no-children --percent-limit 1.0 2>/dev/null | grep -E "^ +[0-9]" | head -25
echo "--- magma stats of interest:"
grep -oE '"(WriteAmp|FlushQueueSize|NTablesCreated|WriteCacheMemUsed|WALMemUsed|NCompacts|NFlushes|TotalDiskUsage|LogicalDataSize|NSyncs|WALDiskUsage)": [0-9.]+' /tmp/magma_$TAG.json | head -20
echo "--- cumulative (children) top entries:"
perf report -i /tmp/flux_$TAG.perf --children --percent-limit 2.5 2>/dev/null | grep -E "^ +[0-9]" | head -45
echo "--- cpu by thread role (perf):"
perf report -i /tmp/flux_$TAG.perf --no-children --sort comm 2>/dev/null | grep -E "^ +[0-9]" | head -10
for role in fx:io fx:writer mg:flusher; do
  echo "--- top symbols in $role:"
  perf report -i /tmp/flux_$TAG.perf --no-children --comm $role --sort sym --percent-limit 2 2>/dev/null | grep -E "^ +[0-9]" | cut -c1-140 | head -14
done
echo "--- tuner:"; curl -s --max-time 5 http://127.0.0.1:18091/stats/tuner | tr -d '\n ' | cut -c1-600
if [ -n "${DWARF:-}" ]; then
  echo "--- writer dwarf cumulative:"
  perf report -i /tmp/flux_$TAG.dw.perf --children --percent-limit 2.5 --no-demangle 2>/dev/null | grep -E "^ +[0-9]" | grep -vE "kallsyms|unknown|inlined|clone3|start_thread|libstdc" | cut -c1-165 | c++filt | cut -c1-170 | head -45
fi
