#!/bin/bash
#
# Cache A/B: restart the server with EXTRA flags via the recipe, warm the
# cache with GETs over a keyspace, then measure 40s of GETs while sampling
# server CPU, disk reads, RSS and the cache hit ratio over the window.
#
# usage: cache_bench.sh "<label>" "<EXTRA server flags>" <get-keyspace> <warm-seconds>
#   e.g. cache_bench.sh "cache8G hot4M" "--cache-size 8589934592" 4000000 60
#
# Env: SERVER_HOST, CLIENT_HOST (ssh targets), SERVER_ADDR (host:port the
# client connects to), CLIENT_BIN, STATS_URL, SSH. The server side expects the
# tuned recipe at /tmp/recipe.sh (see README) honouring DATA_DIR/BLOCK/EXTRA.
#
# Judge by CPU per op: at 1 KB values the link saturates around 1.11M GET/s,
# so throughput will not move between runs but CPU and disk reads will.
LBL="$1"; EX="$2"; KEYS="$3"; WARM="${4:-30}"
S51="${SSH:-ssh} ${SERVER_HOST:?set SERVER_HOST, e.g. root@10.0.0.1}"
S52="${SSH:-ssh} ${CLIENT_HOST:?set CLIENT_HOST, e.g. root@10.0.0.2}"
EB=${CLIENT_BIN:-/root/echobench2}
C="-host ${SERVER_ADDR:?set SERVER_ADDR host:port} -mode get -keys $KEYS -vbuckets 256 -conns 64 -pipeline 256 -batch 64"
for a in 1 2 3 4; do
  $S51 "setsid env DATA_DIR=/data/kvserver-ab-1k-s8 BLOCK=1024 EXTRA='$EX' bash /tmp/recipe.sh > /tmp/recipe.out 2>&1 < /dev/null & disown; sleep 2; echo issued" 2>&1 | grep -q issued && break
  sleep 10
done
for a in $(seq 1 25); do
  o=$($S51 'tail -1 /tmp/recipe.out 2>/dev/null' 2>&1)
  echo "$o" | grep -qE '^UP|Aborted|FAIL' && break
  sleep 20
done
echo "$o" | grep -q '^UP' || { echo "$LBL: SERVER FAILED: $o"; exit 1; }
$S52 "$EB $C -runtime ${WARM}s" > /dev/null 2>&1
st0=$($S51 'curl -s --max-time 5 ${STATS_URL:-http://127.0.0.1:18091/stats/dispatcher}' 2>&1)
$S52 "$EB $C -runtime 40s" > ${OUT:-/tmp/cache_bench_out.txt} 2>&1 &
st=$($S51 'sleep 12; p=$(pgrep -f "fluxkv_server --port 14001"|head -1); HZ=$(getconf CLK_TCK)
  read -r _ _ _ _ _ _ _ _ _ _ _ _ _ u1 s1 rest < /proc/$p/stat; sleep 18
  read -r _ _ _ _ _ _ _ _ _ _ _ _ _ u2 s2 rest < /proc/$p/stat
  awk -v u=$((u2-u1)) -v s=$((s2-s1)) -v hz=$HZ "BEGIN{ut=u/hz;st=s/hz;printf \"cpu %.1f cores  \",(ut+st)/18}"
  iostat -x 4 2 2>/dev/null | awk "/^md0/{r=\$2;m=\$3/1024} END{if(r>0)printf \"md0 %.0f r/s  %.2f KB/read  \", r, m*1024/r; else printf \"md0 0 r/s  \"}"
  grep -oE "VmRSS:\s+[0-9]+" /proc/$p/status | awk "{printf \"rss %.1f GB\", \$2/1048576}"' 2>&1)
wait
st1=$($S51 'curl -s --max-time 5 ${STATS_URL:-http://127.0.0.1:18091/stats/dispatcher}' 2>&1)
g() { echo "$2" | grep -oE "\"$1\": [0-9.]+" | grep -oE '[0-9.]+$'; }
h0=$(g cache_hits "$st0"); m0=$(g cache_misses "$st0"); h1=$(g cache_hits "$st1"); m1=$(g cache_misses "$st1")
hr=$(awk -v h=$((h1-h0)) -v m=$((m1-m0)) 'BEGIN{if(h+m>0)printf "%.1f%%",100*h/(h+m); else print "n/a"}')
echo "[$LBL keys=$KEYS] $(grep -oE 'rate=[0-9]+' ${OUT:-/tmp/cache_bench_out.txt} | tail -1) $(grep -oE 'errs=[0-9]+' ${OUT:-/tmp/cache_bench_out.txt} | tail -1)  $st  hit-ratio(window) $hr  items=$(g cache_items "$st1") cache_bytes=$(g cache_bytes "$st1")"
