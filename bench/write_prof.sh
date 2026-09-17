#!/bin/bash
# DWARF call-graph profile of each thread role in a running fluxkv_server
# under a fixed client load. Env: CONNS PIPE VALSIZE ROLES N(tids per role)
set -u
FB=/root/fluxbench
PID=$(pgrep -f "fluxkv_serve[r] --port 14001" | head -1)
[ -n "$PID" ] || { echo "no server"; exit 1; }
ROLES=${ROLES:-"fx:writer fx:io mg:flusher mg:compactor"}
N=${N:-4}
$FB -host 127.0.0.1:14001 -mode set -keys 256000000 -vbuckets 256 -keylen 8 \
    -valsize ${VALSIZE:-8} -zipf 0.99 -conns ${CONNS:-128} -pipeline ${PIPE:-128} -batch 64 -pregen 64 \
    -runtime 60s > /tmp/fb_prof.txt 2>&1 &
FBP=$!
sleep 12
for role in $ROLES; do
  TIDS=$(grep -l "^$role" /proc/$PID/task/*/comm | head -$N | awk -F/ '{print $5}' | paste -sd,)
  perf record --call-graph dwarf,32768 -F 300 -t $TIDS -o /tmp/prof_${role//:/_}.perf -- sleep 5 >/dev/null 2>&1
done
wait $FBP
tail -1 /tmp/fb_prof.txt
for role in $ROLES; do
  echo "=================== $role (cumulative, >= ${LIM:-2.5}%)"
  perf report -i /tmp/prof_${role//:/_}.perf --children --percent-limit ${LIM:-2.5} --no-demangle 2>/dev/null \
    | grep -E "^ +[0-9]" | grep -vE "kallsyms|unknown|inlined|clone3|start_thread|libstdc\+\+.*0x" \
    | cut -c1-175 | c++filt | cut -c1-165 | head -${TOP:-40}
done
