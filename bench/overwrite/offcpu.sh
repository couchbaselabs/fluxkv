#!/bin/bash
# Where are the server threads waiting? Samples kernel wchan + user stack of
# every thread N times and aggregates by role. usage: offcpu.sh <role-prefix> [samples]
P=${SERVER_PID:-$(pgrep -x fluxkv_server)}; ROLE=${1:-fx:writer}; N=${2:-40}
tids=$(for t in /proc/$P/task/*; do c=$(cat $t/comm); [[ "$c" == $ROLE* ]] && echo ${t##*/}; done)
echo "role=$ROLE threads=$(echo $tids | wc -w) samples=$N"
for i in $(seq 1 $N); do
  for t in $tids; do
    st=$(awk "/^State:/{print \$2}" /proc/$P/task/$t/status 2>/dev/null)
    w=$(cat /proc/$P/task/$t/wchan 2>/dev/null)
    echo "$st $w"
  done
  sleep 0.05
done | sort | uniq -c | sort -rn | head -12
echo "--- kernel stacks of sleeping $ROLE threads (top frames, aggregated):"
for i in $(seq 1 10); do
  for t in $tids; do
    st=$(awk "/^State:/{print \$2}" /proc/$P/task/$t/status 2>/dev/null)
    [ "$st" = "S" -o "$st" = "D" ] && cat /proc/$P/task/$t/stack 2>/dev/null | head -6 | tr "\n" "|"; echo
  done
  sleep 0.1
done | grep -v "^$" | sort | uniq -c | sort -rn | head -8
