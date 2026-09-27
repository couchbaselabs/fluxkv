#!/bin/bash
# Resumable per-box queue: runs each line of $GOLD_DIR/SWEEP (ID|TAG|VAR=value|...; values may hold
# spaces) through gold_run.sh with those env vars and GOLD_TAG=TAG. Rows with a result are skipped;
# a failed row is tried twice in all, across relaunches. gold.py appends rows and relaunches it.
H=${GOLD_HOME:-$(cd "$(dirname "$0")" && pwd)}; G=${GOLD_DIR:?run dir}
mkdir -p $G/tries $G/out; exec 9> $G/sweep.lock; flock -n 9 || { echo "$(date -u +%FT%TZ) sweep already running"; exit 0; }
echo "$(date -u +%FT%TZ) sweep start"
while IFS= read -r line; do
  [ -z "$line" ] && continue
  IFS='|' read -r -a f <<<"$line"; id=${f[0]}; tag=${f[1]}; rid=$id$tag
  [ -f $G/res/$rid.json ] && continue
  tries=$(cat $G/tries/$rid 2>/dev/null || echo 0); [ $tries -ge 2 ] && continue
  echo $((tries+1)) > $G/tries/$rid  # out/$rid.* is cleared by gold_run.sh
  echo "$(date -u +%FT%TZ) run $rid"
  ( for kv in "${f[@]:2}"; do export "$kv"; done; export GOLD_HOME=$H GOLD_DIR=$G GOLD_TAG=$tag; bash $H/gold_run.sh $id > /dev/null 2>&1 )
  if [ -f $G/res/$rid.json ]; then echo "$(date -u +%FT%TZ) done $rid"
  else echo "$(date -u +%FT%TZ) FAIL $rid: $(cat $G/out/$rid.fail 2>/dev/null)"; cp $G/out/$rid.log $G/tries/$rid.try$((tries+1)).log 2>/dev/null; fi
done < $G/SWEEP
echo "$(date -u +%FT%TZ) sweep end"
