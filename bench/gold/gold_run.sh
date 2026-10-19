#!/bin/bash
# Run one golden row by ID (T1-T20, L1-L20, S1-S4; see suite.py) on this box.
# Fresh data dir + fstrim, load, phases, 60 s samples. On success writes $GOLD_DIR/res/ID$GOLD_TAG.json.
#   GOLD_HOME  install dir holding bin/, lib/ and these scripts (default: this script's dir)
#   GOLD_DIR   run dir holding res/ and out/ (default: $GOLD_HOME)
#   GOLD_DATA  data dir, wiped per row (default: /data/gold)
#   GOLD_TAG   result name suffix (E rows: _m50 ...); an L row paces from T<n>$GOLD_TAG
#   GOLD_EXTRA extra server flags; GOLD_SMOKE=1 runs 1% of the keys with 120 s phases
#   GOLD_SERVER fluxkv (default) or couchbase: the local Couchbase Server, one fresh magma bucket per row
# usage: gold_run.sh ID
set -u
ID=${1:?id}; H=${GOLD_HOME:-$(cd "$(dirname "$0")" && pwd)}; G=${GOLD_DIR:-$H}
RID=$ID${GOLD_TAG:-}; OUT=$G/out/$RID; mkdir -p $G/out $G/res; rm -f $OUT.*; touch $OUT.start
exec > >(tee $OUT.log) 2>&1
ulimit -n 1048576; ulimit -c unlimited
# No MAGMA_FLUSH_DELAY_US: its 100 us response timer rounds up to the ~1 ms epoll tick on an idle loop.
export FLUXKV_NO_CRASH_HANDLER=1 LD_LIBRARY_PATH=$H/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}; unset MAGMA_FLUSH_DELAY_US
KV=${GOLD_KV:-$H/bin/fluxkv_server}; FB=${GOLD_FB:-$H/bin/fluxbench}
export GOLD_DATA=${GOLD_DATA:-/data/gold}; DD=$GOLD_DATA; VB=128
export GOLD_SERVER=${GOLD_SERVER:-fluxkv}
[ $GOLD_SERVER = couchbase ] && export GOLD_DATA=/data/cbgold/data && DD=$GOLD_DATA
[ $GOLD_SERVER = couchbase ] && : "${GOLD_CB_USER:?the Couchbase login comes from gold.py run --couchbase}" "${GOLD_CB_PASS:?}"
GB=$((1024*1024*1024)); CACHE_BYTES=$((32*GB))

# --- spec -------------------------------------------------------------------
kind=${ID:0:1}; n=${ID:1}
if [ $kind = S ]; then VS=1024; KEYS=100000000; OP=overwrite; RES=0; DUR=$(( (n-1)%2 )); BLIND=$(( (n-1)/2 ))
else
  if [ $n -le 10 ]; then VS=1024; m=$n; KD=100000000; KR=20000000; else VS=8; m=$((n-10)); KD=1000000000; KR=200000000; fi
  RES=0; DUR=0; BLIND=0
  case $m in
    1) OP=read; RES=1; KEYS=$KR;; 2) OP=read; KEYS=$KD;;
    *) KEYS=$KD; i=$((m-3)); [ $i -lt 4 ] && OP=insert || OP=overwrite
       DUR=$(( (i%4)/2 )); BLIND=$(( i%2 ));;
  esac
fi
[ -n "${GOLD_SMOKE:-}" ] && KEYS=$((KEYS/100))
PAIR=${GOLD_PAIR:-T$n${GOLD_TAG:-}}  # the throughput row an L row paces from
SRV="--shards 8 --vbuckets $VB --mem-quota $((16*GB)) --flushers 40 --write-queue-mem 1073741824
  --no-compression --index-compression-lz4 --io-queue-depth 16 --stats-port 18093 --compactors 64
  --batch-sort always --shared-wal --shared-wal-chunk-size 8388608 --shared-wal-chunks 24
  --shared-wal-flushers 4 --lsd-frag-ratio 0.5 --mem-lwm-ratio 0.8 --readers 128 --writers 32
  --key-warm-new-tables --sstable-write-buffer 1048576 --key-block-size 4096 --data-block-size 1024
  --lsd-levels 3 --lsd-tiered-l0"
IOT=24; [ $RES = 1 ] && { SRV="$SRV --cache-size $CACHE_BYTES"; [ $kind = T ] && IOT=72; }
SRV="$SRV --io-threads $IOT ${GOLD_EXTRA:-}"
[ -n "${GOLD_ENV:-}" ] && export $GOLD_ENV
# Saturated resident reads keep the response-coalescing timer (21.8M vs 7.9M GET/s at 1 KB);
# latency rows must not have it.
[ $kind = T ] && [ $RES = 1 ] && [ -z "${MAGMA_FLUSH_DELAY_US:-}" ] && export MAGMA_FLUSH_DELAY_US=100
[ $DUR = 1 ] && SRV="$SRV --durable --async-durable"
[ $BLIND = 1 ] && SRV="$SRV --blind-writes"
C="-host 127.0.0.1:14002 -vbuckets $VB -keylen 12 -valsize $VS -scramble -randvals ${GOLD_CLIENT:-}"
CDUR=""
if [ $GOLD_SERVER = couchbase ]; then
  C="-host 127.0.0.1:11210 -vbuckets $VB -keylen 12 -valsize $VS -scramble -randvals -user $GOLD_CB_USER -pass $GOLD_CB_PASS -bucket gold ${GOLD_CLIENT:-}"
  # Durability is per request: the load stays non-durable, measured SETs ask for persistToMajority.
  [ $DUR = 1 ] && CDUR="-durability persistToMajority"
fi
echo "=== $RID kind=$kind op=$OP vs=$VS keys=$KEYS resident=$RES durable=$DUR blind=$BLIND"
echo "build: $(cat $H/bin/BUILD_ID 2>/dev/null) server=$(md5sum $KV | cut -c1-8) client=$(md5sum $FB | cut -c1-8) kernel=$(uname -r)"
echo "health: gov=$(sort -u /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | xargs) aio=$(cat /proc/sys/fs/aio-max-nr)"
if [ $GOLD_SERVER = couchbase ]; then
  echo "server: couchbase $(cat /opt/couchbase/VERSION.txt) $(curl -sf -u "$GOLD_CB_USER:$GOLD_CB_PASS" http://127.0.0.1:8091/pools/default/settings/memcached/global) client-durability: ${CDUR:-none}"
else
  echo "server: $(echo $SRV | xargs) env: ${GOLD_ENV:-none}"
fi

# --- helpers ----------------------------------------------------------------
fail() { echo "FAIL: $*"; echo "$*" > $OUT.fail; pkill -f "port 1400[2]"; exit 1; }
sample() { # one line: tag t, then the counters named by F in gold_summ.py
  python3 - "$1" "$2" <<'PY'
import sys, json, urllib.request, os, subprocess, time, re
def get(p):
    try: return json.load(urllib.request.urlopen("http://127.0.0.1:18093"+p, timeout=10))
    except Exception: return {}
s=get("/stats"); m=get("/stats/magma"); ks=m.get("keyStats",{}); ss=m.get("seqStats",{})
CB = os.environ.get("GOLD_SERVER") == "couchbase"
if CB:  # the same columns from cbstats; SETs count applied mutations (cmd_set includes TmpFail'd ones),
        # cache misses are background fetches
    try:
        out = subprocess.run(["/opt/couchbase/bin/cbstats", "127.0.0.1:11210", "-u", os.environ["GOLD_CB_USER"], "-p", os.environ["GOLD_CB_PASS"],
                              "-b", "gold", "all"], capture_output=True, text=True, timeout=30).stdout
    except Exception: out = ""
    c = {}
    for l in out.splitlines():
        k, _, v = l.partition(":")
        try: c[k.strip()] = float(v)
        except ValueError: pass
    g, bg = c.get("cmd_get", 0), c.get("ep_bg_fetched", 0)
    s = {"cmd_set_resp": c.get("vb_active_ops_create", 0) + c.get("vb_active_ops_update", 0), "cmd_get_resp": g, "cmd_get_resp_miss": c.get("get_misses", 0),
         "tmp_fails": c.get("ep_tmp_oom_errors", 0), "cache_hits": g - bg, "cache_misses": bg}
    m = {"BytesIncoming": c.get("bytes_read", 0), "BlockCacheMemUsed": c.get("ep_magma_block_cache_mem_used", 0),
         "BlockCacheHits": c.get("ep_magma_block_cache_hits", 0), "BlockCacheMisses": c.get("ep_magma_block_cache_misses", 0),
         "BlockCacheQuota": c.get("ep_max_size", 0) * c.get("ep_magma_mem_quota_ratio", 0),
         "IndexResidentRatio": c.get("ep_magma_index_resident_ratio", 0),
         "TotalMemUsed": c.get("mem_used", 0)}
cpu=open("/proc/stat").readline().split()[1:]; cpu=list(map(int,cpu))
def rss():  # server VmRSS and VmHWM in bytes
    try:
        pid=subprocess.run(["pgrep","-x","memcached"] if CB else ["pgrep","-f","port 1400[2]"],capture_output=True,text=True).stdout.split()[0]
        st=dict(l.split(":",1) for l in open("/proc/%s/status"%pid))
        return [int(st["VmRSS"].split()[0])*1024, int(st["VmHWM"].split()[0])*1024]
    except Exception: return [0,0]
dw=dr=tk=nd=ri=wi=0
for l in open("/proc/diskstats"):
    f=l.split()
    if re.fullmatch(os.environ.get("GOLD_DISKS", r"nvme\d+n1"), f[2]): dr+=int(f[5])*512; dw+=int(f[9])*512; tk+=int(f[12]); nd+=1; ri+=int(f[3]); wi+=int(f[7])
du=subprocess.run(["du","-sm",os.environ["GOLD_DATA"]],capture_output=True,text=True).stdout.split()
print(sys.argv[1], sys.argv[2], s.get("cmd_set_resp",0), s.get("cmd_get_resp",0), s.get("cmd_get_resp_miss",0),
      s.get("tmp_fails",0), s.get("cache_hits",0), s.get("cache_misses",0), m.get("BytesIncoming",0),
      dw, dr, du[0] if du else 0, cpu[3]+cpu[4], sum(cpu[:8]), tk, nd, "%.3f" % time.time(), ri, wi,
      m.get("TotalIndexBlocksSize",0), ks.get("ActiveDataBlocksSize",0), m.get("BlockCacheMemUsed",0), m.get("BlockCacheQuota",0), m.get("BlockCacheHits",0), m.get("BlockCacheMisses",0),
      ks.get("ActiveDataBlocksCompressedSize",0), ss.get("BlockLocatorMemUsed",0), ss.get("NBlockLocatorFinds",0), ss.get("NBlockLocatorFallbacks",0), m.get("NValuePtrHits",0), m.get("NValuePtrMisses",0), *rss(), m.get("TotalMemUsed",0), m.get("IndexResidentRatio",0))
PY
}
alive() { if [ $GOLD_SERVER = couchbase ]; then pgrep -x memcached >/dev/null; else pgrep -f "port 1400[2]" >/dev/null; fi; }
cbrest() { curl -sf -u "$GOLD_CB_USER:$GOLD_CB_PASS" "$@"; }
cbstat() { /opt/couchbase/bin/cbstats 127.0.0.1:11210 -u "$GOLD_CB_USER" -p "$GOLD_CB_PASS" -b gold "$@" 2>/dev/null; }
cb_ready() { cbrest http://127.0.0.1:8091/pools/default/buckets/gold 2>/dev/null | grep -q '"status":"healthy"' && cbstat all >/dev/null; }
# Fresh bucket for this row: delete the last one, cap the service's memory (memcached + page cache), trim, create.
# In-memory rows get 64G and a 48 GiB bucket, the rest 32G and 16 GiB; ns_server reads the cap only at start.
cb_reset() {
  local mem=32768 quota=30720 bq=16384
  [ $RES = 1 ] && { mem=65536; quota=61440; bq=49152; }
  /opt/couchbase/bin/couchbase-cli bucket-delete -c 127.0.0.1 -u "$GOLD_CB_USER" -p "$GOLD_CB_PASS" --bucket gold >/dev/null 2>&1
  for _ in $(seq 120); do cbrest http://127.0.0.1:8091/pools/default/buckets/gold >/dev/null 2>&1 || break; sleep 1; done
  # Bucket dirs are named by bucket uuid; drop any a failed or deleted bucket left, so du sees this row only.
  find $DD -mindepth 1 -maxdepth 1 -type d ! -name '@*' ! -name '.delete' -exec rm -rf {} +
  if [ "$(systemctl show couchbase-server -p MemoryMax --value)" != $((mem*1048576)) ]; then
    # Stopped first: shrinking a full cgroup in place makes the kernel reclaim inline and systemctl time out.
    systemctl stop couchbase-server; systemctl set-property couchbase-server MemoryMax=${mem}M; systemctl daemon-reload
    [ "$(systemctl show couchbase-server -p MemoryMax --value)" = $((mem*1048576)) ] || fail "MemoryMax ${mem}M"
    systemctl start couchbase-server
    for _ in $(seq 240); do cbrest http://127.0.0.1:8091/pools/default >/dev/null 2>&1 && break; sleep 1; done
  fi
  cbrest -X POST http://127.0.0.1:8091/pools/default -d memoryQuota=$quota >/dev/null || fail "memory quota $quota"
  sync; echo "trim: $(fstrim -v "$(df --output=target $DD | tail -1)")"
  # A 1B-key bucket can still be shutting down after it leaves the REST API, and creates fail until it is gone.
  for _ in $(seq 60); do
    /opt/couchbase/bin/couchbase-cli bucket-create -c 127.0.0.1 -u "$GOLD_CB_USER" -p "$GOLD_CB_PASS" --bucket gold \
      --bucket-type couchbase --bucket-ramsize $bq --bucket-replica 0 --wait >/dev/null 2>&1 && break
    cbrest http://127.0.0.1:8091/pools/default/buckets/gold >/dev/null 2>&1 && break
    sleep 10
  done
  local uuid=$(cbrest http://127.0.0.1:8091/pools/default/buckets/gold | python3 -c "import json,sys; print(json.load(sys.stdin)['uuid'])" 2>/dev/null)
  [ -n "$uuid" ] || fail "bucket create"
  find $DD -mindepth 1 -maxdepth 1 -type d ! -name '@*' ! -name '.delete' ! -name "$uuid" -exec rm -rf {} +
  if [ $BLIND = 1 ]; then
    # Couchbase's blind write. memcached reads extra_config_string at bucket start, and magma refuses to reopen
    # files made with the other setting, so the still-empty bucket's files go and magma recreates them.
    cbrest http://127.0.0.1:8091/diag/eval --data 'ns_bucket:update_bucket_props("gold",[{extra_config_string, "magma_enable_upsert=true"}]).' >/dev/null ||
      fail "diag/eval extra_config_string"
    systemctl stop couchbase-server; rm -rf "$DD/${uuid:?bucket uuid}"; systemctl start couchbase-server
    for _ in $(seq 240); do cbrest http://127.0.0.1:8091/pools/default >/dev/null 2>&1 && break; sleep 1; done
  fi
  for _ in $(seq 300); do cb_ready && break; sleep 1; done
  cb_ready || fail "bucket not healthy"
  [ $BLIND = 1 ] && { cbstat config | grep -Eq "ep_magma_enable_upsert: +true" || fail "magma_enable_upsert did not land"; }
  echo "couchbase: bucket ${bq} MB, MemoryMax ${mem} MB, quota ${quota} MB, $(cbstat config | grep -E "magma_enable_upsert|num_vbuckets|bucket_type" | tr -s ' ' | xargs)"
}
spin() { # kernel lock spin share over 10 s, all CPUs (IOVA allocator health)
  perf record -a -F 99 -o $OUT.perf -- sleep 10 >/dev/null 2>&1
  perf report -i $OUT.perf --no-children --sort symbol --stdio 2>/dev/null |
    awk '/native_queued_spin_lock_slowpath/{print $1; f=1} END{if(!f) print "0%"}' | head -1 > $OUT.spin
  rm -f $OUT.perf
}
# phase TAG SECS CLIENT-ARGS...: run the client, sample every 60 s, keep its output
phase() {
  local tag=$1 secs=$2; shift 2
  [ -n "${GOLD_SMOKE:-}" ] && secs=120
  echo "--- phase $tag ${secs}s: $*"
  local dur=""; case " $* " in *" -mode set "*) dur=$CDUR;; esac
  $FB $C $dur "$@" -runtime ${secs}s > $OUT.$tag.client 2>&1 &
  local cp=$! t=0
  sample $tag 0 >> $OUT.samples
  [ $secs -ge 120 ] && ( sleep $((secs/2)); spin ) &
  while [ $t -lt $secs ]; do
    sleep 60; t=$((t+60)); alive || fail "server died in $tag at ${t}s"
    sample $tag $t >> $OUT.samples; tail -1 $OUT.samples
  done
  wait $cp; grep -E "^(DONE|LAT|STATUS)" $OUT.$tag.client | sed 's/^/    /'
}
tmax() { python3 -c "import json;print(int(json.load(open('$G/res/$PAIR.json'))['ops_avg']))" 2>/dev/null; }

# --- server -----------------------------------------------------------------
if [ $kind = L ]; then TM=$(tmax); [ -n "$TM" ] || fail "no result for $PAIR yet"; fi
if [ $GOLD_SERVER = couchbase ]; then
  cb_reset
else
  pkill -f "port 1400[2]"; for _ in $(seq 60); do alive || break; sleep 1; done
  rm -rf $DD; mkdir -p $DD; sync; echo "trim: $(fstrim -v "$(df --output=target $DD | tail -1)")"
  cd $G/out  # core files land next to the row's logs
  setsid bash -c "exec $KV --port 14002 --data-dir $DD --hostname 127.0.0.1 --bucket default $(echo $SRV)" > $OUT.server 2>&1 &
  for _ in $(seq 240); do grep -q "listening on" $OUT.server && break; sleep 1; done
  grep -q "listening on" $OUT.server || fail "server did not start"
fi

# --- load -------------------------------------------------------------------
t0=$(date +%s)
$FB $C -mode set -keys $KEYS -conns 112 -pipeline 128 -runtime 7200s > $OUT.load 2>&1
lops=$(grep -oE "^DONE ops=[0-9]+" $OUT.load | grep -oE "[0-9]+$")
echo "load: ${lops:-0}/$KEYS in $(( $(date +%s)-t0 ))s $(grep ^LAT $OUT.load)"
[ "${lops:-0}" -ge $((KEYS*999/1000)) ] || fail "load incomplete"
sleep 30
sample load 0 >> $OUT.samples

# --- phases -----------------------------------------------------------------
W="-conns 112 -pipeline 128 -window"; P="$W"
OVW="-mode set -random -keys $KEYS"
INS() { echo "-mode set -keys 100000000000 -startkey $((KEYS + $1*10000000000))"; }  # disjoint fresh ranges
# E rungs (GOLD_CB_BUCKET_MB): the data loaded at full size, then the bucket shrinks and the cgroup gets only
# 4 GiB more, so the page cache cannot stand in for the memory taken away. Loading at a 100 MB bucket runs
# at ~13K SETs/s, hours per row.
cb_shrink() {
  local bq=$GOLD_CB_BUCKET_MB mem=$((GOLD_CB_BUCKET_MB + 4096)) t0=$(date +%s)
  /opt/couchbase/bin/couchbase-cli bucket-edit -c 127.0.0.1 -u "$GOLD_CB_USER" -p "$GOLD_CB_PASS" --bucket gold \
    --bucket-ramsize $bq >/dev/null || fail "bucket-edit $bq"
  cbrest -X POST http://127.0.0.1:8091/pools/default -d memoryQuota=$((bq > 256 ? bq : 256)) >/dev/null || fail "memory quota"
  systemctl stop couchbase-server; systemctl set-property couchbase-server MemoryMax=${mem}M; systemctl daemon-reload
  [ "$(systemctl show couchbase-server -p MemoryMax --value)" = $((mem*1048576)) ] || fail "MemoryMax ${mem}M"
  systemctl start couchbase-server
  # Secondary warmup loads values in the background after the bucket comes up; its reads must not overlap the phases.
  warm() { [ "$(cbstat warmup | grep -cE "ep_(warmup_thread|secondary_warmup_status): +complete")" = 2 ]; }
  for _ in $(seq 1800); do cb_ready && warm && break; sleep 1; done
  warm || fail "warmup"
  echo "shrink: bucket ${bq} MB, MemoryMax ${mem} MB, restart + warmup $(( $(date +%s)-t0 ))s, $(cbstat all | grep -E " (ep_max_size|curr_items):" | tr -s ' ' | xargs)"
}
if [ $OP = read ] && [ $RES = 0 ]; then  # one overwrite pass, then settle
  $FB $C -mode set -keys $KEYS -conns 112 -pipeline 128 -runtime 7200s > $OUT.pass 2>&1
  echo "overwrite pass: $(grep -E "^DONE" $OUT.pass)"; sleep 120
fi
[ $GOLD_SERVER = couchbase ] && [ -n "${GOLD_CB_BUCKET_MB:-}" ] && cb_shrink
case $kind in
  T)
    if [ $OP = read ] && [ $RES = 1 ]; then
      RC=${GOLD_RCONNS:-96}; RP=${GOLD_RPIPE:-1024}; RB=${GOLD_RBATCH:-128}
      PG=$(( (KEYS + RC*RB - 1) / (RC*RB) ))
      phase run ${GOLD_SECS:-300} -mode get -keys $KEYS -conns $RC -pipeline $RP -batch $RB -pregen $PG
    elif [ $OP = read ]; then phase run ${GOLD_SECS:-900} -mode get -keys $KEYS $W
    elif [ $OP = insert ]; then phase run ${GOLD_SECS:-900} $(INS 0) $W
    else phase run ${GOLD_SECS:-900} $OVW $W; fi ;;
  L)
    BASE=$TM
    if [ $OP = read ] && [ $RES = 1 ]; then
      # Resident reads outrun the latency client; pace from what it reaches unpaced.
      phase warm 60 -mode get -keys $KEYS $W
      CL=$(grep -oE "rate=[0-9]+" $OUT.warm.client | grep -oE "[0-9]+")
      [ -n "$CL" ] && [ $CL -lt $TM ] && BASE=$CL
    elif [ $OP = read ]; then phase warm 300 $OVW $W; sleep 60
    elif [ $OP = insert ]; then phase warm 600 $(INS 0) $W
    else phase warm 600 $OVW $W; fi
    k=0
    for f in ${GOLD_FRACS:-5 25 50 75}; do
      k=$((k+1)); R=$((BASE*f/100))
      if [ $OP = read ]; then phase r$f 300 -mode get -keys $KEYS $P -rate $R
      elif [ $OP = insert ]; then phase r$f 300 $(INS $k) $P -rate $R
      else phase r$f 300 $OVW $P -rate $R; fi
    done; echo $BASE > $OUT.base ;;
  S) phase run ${GOLD_SECS:-7200} $OVW $W ;;
esac
sleep 30; sample end 0 >> $OUT.samples
[ $GOLD_SERVER = couchbase ] || { pkill -f "port 1400[2]"; sleep 3; }
find $G/out -maxdepth 1 -name "core*" -newer $OUT.start 2>/dev/null | grep -q . && echo "core dump present" && echo core > $OUT.fail

# --- summary ----------------------------------------------------------------
GOLD_DIR=$G GOLD_RID=$RID python3 $H/gold_summ.py $ID $OP $VS $KEYS $RES "${TM:-0}" "$(cat $OUT.spin 2>/dev/null || echo ?)" > $OUT.sum || fail "summary"
cat $OUT.sum
[ -f $OUT.fail ] && exit 1
cp $OUT.sum $G/res/$RID.json
echo "GOLDDONE $RID"
