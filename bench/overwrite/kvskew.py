#!/usr/bin/env python3
# Per-kvstore disk vs magma counters, largest first. Splits "compaction
# stuck" (no compacts / all retries) from "compaction runs but reclaims
# nothing" (compact bytes normal, disk still growing).
import json, os, subprocess, sys, urllib.request
dd = sys.argv[1] if len(sys.argv) > 1 else "/data/ss/default"
live_mb = float(sys.argv[2]) if len(sys.argv) > 2 else 40e6 * 1032 / 2**20 / 64
st = {d["kvid"]: d for d in json.load(urllib.request.urlopen(os.environ.get("KVSTATS_URL", "http://127.0.0.1:18092/stats/kvstores"), timeout=8))}
du = {}
for line in subprocess.run(["du", "-sm"] + sorted(p for p in __import__("glob").glob(dd + "/shard-*/kvstore-*")), capture_output=True, text=True).stdout.splitlines():
    mb, path = line.split("\t"); du[int(path.rsplit("-", 1)[1])] = int(mb)
rows = sorted(du.items(), key=lambda kv: -kv[1])
print("  kvid shard   du_MB  x_live   NSets  NFlush  NCompact  NRetry  compactW_MB  writeW_MB  files")
for kvid, mb in rows[:8] + [("...", 0)] + rows[-4:]:
    if kvid == "...": print("  ..."); continue
    s = st.get(kvid, {})
    print("  %4s %5s %7d %6.2f %8.2fM %6d %9d %7d %11d %10d %6d" % (kvid, s.get("shard"), mb, mb / live_mb, s.get("NSets", 0) / 1e6, s.get("NFlushes", 0), s.get("NCompacts", 0), s.get("NRetryCompacts", 0), s.get("NWriteBytesCompact", 0) / 2**20, s.get("NWriteBytes", 0) / 2**20, s.get("NTableFiles", 0)))
