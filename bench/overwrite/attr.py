#!/usr/bin/env python3
# Per-component write attribution over a window, from two magma stats
# snapshots (one shard). Every figure is bytes written / seq BytesIncoming
# in the same window, so rows add up to the shard's physical write amp
# excluding the shared WAL and any fluxkv-side writes.
import json, os, sys, time, urllib.request
def snap():
    d = json.load(urllib.request.urlopen(os.environ.get("STATS_URL", "http://127.0.0.1:18092/stats/magma"), timeout=8))
    s, k, l = d["seqStats"], d["keyStats"], d.get("localStats", {})
    r = {"in": s["BytesIncoming"], "seqW": s["Writer"]["NWriteBytes"],
         "seqC": s["CompactWriter"]["NWriteBytes"], "seqFC": s["FileCountCompactWriter"]["NWriteBytes"],
         "keyIn": k["BytesIncoming"], "keyW": k["Writer"]["NWriteBytes"], "keyC": k["CompactWriter"]["NWriteBytes"],
         "locW": l.get("Writer", {}).get("NWriteBytes", 0), "locC": l.get("CompactWriter", {}).get("NWriteBytes", 0),
         "retry": s["NRetryCompacts"], "ncomp": s["NCompacts"], "dlc": s["NDataLevelCompacts"],
         "ifc": s["NInternalFragmentationCompacts"], "wc": s["NWriterCompacts"], "fail": s["NFailedCompacts"], "ttl": s["NTTLCompacts"], "fcc": s["NFileCountCompacts"], "l0local": s.get("NNonL0LocalCompacts", 0),
         "fsw": d["FSPhysicalWriteBytes"], "fsr": d["FSPhysicalReadBytes"], "fsrl": d["FSReadBytes"], "nrb": d["NReadBytes"],
         "seqCR": s["CompactReader"]["NReadBytes"], "seqR": s["Reader"]["NReadBytes"], "seqIR": s["IterateReader"]["NReadBytes"],
         "keyCR": k["CompactReader"]["NReadBytes"], "keyR": k["Reader"]["NReadBytes"], "keyIR": k.get("IterateReader", {}).get("NReadBytes", 0), "topin": d["BytesIncoming"], "disk": d["ActiveDiskUsage"],
         "lv": [(x["NWriteBytes"], x["NumTables"], x["PhysicalSize"], x["LogicalSize"], x["TargetSize"]) for x in s["LevelStats"]]}
    return r
def report(a, b, secs):
    di = b["in"] - a["in"]
    if di <= 0: print("  no seq ingest in window"); return
    f = lambda k: (b[k] - a[k]) / di
    print("  window %ds  shard ingest %.2f GiB  (%.0f MiB/s)" % (secs, di / 2**30, di / secs / 2**20))
    print("  seq flush   %.3f   seq compact %.3f  (filecount %.3f)" % (f("seqW"), f("seqC"), f("seqFC")))
    dki = b["keyIn"] - a["keyIn"]
    print("  key flush   %.3f   key compact %.3f   (key ingest %.3f of seq ingest; key-tree WA %.2f)" % (
        f("keyW"), f("keyC"), dki / di,
        ((b["keyW"] - a["keyW"]) + (b["keyC"] - a["keyC"])) / dki if dki > 0 else 0))
    print("  loc flush   %.3f   loc compact %.3f" % (f("locW"), f("locC")))
    print("  sum(above)  %.3f   bucket FSPhysicalWrite/BytesIncoming %.3f" % (
        sum(f(k) for k in ("seqW", "seqC", "seqFC", "keyW", "keyC", "locW", "locC")),
        (b["fsw"] - a["fsw"]) / max(1, b["topin"] - a["topin"])))
    dti = max(1, b["topin"] - a["topin"])
    print("  READS / ingest (shard0 logical): seq compact %.3f  seq lookup %.3f  seq iterate %.3f | key compact %.3f  key lookup %.3f  key iterate %.3f" % (
        f("seqCR"), f("seqR"), f("seqIR"), f("keyCR"), f("keyR"), f("keyIR")))
    print("  READS bucket: FSReadBytes(logical) %.3f  FSPhysicalRead %.3f  of ingest  -> physical/logical %.2f" % (
        (b["fsrl"] - a["fsrl"]) / dti, (b["fsr"] - a["fsr"]) / dti, (b["fsr"] - a["fsr"]) / max(1, b["fsrl"] - a["fsrl"])))
    print("  seq per-level bytes written / ingest: " + "  ".join(
        "L%d=%.3f" % (i, (b["lv"][i][0] - a["lv"][i][0]) / di) for i in range(len(b["lv"]))))
    print("  seq level shape now: " + "  ".join("L%d[n=%d phys=%.0fM log=%.0fM]" % (i, t[1], t[2] / 2**20, t[3] / 2**20) for i, t in enumerate(b["lv"])))
    print("  compacts %d  retries(guard/unavail) %d  dataLevel %d  intFrag %d  fileCount %d  nonL0local %d" % (
        b["ncomp"] - a["ncomp"], b["retry"] - a["retry"], b["dlc"] - a["dlc"], b["ifc"] - a["ifc"], b["fcc"] - a["fcc"], b["l0local"] - a["l0local"]))
    print("  writerStallCompacts %d  failed %d  l0ttl %d" % (b["wc"] - a["wc"], b["fail"] - a["fail"], b["ttl"] - a["ttl"]))
    print("  seq level targets: " + "  ".join("L%d=%.0fM" % (i, t[4] / 2**20) for i, t in enumerate(b["lv"])))
    print("  bucket ActiveDiskUsage %.1f GiB" % (b["disk"] / 2**30))
if __name__ == "__main__":
    secs = int(sys.argv[1]) if len(sys.argv) > 1 else 120
    a = snap(); time.sleep(secs); b = snap(); report(a, b, secs)
