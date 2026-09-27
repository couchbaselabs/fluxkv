#!/usr/bin/env python3
# Summarise one golden row from out/ID.samples + client outputs -> JSON on stdout (called by gold_run.sh).
import sys, json, re, os
ID, OP, VS, KEYS, RES, TM, SPIN = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), sys.argv[5] == "1", int(sys.argv[6]), sys.argv[7]
O = os.environ["GOLD_DIR"] + "/out/" + os.environ.get("GOLD_RID", ID)
F = "set get miss tmpf chit cmiss bin devw devr du idle tot ticks nd ts rios wios idxb kleaf bcused bcquota bchit bcmiss kleafc locmem locfind locfb vphit vpmiss rss hwm magmamem".split()
rows = []
for l in open(O + ".samples"):
    f = l.split()
    rows.append(dict(tag=f[0], t=int(f[1]), **{k: float(v) for k, v in zip(F, f[2:])}))
def d(a, b, k): return b.get(k, 0) - a.get(k, 0)
def windows(tag):
    r = [x for x in rows if x["tag"] == tag]
    return [(b["t"], a, b) for a, b in zip(r, r[1:])]
def lat(tag):
    try: txt = open(f"{O}.{tag}.client").read()
    except OSError: return {}
    m = re.search(r"^LAT (.*)$", txt, re.M)
    return {k: v for k, v in re.findall(r"(p\d+|max)=(\d+us)", m.group(1))} if m else {}
def span(a, b):
    sec = b["ts"] - a["ts"]
    ops = d(a, b, "get") if OP == "read" else d(a, b, "set")
    out = dict(secs=round(sec), cpu_busy=round(100 * (1 - d(a, b, "idle") / max(1, d(a, b, "tot"))), 1),
               disk_util=round(100 * d(a, b, "ticks") / max(1, sec * 1000 * b["nd"]), 1),
               dev_w_MBs=round(d(a, b, "devw") / sec / 1e6), dev_r_MBs=round(d(a, b, "devr") / sec / 1e6))
    if OP == "read":
        out["dev_reads_KB_per_op"] = round(d(a, b, "devr") / max(1, ops) / 1024, 2)
        if "rios" in b: out["read_ios_per_get"] = round(d(a, b, "rios") / max(1, ops), 3)
        if RES:
            h, m = d(a, b, "chit"), d(a, b, "cmiss")
            out["cache_hit"] = round(h / max(1, h + m), 5)
    else:
        user = ops * (VS + 12)
        out["devWA_vs_ingest"] = round(d(a, b, "devw") / max(1, d(a, b, "bin")), 2)
        out["devWA_vs_user"] = round(d(a, b, "devw") / max(1, user), 2)
    if "idxb" in b:
        need = b["idxb"] + b["kleaf"]
        out["index_GB"] = round(b["idxb"] / 1e9, 2); out["key_leaf_GB"] = round(b["kleaf"] / 1e9, 2)
        out["block_cache_GB"] = round(b["bcused"] / 1e9, 2); out["block_cache_quota_GB"] = round(b["bcquota"] / 1e9, 2)
        if b.get("kleafc"):
            out["key_leaf_compressed_GB"] = round(b["kleafc"] / 1e9, 2)
        if b.get("locmem"):
            out["locator_MB"] = round(b["locmem"] / 1e6, 2)
            out["locator_finds"] = int(b["locfind"]); out["locator_fallbacks"] = int(b["locfb"])
        if b.get("rss"):
            rs = [x["rss"] for x in rows if x["tag"] == b["tag"] and a["t"] <= x["t"] <= b["t"] and x.get("rss")] or [b["rss"]]
            out["rss_GB"] = round(sum(rs) / len(rs) / 1e9, 2); out["rss_peak_GB"] = round(b["hwm"] / 1e9, 2)
            out["magma_mem_GB"] = round(b["magmamem"] / 1e9, 2)
        if "vpmiss" in b:
            h, m = d(a, b, "vphit"), d(a, b, "vpmiss")
            out["value_ptr_miss"] = round(m / max(1, h + m), 4)
            # GETs that searched the seqIndex: stale pointers plus GETs whose
            # pointer compaction purged. The locator counts every such search.
            g = d(a, b, "get")
            out["value_ptr_served"] = round(h / max(1, g), 4)
            out["seq_search_share"] = round(1 - h / max(1, g), 4)
        out["index_residency"] = round(min(1.0, b["bcquota"] / max(1, need)), 3)
        h, m = d(a, b, "bchit"), d(a, b, "bcmiss"); out["block_cache_hit"] = round(h / max(1, h + m), 4)
    return out
load = next(x for x in rows if x["tag"] == "load"); end = rows[-1]
inserted = end["set"] - load["set"] if OP == "insert" else 0
live = (KEYS + inserted) * (VS + 12)
res = dict(id=ID, op=OP, vs=VS, keys=KEYS, inserted=int(inserted), live_GB=round(live / 1e9, 1),
           du_GB=round(end["du"] * 1048576 / 1e9, 1), spin_lock=SPIN)
if OP != "read": res["spaceAmp"] = round(end["du"] * 1048576 / live, 2)
if ID[0] in "TS":
    w = windows("run")
    lo, hi = (120, 300) if (ID[0] == "T" and OP == "read" and RES) else (180, 840 if ID[0] == "T" else 7140)
    sel = [(t, a, b) for t, a, b in w if lo <= t <= hi] or w
    rates = [((d(a, b, "get") if OP == "read" else d(a, b, "set")) / (b["ts"] - a["ts"])) for t, a, b in sel]
    res["ops_avg"] = round(sum(rates) / len(rates))
    res["window"] = f"{sel[0][0]}-{sel[-1][0]}s"
    res.update(span(sel[0][1], sel[-1][2]))
    res["client"] = lat("run")
    # Run averages over the window's sample points: inserts grow the store, so end-of-run size
    # and residency overstate what the run saw on average.
    pts = [x for x in rows if x["tag"] == "run" and lo <= x["t"] <= hi and x.get("idxb")]
    if pts:
        res["du_avg_GB"] = round(sum(x["du"] for x in pts) / len(pts) * 1048576 / 1e9, 1)
        res["index_blocks_res_avg"] = round(sum(min(1.0, x["bcquota"] / x["idxb"]) for x in pts) / len(pts), 3)
        res["index_res_avg"] = round(sum(x["bcquota"] / (x["idxb"] + x["kleaf"]) for x in pts) / len(pts), 3)
    if ID[0] == "S":
        allr = [((d(a, b, "set")) / (b["ts"] - a["ts"])) for t, a, b in w]
        res["ops_series_60s"] = [round(x) for x in allr]
        r = [x for x in rows if x["tag"] == "run"]
        res["du_series_GB"] = [round(x["du"] * 1048576 / 1e9, 1) for x in r]
else:
    res["t_max"] = TM
    try: res["rate_base"] = int(open(O + ".base").read())
    except OSError: res["rate_base"] = TM
    for f in sorted({x["tag"] for x in rows if re.fullmatch(r"r\d+", x["tag"])}, key=lambda t: int(t[1:])):
        w = windows(f)
        if not w: continue
        s = span(w[0][1], w[-1][2]); ops = (d(w[0][1], w[-1][2], "get") if OP == "read" else d(w[0][1], w[-1][2], "set"))
        s["target"] = res["rate_base"] * int(f[1:]) // 100; s["achieved"] = round(ops / max(1, s["secs"]))
        s.update(lat(f)); res[f] = s
if RES and OP == "read":
    ch = [v.get("cache_hit", 1) for k, v in res.items() if isinstance(v, dict)] + [res.get("cache_hit", 1)]
    res["resident_valid"] = min(ch) >= 0.999
print(json.dumps(res, indent=1))
