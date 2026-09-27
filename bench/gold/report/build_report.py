#!/usr/bin/env python3
"""Build the golden benchmark HTML report from per-row result JSONs.

usage: build_report.py RESULTS_DIR [-o OUT.html] [--run RUN.json]

RESULTS_DIR holds one <ROW>.json per row as written by gold_summ.py (T1..T20,
L1..L20, S1..S4 and the E rungs such as T2_m50 / L2_m50). RUN.json (default
RESULTS_DIR/run.json) carries what the results cannot: dates, build ids,
hardware, config and which host ran each row.
"""
import argparse, json, os, re, statistics as st, sys

HERE = os.path.dirname(os.path.abspath(__file__))
R4 = ("r5", "r25", "r50", "r75")
E_READ = [("", "100%", "10.9 GB"), ("_m50", "50%", "5.0 GB"), ("_m10", "10%", "1.0 GB"), ("_m3", "~4%", "0.41 GB"),
          ("_m1", "~1.4%", "0.14 GB"), ("_m05", "~0.7%", "0.07 GB"), ("_m01", "~0.1%", "0.014 GB")]
E_WRITE = [("", "100%", "10.9 GB"), ("_m10", "10%", "1.0 GB"), ("_m5", "5%", "0.50 GB")]
ROWS = (["T%d" % n for n in range(1, 21)] + ["L%d" % n for n in range(1, 21)] + ["S1", "S2", "S3", "S4"]
        + [p + "2" + t for t, _, _ in E_READ[1:] for p in "TL"] + [p + i + t for i in "78" for t, _, _ in E_WRITE[1:] for p in "TL"])
RUN_DEFAULTS = {"dates": "", "builds": {}, "hardware": "", "host_of": {}, "results_path": "gold_summ.py results",
                "config": ["learned seq locator", "no value pointers", "compact metadata"],
                "seq_lookup": "learned locator", "value_ptrs": "off", "metadata": "compact"}
# Memory given to the server at 100% residency: the 16 GiB quota.
MEM = 16 * 1.073741824
LIVE = 103.6  # GB of live data in the 1 KB overwrite rows: 100M x (12 + 1024) B


def load(results, run_path):
    g = {}
    for row in ROWS:
        p = os.path.join(results, row + ".json")
        if os.path.exists(p):
            g[row] = json.load(open(p))
    missing = [r for r in ROWS if r not in g]
    if missing:
        sys.exit("build_report: missing results for %d rows: %s" % (len(missing), " ".join(missing)))
    run = dict(RUN_DEFAULTS)
    if os.path.exists(run_path):
        run.update(json.load(open(run_path)))
    return g, run


def us(s): return int(s[:-2])


def f3(v):  # 1.01M, 968K, 22.0M: three significant figures, trailing zero dropped below 10M
    if v >= 1e7: return "%.1fM" % (v / 1e6)
    if v >= 1e6: return ("%.2fM" % (v / 1e6)).replace("0M", "M")
    return "%dK" % round(v / 1e3)


def rng(a, fmt):
    lo, hi = fmt % min(a), fmt % max(a)
    return lo if lo == hi else lo + "–" + hi


def lrow(d): return [[d[r]["achieved"], d[r]["target"], us(d[r]["p50"]), us(d[r]["p99"]), us(d[r]["p999"])] for r in R4]


def tsum(d):
    return {"ops": d.get("ops_avg"), "ios": d.get("read_ios_per_get"), "res": d.get("index_residency"), "hit": d.get("block_cache_hit"),
            "sa": d.get("spaceAmp"), "wa": d.get("devWA_vs_ingest"), "bc": d.get("block_cache_quota_GB"), "keys": d.get("keys"),
            "leaf": d.get("key_leaf_GB"), "idx": d.get("index_GB"), "mbs": d.get("dev_r_MBs"), "kbpg": d.get("dev_reads_KB_per_op"), "du": d.get("du_GB"), "rss": d.get("rss_GB")}


def pairs_caveat(host_of):
    # Durable/non-durable pairs are compared in the text, so say whether each shared a box.
    groups = [("T3–T6", ["T3", "T4", "T5", "T6"]), ("T7–T10", ["T7", "T8", "T9", "T10"]), ("S1–S4", ["S1", "S2", "S3", "S4"])]
    pairs = [("T3", "T5"), ("T4", "T6"), ("T7", "T9"), ("T8", "T10"), ("S1", "S2"), ("S3", "S4")]
    if not host_of:
        return "Same-box pairs", "Each durable and non-durable pair ran on one box."
    where = ", ".join("%s on %s" % (lbl, "/".join(sorted({host_of.get(r, "?") for r in ids}))) for lbl, ids in groups)
    if all(host_of.get(a) == host_of.get(b) for a, b in pairs):
        return "Same-box pairs", "Each durable and non-durable pair ran on one box: %s." % where
    return "Cross-box pairs", "Some durable and non-durable pairs ran on different boxes: %s. Same-box durability ratios come from the S rows." % where


def build(g, run):
    lat = {k: lrow(g[k]) for k in g if re.fullmatch(r"L\d+", k)}
    # The averaging window ends at 7140 s; the last two 60 s windows are cut short by the run stopping.
    sus = {k: {"ops": g[k]["ops_series_60s"][:-2], "du": g[k]["du_series_GB"][1:-2]} for k in g if re.fullmatch(r"S\d", k)}
    E = {}
    for k in g:
        if re.fullmatch(r"T[278](_m\w+)?", k): E[k] = tsum(g[k])
        if re.fullmatch(r"L[278](_m\w+)?", k): E[k] = dict(zip(R4, lrow(g[k])))
    EPLAN = {"reads": [{"tag": t, "target": a, "cache": c, "st": "done"} for t, a, c in E_READ],
             "writes": [{"tag": t, "target": a, "cache": c, "st": {"T7": "done", "T8": "done"}} for t, a, c in E_WRITE]}
    data = {"lat": lat, "sus": sus, "E": E, "EPLAN": EPLAN}

    KIND = ["read", "read", "insert", "insert", "insert", "insert", "overwrite", "overwrite", "overwrite", "overwrite"]
    DUR = [None, None, "nd", "nd", "du", "du", "nd", "nd", "du", "du"]; LK = [None, None, "on", "off", "on", "off", "on", "off", "on", "off"]
    W = []
    for n in list(range(3, 11)) + list(range(13, 21)):
        d = g["T%d" % n]; m = (n - 1) % 10
        W.append(["T%d" % n, "1 KB" if n <= 10 else "8 B", KIND[m], DUR[m], LK[m], d["spaceAmp"], d["devWA_vs_ingest"], d["devWA_vs_user"], d["ops_avg"], d["cpu_busy"], d["disk_util"], d["rss_GB"]])

    def rd(i, vs, ds, extra):
        d = g[i]; r = {"id": i, "vs": vs, "ds": ds, "ops": d["ops_avg"], "cpu": round(d["cpu_busy"]), "disk": round(d["disk_util"]), "rss": d["rss_GB"]}
        r.update(extra(d)); return r
    R = [rd("T1", "1 KB", "20M keys in memory", lambda d: {"ios": 0, "shape": "80 IO threads · 96 × 128"}),
         rd("T11", "8 B", "200M keys in memory", lambda d: {"ios": 0, "shape": "80 IO threads · 80 × 2048"}),
         rd("T2", "1 KB", "100M keys on disk", lambda d: {"ios": d["read_ios_per_get"], "kb": d["dev_reads_KB_per_op"]}),
         rd("T12", "8 B", "1B keys on disk", lambda d: {"ios": d["read_ios_per_get"], "kb": d["dev_reads_KB_per_op"]})]

    ops = lambda i: g[i]["ops_avg"]
    S = {}
    for k in ("S1", "S2", "S3", "S4"):
        o = sus[k]["ops"][10:]; m = sum(o) / len(o); du = sus[k]["du"][10:]
        S[k] = {"sd": 2 * st.pstdev(o) / m * 100, "run": sum(du) / len(du) / LIVE, "du": sum(du) / len(du), "sa": g[k]["spaceAmp"], "wa": g[k]["devWA_vs_ingest"], "rss": g[k]["rss_GB"]}
    p50 = [us(g["L%d" % i]["r5"]["p50"]) for i in range(1, 11)]; p99 = [us(g["L%d" % i]["r5"]["p99"]) for i in range(1, 11)]
    d999 = [us(g["L%d" % i]["r25"]["p999"]) for i in (5, 6, 9, 10, 15, 16, 19, 20)]
    i75 = [us(g["L%d" % i]["r75"]["p99"]) for i in range(3, 7)]
    o75 = [us(g["L%d" % i]["r75"]["p99"]) for i in (7, 8)]
    nb = [1 - ops("S1") / ops("S3"), 1 - ops("S2") / ops("S4")]; dur = [1 - ops("S2") / ops("S1"), 1 - ops("S4") / ops("S3")]
    sa_rest = [S[k]["sa"] for k in S]
    t2du = [g["T2"]["du_GB"], g["L2"]["du_GB"]]; owdu = [g[i]["du_GB"] for i in ("T7", "T8", "T9", "T10", "L7", "L8", "L9", "L10")]
    sdu = (S["S1"]["du"] + S["S2"]["du"]) / 2
    ins = ["T3", "T4", "T5", "T6"]
    # Inserts grow the store all run long, so their residency is the run average, not the end state.
    avgdu = [round(g[i]["du_avg_GB"]) for i in ins]
    avgres = ("~%d%%" % round(sum(g[i]["index_blocks_res_avg"] for i in ins) / 4 * 100),
              "~%d%%" % round(sum(g[i]["index_res_avg"] for i in ins) / 4 * 100))
    l75du = [g["L%d" % i]["du_GB"] for i in range(3, 7)]
    l75i = [g["L%d" % i]["r75"]["block_cache_quota_GB"] / g["L%d" % i]["r75"]["index_GB"] * 100 for i in range(3, 7)]
    l75l = [g["L%d" % i]["r75"]["index_residency"] * 100 for i in range(3, 7)]
    insk = [g[i]["inserted"] for i in ins]; insdu = [g[i]["du_GB"] for i in ins]
    spin = max(float(g[k]["spin_lock"].rstrip("%")) for k in g if re.fullmatch(r"[TS]\d+", k))
    b = run["builds"]; ph, pt = pairs_caveat(run["host_of"])

    H = {"dates": run["dates"], "S1ops": f3(ops("S1")), "S2ops": f3(ops("S2")), "S3ops": f3(ops("S3")), "S4ops": f3(ops("S4")),
         "S1sd": "%.1f" % S["S1"]["sd"], "T2ops": f3(ops("T2")), "T2rss": "%.1f" % g["T2"]["rss_GB"], "T2ios": "%.3f" % g["T2"]["read_ios_per_get"],
         "T1ops": f3(ops("T1")), "T1cpu": "%d" % round(g["T1"]["cpu_busy"]), "T11ops": f3(ops("T11")), "T11cpu": "%d" % round(g["T11"]["cpu_busy"]),
         "p50lo": "%d" % min(p50), "p50hi": "%d" % max(p50), "p99max": "%.1f" % ((max(p99) // 100 + 1) / 10),
         "magma": b.get("magma", "?"), "fluxkv": b.get("fluxkv", "?"), "client": b.get("client", "?"),
         "hardware": run["hardware"], "config": "<span>%s</span>" % " · ".join(run["config"]) if run["config"] else "",
         "saRest": "%.2f–%.2f" % (min(sa_rest), max(sa_rest)), "nbCost": "%.0f–%.0f" % (min(nb) * 100, max(nb) * 100),
         "durCost": "%.0f–%.0f" % (min(dur) * 100, max(dur) * 100), "T12ios": "%.2f" % g["T12"]["read_ios_per_get"],
         "T12tree": "%d" % round(g["T12"]["key_leaf_GB"] + g["T12"]["index_GB"]),
         "d999": "%d–%d" % (round(min(d999) / 1e3), round(max(d999) / 1e3)), "i75": "%.1f–%.1f" % (min(i75) / 1e6, max(i75) / 1e6),
         "o75": "%d" % (max(o75) // 1000 + 1), "du1k": "%d" % (round(g["T2"]["du_GB"] / 5) * 5), "du8b": "%d" % (round(g["T12"]["du_GB"] / 5) * 5),
         "sa18": "%d" % round(g["T18"]["spaceAmp"]), "idx": "%.1f" % (g["T2"]["index_GB"] + g["T2"]["key_leaf_GB"]),
         "leafB": "%d" % round(g["T2"]["key_leaf_GB"] * 1e9 / g["T2"]["keys"]), "wsB": "%d" % round(g["T2"]["block_cache_GB"] * 1e9 / g["T2"]["keys"]),
         "seqLookup": run["seq_lookup"], "valuePtrs": run["value_ptrs"], "metadata": run["metadata"],
         "spin": "%d" % (int(spin) + 1), "insKeys": "%.1f–%.1f" % (min(insk) / 1e9, max(insk) / 1e9), "insTB": "%.1f" % (sum(insdu) / 4 / 1000),
         "pairHead": ph, "pairText": pt, "L1base": "%.1f" % (g["L1"]["rate_base"] / 1e6), "L11base": "%.1f" % (g["L11"]["rate_base"] / 1e6),
         "footer": run.get("footer") or "Results: " + run["results_path"]}
    srows = [[k, dd, lk, ops(k), "±%.1f%%" % S[k]["sd"], "~%.2f" % S[k]["run"], "%.2f" % S[k]["sa"], S[k]["wa"], S[k]["rss"]]
             for k, dd, lk in (("S1", "nd", "on"), ("S2", "du", "on"), ("S3", "nd", "off"), ("S4", "du", "off"))]
    # Residency rows: label, on disk, memory / on disk (bar at the range's midpoint, %), index blocks
    # resident, index + leaves resident, bar label.
    def share(du):
        pct = [MEM / x * 100 for x in du]
        return round((min(pct) + max(pct)) / 2, 1), rng(pct, "%.1f") + "%"
    rrows = [["T1 / L1 in-memory read", "%d GB" % round(g["T1"]["du_GB"]), 100, "100%", "100%", "100%"],
             ["T2 / L2 disk read", rng(t2du, "%d") + " GB", *share(t2du)[:1], "100%", "100%% (hit %.2f%%)" % (g["T2"]["block_cache_hit"] * 100), share(t2du)[1]],
             ["T7–T10 / L7–L10 overwrite", rng(owdu, "%d") + " GB", *share(owdu)[:1], "100%", "100%", share(owdu)[1]],
             ["S1 / S2 two-hour overwrite", "~%d GB" % round(sdu), *share([sdu])[:1], "100%", "100%", share([sdu])[1]],
             ["T3–T6 insert, run average", rng(avgdu, "%d") + " GB", *share(avgdu)[:1], avgres[0], avgres[1], share(avgdu)[1]],
             ["L3–L6 insert, 75% step", rng([x / 1000 for x in l75du], "%.2f") + " TB", *share(l75du)[:1], "~" + rng(l75i, "%d") + "%", "~%d%%" % round(sum(l75l) / 4), share(l75du)[1]]]

    tpl = open(os.path.join(HERE, "template.html")).read()
    for k, v in (("/*__DATA__*/", "const DATA = %s;" % json.dumps(data, separators=(",", ":"))),
                 ("/*__W__*/", json.dumps(W, ensure_ascii=False)), ("/*__R__*/", json.dumps(R, ensure_ascii=False)),
                 ("/*__SROWS__*/", json.dumps(srows)), ("/*__RROWS__*/", json.dumps(rrows, ensure_ascii=False))):
        tpl = tpl.replace(k, v)
    tpl = re.sub(r"\{\{(\w+)\}\}", lambda m: H[m.group(1)], tpl)
    return tpl


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("results")
    ap.add_argument("-o", "--out", default="gold-report.html")
    ap.add_argument("--run", help="run metadata JSON (default RESULTS/run.json)")
    a = ap.parse_args()
    g, run = load(a.results, a.run or os.path.join(a.results, "run.json"))
    html = build(g, run)
    open(a.out, "w").write(html)
    print("%s: %d bytes" % (a.out, len(html)))


if __name__ == "__main__":
    main()
