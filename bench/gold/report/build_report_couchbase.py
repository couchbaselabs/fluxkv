#!/usr/bin/env python3
"""Build the golden report for a Couchbase Server run (gold.py run --couchbase).

usage: build_report_couchbase.py RESULTS_DIR [-o OUT.html]

template_v3.html with fluxkv's parts (Document Cache, block cache rungs, key-tree sizes) replaced by
Couchbase's: the bucket quota inside a cgroup, magma's own residency stats, and E rungs that shrink the
bucket (suite.CB_E_READ / CB_E_WRITE). RESULTS_DIR/run.json as for build_report.py; its optional
"residency_note" (HTML) is appended to the residency footnote.
"""
import argparse, json, os, re, statistics as st, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(HERE))
import build_report as br  # noqa: E402
import suite  # noqa: E402

E_ROWS = ([p + "2" + t for t, _, _ in suite.CB_E_READ for p in "TL"]
          + [p + n + t for n in "78" for t, _, _ in suite.CB_E_WRITE for p in "TL"])
ROWS = ["T%d" % n for n in range(1, 21)] + ["L%d" % n for n in range(1, 21)] + ["S1", "S2", "S3", "S4"] + E_ROWS
GIB = 1.073741824  # GB per GiB
CGROUP_EXTRA_MB = 4096  # gold_run.sh cb_shrink: the cgroup is the bucket plus this

TEXT = [
    ("Golden benchmark · fluxkv on magma · run", "Golden benchmark · Couchbase Server on magma · run"),
    ("In memory, fluxkv serves", "In memory, Couchbase serves"),
    ("/sec</b> at one IO per GET.", "/sec</b> at {{T2ios}} IOs per GET."),
    ('<span>magma <b>{{magma}}</b></span><span>fluxkv server <b>{{fluxkv}}</b></span><span>client <b>{{client}}</b></span><span>{{hardware}}</span><span>16 GiB quota · direct IO</span>',
     '<span><b>{{magma}}</b></span><span>client fluxbench <b>{{client}}</b></span><span>{{hardware}}</span><span>bucket 16 GiB · cgroup 32 GiB · page cache inside</span>'),
    ("<dt>Blind</dt><dd>The SET is written without looking up the old value</dd>",
     "<dt>Blind</dt><dd>Bucket setting <code>magma_enable_upsert=true</code>: magma writes the SET without looking up the old version</dd>"),
    ("<dt>Non-blind</dt><dd>The SET first looks up the key, which costs an index read</dd>",
     "<dt>Non-blind</dt><dd>The default: magma looks up the old version to tell an insert from an update</dd>"),
    ("<dt>Durable</dt><dd>Acknowledged once the shared write-ahead log has it on disk</dd>",
     "<dt>Durable</dt><dd>SyncWrite at level persistToMajority: acknowledged once the flusher has it on disk (one node, no replicas)</dd>"),
    ("<dt>Residency</dt><dd>How much of the index fits in the block cache</dd>",
     "<dt>TMPFAIL</dt><dd>Couchbase's reply to a SET it has no memory for yet; the client resends it</dd>"),
    ("<dt>RSS</dt><dd>Server resident memory, averaged over the measured window</dd>",
     "<dt>RSS</dt><dd>memcached resident memory, averaged over the measured window; the page cache is not in it</dd>"),
    ("<li><b>RSS</b> uses jemalloc's defaults, which hold about 2&nbsp;GB of unreturned memory.</li>",
     "<li><b>Memory</b> is one systemd cgroup (<code>MemoryMax</code>) around Couchbase: memcached and the page cache it fills share it.</li>"
     "<li><b>Max write rate</b> pushes Couchbase into TMPFAIL backpressure; only SETs it applied are counted.</li>"),
    ("durable (async-durable, shared WAL)", "durable (SyncWrite persistToMajority)"),
    # bytes_read counts TMPFAIL'd SETs too, so write amp is against the SETs Couchbase applied.
    ("<b>Write amp</b> = device bytes written ÷ bytes the server received.", "<b>Write amp</b> = device bytes written ÷ bytes of the SETs Couchbase applied."),
    ("At 8&nbsp;B the per-key overhead (seq record, key index entry, metadata) is several times the 20&nbsp;B payload, so both grow; blind 8&nbsp;B overwrites reach {{sa18}}× space because their garbage is only found by compaction.",
     "At 8&nbsp;B the per-document metadata (seqno, CAS, flags, expiry, revision) is several times the 20&nbsp;B payload, so both grow; blind 8&nbsp;B overwrites (T18) end at {{sa18}}× space."),
    ("In-memory rows run 80 IO threads and keep the 100&nbsp;µs response-coalescing timer; 8&nbsp;B needs ~164K requests in flight to peak.",
     "In-memory rows give the bucket 48&nbsp;GiB inside a 64&nbsp;GiB cgroup, with the same client shapes as the fluxkv suite."),
    ("<dd>GET of an existing key. <b>In memory</b> rows serve every GET from the Document Cache; <b>on disk</b> rows turn it off, so each GET goes through the index to the data on disk.</dd>",
     "<dd>GET of an existing key. <b>In memory</b> rows hold every document in the bucket quota; <b>on disk</b> rows hold far more data than the 16&nbsp;GiB quota, so most GETs are background fetches from magma.</dd>"),
    ("<dt>Document Cache</dt><dd>fluxkv's in-memory cache of whole documents, in front of magma and its block cache.</dd>",
     "<dt>Bucket quota</dt><dd>Couchbase's memory for documents, metadata and magma; full eviction, so metadata can leave memory too.</dd>"),
    ('<span class="ids">64 rows · T / L / S / E</span>', '<span class="ids">%d rows · T / L / S / E</span>' % len(ROWS)),
    ("plus four two-hour S rows and the E residency rows. All keys are 12&nbsp;B, uniform, in scrambled order, with a 1&nbsp;KB seqIndex block.",
     "plus four two-hour S rows and the E residency rows, which shrink the bucket quota and its cgroup. All keys are 12&nbsp;B, uniform, in scrambled order."),
    ("<dt>1 KB, Document Cache off</dt>", "<dt>1 KB, on disk</dt>"),
    ("<dt>8 B, Document Cache off</dt>", "<dt>8 B, on disk</dt>"),
    ("<dd>20M keys · 32 GiB Document Cache</dd>", "<dd>20M keys · bucket 48 GiB</dd>"),
    ("<dd>200M keys · 32 GiB Document Cache</dd>", "<dd>200M keys · bucket 48 GiB</dd>"),
    ("<dt>Network</dt><dd>none, loopback</dd>", "<dt>Network</dt><dd>none, loopback · SASL PLAIN</dd>"),
    # fluxkv's L11 outruns the latency client; Couchbase's does not.
    ('if (id === "L11" && i >= 2) return [["client", "client-bound"]];\n', ""),
    ('!(id === "L11" && i >= 2) && ', ""),
    (', <span class="flag client">client-bound</span> limited by the latency client', ""),
    ('<span class="brand">fluxkv · golden</span>', '<span class="brand">Couchbase · golden</span>'),
    # Sustain samples may be missing (null); plot and average the rest.
    ('const mk = f => Object.keys(sty).map(k => ({pts: DATA.sus[k][f].map((v, i) => [i + 1, f === "du" ? v / LIVE : v]).slice(2),',
     'const mk = f => Object.keys(sty).map(k => ({pts: DATA.sus[k][f].map((v, i) => [i + 1, v == null ? null : f === "du" ? v / LIVE : v]).slice(2).filter(p => p[1] != null),'),
    ("DATA.sus[k].du.slice(2).map(v => v / LIVE)", "DATA.sus[k].du.slice(2).filter(v => v != null).map(v => v / LIVE)"),
    # Residency: bucket quota rungs instead of block cache rungs.
    ("Memory is the 16&nbsp;GiB quota (direct IO), 10.9&nbsp;GB of it block cache; in-memory rows add a 32&nbsp;GiB Document Cache. The key index is {{idx}}&nbsp;GB at 100M keys.",
     "Memory is the bucket quota, with the cgroup 4&nbsp;GiB above it (memcached overhead, the other Couchbase processes and about 2&nbsp;GB of page cache). magma gets half the bucket quota for its write cache, block cache and bloom filters."),
    ("100M keys × 1&nbsp;KB, the same data in every row; only the block cache shrinks (<code>--write-cache-ratio</code>). Each cache size is in the tables below.",
     "100M keys × 1&nbsp;KB, the same data in every row: each loads at 16&nbsp;GiB, then the bucket quota and its cgroup shrink, down to magma's 100&nbsp;MB minimum. The 16&nbsp;GiB rung has a 20&nbsp;GiB cgroup, so it differs from T2, T7 and T8, which had 32&nbsp;GiB."),
    ("<span>against block cache per key (log scale)</span>", "<span>against bucket quota per key (log scale)</span>"),
    ("<span>get/s against block cache per key</span>", "<span>get/s against bucket quota per key</span>"),
    ("<h3>Reads: 100% down to 3 IOs per GET</h3>", "<h3>Reads: 16&nbsp;GiB down to 100&nbsp;MB</h3>"),
    ("<h3>Overwrites at 10% and 5% index residency</h3>", "<h3>Overwrites at 10% and 5% of the bucket</h3>"),
    ('const memGB = (r, t) => r.tag === "" ? 16 * 1.073741824 : t && t.bc != null ? t.bc + 0.34 : null;',
     "const memGB = (r, t) => t && t.bc != null ? t.bc + 4 * 1.073741824 : null;"),
    ('<th>Target</th><th class="r">Block cache</th><th class="r">Memory per key</th><th class="r">Memory given</th>',
     '<th>Target</th><th class="r">Bucket quota</th><th class="r">Memory per key</th><th class="r">cgroup</th>'),
    ('<th>Workload</th><th>Target</th><th class="r">Block cache</th>', '<th>Workload</th><th>Target</th><th class="r">Bucket quota</th>'),
    ("const base = E[id]; let first = true;", 'const base = E[id + "_c100"]; let first = true;'),
    ('t && base && r.tag ? ((t.ops / base.ops - 1) * 100).toFixed(0) + "%"',
     't && base && r.tag !== "_c100" ? (t.ops >= base.ops ? "+" : "") + ((t.ops / base.ops - 1) * 100).toFixed(0) + "%"'),
    ('With 5% of the index cached, non-blind overwrites keep <b>${keep("T7_m5", "T7")}</b> of their full-cache rate while blind overwrites keep <b>${keep("T8_m5", "T8")}</b>: a non-blind SET reads the key first, and that read now goes to disk.',
     'At 5% of the bucket, non-blind overwrites run at <b>${keep("T7_c5", "T7_c100")}</b> of their 16&nbsp;GiB rate and blind overwrites at <b>${keep("T8_c5", "T8_c100")}</b>.'),
    ("x: [lx(0.08), lx(150)], xt: [0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100].map(lx)", "x: [lx(0.5), lx(300)], xt: [0.5, 1, 2, 5, 10, 20, 50, 100, 200].map(lx)"),
    ('xlabel: "block cache per key"', 'xlabel: "bucket quota per key"'),
]
RESNOTE = ('<p class="note" id="resnote"><b>Data resident</b> is the cgroup (bucket quota + 4&nbsp;GiB) ÷ the data directory\'s size on disk. '
           '<b>Index resident</b> is magma\'s <code>ep_magma_index_resident_ratio</code> at the end of the window and <b>cache hit</b> its block cache hit rate over it. '
           'IOs per GET is left blank where Couchbase served under 1K get/s. %s</p>')
GOAL = re.compile(r'el\("line", \{x1: 56, x2: 560 - 20, y1: y\(3\).*?"goal: 3 IOs per GET"\);', re.S)


def sub1(s, old, new):
    if s.count(old) != 1:
        sys.exit("build_report_couchbase: template text found %d times: %r" % (s.count(old), old[:80]))
    return s.replace(old, new)


def size(mb):
    return "%d GiB" % (mb // 1024) if mb % 1024 == 0 else "%.1f GiB" % (mb / 1024) if mb >= 1024 else "%d MB" % mb


def template(run):
    t = open(os.path.join(HERE, "template_v3.html")).read()
    v = re.search(r"Couchbase Server (\d+\.\d+)", run["builds"].get("magma", ""))
    name = "Couchbase %s" % v.group(1) if v else "Couchbase"
    t = re.sub(r"<title>.*?</title>", "<title>%s Golden Benchmark</title>" % name, t, count=1)
    t = sub1(t, "<h1>fluxkv Golden Benchmark</h1>", "<h1>%s Golden Benchmark</h1>" % name.replace("Couchbase", "Couchbase Server"))
    for old, new in TEXT:
        t = sub1(t, old, new)
    srv = re.search(r'<div class="panel spec"><h3>Server</h3><dl>.*?</dl></div>', t, re.S).group(0)
    t = t.replace(srv, '<div class="panel spec"><h3>Server</h3><dl>'
                  "<dt>Build</dt><dd>{{magma}}, one node</dd>"
                  "<dt>Bucket</dt><dd>magma · full eviction · 128 vBuckets · 0 replicas</dd>"
                  "<dt>Bucket quota</dt><dd>16 GiB (48 GiB in memory)</dd>"
                  "<dt>cgroup MemoryMax</dt><dd>32 GiB (64 GiB in memory)</dd>"
                  "<dt>Reader / writer threads</dt><dd>disk i/o optimized</dd>"
                  "<dt>Durable</dt><dd>SyncWrite persistToMajority</dd>"
                  "<dt>Blind</dt><dd>magma_enable_upsert=true</dd>"
                  "<dt>Compression</dt><dd>passive (default)</dd>"
                  "<dt>Everything else</dt><dd>defaults</dd></dl></div>")
    t = re.sub(r'<p class="note" id="resnote">.*?</p>', lambda m: RESNOTE % run.get("residency_note", ""), t, count=1, flags=re.S)
    t, n = GOAL.subn("", t)
    if n != 1:
        sys.exit("build_report_couchbase: template's 3-IOs goal line not found")
    left = re.findall(r"fluxkv(?! suite)|Document Cache|\{\{(?:idx|leafB|wsB)\}\}|_m(?:50|10|5)\b", t)
    if left:
        sys.exit("build_report_couchbase: fluxkv-only text left: %s" % sorted(set(left)))
    return t


def residency(g):
    E = {}
    mb = dict((t, m) for t, m, _ in suite.CB_E_READ + suite.CB_E_WRITE)
    for k in E_ROWS:
        d, tag = g[k], k[2:]
        if k[0] == "T":
            # Under 1K get/s device reads per GET divide by almost nothing.
            E[k] = {"ops": d["ops_avg"], "ios": d.get("read_ios_per_get") if d["ops_avg"] >= 1000 else None,
                    "res": d.get("index_residency"), "hit": d.get("block_cache_hit"), "sa": d.get("spaceAmp"),
                    "wa": d.get("devWA_vs_user"), "bc": mb[tag] * 1048576 / 1e9, "keys": d.get("keys"),
                    "du": d.get("du_GB"), "rss": d.get("rss_GB")}
        else:
            E[k] = dict(zip(br.R4, br.lrow(d)))
    plan = {"reads": [{"tag": t, "target": a, "cache": size(m), "st": "done"} for t, m, a in suite.CB_E_READ],
            "writes": [{"tag": t, "target": a, "cache": size(m), "st": "done"} for t, m, a in suite.CB_E_WRITE]}
    return E, plan


def build(g, run):
    lat = {k: br.lrow(g[k]) for k in g if re.fullmatch(r"L\d+", k)}
    sus = {k: {"ops": g[k]["ops_series_60s"][:-2], "du": g[k]["du_series_GB"][1:-2]} for k in g if re.fullmatch(r"S\d", k)}
    E, EPLAN = residency(g)
    data = {"lat": lat, "sus": sus, "E": E, "EPLAN": EPLAN}
    KIND = ["read", "read", "insert", "insert", "insert", "insert", "overwrite", "overwrite", "overwrite", "overwrite"]
    DUR = [None, None, "nd", "nd", "du", "du", "nd", "nd", "du", "du"]; LK = [None, None, "on", "off", "on", "off", "on", "off", "on", "off"]
    W = []
    for n in list(range(3, 11)) + list(range(13, 21)):
        d = g["T%d" % n]; m = (n - 1) % 10
        W.append(["T%d" % n, "1 KB" if n <= 10 else "8 B", KIND[m], DUR[m], LK[m], d["spaceAmp"], d["devWA_vs_user"], d["devWA_vs_user"],
                  d["ops_avg"], d["cpu_busy"], d["disk_util"], d["rss_GB"]])

    def rd(i, vs, ds, extra):
        d = g[i]; r = {"id": i, "vs": vs, "ds": ds, "ops": d["ops_avg"], "cpu": round(d["cpu_busy"]), "disk": round(d["disk_util"]), "rss": d["rss_GB"]}
        r.update(extra(d)); return r
    disk = lambda d: {"ios": d["read_ios_per_get"], "kb": d["dev_reads_KB_per_op"]}
    R = [rd("T1", "1 KB", "20M keys in memory", lambda d: {"ios": 0, "shape": "96 × 128"}),
         rd("T11", "8 B", "200M keys in memory", lambda d: {"ios": 0, "shape": "80 × 2048"}),
         rd("T2", "1 KB", "100M keys on disk", disk), rd("T12", "8 B", "1B keys on disk", disk)]
    ops = lambda i: g[i]["ops_avg"]
    S = {}
    for k in ("S1", "S2", "S3", "S4"):
        o = sus[k]["ops"][10:]; m = sum(o) / len(o); du = [v for v in sus[k]["du"][10:] if v is not None]
        S[k] = {"sd": 2 * st.pstdev(o) / m * 100, "run": sum(du) / len(du) / br.LIVE, "sa": g[k]["spaceAmp"], "wa": g[k]["devWA_vs_user"], "rss": g[k]["rss_GB"]}
    p50 = [br.us(g["L%d" % i]["r5"]["p50"]) for i in range(1, 11)]; p99 = [br.us(g["L%d" % i]["r5"]["p99"]) for i in range(1, 11)]
    b = run["builds"]
    H = {"dates": run["dates"], "S1ops": br.f3(ops("S1")), "S2ops": br.f3(ops("S2")), "S3ops": br.f3(ops("S3")), "S4ops": br.f3(ops("S4")),
         "S1sd": "%.1f" % S["S1"]["sd"], "T2ops": br.f3(ops("T2")), "T2rss": "%.1f" % g["T2"]["rss_GB"], "T2ios": "%.2f" % g["T2"]["read_ios_per_get"],
         "T1ops": br.f3(ops("T1")), "T1cpu": "%d" % round(g["T1"]["cpu_busy"]), "T11ops": br.f3(ops("T11")), "T11cpu": "%d" % round(g["T11"]["cpu_busy"]),
         "p50lo": "%d" % min(p50), "p50hi": "%d" % max(p50), "p99max": "%.1f" % ((max(p99) // 100 + 1) / 10),
         "magma": b.get("magma", "?"), "client": b.get("client", "?"), "hardware": run["hardware"],
         "config": "<span>%s</span>" % " · ".join(run["config"]) if run["config"] else "",
         "du1k": "%d" % (round(g["T2"]["du_GB"] / 5) * 5), "du8b": "%d" % (round(g["T12"]["du_GB"] / 5) * 5),
         "sa18": "%d" % round(g["T18"]["spaceAmp"]),
         "nBoxes": "%d" % len(set(run["host_of"].values())) if run["host_of"] else "?",
         "ins1k": br.f3(max(ops(i) for i in ("T3", "T4", "T5", "T6"))), "upd1k": br.f3(max(ops(i) for i in ("T7", "T8", "T9", "T10"))),
         "footer": run.get("footer") or "Results: " + run["results_path"]}
    srows = [[k, dd, lk, ops(k), "±%.1f%%" % S[k]["sd"], "~%.2f" % S[k]["run"], "%.2f" % S[k]["sa"], S[k]["wa"], S[k]["rss"]]
             for k, dd, lk in (("S1", "nd", "on"), ("S2", "du", "on"), ("S3", "nd", "off"), ("S4", "du", "off"))]
    t = template(run)
    for k, v in (("/*__DATA__*/", "const DATA = %s;" % json.dumps(data, separators=(",", ":"))),
                 ("/*__W__*/", json.dumps(W, ensure_ascii=False)), ("/*__R__*/", json.dumps(R, ensure_ascii=False)),
                 ("/*__SROWS__*/", json.dumps(srows))):
        t = t.replace(k, v)
    return re.sub(r"\{\{(\w+)\}\}", lambda m: H[m.group(1)], t)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("results")
    ap.add_argument("-o", "--out", default="gold-report.html")
    a = ap.parse_args()
    g = {r: json.load(open(os.path.join(a.results, r + ".json"))) for r in ROWS if os.path.exists(os.path.join(a.results, r + ".json"))}
    missing = [r for r in ROWS if r not in g]
    if missing:
        sys.exit("build_report_couchbase: missing results for %d rows: %s" % (len(missing), " ".join(missing)))
    run = dict(br.RUN_DEFAULTS); run.update(json.load(open(os.path.join(a.results, "run.json"))))
    html = build(g, run)
    open(a.out, "w").write(html)
    print("%s: %d bytes" % (a.out, len(html)))


if __name__ == "__main__":
    main()
