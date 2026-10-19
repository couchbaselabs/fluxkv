#!/usr/bin/env python3
"""fluxkv golden run against a Couchbase Server golden run (gold.py run --couchbase), row by row.

usage: compare_couchbase.py FLUXKV_DIR COUCHBASE_DIR [-o OUT.html] [--notes NOTES.html]

Each dir holds one <row>.json per row plus run.json, as gold.py collect writes them. Couchbase rows
not there yet show as pending, so a running Couchbase run can be compared. NOTES.html (a <ul class="cav">
of setup notes and caveats) goes in the Setup section. Residency rungs pair in order: fluxkv's block cache
rungs (build_report.E_READ / E_WRITE) against Couchbase's bucket rungs (suite.CB_E_READ / CB_E_WRITE).
"""
import argparse, glob, itertools, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(HERE))
import build_report as br  # noqa: E402
import suite  # noqa: E402

R4 = br.R4
ALL = ["T%d" % n for n in range(1, 21)] + ["L%d" % n for n in range(1, 21)] + ["S1", "S2", "S3", "S4"]
KIND = ["read · in memory", "read · disk", "insert", "insert", "insert", "insert", "overwrite", "overwrite", "overwrite", "overwrite"]
DUR = [None, None, "non-durable", "non-durable", "durable", "durable", "non-durable", "non-durable", "durable", "durable"]
LK = [None, None, "non-blind", "blind", "non-blind", "blind", "non-blind", "blind", "non-blind", "blind"]
us = br.us
comma = lambda v: "{:,}".format(int(v))


def load(d):
    rows = {os.path.basename(f)[:-5]: json.load(open(f)) for f in glob.glob(os.path.join(d, "*.json")) if not f.endswith("run.json")}
    return rows, json.load(open(os.path.join(d, "run.json")))


def lt(u):
    return "%d µs" % u if u < 1000 else "%.2f ms" % (u / 1e3) if u < 1e4 else "%.1f ms" % (u / 1e3) if u < 1e5 else "%d ms" % round(u / 1e3) if u < 1e6 else "%.1f s" % (u / 1e6)


def hc(u):
    h = "h0" if u < 1e3 else "h1" if u < 1e4 else "h2" if u < 1e5 else "h3" if u < 1e6 else "h4"
    return '<span class="hc %s" style="padding:1px 6px">%s</span>' % (h, lt(u))


def rate(v):
    return "%.2fM" % (v / 1e6) if v >= 1e6 else "%dK" % round(v / 1e3)


def ratio(f, c):
    # fluxkv ÷ Couchbase: above 1 fluxkv is faster.
    x = f / c
    cls = "up" if x > 1.05 else "dn" if x < 0.95 else ""
    return '<td class="r dl %s">%s×</td>' % (cls, "%.1f" % x if x >= 10 else "%.2f" % x)


def gb(v):
    return "%.1f GB" % v if v >= 10 else "%.2f GB" % v if v >= 1 else "%d MB" % round(v * 1000)


def pct(v):
    return "–" if v is None else "%.1f%%" % (v * 100) if v < 0.1 else "%d%%" % round(v * 100)


def name(i):
    if i[0] == "S":
        n = int(i[1:])
        return "1 KB overwrite · %s · %s · 2 h" % ("durable" if (n - 1) % 2 else "non-durable", "blind" if (n - 1) // 2 else "non-blind")
    n = int(i[1:]); m = (n - 1) % 10
    return ("1 KB " if n <= 10 else "8 B ") + KIND[m] + (" · %s · %s" % (DUR[m], LK[m]) if DUR[m] else "")


def throughput(F, C):
    rows = ""
    for i in ["T%d" % n for n in range(1, 21)] + ["S1", "S2", "S3", "S4"]:
        if i in ("T1", "T11"):
            rows += '<tr class="gh"><td colspan="10">%s</td></tr>' % ("1 KB values" if i == "T1" else "8 B values")
        if i == "S1":
            rows += '<tr class="gh"><td colspan="10">Two-hour 1 KB overwrites</td></tr>'
        f, c = F[i], C.get(i)
        if c is None:
            rows += '<tr><td class="id">%s</td><td>%s</td><td class="r"><b>%s</b></td><td class="r" colspan="7"><span class="off">pending</span></td></tr>' % (i, name(i), comma(f["ops_avg"]))
            continue
        rd = i[0] == "T" and (int(i[1:]) - 1) % 10 < 2
        amp = ("%.2f · %.2f IO" % (f["read_ios_per_get"], c["read_ios_per_get"]) if f.get("read_ios_per_get") or c.get("read_ios_per_get") else "–") if rd \
            else "%.2f× · %.2f×" % (f["spaceAmp"], c["spaceAmp"])
        wa = "–" if rd else "%.1f× · %.1f×" % (f["devWA_vs_user"], c["devWA_vs_user"])
        rows += ('<tr><td class="id">%s</td><td>%s</td><td class="r"><b>%s</b></td><td class="r">%s</td>%s<td class="r">%s</td><td class="r">%s</td>'
                 '<td class="r">%.0f · %.0f%%</td><td class="r">%.0f · %.0f%%</td><td class="r">%.1f · %.1f GB</td></tr>') % (
            i, name(i), comma(f["ops_avg"]), comma(c["ops_avg"]), ratio(f["ops_avg"], c["ops_avg"]), amp, wa,
            f["cpu_busy"], c["cpu_busy"], f["disk_util"], c["disk_util"], f["rss_GB"], c["rss_GB"])
    return ('<table class="pin"><thead><tr><th>ID</th><th>Workload</th><th class="r">fluxkv</th><th class="r">Couchbase</th><th class="r">fluxkv ÷ CB</th>'
            '<th class="r">Space amp / IOs per GET</th><th class="r">Write amp</th><th class="r">CPU busy</th><th class="r">Disk busy</th><th class="r">RSS</th></tr></thead><tbody>'
            + rows + "</tbody></table>")


def latency(F, C):
    rows = ""
    for n in range(1, 21):
        if n in (1, 11):
            rows += '<tr class="gh"><td colspan="6">%s</td></tr>' % ("1 KB values" if n == 1 else "8 B values")
        f, c = F["L%d" % n], C.get("L%d" % n)
        if c is None:
            cells = "".join('<td class="r">%s <span class="off">· pending</span></td>' % hc(us(f[r]["p99"])) for r in R4)
        else:
            cells = "".join('<td class="r">%s <span class="off">·</span> %s<br><span class="off ld">%s · %s</span></td>' % (
                hc(us(f[r]["p99"])), hc(us(c[r]["p99"])), rate(f[r]["achieved"]), rate(c[r]["achieved"])) for r in R4)
        rows += '<tr><td class="id">L%d</td><td>%s</td>%s</tr>' % (n, name("T%d" % n), cells)
    return ('<table class="pin"><thead><tr><th>ID</th><th>Workload</th><th class="r">p99 @ 5%</th><th class="r">p99 @ 25%</th>'
            '<th class="r">p99 @ 50%</th><th class="r">p99 @ 75%</th></tr></thead><tbody>' + rows + "</tbody></table>")


def residency(F, C):
    # fluxkv memory is its 16 GiB quota at 100%, otherwise block cache plus 0.34 GB of pinned memtables.
    rows = ""
    for t, fx, cb in (("T2", br.E_READ, suite.CB_E_READ), ("T7", br.E_WRITE, suite.CB_E_WRITE), ("T8", br.E_WRITE, suite.CB_E_WRITE)):
        rd = t == "T2"
        rows += '<tr class="gh"><td colspan="9">%s</td></tr>' % {"T2": "1 KB disk reads (T2 / L2)", "T7": "1 KB non-blind overwrites (T7 / L7)",
                                                                  "T8": "1 KB blind overwrites (T8 / L8)"}[t]
        for (ft, flabel, _), crung in itertools.zip_longest(fx, cb):
            f, lf = F[t + ft], F["L" + t[1:] + ft]
            fmem = br.MEM if ft == "" else f["block_cache_quota_GB"] + 0.34
            ct, mb, clabel = crung if crung else (None, None, None)
            c, lc = (C.get(t + ct), C.get("L" + t[1:] + ct)) if ct else (None, None)
            label = flabel if clabel in (None, flabel) else "%s · %s" % (flabel, clabel)
            cmem = ('%s <span class="off">· cgroup %s</span>' % (gb(mb * 1048576 / 1e9), gb((mb + 4096) * 1048576 / 1e9)) if ct
                    else '<span class="off">below magma\'s 100 MB minimum</span>')
            if c is None:
                cops, rat = '<span class="off">%s</span>' % ("pending" if ct else "–"), '<td class="r">–</td>'
            elif c["ops_avg"] < 1000:
                cops, rat = comma(c["ops_avg"]), '<td class="r off">NoMemory</td>'
            else:
                cops, rat = comma(c["ops_avg"]), ratio(f["ops_avg"], c["ops_avg"])
            if rd:
                ci = c.get("read_ios_per_get") if c and c["ops_avg"] >= 1000 else None
                extra = "%.2f · %s" % (f["read_ios_per_get"], "%.2f" % ci if ci is not None else "–")
            else:
                extra = "%.2f× · %s" % (f["spaceAmp"], "%.2f×" % c["spaceAmp"] if c else "–")
            p75 = '%s <span class="off">·</span> %s' % (hc(us(lf["r75"]["p99"])), hc(us(lc["r75"]["p99"])) if lc else '<span class="off">–</span>')
            rows += ('<tr><td><b>%s</b></td><td class="r">%s</td><td class="r">%s</td><td class="r"><b>%s</b></td><td class="r">%s</td>%s'
                     '<td class="r">%s</td><td class="r">%s · %s</td><td class="r">%s</td></tr>') % (
                label, gb(fmem), cmem, comma(f["ops_avg"]), cops, rat, extra, pct(f.get("index_residency")),
                pct(c.get("index_residency")) if c else "–", p75)
    return ('<table class="pin"><thead><tr><th>Target</th><th class="r">fluxkv memory</th><th class="r">Couchbase bucket</th>'
            '<th class="r">fluxkv</th><th class="r">Couchbase</th><th class="r">fluxkv ÷ CB</th><th class="r">IOs per GET / space amp</th>'
            '<th class="r">Index resident</th><th class="r">p99 @ 75%</th></tr></thead><tbody>' + rows + "</tbody></table>")


def panel(title, r, server):
    b = r["builds"]
    return '<div class="panel"><h3>%s · %s</h3><dl><dt>server</dt><dd>%s</dd><dt>client</dt><dd>fluxbench %s</dd><dt>config</dt><dd>%s</dd></dl></div>' % (
        title, r["dates"], server(b), b["client"], ", ".join(r["config"]))


def build(fx_dir, cb_dir, notes):
    F, rf = load(fx_dir)
    C, rc = load(cb_dir)
    done = [r for r in ALL if r in C]
    src = open(os.path.join(HERE, "template_v3.html")).read()
    head = src[:src.index("</style>")] + """.dl{font:600 12.5px var(--mono)} .dl.up{color:var(--good)} .dl.dn{color:var(--bad)}
.ld{font:11px var(--mono)}
.cfg{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,340px),1fr));gap:20px}
.cfg .panel{padding:18px;display:grid;gap:8px}
.cfg dl{display:grid;grid-template-columns:auto 1fr;gap:6px 16px;margin:0;font-size:14px}
.cfg dt{color:var(--muted)} .cfg dd{margin:0;text-align:right;font-variant-numeric:tabular-nums}
</style>
"""
    v = re.search(r"Couchbase Server (\d+\.\d+)", rc["builds"].get("magma", ""))
    cb = "Couchbase %s" % v.group(1) if v else "Couchbase"
    head = head.replace("<title>fluxkv Golden Benchmark</title>", "<title>fluxkv vs %s</title>" % cb)
    progress = "%d of %d Couchbase rows done." % (len(done), len(ALL))
    if len(done) < len(ALL):
        progress += "</b> The rest are running; pending rows show fluxkv alone. <b>"
    return head + """
<nav class="top" aria-label="Sections"><div class="in"><span class="brand">fluxkv vs Couchbase · golden</span><a href="#notes">Setup</a><a href="#tput">Throughput</a><a href="#lat">Latency</a><a href="#res">Residency</a></div></nav>
<div class="wrap">
<header class="hero">
  <span class="eyebrow">Golden benchmark · same 44 rows and residency ladder, boxes, datasets and client</span>
  <h1>fluxkv vs """ + cb.replace("Couchbase", "Couchbase Server") + """</h1>
  <p class="thesis" style="font-size:19px;max-width:70ch"><b>""" + progress + """</b> Every pair reads fluxkv · Couchbase. The ratio is fluxkv ÷ Couchbase, green above 1.05× and red below 0.95×; boxes of the same build vary by up to 5%.</p>
  <div class="cfg">""" + panel("fluxkv", rf, lambda b: "fluxkv %s · magma-research %s" % (b["fluxkv"], b["magma"])) \
        + panel("Couchbase Server", rc, lambda b: b["magma"]) + """</div>
</header>
<section id="notes"><div class="sechead"><div class="row"><span class="eyebrow">Notes</span></div><div class="rule"></div><h2>Setup and caveats</h2></div>
""" + notes + """</section>
<section id="tput"><div class="sechead"><div class="row"><span class="eyebrow">Throughput</span><span class="ids">T1–T20 · S1–S4</span></div><div class="rule"></div><h2>Maximum throughput</h2>
<p class="note">Average of the 60&nbsp;s windows from 180 to 840&nbsp;s (S rows: 180 to 7,140&nbsp;s). Space amp is at the end of the run. Write amp is device bytes written ÷ bytes of the SETs applied (Couchbase also receives the SETs it rejects with TMPFAIL).</p></div>
<div class="panel tbl">""" + throughput(F, C) + """</div></section>
<section id="lat"><div class="sechead"><div class="row"><span class="eyebrow">Latency</span><span class="ids">L1–L20</span></div><div class="rule"></div><h2>p99 at 5, 25, 50 and 75% load</h2>
<p class="note">Each L row is paced from its own server's T maximum, so the same load step is a different rate for each server (second line: achieved rate, fluxkv · Couchbase).</p></div>
<div class="panel tbl">""" + latency(F, C) + """</div></section>
<section id="res"><div class="sechead"><div class="row"><span class="eyebrow">Memory against data</span><span class="ids">E rows</span></div><div class="rule"></div><h2>Residency: one dataset, less memory</h2>
<p class="note">100M keys × 1&nbsp;KB in every row. fluxkv shrinks only its block cache (direct IO); its memory is the 16&nbsp;GiB quota at 100%, otherwise block cache plus 0.34&nbsp;GB of pinned memtables. Couchbase shrinks the whole bucket quota after loading at 16&nbsp;GiB, with the cgroup 4&nbsp;GiB above it (about 2&nbsp;GB of page cache). Rungs pair in order by their share of the 16&nbsp;GiB quota. Couchbase's cgroup is larger than fluxkv's memory at every rung because it includes the page cache; its bucket alone is smaller at the ~4% and ~1.4% rungs. Index resident is fluxkv's block cache ÷ key index, and magma's <code>ep_magma_index_resident_ratio</code> for Couchbase. NoMemory: Couchbase served under 1K get/s, rejecting GETs with status 0x82.</p></div>
<div class="panel tbl">""" + residency(F, C) + """</div></section>
<footer>Built by bench/gold/report/compare_couchbase.py from %s and %s</footer>
</div>""" % (os.path.basename(os.path.normpath(fx_dir)), os.path.basename(os.path.normpath(cb_dir)))


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("fluxkv"); ap.add_argument("couchbase")
    ap.add_argument("-o", "--out", default="compare-couchbase.html")
    ap.add_argument("--notes", help="HTML for the Setup section")
    a = ap.parse_args()
    html = build(a.fluxkv, a.couchbase, open(a.notes).read() if a.notes else "")
    open(a.out, "w").write(html)
    print("%s: %d bytes" % (a.out, len(html)))


if __name__ == "__main__":
    main()
