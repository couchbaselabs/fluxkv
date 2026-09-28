#!/usr/bin/env python3
"""Run the golden benchmark across N machines and build its report.

  gold.py deploy  --hosts h1,h2 --server BIN --client BIN [--lib DIR] [--magma-sha SHA]
  gold.py run     --hosts h1,h2 --name NAME [--extra FLAGS] [--groups ..] [--rows ..] [--smoke] [--out DIR]
  gold.py status  --hosts h1,h2 --name NAME
  gold.py collect --hosts h1,h2 --name NAME --out DIR
  gold.py report  DIR [-o report.html]
  gold.py plan    --hosts h1,h2

`run` is a dispatcher: it hands the longest pending group (suite.py) to whichever box is idle
and keeps polling until every row has a result or has failed twice. All state lives on the
boxes, so a restarted `run` resumes where it left off. SSH uses your keys; if SSHPASS is set,
it goes through `sshpass -e` instead.
"""
import argparse, concurrent.futures as cf, datetime, io, json, os, re, shlex, subprocess, sys, tarfile, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import suite  # noqa: E402

SCRIPTS = ("gold_run.sh", "gold_sweep.sh", "gold_summ.py")


def ssh_argv(a, host, *rest):
    pw = ["sshpass", "-e"] if os.environ.get("SSHPASS") else []
    opts = ["-o", "ConnectTimeout=15", "-o", "StrictHostKeyChecking=accept-new"] + ([] if pw else ["-o", "BatchMode=yes"])
    return pw + ["ssh"] + opts + ["%s@%s" % (a.user, host)] + list(rest)


def rsh(a, host, script, check=True):
    """Run a bash script on host; returns stdout."""
    p = subprocess.run(ssh_argv(a, host, "bash -s"), input=script.encode(), capture_output=True)
    if check and p.returncode:
        raise SystemExit("%s: %s" % (host, p.stderr.decode().strip() or "exit %d" % p.returncode))
    return p.stdout.decode()


def each(hosts, fn):
    with cf.ThreadPoolExecutor(len(hosts)) as ex:
        return dict(zip(hosts, ex.map(fn, hosts)))


def run_dir(a):
    return "%s/runs/%s" % (a.root, a.name)


def label(host):
    return "." + host.rsplit(".", 1)[1] if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", host) else host.split(".")[0]


def git(*args):
    return subprocess.run(["git", "-C", HERE] + list(args), capture_output=True, text=True).stdout.strip()


# --- deploy -------------------------------------------------------------------
PREP = r"""
set -e
for f in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do [ -w $f ] && echo performance > $f; done
[ "$(cat /proc/sys/fs/aio-max-nr)" -ge 1048576 ] || sysctl -qw fs.aio-max-nr=1048576
miss=""; for c in python3 perf fstrim flock setsid du pgrep; do command -v $c >/dev/null || miss="$miss $c"; done
[ -n "$miss" ] && echo "missing:$miss" >&2
mkdir -p "$(dirname "${GOLD_DATA:-/data/gold}")"
echo ok
"""


def deploy(a):
    dirty = "-dirty" if git("status", "--porcelain", "--", "server", "client") else ""
    build = {"fluxkv": a.fluxkv_sha or git("rev-parse", "--short=7", "HEAD") + dirty,
             "client": a.client_sha or git("log", "-1", "--format=%h", "--", "../../client/fluxbench.cc"), "magma": a.magma_sha or "unknown"}
    with tempfile.TemporaryDirectory() as d:
        tgz = os.path.join(d, "gold.tgz")
        with tarfile.open(tgz, "w:gz") as t:
            for s in SCRIPTS:
                t.add(os.path.join(HERE, s), s)
            t.add(a.server, "bin/fluxkv_server"); t.add(a.client, "bin/fluxbench")
            if a.lib:
                t.add(a.lib, "lib")
            info = tarfile.TarInfo("bin/BUILD_ID"); raw = json.dumps(build).encode(); info.size = len(raw)
            t.addfile(info, io.BytesIO(raw))

        def one(h):
            with open(tgz, "rb") as f:
                p = subprocess.run(ssh_argv(a, h, "mkdir -p %s && tar xzf - -C %s" % (shlex.quote(a.root), shlex.quote(a.root))), stdin=f, capture_output=True)
            if p.returncode:
                return "copy failed: " + p.stderr.decode().strip()
            p = subprocess.run(ssh_argv(a, h, "bash -s"), input=PREP.encode(), capture_output=True)
            return (p.stdout.decode().strip() + " " + p.stderr.decode().strip()).strip()
        for h, r in each(a.hosts, one).items():
            print("%-16s %s" % (h, r))
    print("build:", json.dumps(build))


# --- state on a box -------------------------------------------------------------
STATE = r"""
cd %(dir)s 2>/dev/null || exit 0
flock -n sweep.lock true 2>/dev/null && echo "busy 0" || echo "busy 1"
for f in res/*.json; do [ -f "$f" ] && echo "res $(basename $f .json)"; done
for f in tries/*; do case $f in *.log) ;; *) [ -f "$f" ] && echo "tries $(basename $f) $(cat $f)";; esac; done
[ -f SWEEP ] && cut -d'|' -f1,2 SWEEP | tr -d '|' | sed 's/^/queued /'
[ -f sweep.log ] && tail -1 sweep.log | sed 's/^/last /'
"""


def state(a, h):
    try:
        out = rsh(a, h, STATE % {"dir": shlex.quote(run_dir(a))})
    except SystemExit as e:
        return {"down": str(e)}
    s = {"busy": False, "res": set(), "tries": {}, "queued": [], "last": ""}
    for line in out.splitlines():
        k, _, v = line.partition(" ")
        if k == "busy": s["busy"] = v == "1"
        elif k == "res": s["res"].add(v)
        elif k == "tries": n, c = v.split(); s["tries"][n] = int(c)
        elif k == "queued": s["queued"].append(v)
        elif k == "last": s["last"] = v
    return s


def launch(a, h, lines, spec):
    d = run_dir(a)
    body = "\n".join(lines)
    rsh(a, h, """set -e
mkdir -p %(d)s; cd %(d)s
cat > RUN.json <<'SPEC'
%(spec)s
SPEC
cat >> SWEEP <<'ROWS'
%(body)s
ROWS
GOLD_HOME=%(root)s GOLD_DIR=%(d)s nohup setsid bash %(root)s/gold_sweep.sh >> sweep.log 2>&1 < /dev/null &
""" % {"d": shlex.quote(d), "root": shlex.quote(a.root), "spec": json.dumps(spec), "body": body})


def run(a):
    groups = suite.select(a.groups, a.rows)
    extra_env = {"GOLD_SMOKE": "1"} if a.smoke else {}
    spec = {"extra": a.extra, "smoke": a.smoke, "groups": groups}
    started = {}  # host -> time of our last launch (a sweep takes a moment to take its lock)
    while True:
        st = each(a.hosts, lambda h: state(a, h))
        done = set().union(*(s["res"] for s in st.values() if "down" not in s))
        tries = {}
        for s in st.values():
            for n, c in s.get("tries", {}).items():
                tries[n] = max(tries.get(n, 0), c)
        dead = {n for n, c in tries.items() if c >= 2 and n not in done}
        owner = {}
        for h, s in st.items():
            for n in s.get("queued", []):
                owner.setdefault(n, h)
        where = {g: owner.get(suite.GROUPS[g][0].name) for g in groups}
        pending = [g for g in groups if not where[g]]
        left = [r.name for g in groups for r in suite.GROUPS[g] if r.name not in done | dead]
        if not left:
            break
        for h, s in st.items():
            if "down" in s or s["busy"] or time.time() - started.get(h, 0) < 90:
                continue
            mine = [r for g in groups if where[g] == h for r in suite.GROUPS[g] if r.name not in done | dead]
            if mine:  # its sweep stopped with rows left (reboot, kill): resume them
                launch(a, h, [], spec)
            elif pending:
                g = pending.pop(0)
                launch(a, h, [r.line(a.extra) + "".join("|%s=%s" % kv for kv in extra_env.items()) for r in suite.GROUPS[g]], spec)
                where[g] = h
                print("%s  %-16s <- %s (%d rows, ~%d min)" % (now(), h, g, len(suite.GROUPS[g]), suite.minutes(g)), flush=True)
            else:
                continue
            started[h] = time.time()
        total = sum(len(suite.GROUPS[g]) for g in groups)
        print("%s  %d/%d done, %d failed, %d groups pending | %s" % (now(), len(done & names(groups)), total, len(dead & names(groups)), len(pending),
              "  ".join("%s:%s" % (label(h), "down" if "down" in s else (s["last"].split(" ", 1)[-1] if s["busy"] else "idle")) for h, s in st.items())), flush=True)
        time.sleep(a.poll)
    print("%s  all rows finished; failed: %s" % (now(), ", ".join(sorted(dead & names(groups))) or "none"))
    if a.out:
        collect(a)
        if set(groups) == set(suite.GROUPS) and not a.smoke:
            report(argparse.Namespace(dir=a.out, o=os.path.join(a.out, "report.html"), run=None))
        else:
            print("partial or smoke run: no report (it needs all %d rows)" % len(names(suite.GROUPS)))


def names(groups):
    return {r.name for g in groups for r in suite.GROUPS[g]}


def now():
    return datetime.datetime.utcnow().strftime("%H:%M")


def status(a):
    for h, s in each(a.hosts, lambda h: state(a, h)).items():
        if "down" in s:
            print("%-16s down: %s" % (h, s["down"])); continue
        failed = sorted(n for n, c in s["tries"].items() if c >= 2 and n not in s["res"])
        print("%-16s %s  %d/%d done  failed: %s  last: %s" % (h, "busy" if s["busy"] else "idle", len(s["res"] & set(s["queued"])), len(s["queued"]),
              ",".join(failed) or "-", s["last"]))


# --- collect / report -------------------------------------------------------------
def dates(ts):
    a, b = (datetime.datetime.utcfromtimestamp(t) for t in (min(ts), max(ts)))
    if (a.year, a.month) == (b.year, b.month):
        return ("%d–%d %s" % (a.day, b.day, b.strftime("%b %Y"))) if a.day != b.day else a.strftime("%-d %b %Y")
    return "%s – %s" % (a.strftime("%-d %b"), b.strftime("%-d %b %Y"))


def hardware(a, h):
    out = rsh(a, h, r"""
cpu=$(lscpu | sed -n 's/^Model name: *//p' | head -1)
mem=$(awk '/MemTotal/{printf "%d", $2/1048576}' /proc/meminfo)
nv=$(ls /sys/block | grep -c '^nvme[0-9]*n1$'); model=$(cat /sys/block/nvme0n1/device/model 2>/dev/null | xargs)
raid=$(grep -o 'raid[0-9]*' /proc/mdstat 2>/dev/null | head -1 | tr a-z A-Z)
echo "$cpu|$mem|$nv|$model|$raid"
""", check=False).strip().split("|")
    if len(out) < 5:
        return None
    cpu = re.sub(r"\(R\)|\(TM\)|Intel |AMD | CPU.*| Processor.*", "", out[0]).strip()
    disks = "%s× %s NVMe%s" % (out[2], out[3], " " + out[4] if out[4] else "") if out[2] != "0" else "disk"
    return " · ".join([cpu, out[1] + " GB", disks, "loopback"])


def collect(a):
    os.makedirs(a.out, exist_ok=True)
    d = run_dir(a); host_of = {}; starts = []; builds = {}; spec = {}
    for h in a.hosts:
        blob = subprocess.run(ssh_argv(a, h, "cd %s 2>/dev/null && tar cf - res RUN.json 2>/dev/null; true" % shlex.quote(d)), capture_output=True).stdout
        if blob:
            with tempfile.TemporaryDirectory() as t:
                p = os.path.join(t, "r.tar"); open(p, "wb").write(blob)
                with tarfile.open(p) as tf:
                    tf.extractall(t)
                for f in sorted(os.listdir(os.path.join(t, "res"))) if os.path.isdir(os.path.join(t, "res")) else []:
                    os.replace(os.path.join(t, "res", f), os.path.join(a.out, f)); host_of[f[:-5]] = label(h)
                if os.path.exists(os.path.join(t, "RUN.json")):
                    spec = json.load(open(os.path.join(t, "RUN.json")))
        info = rsh(a, h, "cat %s/bin/BUILD_ID 2>/dev/null; echo; stat -c %%Y %s/out/*.start 2>/dev/null" % (shlex.quote(a.root), shlex.quote(d)), check=False).split("\n")
        if info[0].strip():
            builds[h] = json.loads(info[0])
        starts += [int(x) for x in info[1:] if x.strip().isdigit()]
    b = next(iter(builds.values()), {})
    if len({json.dumps(v, sort_keys=True) for v in builds.values()}) > 1:
        print("warning: boxes ran different builds:", json.dumps(builds))
    extra = spec.get("extra", "")
    config = ["index-block seq lookup" if "--no-learned-seq-locator" in extra else "learned seq locator",
              "value pointers" if "--value-ptr-write" in extra else "no value pointers",
              "legacy metadata" if "--no-compact-meta" in extra else "compact metadata"]
    labels = [label(h) for h in a.hosts]
    run = {"dates": dates(starts) if starts else "", "builds": {"magma": b.get("magma", "unknown"), "fluxkv": b.get("fluxkv", "unknown"),
           "client": b.get("client", "unknown")},
           "hardware": a.hardware or hardware(a, a.hosts[0]), "config": config, "host_of": host_of,
           "results_path": "%s/res/*.json on %s" % (d, labels[0] + "–" + labels[-1] if len(labels) > 1 else labels[0])}
    json.dump(run, open(os.path.join(a.out, "run.json"), "w"), indent=1, ensure_ascii=False)
    print("collected %d results into %s" % (len(host_of), a.out))


def report(a):
    cmd = [sys.executable, os.path.join(HERE, "report", "build_report.py"), a.dir, "-o", a.o or os.path.join(a.dir, "report.html")]
    if getattr(a, "run", None):
        cmd += ["--run", a.run]
    subprocess.check_call(cmd)


def plan(a):
    load = {h: 0 for h in a.hosts}
    for g in suite.select(a.groups, a.rows):
        h = min(load, key=load.get); load[h] += suite.minutes(g)
        print("%-16s %-26s %4d min" % (h, g, suite.minutes(g)))
    print("estimated wall clock: %.1f h" % (max(load.values()) / 60))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    def common(sp, name=True):
        sp.add_argument("--hosts", required=True, type=lambda s: [h for h in re.split(r"[,\s]+", open(s[1:]).read() if s.startswith("@") else s) if h],
                        help="comma-separated hosts, or @FILE")
        sp.add_argument("--user", default="root")
        sp.add_argument("--root", default="/root/fluxkv-gold", help="install dir on each box")
        if name:
            sp.add_argument("--name", required=True, help="run name; results live in ROOT/runs/NAME")
    sp = sub.add_parser("deploy"); common(sp, name=False)
    sp.add_argument("--server", required=True); sp.add_argument("--client", required=True)
    sp.add_argument("--lib", help="dir of shared libraries the server needs")
    sp.add_argument("--magma-sha"); sp.add_argument("--fluxkv-sha", help="default: this checkout's HEAD")
    sp.add_argument("--client-sha", help="default: last commit to client/fluxbench.cc here")
    for c in ("run", "plan"):
        sp = sub.add_parser(c); common(sp, name=(c == "run"))
        sp.add_argument("--groups", type=lambda s: s.split(","), help="suite groups (default: all)")
        sp.add_argument("--rows", type=lambda s: s.split(","), help="only groups holding these rows")
        if c == "run":
            sp.add_argument("--extra", default="", help="server flags for every row; pass as --extra='--block-writes-ratio 16 ...'")
            sp.add_argument("--smoke", action="store_true", help="1%% of the keys, 120 s phases")
            sp.add_argument("--poll", type=int, default=60)
            sp.add_argument("--out", help="collect results and build the report here when done")
            sp.add_argument("--hardware")
    sp = sub.add_parser("status"); common(sp)
    sp = sub.add_parser("collect"); common(sp); sp.add_argument("--out", required=True); sp.add_argument("--hardware")
    sp = sub.add_parser("report"); sp.add_argument("dir"); sp.add_argument("-o"); sp.add_argument("--run")
    a = p.parse_args()
    {"deploy": deploy, "run": run, "status": status, "collect": collect, "report": report, "plan": plan}[a.cmd](a)


if __name__ == "__main__":
    main()
