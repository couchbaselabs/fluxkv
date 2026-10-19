"""The golden benchmark suite: 64 rows in groups that must share a box.

A group runs in order on one box: an L row paces from its T row's result there, and rows the
report compares (durable vs non-durable, blind vs non-blind, the four S rows) meet the same disks.
Minutes are measured runtimes on the reference boxes, used to start the longest groups first.
"""

# E rungs fix the per-shard write cache at 324 MiB and raise its share of the block+write
# cache budget, so only the block cache shrinks.
WRITE_CACHE = "--write-cache 339938175 --write-cache-ratio"


class Row:
    def __init__(self, rid, tag="", env=None, minutes=30):
        self.id, self.tag, self.env, self.minutes = rid, tag, dict(env or {}), minutes

    @property
    def name(self):
        return self.id + self.tag

    def line(self, extra=""):
        """One SWEEP line: ID|TAG|VAR=value|..., with extra server flags ahead of the row's own."""
        env = dict(self.env)
        flags = " ".join(x for x in (extra, env.pop("GOLD_EXTRA", "")) if x)
        if flags:
            env["GOLD_EXTRA"] = flags
        return "|".join([self.id, self.tag] + ["%s=%s" % kv for kv in env.items()])


def tl(n, t_min, l_min, env=None, tag=""):
    return [Row("T%d" % n, tag, env, t_min), Row("L%d" % n, tag, env, l_min)]


def rung(n, tag, ratio, t_min, l_min):
    return tl(n, t_min, l_min, {"GOLD_EXTRA": "%s %s" % (WRITE_CACHE, ratio)}, tag)


MEM_1K = {"GOLD_EXTRA": "--io-threads 80", "GOLD_RPIPE": "128"}
MEM_8B = {"GOLD_EXTRA": "--io-threads 80", "GOLD_RCONNS": "80", "GOLD_RPIPE": "2048"}

GROUPS = {
    "read-1k": tl(2, 20, 31),
    "mem-1k": tl(1, 6, 22, MEM_1K),
    "insert-1k": tl(3, 17, 32) + tl(5, 17, 32) + tl(4, 17, 32) + tl(6, 17, 32),
    "overwrite-1k": tl(7, 17, 32) + tl(9, 17, 32) + tl(8, 17, 32) + tl(10, 17, 32),
    "mem-8b": tl(11, 7, 24, MEM_8B),
    "read-8b": tl(12, 63, 74),
    "insert-8b-nonblind": tl(13, 22, 37) + tl(15, 24, 40),
    "insert-8b-blind": tl(14, 20, 35) + tl(16, 23, 38),
    "overwrite-8b-nonblind": tl(17, 22, 37) + tl(19, 25, 40),
    "overwrite-8b-blind": tl(18, 20, 36) + tl(20, 23, 38),
    "sustain": [Row("S%d" % n, minutes=123) for n in (1, 3, 2, 4)],
    "e-read-50": rung(2, "_m50", 0.636, 20, 31),
    "e-read-10": rung(2, "_m10", 0.927, 24, 35),
    "e-read-3": rung(2, "_m3", 0.97, 24, 35),
    "e-read-1": rung(2, "_m1", 0.99, 25, 36),
    "e-read-05": rung(2, "_m05", 0.995, 25, 36),
    "e-read-01": rung(2, "_m01", 0.999, 25, 36),
    "e-overwrite-nonblind-10": rung(7, "_m10", 0.927, 17, 32),
    "e-overwrite-nonblind-5": rung(7, "_m5", 0.964, 17, 32),
    "e-overwrite-blind-10": rung(8, "_m10", 0.927, 17, 32),
    "e-overwrite-blind-5": rung(8, "_m5", 0.964, 17, 32),
}


# Couchbase Server E rungs: the bucket quota after the load (gold_run.sh GOLD_CB_BUCKET_MB), as a share of the
# 16 GiB quota; magma's minimum bucket is 100 MB. Couchbase has no knob that shrinks only a block cache.
CB_E_READ = [("_c100", 16384, "100%"), ("_c50", 8192, "50%"), ("_c10", 1638, "10%"), ("_c4", 655, "~4%"),
             ("_c14", 229, "~1.4%"), ("_c06", 100, "~0.6%")]
CB_E_WRITE = [("_c100", 16384, "100%"), ("_c10", 1638, "10%"), ("_c5", 819, "5%")]


def couchbase():
    """The suite as gold.py --couchbase runs it, with minutes measured on Couchbase Server 8.5.

    Couchbase loads 1B 8 B keys in ~70 min, so each 8 B write group splits into its T/L pairs (an L row
    stays on the box of the T row it paces from) and its durable/non-durable pairs may meet different boxes.
    """
    g = {"read-1k": tl(2, 40, 45), "mem-1k": tl(1, 15, 20, MEM_1K), "insert-1k": GROUPS["insert-1k"],
         "overwrite-1k": GROUPS["overwrite-1k"], "mem-8b": tl(11, 25, 35, MEM_8B), "read-8b": tl(12, 185, 175),
         "sustain": GROUPS["sustain"]}
    for name, pair in (("insert-8b-nonblind", (13, 15)), ("insert-8b-blind", (14, 16)),
                       ("overwrite-8b-nonblind", (17, 19)), ("overwrite-8b-blind", (18, 20))):
        for n in pair:
            g["%s-T%d" % (name, n)] = tl(n, 85, 90)
    for tag, mb, _ in CB_E_READ:
        g["e-read" + tag] = tl(2, 45, 55, {"GOLD_CB_BUCKET_MB": mb}, tag)
    for n in (7, 8):
        for tag, mb, _ in CB_E_WRITE:
            g["e-overwrite-T%d%s" % (n, tag)] = tl(n, 25, 40, {"GOLD_CB_BUCKET_MB": mb}, tag)
    return g


def minutes(group):
    return sum(r.minutes for r in GROUPS[group])


def select(groups=None, rows=None):
    """Groups to run: all, a named subset, or those holding any of the given row names."""
    names = list(GROUPS) if not groups else list(groups)
    unknown = [g for g in names if g not in GROUPS]
    if unknown:
        raise SystemExit("unknown group(s): %s (have: %s)" % (", ".join(unknown), ", ".join(GROUPS)))
    if rows:
        want = set(rows)
        names = [g for g in names if want & {r.name for r in GROUPS[g]}]
    return sorted(names, key=minutes, reverse=True)
