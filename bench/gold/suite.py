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
