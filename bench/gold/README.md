# Golden benchmark

A fixed suite of 64 rows that measures fluxkv throughput, latency, space and memory on two
value sizes. `gold.py` runs it across any number of machines and builds the HTML report.

| Rows | What |
|---|---|
| T1–T20 | Maximum throughput. 1 KB (T1–T10) and 8 B (T11–T20): read in memory, read from disk, then insert and overwrite × durable or not × blind or non-blind |
| L1–L20 | The same workloads paced open loop at 5, 25, 50 and 75% of the matching T row's maximum; latency is timed from each request's scheduled send |
| S1–S4 | Two hours of 1 KB overwrites: non-durable / durable × non-blind / blind |
| E rows | T2 / L2 with the block cache shrunk from 50% to 0.1% of the index, and T7 / T8 / L7 / L8 at 10% and 5% |

Each row starts from a fresh data dir (after `fstrim`), loads its dataset, runs its phases and
writes `res/<row>.json`. See `gold_run.sh` for the exact server flags and client shapes.

## Files

| File | Role |
|---|---|
| `gold.py` | Orchestrator: deploy, run, status, collect, report, plan |
| `suite.py` | The rows, their settings, and the groups that must share a box |
| `gold_run.sh` | Runs one row on a box |
| `gold_sweep.sh` | Per-box queue: runs its rows in order, resumable |
| `gold_summ.py` | Turns a row's samples and client output into its result JSON |
| `report/build_report.py`, `report/template.html` | The HTML report |
| `report/test_report.sh` | Rebuilds a report from saved results and compares it byte for byte |
| `report/build_report_couchbase.py` | The report for a Couchbase Server run |
| `report/compare_couchbase.py` | A fluxkv run against a Couchbase Server run, row by row |

## Machines

Each box runs the server and the client over loopback, so rows never share a network. A box
needs root SSH, `python3`, `perf`, `fstrim`, `flock` and a data filesystem for `/data/gold`
(`GOLD_DATA` overrides). `gold.py deploy` sets the CPU governor to `performance` and raises
`fs.aio-max-nr`, and reports missing tools.

SSH uses your keys. For password logins, export `SSHPASS` and `gold.py` goes through
`sshpass -e`. Never put a password in a file in this repo.

## Running it

Build `fluxkv_server` and `fluxbench` for the boxes, then:

```bash
cd bench/gold
./gold.py deploy --hosts 10.0.0.51,10.0.0.52,10.0.0.53,10.0.0.54 \
    --server ../../build/fluxkv_server --client ../../build/fluxbench --magma-sha <sha>
./gold.py plan --hosts 10.0.0.51,10.0.0.52,10.0.0.53,10.0.0.54   # expected wall clock
nohup ./gold.py run --hosts 10.0.0.51,10.0.0.52,10.0.0.53,10.0.0.54 --name 2026-10-01 \
    --out results/2026-10-01 > gold.log 2>&1 &
```

`run` hands each group of rows to whichever box is idle, longest group first, and polls until
every row has a result or has failed twice. On four boxes the full suite takes about 9 hours;
the four S rows (8 hours, one box) set the floor. When it finishes it collects the results into
`--out` and writes `--out/report.html`.

All run state lives on the boxes under `/root/fluxkv-gold/runs/<name>`, so a `run` that is
stopped or loses its terminal can simply be started again with the same arguments. A box that
reboots mid-row retries that row once.

Other commands:

```bash
./gold.py status  --hosts ... --name 2026-10-01              # per box: busy/idle, done, failed
./gold.py collect --hosts ... --name 2026-10-01 --out DIR    # pull results + run.json
./gold.py report  DIR -o report.html                         # rebuild the report
```

`--groups` or `--rows` limit a run to part of the suite. `--smoke` runs 1% of the keys with
120 s phases, to check a setup end to end in well under an hour. A partial run can be
collected, but the report needs all 64 rows.

## Couchbase Server

`--couchbase` (on `run`, `plan` and `report`) runs the same rows against each box's Couchbase Server
instead of fluxkv, with the suite from `suite.couchbase()`. Each box needs Couchbase Server
initialised with a data path at `/data/cbgold/data` and the data service only; export the cluster
login as `GOLD_CB_USER` and `GOLD_CB_PASS` before `run`, which passes it to the boxes with each row's
settings. Each row creates a fresh magma bucket `gold` (0 replicas) after
`fstrim`, and caps the `couchbase-server` service with a systemd `MemoryMax`, which holds memcached and
its page cache: a 16 GiB bucket in 32 GiB, or 48 GiB in 64 GiB for the in-memory rows.

| fluxkv row setting | Couchbase |
|---|---|
| durable | SyncWrite at persistToMajority (`fluxbench -durability`) |
| blind | bucket setting `magma_enable_upsert=true` |
| E rungs | the bucket quota shrinks after the load (`GOLD_CB_BUCKET_MB`), with the cgroup 4 GiB above it |

The 8 B write groups split into their T/L pairs because Couchbase loads 1B 8 B keys in about 70
minutes; the full suite takes about 16 hours on four boxes. SET rates count applied mutations, since
Couchbase rejects many SETs with TMPFAIL at full rate.

```bash
nohup ./gold.py run --hosts ... --name cb-2026-10-18 --couchbase --out results/cb-2026-10-18 > gold-cb.log 2>&1 &
report/compare_couchbase.py results/2026-10-17 results/cb-2026-10-18 -o compare.html --notes notes.html
```

## The report

`report/build_report.py RESULTS_DIR` reads one `<row>.json` per row plus `run.json`, which
`collect` writes: run dates, build ids, hardware, server config and the box each row ran on.
Every figure in the report comes from these files; edit `run.json` to correct a label.
`--template template_v3.html` builds the redesigned layout: summary and risks first, a key to
row names and terms, and findings linked to their evidence.

`report/test_report.sh RESULTS_DIR EXPECTED.html` rebuilds a saved report and checks that the
output is byte-identical, which guards the template against unintended changes.
`report/testdata/` holds one full run's results and its report:

```bash
report/test_report.sh report/testdata/rows report/testdata/expected.html
```
