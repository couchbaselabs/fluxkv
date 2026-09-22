# Overwrite benchmark

Sustained non-blind overwrites of a fixed live dataset, measuring throughput,
device bandwidth, space amplification and write amplification together.
This is the workload that exposed the magma compaction and key-index
behaviour fixed on `couchbaselabs/magma-research` branch `research`; the
server must be built against that branch for the tuned configuration.

## Host

One host, client and server on loopback: 80 cores, 125 GB RAM, three NVMe in
RAID0 (md0) on XFS. fio ceiling 7.3 GB/s sequential write; kernel readahead
on md0 is 6 MB. CPU governor `performance`.

## Method

- **Dataset:** 38.5 GiB live, 12-byte keys, so key count scales with value
  size (40.06M at 1 KB, 156.6M at 256 B, 574.2M at 64 B). Loaded fresh into
  an empty directory for every arm, after `fstrim`. Every arm is one server
  lifetime; nothing is reused between arms.
- **Overwrite:** uniform random keys over the loaded keyspace, closed-loop
  client `fluxbench -conns 112 -pipeline 128`, non-durable SETs. Never
  `--blind-writes`: a delta is written only when the previous version is
  looked up, and without it nothing is ever marked dead.
- **Windows:** 600 s. The first window of a freshly loaded tree is a
  transient (all ranges age in step); quote the second or later.
- **Space:** `du` of the data directory over true live bytes
  (keys x (value + 8)). magma's `ActiveDiskUsage` undercounts by ~20%
  (files pending delete, WAL) and `Fragmentation` counts only one delta level.
- **Write amp:** device bytes from `/proc/diskstats` over magma
  `BytesIncoming`. `NWriteBytes` is logical accounting; `FSPhysicalReadBytes`
  rounds every read to a 32 KB block. Neither is device truth.
- **Attribution:** `attr.py` diffs two stats snapshots and divides each
  writer's bytes by ingest, per level; `kvskew.py` finds a kvstore that has
  stopped reclaiming; `offcpu.sh` shows where a thread role is waiting.

## Configurations

`configs/baseline.env`, `configs/tuned.env` and `configs/tiered.env` hold the
full server flag set. Shared by both: 8 shards, 16 GiB quota, 1 GiB write queue, shared WAL
(24 x 8 MB chunks, 4 flushers), no value compression, lz4 index compression,
4 KB seq data blocks, `--lsd-frag-ratio 0.5` (Couchbase's production value).

| | baseline | tuned |
|---|---|---|
| kvstores | 64 | 128 |
| writers | 32, pinned | `--auto-tune --max-writers 256` (settles at 80-96) |
| idle-writer spin | 2000 iterations | off (`MAGMA_WRITER_SPIN_ITERS=0`) |
| key-index tables | never cached until read | `--key-warm-new-tables` |
| sstable write buffer | 64 KB (magma default) | 1 MB |
| key-index block | 32 KB | 4 KB |

## Results, 1 KB values

Second 600 s window of each arm unless noted. Each row adds one change to
the previous, so the effect of each is visible; the magma compaction fixes
from the same branch were already in place for all rows.

| step | ops/s | ingest MB/s | dev write MB/s | dev read MB/s | space amp | dev WA |
|---|---|---|---|---|---|---|
| baseline | 424K | 438 | 1,708 | 1,163 | 2.39 | 3.90 |
| writers = kvstores (64) | 599K | 618 | 2,378 | 1,610 | 2.43 | 3.85 |
| + warm new key-index tables | 816K | 842 | 3,275 | 2,147 | 2.52 | 3.89 |
| + 128 kvstores, 128 writers | 851K | 879 | 3,219 | 2,045 | 2.51 | 3.66 |
| + writer spin off | 865K | 893 | 3,298 | 2,091 | 2.52 | 3.70 |
| + 1 MB sstable write buffer | 903K | 932 | 3,320 | 2,221 | 2.46 | 3.56 |
| + 4 KB key-index blocks | 988K | 1,020 | 3,528 | 2,319 | 2.59 | 3.46 |
| + auto-tuned writers (tuned) | **1,071K** (first window) | 1,105 | 3,774 | 2,374 | 2.64 | 3.41 |
| + 3-level tiered-L0 seqIndex (tiered) | **1,093K** (first window) | 1,127 | 3,811 | 2,372 | **2.08** | 3.38 |

At the tuned point the array is ~88% busy (three devices, `/proc/diskstats`
io ticks) and the CPU ~90%. Per ingested byte the device sees about 6.5
bytes: WAL 1.0, memtable flush 1.0, compaction write 1.3, compaction read
2.7, key index 0.5. The compaction read/write ratio is set by the 0.5
fragmentation ratio (a range is rewritten at ~50% garbage) and is not a
defect.

### Tiered-L0 seqIndex (`configs/tiered.env`)

`--lsd-levels 3 --lsd-tiered-l0` selects magma's `LSDTieredL0`: L0 tiered
with no size floor, L1 sorted deltas sized at ratio x data, L2 data. GC picks
the data table with the most delta bytes per byte, pulls in the overlapping
L1 tables and the L0 tables over that range, and drops the surviving deltas.
Same binary, flags on and off, 30 min each:

| window | arm | ops/s | spaceAmp (du) | devWA | compWA |
|---|---|---|---|---|---|
| 600 | tiered | 1,093K | 2.08 | 3.38 | 1.00 |
| 600 | tuned | 1,056K | 2.55 | 3.44 | 1.07 |
| 1200 | tiered | 1,088K | 2.12 | 3.31 | 0.96 |
| 1200 | tuned | 1,037K | 2.66 | 3.46 | 1.12 |
| 1800 | tiered | 1,051K | 2.07 | 3.33 | 0.98 |
| 1800 | tuned | 996K | 2.50 | 3.47 | 1.14 |

Read check after the run: 71K get/s at 6.25 KB per get (tuned 53K, 6.66 KB).
Space amp lands at the 0.5 fragmentation budget because garbage no longer
has to cascade through three delta levels before it can be reclaimed.

Before the compaction fixes on `research`, the baseline configuration ran at
382K ops/s with device WA 4.55 at the same space amp; the fixes are
documented commit by commit on that branch.

### What did not help

Measured, each as a single change on the best configuration at the time,
and removed again from both trees: bloom accuracy 0.999 (misses were
first-touch on fresh blocks, not false positives); a larger block cache (it
was never full); caching compaction *input* blocks; user-space readahead for
compaction (identical device reads at 1 MiB, 256 KB, 64 KB, 32 KB and 0: the
buffered sequential fd already gets the kernel's readahead); a smaller delta
buffer above the delta level (less churn, more compaction, net loss);
compacting the cheapest delta range first (neutral under uniform keys);
client pipeline 512 (server-bound; TMPFAILs rose).

## Other value sizes (baseline configuration, before key-index tuning)

| value | keys | ops/s | space amp | dev WA | key-tree WA |
|---|---|---|---|---|---|
| 1024 B | 40.1M | 407K | 2.34 | 3.87 | 6.1 |
| 256 B | 156.6M | 458K | 3.31 | 5.98 | 11.7 |
| 64 B | 574.2M | 712K | 4.68 | 14.70 | 21.0 |

Below ~256 B the key index dominates: at 64 B it is as large as the data and
pays leveled-LSM write amp. `--key-level-multiplier 3` halved key-tree write
amp (11.9 -> 6.1) at 64 B on a 12 GiB live set; see the worklog for that
matrix. `value_sweep.sh` runs the sweep.

## Running

```bash
# build fluxkv against magma-research/research, then:
CONFIG=baseline DATA_DIR=/data/fluxkv-ow bench/overwrite/overwrite.sh
CONFIG=tuned    DATA_DIR=/data/fluxkv-ow bench/overwrite/overwrite.sh
CONFIG=tiered   DATA_DIR=/data/fluxkv-ow bench/overwrite/overwrite.sh
CONFIG=tuned    DATA_DIR=/data/fluxkv-ow RUN=5400 bench/overwrite/overwrite.sh   # 90 min sustain
CONFIG=tuned    DATA_DIR=/data/fluxkv-ow VS=256 LOAD=1500 bench/overwrite/overwrite.sh
```

`EXTRA="--flag value"` appends server flags for one-off arms; `DEVICES` names
the block devices behind `DATA_DIR` if they are not `nvme*n1`. Output goes to
`/tmp/overwrite-<tag>/` with the server log, load and overwrite client output,
per-window attribution and the tuner's final state.
