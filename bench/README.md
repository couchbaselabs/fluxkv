# Benchmarks

Scripts for running storage-engine experiments against fluxkv.

| Script | Purpose |
|---|---|
| `run_server.sh` | Start a server for a run and wait until it listens |
| `measure.sh` | Measure a running server at a peak and a moderate load point |
| `io_matrix.sh` | Sweep {libaio, io_uring} x {DirectIO, buffered} |
| `cgroup_run.sh` | Start the server under a systemd memory cap |
| `cgroup_stats.sh` | Show the memory breakdown of a capped server during a run |

All scripts take configuration from environment variables and have defaults
that match the read benchmarks.

## Loading a dataset

Use `load.sh`. It loads and then verifies the dataset is complete:

```bash
DATA_DIR=/data/fluxkv-1k EXTRA_ARGS="--no-compression --write-queue-mem 8589934592" \
  bench/run_server.sh
CLIENT_HOST=root@10.0.0.2 KEYS=200000000 bench/load.sh
```

`-randvals` (on by default in `load.sh`) writes incompressible data. Use it
whenever compression is part of what you are measuring, otherwise the values
compress away and the disk numbers mean nothing.

### A write the server cannot queue is thrown away

This is the trap worth knowing about, because nothing obvious reveals it.

The write queue defaults to **256MB**. A SET that does not fit is answered
with TMPFAIL and that key is never stored. Loading 200M keys at ~1GB/s had
**95.7M writes (48%) rejected**. On disk it looked healthy - hundreds of GB,
server fine, no errors logged. The shortfall only showed up much later as a
read miss rate that climbed with key order, because the queue filled as the
load progressed.

Two things keep a load honest:

- Raise `--write-queue-mem` (8GB works for a 200M x 1KB load).
- Keep concurrency moderate. Pushing harder does not load faster, it just
  gets writes rejected: `16x4` sustained ~470K/s with zero rejections, while
  `32x32` reached ~800K/s and shed 14% of the dataset.

After any load, check `tmp_fails` is zero and `cmd_set_resp` equals the key
count. `load.sh` does this and fails if the dataset is short.

Starting from an empty directory matters too. Reloading over an existing
dataset leaves compaction debt that competes with the incoming writes and
brings the rejections back.

## Running the IO matrix

```bash
DATA_DIR=/data/fluxkv-1k KEYS=500000000 bench/io_matrix.sh
```

## Running with a memory cap

```bash
DATA_DIR=/data/fluxkv-1k MEM_MAX=24G MEM_HIGH=20G \
  MEM_QUOTA=$((16*1024*1024*1024)) MAGMA_ASYNC_IO=uring MAGMA_DIRECT_IO=0 \
  bench/cgroup_run.sh

# during the load run
UNIT=fluxkv-bench bench/cgroup_stats.sh
```

## Measured results

`bench/read_benchmark.sh` reproduces this. 200M x 1KB incompressible keys
(233G on disk), 100G quota, `--no-compression`, DirectIO, client on a separate
node over 10GbE:

| | value |
|---|---|
| **Throughput** | **1,028,000 ops/s, zero errors** |
| **Network** | **9.62 Gbit/s - 96% of the 10GbE link** |
| Disk | 1,084,799 reads/s, 4.80 GB/s |
| Read amplification | 1.055 disk reads per GET |
| CPU | 98% busy |
| **Memory (RSS)** | **17.39 GB**, peak 17.43 GB, of a 100 GB quota |
| Latency | p50 31.8ms, p90 33.5ms, p99 35.0ms |

Network, CPU and disk are all at their limit together. Every GET costs about
one disk read, so this is genuinely served from disk rather than from cache.

### What each setting is worth

Only one setting actually moved the number:

| change | result |
|---|---|
| quota 64G -> 100G | 691K -> **1,005K** ops/s |
| 64 conns x 512 pipeline | 1,012K -> 1,028K (vs 256x128, same in-flight) |

Everything else lands within ~2% of 1.01M, because the system is saturated on
three resources at once and no single knob moves a saturated system. Measured
back to back, libaio at 64x512, identical warmup:

| config | throughput |
|---|---|
| baseline (io-threads 32) | 1,015,831/s |
| `--no-hot-stats` | 1,018,889/s |
| io-threads 16 | 1,009,747/s |
| io_uring | 1,028,000/s |

**A single sample is not a measurement.** Earlier runs recorded io-threads 16
at 476K and `--no-hot-stats` at 545K, and both were written up as real
regressions. Neither reproduced: re-measured under controlled conditions they
are 1,009,747 and 1,018,889. Something transient - most likely background
compaction left over from repeated dataset reloads - was interfering during
that window.

The `--no-hot-stats` claim should have been caught by reading the code rather
than trusting the number. The flag sets one boolean that makes `hotStatAdd` and
`hotStatSub` skip an atomic increment. The counters it skips are written and
never read except by the stats endpoint, so the flag can only remove work. A
result contradicting that was evidence of a bad measurement, not a surprising
finding.

### libaio vs io_uring

Both measured at 64x512 on the same dataset, back to back. `MAGMA_ASYNC_IO`
selects the backend; **libaio is the default**, io_uring is opt-in with
`MAGMA_ASYNC_IO=uring`.

| | libaio (default) | io_uring |
|---|---|---|
| Throughput | 1,015,831 ops/s | **1,028,000 ops/s** |
| p50 latency | 32.1ms | 31.8ms |
| p99 latency | 37.4ms | **35.0ms** |
| Network | 9.56 Gbit/s | 9.62 Gbit/s |
| Disk | 1,078,554 r/s | 1,084,799 r/s |
| CPU idle | 7.4% | 1.9% |
| RSS | 17.39 GB | 17.39 GB |

io_uring is 1.2% faster with a slightly tighter tail. Both clear a million
operations a second and saturate the link, so on this hardware the backend is
not what decides the result - the earlier measurements at 256x128 (999,791 vs
1,004,706) agree. Pick either.

**Report RSS.** The quota is a ceiling, not consumption: the server asked for
100G and used 17.4G. That 17.4G is the *index* - caching it is what dropped
amplification from 1.57 to 1.06 and took throughput from 691K to over 1M.
Raising the quota past ~20G changes nothing, because the index already fits.

### Tuned configuration: 1.11M GET/s at 33 cores

25M x 1KB keys (31G on disk), 8 shards, 40G quota, 1KB data blocks, client on
a separate node at 64 conns x 256 pipeline x 64 batch:

```
MAGMA_FLUSH_DELAY_US=100 fluxkv_server \
  --shards 8 --vbuckets 256 --mem-quota 42949672960 \
  --io-threads 6 --readers 40 --writers 8 --flushers 8 \
  --write-queue-mem 1073741824 --no-compression --index-compression-lz4 \
  --data-block-size 1024 --io-queue-depth 16
```

| | value |
|---|---|
| Throughput | **1,114,493 ops/s, zero errors** |
| CPU | **32.6 cores** (user 23.3, sys 9.3) |
| CPU per op | **29.3 us** |
| Disk | 1,126,101 reads/s |
| Network | 10.23 Gbit/s - saturated |
| RSS | 1.75 GB |
| Latency | p50 8.3ms, p99 44.6ms |

### What each setting is worth

Each row reverts exactly one setting from the configuration above, same
dataset and client. The delta is that setting's contribution.

| configuration | throughput | CPU | Δ CPU |
|---|---|---|---|
| tuned (all on) | 1,114,493/s | **32.6** | - |
| readers 64 (was 40) | 1,115,651/s | 49.0 | **+16.4** |
| io-threads 32 (was 6) | 1,114,533/s | 41.6 | **+9.0** |
| flush-delay 0 (was 100us) | 1,094,721/s | 36.0 | **+3.4** |
| dispatch-batch 1 (was 16) | 1,114,729/s | 34.1 | **+1.5** |
| shards 32 (was 8) | 1,049,695/s | 32.1 | -0.5, but -5.8% throughput |

Two thirds of the saving is simply running fewer threads. The flush delay is
the only change that improved throughput and CPU together, and it is almost
entirely system time (sys 12.3 -> 9.3) because it removes sendmsg calls.

8 shards is not a CPU win; it is a throughput win at the same CPU - 29.3 us
per op against 30.6.

### Why fewer reader threads cost less CPU

Not contention, and not idle threads burning cycles - that was the first guess
and it was wrong.

A reader takes ownership of a vbucket queue and sweeps whatever has
accumulated. Fewer readers means requests pile up longer before anyone gets to
them, so each sweep is bigger and the fixed per-sweep cost - FetchBuffer setup,
building the OperationsList, the GetDocs call and its coroutine fanout, the
dispatch flush, releaseVBQueue - is amortised over more requests.

| readers | throughput | CPU | avg batch | ops/core |
|---|---|---|---|---|
| 64 | 1,115,246/s | 49.1 | 11.5 | 22,714 |
| 40 | 1,115,013/s | 32.9 | 31.8 | 33,891 |
| 24 | 924,986/s | 22.2 | 51.5 | **41,666** |
| 16 | 671,625/s | 17.0 | 40.7 | 39,507 |

At 64 readers the work is identical (72.2M vs 71.8M items) but split across
2.8x as many batches - 6.27M against 2.26M. The extra 4M batches per second at
roughly 4 us each accounts for ~16 cores, which is what was measured.

40 is the knee: same throughput as 64 for two thirds of the CPU. Below it,
throughput falls sharply because a reader owns one vbucket at a time while it
waits on IO, so too few readers cannot keep the disk busy.

Note the rule is not "fewer readers, bigger batches". Batch size follows queue
depth, which is arrival rate over readers. At 16 readers throughput had already
collapsed, so batches got *smaller* again (40.7) - starvation dominates. Batches
grow as readers are removed only while throughput holds.

If the goal is efficiency rather than peak throughput, 24 readers is 23% better
per core and gives up 17% of throughput.

### Data block size: 4 KB vs 1 KB

`--data-block-size` sets `SeqTreeBlockSize`, the blocks holding document
values. It is a write-time property, so changing it needs a fresh load. With
1 KB values, the 4 KB default reads four times the bytes it returns.

Both loaded as 200M x 1KB, same everything else, measured at 64x512:

| | 4 KB | 1 KB |
|---|---|---|
| Throughput | 1,015,831/s | **1,099,550/s** (+8.2%) |
| Disk bandwidth | 4842 MB/s | **1754 MB/s** (-64%) |
| Bytes per disk read | 4.46 KB | 1.56 KB |
| Reads per GET | 1.055 | 1.02 |
| CPU | 61.0 cores | 67.4 cores |
| **CPU per op** | **60.0 us** | **61.3 us** |
| p50 latency | 32.1 ms | **15.7 ms** |
| p99 latency | **37.4 ms** | 105.8 ms |
| NIC | 9.56 Gbit/s | 10.20 Gbit/s |
| Dataset on disk | 233 G | 273 G (+17%) |

Cutting the block size cut disk bandwidth by nearly two thirds and halved
median latency, for 8% more throughput - and the throughput gain is probably
understated, because at 10.20 Gbit/s the link is full and the disk has
headroom it cannot use.

**CPU per op did not move: 60.0us to 61.3us.** Reading a quarter of the bytes
changed nothing, which says the cost is per-operation and not per-byte. CRC
and memcpy are not what is expensive here; syscalls and scheduling are. Block
sizing will not reduce the core count.

The cost is the tail: p99 went from 37ms to 106ms and p999 to 182ms. Median
improves, the spread gets much worse - consistent with running fully
NIC-saturated, where queueing dominates. If p99 matters, 1 KB blocks are a
regression at this load point.

**The ceiling is CPU, not the wire.** Running the client on the server itself,
removing the network entirely, was *slower* - 778K - because the client then
competes for CPU. Profiling shows 43.8% of CPU is syscall overhead (sendmsg
13%, io_uring_enter 13%, epoll 12%), which is why fewer, deeper connections
help.

## Reading the numbers

**Check the errors before believing the rate.** A GET for a key that was
never stored returns a 24-byte KeyNotFound with no value. It is cheap, so a
run against an incomplete dataset reports a *higher* rate than a correct one.
An earlier matrix here was measured at "906-920K/s" and was 100% misses.
`measure.sh` now refuses to print a rate when operations failed.

**Measure two load points.** Peak throughput alone hides real differences. In
the IO matrix every configuration peaked near 910K/s, because the ceiling is
CPU and network bound rather than IO bound. The moderate point separated the
same configurations by 1.6x.

**Warm up, and drop caches between configurations.** `run_server.sh` drops
caches on start; `measure.sh` discards two warm-up passes. Without both, a run
inherits the previous run's page cache.

**Under buffered IO, process RSS lies.** The memory sits in the kernel page
cache, which is charged to the cgroup but is not part of process RSS. A server
showing 200MB RSS can be using 20GB. Read `cgroup_stats.sh`, not `top`.

**A memory cap above the working set changes nothing.** Check
`high breaches` in `cgroup_stats.sh`. If it is zero, the cap never bound and
the run is not testing what you think it is.
