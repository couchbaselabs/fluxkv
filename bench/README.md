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

### Document cache: 1.11M GET/s at 3.7 cores when it hits

`--cache-size N` puts a write-through document cache (server/cache, S3-FIFO)
in front of magma. A hit is answered on the IO thread with no reader-thread
hop; a miss takes the normal path and fills the cache. Measured with
`cache_bench.sh` on the 25M x 1KB dataset, tuned configuration, 8 GiB cache,
client on a second machine at 64x256, batch 64:

| GET keyspace | cache | Throughput | CPU | Disk reads/s | RSS | Hit ratio |
|---|---|---|---|---|---|---|
| 4M of 25M | off | 1,114,992/s | 32.5 cores | 1,126,592 | 0.6 GB | - |
| 4M of 25M | 8 GiB | 1,116,313/s | **3.7 cores** | **5** | 4.8 GB | 100% |
| 25M | off | 1,115,211/s | 32.7 cores | 1,126,415 | 1.7 GB | - |
| 25M | 8 GiB | 1,116,422/s | 30.4 cores | 496,808 | 10.4 GB | 50.7% |

Fully cached, the same NIC-bound 1.11M GET/s costs **8.8x less CPU** - 3.3us
per op against 29.2 - and the disk is idle. 4.0M items occupy 4.49 GB
(~1.12 KB per 1 KB document), so the byte budget tracks RSS.

**Half cached, the CPU barely moves.** Disk reads halve but CPU drops 7%,
where a linear model predicts ~18 cores. The counters say why: the average
read batch collapsed from 31.8 to 4.55. Misses now arrive at half the rate,
spread across 40 readers x 256 vbuckets, so each reader wakes for a handful
of keys and the per-batch cost dominates - the same mechanism that made 64
readers cost 16 cores more than 40 (above). Each miss also fills: 75M
PutIfAbsent and 67M evictions over the run. Two things to try, in order:
fewer readers with the cache on, and filling only on a second miss (the
ghost queue already knows) so a near-uniform miss stream stops churning the
cache.

The 50.7% is worth a caveat: 8 GiB holds 30% of the keyspace, and a uniform
client would hit about that often. echobench2's key selection is evidently
not uniform, so a real skewed generator is needed for the second workload.

### Small values: 79M GET/s on loopback

With 8-byte values the protocol is the payload - an mcbp GET is a 32-byte
request and a 36-byte response - so this measures the request path, not the
store. Dataset is 25M x 8B, 8-byte keys, entirely in the document cache.
`small_value_bench.sh` runs one point; sweep it over io-threads and sessions.

Client on a second machine over 10 GbE:

| io-threads | sessions | batch | Throughput | CPU | us/op | NIC |
|---|---|---|---|---|---|---|
| 16 | 128 | 64 | 25,734,224 | 13.6 | 0.53 | tx 8.24 Gbit/s |
| 32 | 256 | 64 | **31,335,655** | 24.9 | 0.80 | tx 10.16 Gbit/s |
| 48 | 256 | 64 | 31,307,206 | 25.4 | 0.81 | tx 10.15 Gbit/s |

**The link, not the server.** Three configurations land within 0.1% of each
other with tx pinned at 10.15-10.16 Gbit/s. A 36-byte response caps mcbp at
about 32.6M ops/s on a 10 GbE link, and at 31.3M we are touching it. Note the
binding direction is transmit: the response is larger than the request.

Client on the server itself, loopback, same binary:

| io-threads | sessions | pipeline | Throughput | CPU | us/op | ops/core |
|---|---|---|---|---|---|---|
| 16 | 128 | 256 | 29,359,002 | 15.8 | **0.54** | 1.86M |
| 32 | 256 | 512 | 49,815,021 | 32.0 | 0.64 | 1.56M |
| 64 | 192 | 512 | **79,143,698** | 59.8 | 0.76 | 1.32M |

Loopback removes the NIC and the same server does 2.5x more. It also puts the
client on the same 80 cores, so read the server's own CPU, not box load.

**Per-op cost rises with thread count**, 0.54us at 16 IO threads to 0.76us at
64, and the cause is still open. The obvious suspect - readers sharing a
cache shard's SharedMutex line - was tested and ruled out: raising the shard
count 128x (256 -> 32768, about one shard per 760 items) moved efficiency by
-4%, the wrong way, presumably because more shard structs cost locality.

| shards | io=16 | io=64 |
|---|---|---|
| 256 | 27.4M, 1.71M ops/core | 70.8M, 1.18M ops/core |
| 4096 | 26.5M, 1.65M ops/core | 69.1M, 1.14M ops/core |
| 32768 | 24.6M, 1.54M ops/core | 68.5M, 1.13M ops/core |

What is left to check: the memcmp ending every F14 lookup (5% of CPU in an
earlier profile), and plain memory/LLC pressure from 70M random lookups a
second into a 25M-entry map - which no locking change would fix.

Pipeline depth matters more than batch: at 192 sessions, depth 256 gives
44.1M and depth 512 gives 78.6M. Depth 1024 adds nothing.

### Auto-tuning: the hand-tuned numbers without the sweep

Every result above came from a sweep over `--io-threads` and `--readers`.
`--auto-tune` replaces the sweep: the flags become starting points, and a
tuner thread (server/tuner.cc) resizes both pools at run time. Reader
threads are added and retired through the shared task queue; IO threads are
added and retired by moving live connections between event loops
(`Connection::migrateTo`), which waits for a connection's requests in
flight to be answered and then detaches and re-attaches the socket.

The tuner works by trial. Busy fraction picks the direction: a pool whose
threads are mostly busy is offered more; otherwise fewer are tried, and
fewer are also tried on a busy pool once growing it has failed. Every change
is a quarter of the pool, the tuner waits for it to take effect, and keeps it
only if throughput responded: up by more than 1% for a grow, down by less
than 0.5% for a shrink (the asymmetry stops it cycling at the boundary). A
failed step is undone and retried at half size; once the smallest step fails
that direction backs off until the load changes. Busy fraction alone cannot
size a pool blocked on disk with a backlog - 64 readers and 40 readers both
read 100% busy - and only the failed grow tells it to try fewer.

Measured with `autotune_bench.sh` from the floor of one IO thread and one
reader per shard, so nothing is inherited from the hand tuning:

| workload | converged to | time | throughput | server cores | hand-tuned |
|---|---|---|---|---|---|
| 8 B GET, loopback, 192x512x64 | io=64, readers=8 | 380 s | 76.7-79.1M/s | 60.2 | 79.1M at io=64, 59.8 cores |
| 1 KB GET, networked, 64x256x64 | io=3, readers=40 | 80 s | 1.1163-1.1168M/s | 31.6-33.0 | 1.114M at io=6/40, 32.8 cores |

The 8 B run reached 78 loops at 200 s (a grow judged while the previous size
was still ramping) and shrank back to 64 over the next three minutes; the
1 KB run is link-bound and the tuner's job there is only to find the fewest
threads that hold 1.11M. Both runs finished with zero client errors and no
connection dropped, with connections moved between loops hundreds of times
under 512-deep pipelines.

Things learned building it, in the order they cost time:

- **Thread churn breaks round-robin counter slots.** Pools that grow and
  shrink create hundreds of threads over a run; with slots handed out
  round-robin, two busy IO threads soon shared a counter cache line and the
  8 B case lost 10-15%. Slots are now leased to the least-occupied index
  and returned on thread exit (server/statslot.cc).
- **A saturated loop does not drain its cross-thread queue.** A plan posted
  with `runInEventBaseThread` to a loop with 192 always-ready sockets ran
  18 s later, with 2.6 ms iterations. Work for a loop now goes into a
  mailbox flagged so the read path schedules it as a loop callback within
  one iteration (`IOThread::Post`).
- **Balance is everything for pinned connections.** A loop with 7
  connections next to loops with 3 runs at 100% while they idle at 64%,
  and the pool's mean busy hides it. Rebalancing levels max-to-min until
  every loop is within one connection, and reruns whenever no move is in
  flight.
- **Judge a change only after it has taken effect**, and only against a
  stable baseline. Pools report `Settled()`; the tuner waits for it, then
  averages two windows (four for a step under 2%).
- **Two pools that are both bottlenecks** each show only a small gain when
  grown alone. Any rule demanding a gain proportional to the step deadlocks
  them at 1 IO thread and 8 readers; plain "more than 1%" does not.

### Writes: 4.5M async SET/s on loopback, CPU-bound

8-byte keys and values, Zipf 0.99 over 256M keys, 100% SET, async
acknowledgement, 8 shards x 32 vbuckets, 64 writers. Measured with
`write_diag.sh`; the accepted rate comes from the server's own
`write_batch_items` counter, because the client's rate counts TmpFail
replies once the write queue is full.

```
--mem-quota 68719476736 --writers 64 --flushers 32 --compactors 128 \
--shared-wal --shared-wal-flushers 1 --shared-wal-chunk-size 2097152 \
--shared-wal-chunks 24 --shared-wal-flush-us 50 \
--blind-writes --write-coalesce-us 2000 --auto-tune
```

| step | accepted SET/s | server cores | writer cores |
|---|---|---|---|
| per-shard write-ahead logs | 1.70M | - | - |
| one shared log | 3.12M | 48.7 | - |
| allocations routed to jemalloc | 3.30M | 51 | 38.7 |
| `--blind-writes` | 3.24M | 45 | 23.7 |
| shared log no longer fsync-waits per batch + write coalescing | 3.20M | 43 | 20.3 |
| 64 GB quota, client 128 conns x 128 deep | 4.03M | 67 | 33.6 |
| request recycling + inline bodies | 4.32M | 64 | 27.8 |
| 32 flushers / 128 compactors | **4.50M** | 71 | 32.8 |

At the last row the box has 5% idle (73.7 of 80 cores, client 1.7). Disk
is 0.88 GB/s of a 7.3 GB/s `fio` sequential-write ceiling.

At **1 KB values** the same configuration accepts **1.11M SET/s, 2.8 GB/s
written**, at 53 server cores with 21% idle. That point is bound by the
memtable flush path (flush queue backs up, the write-cache throttle
refuses writes), not by CPU or disk.

What each change was worth is in the commit messages of `e83b1d4` and
`aeccf07`. The two that matter most: the shared log waited for fsync
inside every write batch (98% of writer sleep time), and once that was
gone the server had no batching of its own, so `--write-coalesce-us` is
not optional with the shared log in async mode.

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
