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
