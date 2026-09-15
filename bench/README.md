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

200M x 1KB incompressible keys (257G on disk), 64G quota, `--no-compression`,
libaio + DirectIO, 10GbE between client and server:

| | value |
|---|---|
| Client throughput | 691,149 ops/s, zero errors |
| Data returned | 693 MB/s of real 1KB values |
| Disk | ~1,083,000 reads/s, 4.84 GB/s |
| Read amplification | 1.57 disk reads per GET |
| Network | 5.5 Gbit/s - not the limit |

The disk sustains over 1M IOPS, but each GET costs 1.57 of them, so the
client sees 691K. This is disk bound, not network bound: index and bloom
lookups miss the 64G cache against a 257G dataset. Getting client throughput
to ~1M means getting amplification near 1.0 - a larger quota, or a dataset
whose index fits.

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
