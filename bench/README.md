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

`fluxbench` writes with `-mode set`. Size the value and key count to the
dataset you want:

```bash
DATA_DIR=/data/fluxkv-1k bench/run_server.sh
./build/fluxbench -host 127.0.0.1:12210 -mode set \
    -keys 500000000 -valsize 1024 -vbuckets 256 \
    -conns 32 -pipeline 64 -randvals -runtime 3600s
```

`-randvals` writes incompressible data. Use it whenever compression is part
of what you are measuring, otherwise the values compress to nothing and the
disk numbers mean nothing.

Watch for `STATUS TmpFail=...` in the output. That is magma applying
backpressure because the load is outrunning the disk, not an error. Lower the
pipeline depth if you want a clean load.

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

## Reading the numbers

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
