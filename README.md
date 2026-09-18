# fluxkv

A minimal key-value server that speaks the memcached binary protocol (mcbp) and
stores data in [magma](https://github.com/couchbase/magma), Couchbase's
document storage engine.

fluxkv exists to measure what a storage engine can do when almost nothing sits
between the network and the disk. It is deliberately small: no DCP, no
replication, no rebalance, no views. That makes it a clean baseline to compare
against full Couchbase `memcached` (kv_engine + ep-engine), and a fast harness
for storage-engine experiments (io_uring vs libaio, DirectIO vs buffered,
block sizes, compression, cache sizing).

## Repository layout

| Path | Contents |
|---|---|
| `server/` | The fluxkv server (mcbp protocol, connection handling, magma engine) |
| `client/` | Load generator / client used to drive benchmarks |
| `bench/` | Experiment scripts (server start configs, measurement harnesses) |
| `tests/` | Tests |
| `cmake/` | CMake find-modules for locating magma and its dependencies |
| `docs/` | Design notes and benchmark results |

## Build model

fluxkv does **not** build magma. magma is part of the Couchbase Server source
tree and pulls in Couchbase platform libraries, so it cannot be built in
isolation.

Instead: **build Couchbase Server separately, then point fluxkv at it.**
fluxkv compiles its own sources and links the prebuilt magma library.

```
  Couchbase Server tree                 fluxkv (this repo)
  ---------------------                 ------------------
  source/  magma/include/libmagma/  --> headers
           platform/, folly, spdlog  --> headers
  build/   libmagma_shared.so        --> linked
```

Configure with two paths:

```bash
cmake -B build \
  -DCOUCHBASE_SOURCE_DIR=/path/to/couchbase/source \
  -DCOUCHBASE_BUILD_DIR=/path/to/couchbase/build
cmake --build build -j
```

### Dependencies pulled from the Couchbase tree

The server sources include these directly, so the Couchbase build must provide
them:

- `magma` — `include/libmagma/{magma,operations,slice}.h`, `libmagma_shared.so`
- `platform` — `platform/base64.h`, `platform/cb_arena_malloc.h`
- `cbcrypto` — `cbcrypto/digest.h`
- `folly` — async socket, `EventBase`, `IOBuf`, `MPMCQueue`
- `spdlog`, `nlohmann/json`, `libevent`

## Quick start

Build the server and the client, then run the smoke test:

```bash
cmake -B build -G Ninja \
  -DCOUCHBASE_SOURCE_DIR=/path/to/couchbase/source \
  -DCOUCHBASE_BUILD_DIR=/path/to/couchbase/build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Build only the client, on a load-generator machine with no Couchbase tree:

```bash
cmake -B build -DFLUXKV_BUILD_SERVER=OFF && cmake --build build -j
```

Run a server and measure it:

```bash
DATA_DIR=/data/fluxkv-1k bench/run_server.sh
HOST=127.0.0.1:12210 KEYS=500000000 bench/measure.sh
```

See `bench/README.md` for loading datasets and running the experiments.

## Performance

Measured on one host: 80 cores, 125 GB RAM, three NVMe drives in RAID0 on XFS.
The array does 7.64 GB/s sequential write, a 4 KB synchronous direct write in
25 us, and a 4 KB direct read in about 15 us. Client and server share the box
and talk over loopback, so the client competes for the same cores.

**Set the CPU governor to `performance` before measuring anything.** Under
`schedutil` an idle box sits at 800 MHz and every latency figure is 2-4x
worse, while peak throughput barely moves - so the headline number looks fine
and only the latency work is wrong.

8-byte keys throughout, uniform random. "pipe" is requests in flight per
connection. All figures are zero-error unless a rejected column says otherwise.

### Durable writes

Acknowledged only after the write-ahead log reaches disk. 64 GiB memory quota,
24 GiB write queue, document cache off.

| value | load | throughput | p50 | p99 | p99.9 |
|---|---|---|---|---|---|
| 8 B | 1 conn | 12.9K/s | 75 us | 117 us | 148 us |
| 8 B | 8 conns | 94K/s | 80 us | 117 us | 170 us |
| 8 B | 32 conns | 326K/s | 90 us | 154 us | 301 us |
| 8 B | 32 conns, pipe 8 | 1.26M/s | 142 us | 464 us | 1.5 ms |
| 8 B | 112 conns, pipe 128 | 4.90M/s | 1.58 ms | 4.3 ms | 8.2 ms |
| 8 B | 112 conns, pipe 512 | 6.7M burst / 6.0M at 90 s / 5.8M sustained | 3.4-4.9 ms | 10-25 ms | ~70 ms |
| 1 KB | 1 conn | 11.2K/s | 77 us | 181 us | 304 us |
| 1 KB | 32 conns | 263K/s | 103 us | 195 us | 529 us |
| 1 KB | 32 conns, pipe 8 | 782K/s (0.80 GB/s) | 207 us | 451 us | 2.9 ms |
| 1 KB | 112 conns, pipe 128 | 1.65M/s (1.69 GB/s) | 3.4 ms | ~65 ms | ~250 ms |

A durable write at low rate costs 75 us, of which 25 us is the device and
about 15 us is two thread handoffs. Below ~260K/s a 1 KB document costs the
same as an 8-byte one: the cost is the round trip and the log commit, not the
payload.

**Run length matters.** A 45 s run of an LSM finishes before compaction debt
accumulates. Quote burst and sustained separately.

### Non-durable writes

Acknowledged on acceptance into the write queue. Open-loop, so the client
offers more than the engine absorbs and the excess is rejected.

| value | load | successful | rejected | p50 | p99 |
|---|---|---|---|---|---|
| 8 B | 1 conn | 27.3K/s | 0 | 23 us | 167 us |
| 8 B | 32 conns | 568K/s | 0 | 37 us | 444 us |
| 8 B | 32 conns, pipe 8 | 3.59M/s | 0 | 52 us | 224 us |
| 8 B | 112 conns, pipe 128 | 7.40M/s | 7.9M/s | 760 us | 2.6 ms |
| 8 B | 112 conns, pipe 512 | 7.75M/s | 7.7M/s | 3.1 ms | 10.5 ms |

Durability costs **1.3x at deep pipelining and 2.8x at the knee**: group
commit amortizes the log write across more records as batches grow.

### Reads

`--cache-size` defaults to 0, which disables the document cache. It is the
single largest lever on read performance.

| dataset | doc cache | load | throughput | p50 | p99 | device reads/GET |
|---|---|---|---|---|---|---|
| 8 B, 300M keys, 49 GB | off | 32 conns | 107K/s | 280 us | 616 us | 1.95 |
| 8 B, 300M keys, 49 GB | off | 112, pipe 128 | 683K/s | 10.1 ms | 20.7 ms | 1.98 |
| 8 B, 300M keys, 49 GB | 32 GiB | 32 conns | 573K/s | 29 us | 382 us | 0.30 |
| 8 B, 300M keys, 49 GB | 32 GiB | 112, pipe 128 | 3.42M/s | 969 us | 6.8 ms | 0.33 |
| 1 KB, 200M keys, 186 GB | off | 32 conns | 156K/s | 184 us | 441 us | 1.01 |
| 1 KB, 200M keys, 186 GB | 64 GiB | 32 conns | 294K/s | 44 us | 318 us | - |
| 1 KB, 200M keys, 186 GB | 64 GiB | 112, pipe 128 | 3.20M/s | 1.07 ms | 7.6 ms | - |
| 8 B, 10M keys, 1.6 GB | 16 GiB (100% hit) | 8 conns | 352K/s | 22 us | 28 us | 0 |
| 8 B, 10M keys, 1.6 GB | 16 GiB | 32 conns, pipe 8 | 5.91M/s | 37 us | 51 us | 0 |
| 8 B, 10M keys, 1.6 GB | 16 GiB | 112, pipe 512 | 26.4M/s | 1.55 ms | 3.8 ms | 0 |
| 8 B, 10M keys, 1.6 GB | 16 GiB | 80, pipe 1024, batch 256 | 80.4M/s | not measurable | - | 0 |

The cache helps medians far more than tails: a hit costs ~30 us while a miss
still pays the full disk path, so p99 stays in the miss population until the
hit rate approaches 99%.

**Open question.** At equal key counts with a 17 GB block cache and no
document cache, an 8-byte read costs 1.95 device reads and a 1 KB read costs
1.01, making 8 B 40% slower despite a fifth of the data. Key count, index size
(311 MB against a 17 GB cache), data block size and read size are all ruled
out. The candidate is the seqIndex-then-keyIndex fall-through in
`KVStore::Get`.

### Reading these numbers

- Most points are single runs against 8-12% window noise.
- LSM state dominates: the same config measured 3.99M and 6.35M depending on
  whether its arm ran first or eighth against one server. Use a fresh data
  directory per arm and alternate A/B order.
- `-pregen` is throughput-only, so the batched read figures have no latency at
  all. Never pair a saturation throughput with a latency from another run.

## Status

Verified on Linux (GCC 13.3, C++23) against a Couchbase build:

- `fluxkv_server` compiles and links with zero undefined symbols
- `fluxbench` builds with no Couchbase dependency
- the smoke test passes: writes and reads return what was stored
- `bench/run_server.sh` and `bench/measure.sh` run a real measurement

Not done yet: unit tests for the protocol layer, and a documented dataset
load recipe with results in `docs/`.
