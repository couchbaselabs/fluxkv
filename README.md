# fluxkv

A minimal key-value server that speaks the memcached binary protocol (mcbp) and
stores data in [magma](https://github.com/couchbase/magma), Couchbase's LSM
storage engine.

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

## Status

Early. The repo is being assembled in small, reviewable commits. See
`docs/` for what has been verified to build and run.
