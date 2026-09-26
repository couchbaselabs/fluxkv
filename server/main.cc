#include "engine.h"
#include "include/libmagma/magma.h"
#include "include/libmagma/storage_format_version.h"
#include "metadata.h"
#include "server.h"

// magma.h only forward-declares SharedWALHandle; creating one needs the
// definition, which lives in magma's own tree rather than its public headers.
#include "magma/shared_wal.h"

#include <platform/cb_arena_malloc.h>
#include <spdlog/spdlog.h>

#include <getopt.h>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <execinfo.h>
#include <fcntl.h>
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <string>
#include <atomic>
#include <thread>

using namespace magma;
using namespace magma::kvserver;

static Server* gServer = nullptr;
static std::atomic<int> gSignal{0};

// Only async-signal-safe work here. Stop() used to run from this handler:
// it joins threads and locks a mutex, and a second SIGTERM during the 5-15 s
// shutdown re-entered it on a random thread while the IO threads were being
// destroyed, which segfaulted or aborted on a double join.
static void signalHandler(int sig) {
    gSignal.store(sig, std::memory_order_relaxed);
    if (gServer) {
        gServer->RequestStop();
    }
}

static void crashHandler(int sig) {
    void* frames[32];
    int n = backtrace(frames, 32);
    int fd = open("/data/crash.log", O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd >= 0) {
        dprintf(fd, "\n=== CRASH: signal %d (%s) ===\n", sig, strsignal(sig));
        backtrace_symbols_fd(frames, n, fd);
        close(fd);
    }
    fprintf(stderr, "\n=== CRASH: signal %d (%s) ===\n", sig, strsignal(sig));
    backtrace_symbols_fd(frames, n, STDERR_FILENO);
    _exit(128 + sig);
}

struct Config {
    uint16_t port = 11210;
    std::string dataDir = "./data";
    std::string bucket = "default";
    std::string hostname = "127.0.0.1";
    uint16_t shards = 4;
    // Defaults are tuned for an 80-core box driving NVMe at iobench peak.
    // Scheduler thrash from over-provisioned threads erodes coroutine cycle
    // time and caps aqu-sz at the disk well below NVMe peak — see the
    // worklog 2026-04-26 entry. Lean defaults below + readers=64 + io=16
    // produce 935-949K cold reads (97%% of iobench 4 KB qd=128 = 977.9K)
    // on .51, vs the prior 870-922K with io=32 / writers=32.
    uint16_t ioThreads = 16;
    uint16_t writers = 8;
    uint16_t readers = 64;
    size_t memQuota = 1024ULL * 1024 * 1024; // 1GB
    // Flushers/compactors don't measurably affect cold-read throughput —
    // they sleep on futex when there's nothing to do, so the scheduler
    // skips them. Defaults sized for write/compaction workloads:
    //   flushers=4   — one per ~8 shards is plenty for memtable→L0 flushes
    //   compactors=16 — leveled compaction can run many in parallel
    uint16_t flushers = 4;
    uint16_t compactors = 16;
    uint16_t vbuckets = 16;
    uint16_t statsPort = 80;
    bool durable = false;
    size_t writeQueueMem = 256ULL * 1024 * 1024; // 256MB
    size_t echoGetSize = 0; // 0 = disabled; >0 = echo fixed-size value on GET
    size_t ioQueueDepth = 16; // magma per-batch coroutine read parallelism
    size_t maxReadBatch = 256; // per-reader-thread sweep cap
    uint32_t maxReadOwners = 1; // max concurrent readers sharing one vbucket
    bool cacheDecompressed = false; // store data blocks decompressed
    bool compressIndexCache = false; // cache index blocks compressed
    bool dataBlockAutoTune = false; // keep physical data block ~= target
    bool indexBlockAutoTune = false; // keep physical index block ~= target
    bool align512 = false; // 512-byte block-padding (NVMe logical sector)
    bool noHotStats = false; // disable per-op stat atomics
    bool noBlockCache = false; // disable EnableDataBlockCaching entirely
    bool noCompression = false; // SSTables write & read uncompressed
    bool indexCompressionLZ4 = false; // keep LZ4 on index blocks only
    bool noValuePtrRead = false; // disable magma's value-pointer fast path
    bool noValuePtrWrite = false; // store no value pointers in the key index
    bool learnedSeqLocator = false; // seqIndex blocks located by a learned model
    bool compactMeta = false; // write DocMeta in its compact form
    // Sets SeqTreeBlockSize ONLY -- the data blocks holding document values.
    // KeyTreeBlockSize stays at magma's 4096 default: shrinking it too adds
    // key-index reads (IO/GET 1.13 -> 1.35) and cost 7-11% throughput.
    size_t dataBlockSize = 0; // 0 = magma default (4096) -> SeqTreeBlockSize
    // Document cache in front of magma (server/cache). 0 = disabled.
    size_t cacheSize = 0;
    size_t cacheShards = 256;
    std::string cachePolicy = "s3fifo";
    // Size IO threads and readers from load at run time. --io-threads and
    // --readers become the starting points; these are the ceilings.
    bool autoTune = false;
    size_t maxIoThreads = 0; // 0 = hardware threads
    size_t maxReaders = 0; // 0 = 4 x hardware threads
    size_t maxWriters = 0; // 0 = 2 x hardware threads
    int sharedWalPrewrite = -1; // -1 = magma default (on)
    // 256 MB: big enough that rotation is rare, small enough that the
    // recycle pool fills and a pre-write pass is cheap.
    size_t sharedWalSegmentSize = 256ull << 20;
    // One write-ahead log shared by every shard, instead of one per shard.
    // Shards stop serialising against each other on their own log mutex and
    // one durability flush covers writes from all of them.
    // Per-shard write cache in bytes; 0 derives it from the quota.
    size_t writeCache = 0;
    bool sharedWal = false;
    std::string sharedWalPath; // defaults to <data-dir>/shared-wal
    size_t sharedWalFlushers = 2;
    size_t sharedWalChunks = 8;
    size_t sharedWalChunkSize = 8u << 20;
    size_t sharedWalFlushUs = 0; // shwal MinFlushIntervalUs
    // Wait for the shared log to be durable inside every WriteDocs.
    // -1 follows --durable: an async server acknowledges before persistence,
    // so making each writer sleep through a group-commit flush buys nothing.
    int sharedWalSyncCommit = -1;
    // Skip magma's lookup of the previous version on every set. magma then
    // cannot report insert-vs-update per document (nothing here consumes
    // that) and leaves the old seqIndex entry to compaction GC.
    bool blindWrites = false;
    // Write coalescing, see gWriteCoalesceNs in engine.h.
    size_t writeCoalesceUs = 0;
    size_t minWriteBatch = 64;
    std::string batchSort = "auto"; // auto | always | never
    double sortDupThreshold = 0.05;
    // LSM shape. 0 = magma's default. Compaction CPU is set by how often
    // sstables are rewritten, so table size, base level size and the L0
    // table count are the knobs that matter for a write-only workload.
    size_t lsmSSTableSize = 0;
    size_t lsmBaseLevelSize = 0;
    // Fraction of the seqIndex data level that delta levels may reach before
    // they are merged down. magma's own default is 0.25; Couchbase ships 0.5
    // (magma_fragmentation_percentage=50). Design write amp is 1/ratio, so
    // this is the single largest lever on write amplification.
    double lsdFragRatio = 0.5;

    // Level size multiplier for the key index only. On small documents the
    // key index dominates write amplification; a leveled tree costs about
    // T*ln(R)/ln(T), so a smaller T trades levels for writes. 0 = magma's.
    int keyLevelMultiplier = 0;

    // Warm each new key-index table into the block cache on creation, so
    // lookup-on-set does not pay a device read on freshly written blocks.
    bool keyWarmNewTables = false;

    // sstable writer buffer (magma default 64 KB). Device writes averaged
    // 29 KB at 70K IOPS with the array 94% busy.
    size_t sstableWriteBuffer = 0;

    // Key-index data block size (magma default 32 KB). Every lookup-on-set
    // decompresses one block to find a 12-byte key; at ~1M writes/s LZ4 and
    // the in-block scan were ~8% of server CPU.
    size_t keyBlockSize = 0;

    // seqIndex LSD level count (magma default 5) and tiered level-0. The
    // tiered mode wants 3 levels: L0 tiered, L1 deltas, L2 data.
    int lsdLevels = 0;
    bool lsdTieredL0 = false;
    double memLwmRatio = 0;
    double writeCacheRatio = 0; // 0 keeps magma's default
    bool tuneWritersOnly = false;
    std::string tunePools; // empty = all
    std::string blockCachePolicy;
    size_t readAhead = 0;
    bool noIndexCompression = false;
    size_t lsmLevel0Tables = 0;
    size_t lsmMinCompactSize = 0;
    int lsmLevelMultiplier = 0;
    // Run magma without a write-ahead log. Writes then live only in the
    // memtable until it is flushed, so a crash loses everything since the
    // last flush. Measurement only: it prices the log, it is not a mode to
    // serve from.
    bool noWal = false;
    // Acknowledge a durable write from a completion thread when the shared
    // log reports it persisted, instead of blocking the writer inside
    // WriteDocs. Needs --durable and --shared-wal.
    bool asyncDurable = false;
};

static void printUsage(const char* prog) {
    std::cerr << "Usage: " << prog << " [options]\n"
              << "  --port N          TCP listen port (default: 11210)\n"
              << "  --data-dir PATH   Data directory (default: ./data)\n"
              << "  --bucket NAME     Bucket name (default: default)\n"
              << "  --hostname HOST   Hostname for cluster config (default: "
                 "127.0.0.1)\n"
              << "  --shards N        Number of magma instances (default: 4)\n"
              << "  --io-threads N    IO EventBase threads (default: 16; "
                 "lean to avoid scheduler thrash on the read hot path)\n"
              << "  --writers N       Writer pool threads (default: 8)\n"
              << "  --readers N       Reader pool threads (default: 64; "
                 "matches measured aqu-sz peak — adding more regresses)\n"
              << "  --mem-quota N     Total memory quota in bytes (default: "
                 "1GB)\n"
              << "  --flushers N      Magma flusher threads (default: 4)\n"
              << "  --compactors N    Magma compactor threads (default: 16)\n"
              << "  --vbuckets N      Number of vbuckets (default: 16)\n"
              << "  --stats-port N    HTTP stats port (default: 80)\n"
              << "  --durable         Wait for WriteDocs before responding\n"
              << "  --write-queue-mem N  Write queue memory limit bytes "
                 "(default: 256MB)\n"
              << "  --echo-get N      Return preallocated N-byte value on "
                 "GET (bypass magma, measure network throughput)\n"
              << "  --io-queue-depth N   magma GetDocs coroutine fanout per "
                 "batch (default 16; raises NVMe queue depth)\n"
              << "  --max-read-batch N   reader-thread sweep cap (default 256)\n"
              << "  --max-read-owners N  concurrent readers allowed on one "
                 "vbucket's backlog (default 1)\n"
              << "  --cache-decompressed-data  store data blocks decompressed "
                 "in block cache (skips LZ4 on cache hits, costs ~6× memory)\n"
              << "  --align-512           pad SSTable blocks to 512 bytes "
                 "(NVMe logical sector) instead of 4 KB\n"
              << "  --no-hot-stats        skip per-op stat increments "
                 "(cmd_get/queued_gets/etc.) for max throughput\n"
              << "  --index-compression-lz4  LZ4 on INDEX blocks only "
                 "(data/compacted follow --no-compression)\n"
              << "  --no-value-ptr-read   disable magma's value-pointer "
                 "fast path (forces a full seqIndex lookup per GET)\n"
              << "  --no-value-ptr-write  store no value pointers in the key "
                 "index (smaller leaves; pair with --learned-seq-locator)\n"
              << "  --learned-seq-locator locate seqIndex data blocks with a "
                 "learned model instead of the index blocks\n"
              << "  --compact-meta        write document metadata in the "
                 "compact form (needs magma storage format 2)\n"
              << "  --cache-size N        document cache budget in bytes "
                 "(default 0 = off); write-through, read-fill\n"
              << "  --cache-shards N      lock shards in the cache (default "
                 "256)\n"
              << "  --cache-policy NAME   eviction policy: s3fifo (default)\n"
              << "  --auto-tune           size IO threads and readers from "
                 "load; --io-threads/--readers are the starting points\n"
              << "  --max-io-threads N    ceiling for --auto-tune (default: "
                 "hardware threads)\n"
              << "  --max-readers N       ceiling for --auto-tune (default: "
                 "4 x hardware threads)\n"
              << "  --tune-writers-only   with --auto-tune, hold IO threads and "
                 "readers at their configured sizes\n"
              << "  --max-writers N       ceiling for --auto-tune (default: "
                 "2 x hardware threads)\n"
              << "  --write-cache N       per-shard write cache in bytes "
                 "(default: half the per-shard quota). This is the threshold "
                 "magma throttles writers against\n"
              << "  --shared-wal          one write-ahead log shared by all "
                 "shards instead of one per shard\n"
              << "  --shared-wal-path P   where the shared log lives "
                 "(default: <data-dir>/shared-wal)\n"
              << "  --shared-wal-flushers N   flusher threads for the shared "
                 "log (default 2)\n"
              << "  --shared-wal-chunks N     in-flight chunks (default 8)\n"
              << "  --shared-wal-chunk-size N bytes per chunk (default 8MB)\n"
              << "  --shared-wal-flush-us N   idle backoff for the log's "
                 "flusher threads; they flush on arrival when busy (default "
                 "0, which floors at the log's own 10 us)\n"
              << "  --shared-wal-segment-size N  log segment bytes (default "
                 "256MB). Smaller segments recycle sooner and make "
                 "--shared-wal-prewrite cheap\n"
              << "  --shared-wal-prewrite 0|1  write zeros through a new log "
                 "segment before use, so the first write into it does not pay "
                 "for extent conversion (default 1)\n"
              << "  --shared-wal-sync-commit 0|1  wait for the shared log to "
                 "be durable in every write batch (default: 1 with --durable, "
                 "else 0)\n"
              << "  --blind-writes        do not look up the previous version "
                 "of a document on set\n"
              << "  --write-coalesce-us N wait up to N us before writing a "
                 "vbucket again when fewer than --min-write-batch items are "
                 "queued (default 0, disabled: it costs latency at low rate "
                 "and measures as noise at high rate)\n"
              << "  --min-write-batch N   (default 64)\n"
              << "  --batch-sort MODE     sort write batches by key: auto "
                 "(when a sample says the batch repeats keys), always, never "
                 "(default auto)\n"
              << "  --sort-dup-threshold F  duplicate fraction at which auto "
                 "sorts (default 0.05)\n"
              << "  --lsm-sstable-size N     max sstable bytes (magma "
                 "default 2MB)\n"
              << "  --lsd-levels N       seqIndex LSD levels (magma default 5)\n"
              << "  --lsd-tiered-l0      tiered level-0 seqIndex GC (needs "
                 "--lsd-levels 3 and the magma research branch)\n"
              << "  --tune-pools LIST    comma list of io,readers,writers that "
                 "--auto-tune may resize; the rest stay at their configured "
                 "sizes (default all)\n"
              << "  --block-cache-policy P  block cache eviction: LRU (magma "
                 "default), DFR (delayed FIFO reinsertion) or C2QP (Clock2Q+)\n"
              << "  --read-ahead N       compaction read-ahead buffer bytes; with "
                 "direct IO the reads bypass the page cache (magma default 0: "
                 "buffered fd, kernel readahead)\n"
              << "  --no-index-compression  override --index-compression-lz4\n"
              << "  --write-cache-ratio R  share of the block+write cache budget given "
                 "to the write cache (magma default 0.2); the block cache gets the "
                 "rest, so with --write-cache fixed this shrinks only the block cache\n"
              << "  --mem-lwm-ratio R    share of --mem-quota for the block cache "
                 "and write cache; the rest is the bloom filter quota (magma "
                 "default 0.2)\n"
              << "  --key-block-size N   key-index data block size in bytes "
                 "(magma default 32768)\n"
              << "  --sstable-write-buffer N  sstable writer buffer in bytes "
                 "(magma default 65536)\n"
              << "  --key-warm-new-tables  read each new key-index table "
                 "into the block cache on creation\n"
              << "  --key-level-multiplier N  key index level size "
                 "multiplier (default 0, meaning magma's 10). Smaller trades "
                 "levels for write amplification\n"
              << "  --lsd-frag-ratio F   seqIndex delta level size as a "
                 "fraction of the data level (default 0.5, matching "
                 "Couchbase; magma's own default is 0.25). Design write amp "
                 "is 1/F\n"
              << "  --lsm-base-level-size N  max bytes in the base level "
                 "(default 4MB)\n"
              << "  --lsm-level0-tables N    L0 tables before compacting "
                 "(default 16)\n"
              << "  --lsm-min-compact-size N bytes a compaction tries to "
                 "cover (default 4MB)\n"
              << "  --lsm-level-multiplier N size ratio between levels "
                 "(default 10)\n"
              << "  --async-durable       in --durable mode, acknowledge from "
                 "a completion thread when the shared log reaches the write, "
                 "instead of blocking a writer thread on it\n"
              << "  --no-wal              run magma with no write-ahead log. "
                 "UNSAFE: a crash loses every write since the last memtable "
                 "flush. For measuring the log's cost only\n"
              << "  --help            Show this help\n";
}

static Config parseArgs(int argc, char* argv[]) {
    Config cfg;

    static struct option longOpts[] = {
            {"port", required_argument, nullptr, 'p'},
            {"data-dir", required_argument, nullptr, 'd'},
            {"bucket", required_argument, nullptr, 'b'},
            {"hostname", required_argument, nullptr, 'H'},
            {"shards", required_argument, nullptr, 's'},
            {"io-threads", required_argument, nullptr, 'i'},
            {"writers", required_argument, nullptr, 'w'},
            {"readers", required_argument, nullptr, 'r'},
            {"mem-quota", required_argument, nullptr, 'm'},
            {"flushers", required_argument, nullptr, 'f'},
            {"compactors", required_argument, nullptr, 'c'},
            {"vbuckets", required_argument, nullptr, 'v'},
            {"stats-port", required_argument, nullptr, 'S'},
            {"durable", no_argument, nullptr, 'D'},
            {"write-queue-mem", required_argument, nullptr, 'Q'},
            {"echo-get", required_argument, nullptr, 'E'},
            {"io-queue-depth", required_argument, nullptr, 1001},
            {"max-read-batch", required_argument, nullptr, 1002},
            {"max-read-owners", required_argument, nullptr, 1085},
            {"cache-decompressed-data", no_argument, nullptr, 1003},
            {"compress-index-cache", no_argument, nullptr, 1009},
            {"enable-data-block-autotuning", no_argument, nullptr, 1010},
            {"enable-index-block-autotuning", no_argument, nullptr, 1011},
            {"align-512", no_argument, nullptr, 1004},
            {"no-hot-stats", no_argument, nullptr, 1005},
            {"no-block-cache", no_argument, nullptr, 1006},
            {"no-compression", no_argument, nullptr, 1007},
            {"index-compression-lz4", no_argument, nullptr, 1012},
            {"no-value-ptr-read", no_argument, nullptr, 1013},
            {"learned-seq-locator", no_argument, nullptr, 1100},
            {"compact-meta", no_argument, nullptr, 1101},
            {"no-value-ptr-write", no_argument, nullptr, 1102},
            {"data-block-size", required_argument, nullptr, 1008},
            {"dispatch-batch", required_argument, nullptr, 1018},
            {"cache-size", required_argument, nullptr, 1020},
            {"cache-shards", required_argument, nullptr, 1021},
            {"cache-policy", required_argument, nullptr, 1022},
            {"auto-tune", no_argument, nullptr, 1030},
            {"max-io-threads", required_argument, nullptr, 1031},
            {"max-readers", required_argument, nullptr, 1032},
            {"max-writers", required_argument, nullptr, 1060},
            {"durable-spin", required_argument, nullptr, 1061},
            {"trace-latency", no_argument, nullptr, 1062},
            {"shared-wal-prewrite", required_argument, nullptr, 1063},
            {"shared-wal-segment-size", required_argument, nullptr, 1064},
            {"completion-slice", required_argument, nullptr, 1065},
            {"write-cache", required_argument, nullptr, 1045},
            {"shared-wal", no_argument, nullptr, 1040},
            {"shared-wal-path", required_argument, nullptr, 1041},
            {"shared-wal-flushers", required_argument, nullptr, 1042},
            {"shared-wal-chunks", required_argument, nullptr, 1043},
            {"shared-wal-chunk-size", required_argument, nullptr, 1044},
            {"shared-wal-sync-commit", required_argument, nullptr, 1046},
            {"shared-wal-flush-us", required_argument, nullptr, 1050},
            {"blind-writes", no_argument, nullptr, 1047},
            {"write-coalesce-us", required_argument, nullptr, 1048},
            {"min-write-batch", required_argument, nullptr, 1049},
            {"batch-sort", required_argument, nullptr, 1051},
            {"sort-dup-threshold", required_argument, nullptr, 1052},
            {"lsm-sstable-size", required_argument, nullptr, 1053},
            {"lsm-base-level-size", required_argument, nullptr, 1054},
            {"lsd-frag-ratio", required_argument, nullptr, 1066},
            {"key-level-multiplier", required_argument, nullptr, 1067},
            {"key-warm-new-tables", no_argument, nullptr, 1072},
            {"sstable-write-buffer", required_argument, nullptr, 1074},
            {"key-block-size", required_argument, nullptr, 1075},
            {"lsd-levels", required_argument, nullptr, 1077},
            {"lsd-tiered-l0", no_argument, nullptr, 1078},
            {"mem-lwm-ratio", required_argument, nullptr, 1079},
            {"tune-writers-only", no_argument, nullptr, 1080},
            {"read-ahead", required_argument, nullptr, 1081},
            {"tune-pools", required_argument, nullptr, 1083},
            {"block-cache-policy", required_argument, nullptr, 1084},
            {"write-cache-ratio", required_argument, nullptr, 1099},
            {"no-index-compression", no_argument, nullptr, 1082},
            {"lsm-level0-tables", required_argument, nullptr, 1055},
            {"lsm-min-compact-size", required_argument, nullptr, 1056},
            {"lsm-level-multiplier", required_argument, nullptr, 1057},
            {"no-wal", no_argument, nullptr, 1058},
            {"async-durable", no_argument, nullptr, 1059},
            {"help", no_argument, nullptr, 'h'},
            {nullptr, 0, nullptr, 0}};

    int opt;
    while ((opt = getopt_long(argc,
                              argv,
                              "p:d:b:H:s:i:w:r:m:f:c:v:S:DQ:E:h",
                              longOpts,
                              nullptr)) != -1) {
        switch (opt) {
        case 'p':
            cfg.port = static_cast<uint16_t>(atoi(optarg));
            break;
        case 'd':
            cfg.dataDir = optarg;
            break;
        case 'b':
            cfg.bucket = optarg;
            break;
        case 'H':
            cfg.hostname = optarg;
            break;
        case 's':
            cfg.shards = static_cast<uint16_t>(atoi(optarg));
            break;
        case 'i':
            cfg.ioThreads = static_cast<uint16_t>(atoi(optarg));
            break;
        case 'w':
            cfg.writers = static_cast<uint16_t>(atoi(optarg));
            break;
        case 'r':
            cfg.readers = static_cast<uint16_t>(atoi(optarg));
            break;
        case 'm':
            cfg.memQuota = strtoull(optarg, nullptr, 10);
            break;
        case 'f':
            cfg.flushers = static_cast<uint16_t>(atoi(optarg));
            break;
        case 'c':
            cfg.compactors = static_cast<uint16_t>(atoi(optarg));
            break;
        case 'v':
            cfg.vbuckets = static_cast<uint16_t>(atoi(optarg));
            break;
        case 'S':
            cfg.statsPort = static_cast<uint16_t>(atoi(optarg));
            break;
        case 'D':
            cfg.durable = true;
            break;
        case 'Q':
            cfg.writeQueueMem = strtoull(optarg, nullptr, 10);
            break;
        case 'E':
            cfg.echoGetSize = strtoull(optarg, nullptr, 10);
            break;
        case 1001:
            cfg.ioQueueDepth = strtoull(optarg, nullptr, 10);
            break;
        case 1002:
            cfg.maxReadBatch = strtoull(optarg, nullptr, 10);
            break;
        case 1085:
            cfg.maxReadOwners =
                    static_cast<uint32_t>(strtoul(optarg, nullptr, 10));
            break;
        case 1003:
            cfg.cacheDecompressed = true;
            break;
        case 1009:
            cfg.compressIndexCache = true;
            break;
        case 1010:
            cfg.dataBlockAutoTune = true;
            break;
        case 1011:
            cfg.indexBlockAutoTune = true;
            break;
        case 1004:
            cfg.align512 = true;
            break;
        case 1005:
            cfg.noHotStats = true;
            break;
        case 1006:
            cfg.noBlockCache = true;
            break;
        case 1007:
            cfg.noCompression = true;
            break;
        case 1012:
            cfg.indexCompressionLZ4 = true;
            break;
        case 1013:
            cfg.noValuePtrRead = true;
            break;
        case 1100:
            cfg.learnedSeqLocator = true;
            break;
        case 1101:
            cfg.compactMeta = true;
            break;
        case 1102:
            cfg.noValuePtrWrite = true;
            break;
        case 1008:
            cfg.dataBlockSize = strtoull(optarg, nullptr, 10);
            break;
        case 1018:
            magma::kvserver::gDispatchBatch = strtoull(optarg, nullptr, 10);
            break;
        case 1020:
            cfg.cacheSize = strtoull(optarg, nullptr, 10);
            break;
        case 1021:
            cfg.cacheShards = strtoull(optarg, nullptr, 10);
            break;
        case 1022:
            cfg.cachePolicy = optarg;
            break;
        case 1030:
            cfg.autoTune = true;
            break;
        case 1031:
            cfg.maxIoThreads = strtoull(optarg, nullptr, 10);
            break;
        case 1032:
            cfg.maxReaders = strtoull(optarg, nullptr, 10);
            break;
        case 1060:
            cfg.maxWriters = strtoull(optarg, nullptr, 10);
            break;
        case 1061:
            kvserver::gDurableSpinIters = atoi(optarg);
            break;
        case 1062:
            kvserver::gTraceLatency = true;
            break;
        case 1063:
            cfg.sharedWalPrewrite = atoi(optarg);
            break;
        case 1064:
            cfg.sharedWalSegmentSize = strtoull(optarg, nullptr, 10);
            break;
        case 1065:
            kvserver::gCompletionSlice = strtoull(optarg, nullptr, 10);
            break;
        case 1045:
            cfg.writeCache = strtoull(optarg, nullptr, 10);
            break;
        case 1040:
            cfg.sharedWal = true;
            break;
        case 1041:
            cfg.sharedWalPath = optarg;
            break;
        case 1042:
            cfg.sharedWalFlushers = strtoull(optarg, nullptr, 10);
            break;
        case 1043:
            cfg.sharedWalChunks = strtoull(optarg, nullptr, 10);
            break;
        case 1044:
            cfg.sharedWalChunkSize = strtoull(optarg, nullptr, 10);
            break;
        case 1050:
            cfg.sharedWalFlushUs = strtoull(optarg, nullptr, 10);
            break;
        case 1046:
            cfg.sharedWalSyncCommit = atoi(optarg) != 0 ? 1 : 0;
            break;
        case 1047:
            cfg.blindWrites = true;
            break;
        case 1048:
            cfg.writeCoalesceUs = strtoull(optarg, nullptr, 10);
            break;
        case 1049:
            cfg.minWriteBatch = strtoull(optarg, nullptr, 10);
            break;
        case 1051:
            cfg.batchSort = optarg;
            break;
        case 1052:
            cfg.sortDupThreshold = strtod(optarg, nullptr);
            break;
        case 1053:
            cfg.lsmSSTableSize = strtoull(optarg, nullptr, 10);
            break;
        case 1054:
            cfg.lsmBaseLevelSize = strtoull(optarg, nullptr, 10);
            break;
        case 1066:
            cfg.lsdFragRatio = strtod(optarg, nullptr);
            break;
        case 1067:
            cfg.keyLevelMultiplier = atoi(optarg);
            break;
        case 1072:
            cfg.keyWarmNewTables = true;
            break;
        case 1074:
            cfg.sstableWriteBuffer = strtoull(optarg, nullptr, 10);
            break;
        case 1075:
            cfg.keyBlockSize = strtoull(optarg, nullptr, 10);
            break;
        case 1077:
            cfg.lsdLevels = atoi(optarg);
            break;
        case 1078:
            cfg.lsdTieredL0 = true;
            break;
        case 1099:
            cfg.writeCacheRatio = std::stod(optarg);
            break;
        case 1079:
            cfg.memLwmRatio = std::stod(optarg);
            break;
        case 1080:
            cfg.tuneWritersOnly = true;
            break;
        case 1081:
            cfg.readAhead = strtoull(optarg, nullptr, 10);
            break;
        case 1082:
            cfg.noIndexCompression = true;
            break;
        case 1083:
            cfg.tunePools = optarg;
            break;
        case 1084:
            cfg.blockCachePolicy = optarg;
            break;
        case 1055:
            cfg.lsmLevel0Tables = strtoull(optarg, nullptr, 10);
            break;
        case 1056:
            cfg.lsmMinCompactSize = strtoull(optarg, nullptr, 10);
            break;
        case 1057:
            cfg.lsmLevelMultiplier = atoi(optarg);
            break;
        case 1058:
            cfg.noWal = true;
            break;
        case 1059:
            cfg.asyncDurable = true;
            break;
        case 'h':
        default:
            printUsage(argv[0]);
            exit(opt == 'h' ? 0 : 1);
        }
    }

    return cfg;
}

int main(int argc, char* argv[]) {
    Config cfg = parseArgs(argc, argv);

    spdlog::info("magma-kvserver starting");
    spdlog::info(
            "  port={} shards={} vbuckets={} io-threads={} writers={} "
            "readers={}",
            cfg.port,
            cfg.shards,
            cfg.vbuckets,
            cfg.ioThreads,
            cfg.writers,
            cfg.readers);
    spdlog::info("  data-dir={} bucket={} mem-quota={}MB",
                 cfg.dataDir,
                 cfg.bucket,
                 cfg.memQuota / (1024 * 1024));

    // Configure magma
    Magma::Config magmaCfg;
    magmaCfg.MaxKVStores = cfg.vbuckets;
    // Mimic ep-engine so --mem-quota means the same thing on both servers.
    // memcached computes the per-instance quota as
    //     (bucketQuota / maxShards) * magma_mem_quota_ratio
    // i.e. the value handed to magma is already the POST-ratio magma quota,
    // simply divided by the shard count. --mem-quota here IS that post-ratio
    // magma quota.
    //
    // This previously also divided by MemoryQuotaLowWaterMarkRatio (0.2),
    // which silently handed magma 5x the requested quota: --mem-quota 20 GiB
    // became an aggregate MemoryQuota of 100 GiB (block cache 15.45 GiB),
    // while memcached at the same 20 GiB got 12.35 GiB (block cache 1.84 GiB).
    // That made any "equal memory" comparison meaningless.
    magmaCfg.MemoryQuota = cfg.memQuota / cfg.shards;
    magmaCfg.NumFlushers = cfg.flushers;
    magmaCfg.NumCompactors = cfg.compactors;
    // Raise per-batch coroutine fanout: at default 1, magma::GetDocs serializes
    // disk reads on a single thread. With deep pipelining from clients, lifting
    // this drives per-thread NVMe queue depth from ~aqu-sz=1 toward NVMe peak
    // (10K+ qd128 IOPS per device).
    magmaCfg.IOQueueDepth = cfg.ioQueueDepth;
    // Scale BlockCache partition count with reader threads. Default 64 →
    // at readers=128, ~2 threads/partition fighting on each rwlock; profile
    // showed ~5% in pthread_rwlock + AcquireObject. With one partition per
    // thread (or more), rwlock contention drops toward zero.
    magmaCfg.BlockCacheNumPartitions =
            std::max<size_t>(64, 4 * cfg.readers);
    kvserver::gMaxReadBatch = cfg.maxReadBatch;
    kvserver::gMaxReadOwners = std::max<uint32_t>(1, cfg.maxReadOwners);
    kvserver::gWriteCoalesceNs = cfg.writeCoalesceUs * 1000;
    kvserver::gMinWriteBatch = cfg.minWriteBatch;
    kvserver::gSortDupThreshold = cfg.sortDupThreshold;
    if (cfg.asyncDurable) {
        if (!cfg.durable || !cfg.sharedWal) {
            spdlog::error("--async-durable needs --durable and --shared-wal");
            return 1;
        }
        // The writer no longer waits, so the log must not make it wait
        // inside EndTxn either.
        if (cfg.sharedWalSyncCommit < 0) {
            cfg.sharedWalSyncCommit = 0;
        }
        kvserver::gAsyncDurable = true;
        spdlog::info("  async-durable: acknowledging from the log watermark");
    }
    if (cfg.batchSort == "always") {
        kvserver::gBatchSort = kvserver::BatchSort::Always;
    } else if (cfg.batchSort == "never") {
        kvserver::gBatchSort = kvserver::BatchSort::Never;
    } else if (cfg.batchSort != "auto") {
        spdlog::error("--batch-sort must be auto, always or never");
        return 1;
    }
    if (cfg.noHotStats) {
        kvserver::gStatsHotPath = false;
    }
    // --no-block-cache turns off the entire global block cache. This kills
    // index/bloom caching too — used only as a diagnostic to expose true
    // disk-read counts.
    magmaCfg.EnableBlockCache = !cfg.noBlockCache;
    // --cache-decompressed-data now also enables the data-block cache itself
    // (the decompressed cache is a no-op otherwise). Lets a large mem-quota
    // hold hot data blocks so GETs can skip the disk on a data-≫-RAM dataset.
    magmaCfg.EnableDataBlockCaching = cfg.cacheDecompressed;
    magmaCfg.EnableDecompressedDataBlockCaching = cfg.cacheDecompressed;
    magmaCfg.CacheIndexBlocksCompressed = cfg.compressIndexCache;
    magmaCfg.EnableDataBlockAutoTuning = cfg.dataBlockAutoTune;
    magmaCfg.EnableIndexBlockAutoTuning = cfg.indexBlockAutoTune;
    // --align-512: forwards SeqTree/KeyTree align + BlockAlignSize=512.
    // Tested at 35M × 1KB and FAILED: aligning compressed ~600-byte blocks
    // to a 512-byte boundary adds ~70% padding overhead per block, growing
    // on-disk size 8.7 GB -> 32 GB (3.7×). With our 32 GB block-cache budget,
    // the dataset no longer fits, so cold reads hit far more disk and drop
    // 880K -> 486K. Useful only for single-shot reads on huge values where
    // sector alignment > cache density. Default off; left as a knob.
    if (cfg.align512) {
        magmaCfg.SeqTreeAlignBlocks = true;
        magmaCfg.KeyTreeAlignBlocks = true;
        magmaCfg.BlockAlignSize = 512;
    }
    {
        const char* dio = std::getenv("MAGMA_DIRECT_IO");
        bool useDio = !(dio && (std::strcmp(dio, "0") == 0 || std::strcmp(dio, "off") == 0));
        magmaCfg.EnableDirectIO = useDio;
        magmaCfg.EnableDirectIOWrite = useDio;
    }
    if (cfg.noCompression) {
        magmaCfg.Compression = magma::CompressionConfig::None();
    }
    // Index blocks are read-hot and compress well; keeping LZ4 on them shrinks
    // the keyIndex so more of it stays resident in the block cache, which is
    // what read amplification is actually bound by here.
    if (cfg.indexCompressionLZ4 && !cfg.noIndexCompression) {
        magmaCfg.Compression.IndexCompression =
                magma::CompressionType::Create(magma::CompressionAlgo::LZ4);
    }
    // The value-pointer fast path reads the value's data block directly from
    // the table, skipping the seqIndex bloom+index traversal. Turning it off
    // isolates how much of the read path it actually saves.
    if (cfg.noValuePtrRead) {
        magmaCfg.EnableValuePtrRead = false;
    }
    if (cfg.noValuePtrWrite) {
        magmaCfg.EnableValuePtrWrite = false;
    }
    // Point lookups that miss the value pointer reach a seqIndex data block
    // through the model rather than its index blocks, which then need no
    // cache space for gets.
    if (cfg.learnedSeqLocator) {
        magmaCfg.EnableLearnedSeqIndexLocator = true;
    }
    // Magma stamps each tree with its storage format and refuses newer ones,
    // so tying compact metas to format 2 keeps binaries that cannot read them
    // from opening the data at all.
    if (cfg.compactMeta) {
        if (magma::GetStorageFormatVersion() < 2) {
            spdlog::error("--compact-meta needs magma storage format 2, this "
                          "build has {}",
                          magma::GetStorageFormatVersion());
            return 1;
        }
        kvserver::gCompactMeta = true;
        spdlog::info("  compact-meta: writing compact document metadata");
    }
    if (cfg.dataBlockSize > 0) {
        magmaCfg.SeqTreeBlockSize = cfg.dataBlockSize;
    }
    // One log for every shard. Each shard otherwise has its own write-ahead
    // log and its own flush, so N shards mean N logs competing for the same
    // device; here they append into one log and a single flush covers them.
    std::shared_ptr<SharedWALHandle> sharedWal;
    if (cfg.sharedWal) {
        SharedWALHandle::Options o;
        o.Path = cfg.sharedWalPath.empty()
                         ? cfg.dataDir + "/" + cfg.bucket + "/shared-wal"
                         : cfg.sharedWalPath;
        o.NumFlushers = static_cast<uint32_t>(cfg.sharedWalFlushers);
        o.NumChunks = static_cast<uint32_t>(cfg.sharedWalChunks);
        o.ChunkSize = static_cast<uint32_t>(cfg.sharedWalChunkSize);
        o.MinFlushIntervalUs = cfg.sharedWalFlushUs;
        o.SyncOnCommit = cfg.sharedWalSyncCommit < 0 ? cfg.durable
                                                     : cfg.sharedWalSyncCommit;
        if (cfg.sharedWalPrewrite >= 0) {
            o.PrewriteSegments = cfg.sharedWalPrewrite != 0;
        }
        if (cfg.sharedWalSegmentSize) {
            o.SegmentSize = cfg.sharedWalSegmentSize;
        }
        std::filesystem::create_directories(o.Path);
        auto s = SharedWALHandle::Create(o, sharedWal);
        if (!s.IsOK()) {
            spdlog::error("shared WAL: {}", s.String());
            return 1;
        }
        magmaCfg.SharedWAL = sharedWal;
        gExtraStatsJson = [sharedWal](std::string& out) {
            auto s = sharedWal->Log()->GetStats();
            out = "{\"shwal_appends\":" + std::to_string(s.appends) +
                  ",\"shwal_chunks\":" + std::to_string(s.chunksFlushed) +
                  ",\"shwal_fsyncs\":" + std::to_string(s.fsyncs) +
                  ",\"shwal_forced_seals\":" + std::to_string(s.forcedSeals) +
                  ",\"shwal_backpressure_waits\":" + std::to_string(s.backpressureWaits) +
                  ",\"shwal_leader_flushes\":" + std::to_string(s.leaderFlushes) +
                  ",\"shwal_follower_waits\":" + std::to_string(s.followerWaits) +
                  ",\"shwal_write_us_total\":" + std::to_string(s.writeUsTotal) +
                  ",\"shwal_write_us_max\":" + std::to_string(s.writeUsMax) +
                  ",\"shwal_sync_us_total\":" + std::to_string(s.syncUsTotal) +
                  ",\"shwal_sync_us_max\":" + std::to_string(s.syncUsMax) +
                  ",\"shwal_seal_wait_us_total\":" + std::to_string(s.sealWaitUsTotal) +
                  ",\"shwal_publish_wait_us_total\":" + std::to_string(s.publishWaitUsTotal) +
                  ",\"shwal_durable_lsn\":" + std::to_string(s.durableLSN) +
                  ",\"shwal_tail_lsn\":" + std::to_string(s.tailLSN) + "}";
        };
        spdlog::info(
                "  shared-wal: path={} flushers={} chunks={} chunk={}MB "
                "sync-commit={}",
                o.Path,
                o.NumFlushers,
                o.NumChunks,
                o.ChunkSize / (1024 * 1024),
                o.SyncOnCommit);
    }

    if (cfg.lsmSSTableSize > 0) {
        magmaCfg.LSMMaxSSTableSize = cfg.lsmSSTableSize;
        magmaCfg.LSMLSDMaxSSTableSize = cfg.lsmSSTableSize;
    }
    magmaCfg.LSDFragmentationRatio = cfg.lsdFragRatio;
    magmaCfg.KeyLevelSizeMultiplier = cfg.keyLevelMultiplier;
    magmaCfg.KeyTreeWarmNewTables = cfg.keyWarmNewTables;
    if (cfg.sstableWriteBuffer > 0) {
        magmaCfg.SSTableWriterBufferSize = cfg.sstableWriteBuffer;
    }
    if (cfg.keyBlockSize > 0) {
        magmaCfg.KeyTreeBlockSize = cfg.keyBlockSize;
    }
    if (cfg.lsdLevels > 0) {
        magmaCfg.LSDNumLevels = cfg.lsdLevels;
    }
    magmaCfg.LSDTieredL0 = cfg.lsdTieredL0;
    if (!cfg.blockCachePolicy.empty()) {
        magmaCfg.BlockCacheEvictionPolicy = cfg.blockCachePolicy;
    }
    if (cfg.readAhead > 0) {
        magmaCfg.ReadAheadSize = cfg.readAhead;
    }
    if (cfg.writeCacheRatio > 0) {
        magmaCfg.WriteCacheRatio = cfg.writeCacheRatio;
    }
    if (cfg.memLwmRatio > 0) {
        magmaCfg.MemoryQuotaLowWaterMarkRatio = cfg.memLwmRatio;
    }
    if (cfg.lsmBaseLevelSize > 0) {
        magmaCfg.LSMMaxBaseLevelSize = cfg.lsmBaseLevelSize;
    }
    if (cfg.lsmLevel0Tables > 0) {
        magmaCfg.LSMMaxNumLevel0Tables = cfg.lsmLevel0Tables;
    }
    if (cfg.lsmMinCompactSize > 0) {
        magmaCfg.LSMMinCompactSize = cfg.lsmMinCompactSize;
    }
    if (cfg.lsmLevelMultiplier > 0) {
        magmaCfg.LSMLevelSizeMultiplier = cfg.lsmLevelMultiplier;
    }

    if (cfg.noWal) {
        magmaCfg.EnableWAL = false;
        spdlog::warn(
                "  --no-wal: magma has no write-ahead log. A crash loses "
                "every write since the last memtable flush.");
    }

    magmaCfg.LogLevel = "info";
    magmaCfg.EnableUpdateStatusForSet = !cfg.blindWrites;
    // Write cache is half the per-shard quota.
    //
    // This used to be capped at 256 MB. The cap is the threshold magma
    // throttles writers against, so on a write workload the cache sat six
    // times over it and every writer blocked in the throttle waiting for a
    // flush - half the machine idle and the disk barely used while writes
    // were refused. Raising the quota did nothing because the cap ignored it.
    // --write-cache overrides the derived value.
    size_t perShardQuota = cfg.memQuota / cfg.shards;
    magmaCfg.MaxWriteCacheSize =
            cfg.writeCache > 0 ? cfg.writeCache : perShardQuota / 2;
    magmaCfg.WALBufferSize =
            std::min(perShardQuota / 8, (size_t)16 * 1024 * 1024);

    // Simple fixed-size metadata callbacks — no assertions, no exceptions
    using kvserver::MetaGetHistoryTimestamp;
    using kvserver::MetaGetSeqNum;
    using kvserver::MetaGetValueSize;
    using kvserver::MetaIsTombstone;

    magmaCfg.GetSeqNum = MetaGetSeqNum;
    magmaCfg.GetValueSize = MetaGetValueSize;
    magmaCfg.IsTombstone = MetaIsTombstone;
    magmaCfg.GetHistoryTimeNow =
            [](Magma::KVStoreID) -> std::optional<std::chrono::seconds> {
        return std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch());
    };
    magmaCfg.GetHistoryTimestamp = MetaGetHistoryTimestamp;

    class SimpleCompactionCB : public Magma::CompactionCallback {
        bool operator()(const Slice& keySlice,
                        const Slice& metaSlice,
                        const Slice& valueSlice) override {
            if (MetaIsTombstone(metaSlice)) {
                return true;
            }
            return false;
        }
        const UserStats* GetUserStats() override {
            return nullptr;
        }
    };
    magmaCfg.MakeCompactionCallback = [](const Magma::KVStoreID) {
        return std::make_unique<SimpleCompactionCB>();
    };

    // Create and open bucket
    Bucket bucket(cfg.bucket,
                  cfg.dataDir,
                  cfg.shards,
                  cfg.writers,
                  cfg.readers,
                  cfg.vbuckets,
                  cfg.durable,
                  cfg.writeQueueMem,
                  cfg.echoGetSize,
                  magmaCfg);
    spdlog::info("  mode={} write-queue-mem={}MB{}",
                 cfg.durable ? "durable" : "async",
                 cfg.writeQueueMem / (1024 * 1024),
                 cfg.echoGetSize > 0
                         ? fmt::format(" echo-get={}B", cfg.echoGetSize)
                         : "");
    if (cfg.cacheSize > 0) {
        std::string err;
        auto cache = CreateDocCache(
                cfg.cachePolicy, cfg.cacheSize, cfg.cacheShards, &err);
        if (!cache) {
            spdlog::error("{}", err);
            return 1;
        }
        spdlog::info("  cache: policy={} size={}MB shards={}",
                     cache->Policy(),
                     cfg.cacheSize / (1024 * 1024),
                     cfg.cacheShards);
        bucket.SetCache(std::move(cache));
    }
    auto status = bucket.Open();
    if (!status.IsOK()) {
        spdlog::error("Failed to open bucket: {}", status.String());
        return 1;
    }

    spdlog::info("Bucket '{}' opened with {} shards", cfg.bucket, cfg.shards);

    // Install signal handlers
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);
    // crashHandler prints a backtrace and then _exit()s, so the kernel never
    // writes a core. Set FLUXKV_NO_CRASH_HANDLER=1 to leave the default
    // dispositions in place and get a core for post-mortem analysis.
    if (std::getenv("FLUXKV_NO_CRASH_HANDLER") == nullptr) {
        signal(SIGSEGV, crashHandler);
        signal(SIGABRT, crashHandler);
    }

    // Start server (blocks on accept loop)
    Server server(&bucket,
                  cfg.port,
                  cfg.ioThreads,
                  cfg.hostname,
                  cfg.vbuckets,
                  cfg.statsPort);
    gServer = &server;

    if (cfg.autoTune) {
        const size_t hw = std::max(1u, std::thread::hardware_concurrency());
        ThreadTuner::Bounds io{1, cfg.maxIoThreads ? cfg.maxIoThreads : hw};
        // Readers move in whole shards; the floor is one per shard.
        ThreadTuner::Bounds rd{cfg.shards,
                               cfg.maxReaders ? cfg.maxReaders : 4 * hw};
        // Writers move in whole shards too.
        ThreadTuner::Bounds wr{cfg.shards,
                               cfg.maxWriters ? cfg.maxWriters : 2 * hw};
        io.max = std::max(io.max, static_cast<size_t>(cfg.ioThreads));
        rd.max = std::max(rd.max, static_cast<size_t>(cfg.readers));
        if (cfg.tuneWritersOnly) {
            io.min = io.max = cfg.ioThreads;
            rd.min = rd.max = cfg.readers;
        }
        if (!cfg.tunePools.empty()) {
            auto has = [&](const char* n) {
                return ("," + cfg.tunePools + ",").find(std::string(",") + n + ",") !=
                       std::string::npos;
            };
            if (!has("io")) {
                io.min = io.max = cfg.ioThreads;
            }
            if (!has("readers")) {
                rd.min = rd.max = cfg.readers;
            }
            if (!has("writers")) {
                wr.min = wr.max = cfg.writers;
            }
        }
        wr.max = std::max(wr.max, static_cast<size_t>(cfg.writers));
        server.EnableAutoTune(TunerConfig{}, io, rd, wr);
        spdlog::info(
                "  auto-tune: io-threads {}..{} readers {}..{} writers {}..{}",
                io.min,
                io.max,
                rd.min,
                rd.max,
                wr.min,
                wr.max);
    }

    server.Start(); // returns once RequestStop() has been called

    if (int sig = gSignal.load(std::memory_order_relaxed)) {
        spdlog::info("Caught signal {}, shutting down...", sig);
    }
    server.Stop();
    spdlog::info("magma-kvserver shutdown complete");
    return 0;
}
