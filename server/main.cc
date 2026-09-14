#include "engine.h"
#include "include/libmagma/magma.h"
#include "metadata.h"
#include "server.h"

#include <platform/cb_arena_malloc.h>
#include <spdlog/spdlog.h>

#include <getopt.h>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <execinfo.h>
#include <fcntl.h>
#include <iostream>
#include <string>

using namespace magma;
using namespace magma::kvserver;

static Server* gServer = nullptr;

static void signalHandler(int sig) {
    spdlog::info("Caught signal {}, shutting down...", sig);
    if (gServer) {
        gServer->Stop();
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
    size_t maxReadBatch = 64; // per-reader-thread sweep cap
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
    // Sets SeqTreeBlockSize ONLY -- the data blocks holding document values.
    // KeyTreeBlockSize stays at magma's 4096 default: shrinking it too adds
    // key-index reads (IO/GET 1.13 -> 1.35) and cost 7-11% throughput.
    size_t dataBlockSize = 0; // 0 = magma default (4096) -> SeqTreeBlockSize
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
              << "  --max-read-batch N   reader-thread sweep cap (default 64)\n"
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
            {"data-block-size", required_argument, nullptr, 1008},
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
        case 1008:
            cfg.dataBlockSize = strtoull(optarg, nullptr, 10);
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
    if (cfg.indexCompressionLZ4) {
        magmaCfg.Compression.IndexCompression =
                magma::CompressionType::Create(magma::CompressionAlgo::LZ4);
    }
    // The value-pointer fast path reads the value's data block directly from
    // the table, skipping the seqIndex bloom+index traversal. Turning it off
    // isolates how much of the read path it actually saves.
    if (cfg.noValuePtrRead) {
        magmaCfg.EnableValuePtrRead = false;
    }
    if (cfg.dataBlockSize > 0) {
        magmaCfg.SeqTreeBlockSize = cfg.dataBlockSize;
    }
    magmaCfg.LogLevel = "info";
    // Scale write cache to 50% of per-shard quota, cap at 256MB
    size_t perShardQuota = cfg.memQuota / cfg.shards;
    magmaCfg.MaxWriteCacheSize =
            std::min(perShardQuota / 2, (size_t)256 * 1024 * 1024);
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
    auto status = bucket.Open();
    if (!status.IsOK()) {
        spdlog::error("Failed to open bucket: {}", status.String());
        return 1;
    }

    spdlog::info("Bucket '{}' opened with {} shards", cfg.bucket, cfg.shards);

    // Install signal handlers
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);
    signal(SIGSEGV, crashHandler);
    signal(SIGABRT, crashHandler);

    // Start server (blocks on accept loop)
    Server server(&bucket,
                  cfg.port,
                  cfg.ioThreads,
                  cfg.hostname,
                  cfg.vbuckets,
                  cfg.statsPort);
    gServer = &server;

    server.Start(); // blocks until Stop() is called

    spdlog::info("magma-kvserver shutdown complete");
    return 0;
}
