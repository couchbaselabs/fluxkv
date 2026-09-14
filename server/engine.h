#pragma once

#include "include/libmagma/magma.h"
#include "metadata.h"
#include "protocol.h"

#include <folly/AtomicIntrusiveLinkedList.h>
#include <folly/MPMCQueue.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>

#include <array>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace magma {
namespace kvserver {

class Connection;
class Bucket;

// Server-side dispatcher counters
struct DispatcherStats {
    std::atomic<uint64_t> cmdSet{0};
    std::atomic<uint64_t> cmdGet{0};
    std::atomic<uint64_t> cmdDelete{0};
    std::atomic<uint64_t> cmdSetResp{0};
    std::atomic<uint64_t> cmdGetResp{0};
    std::atomic<uint64_t> cmdSetRespErr{0};
    std::atomic<uint64_t> cmdGetRespMiss{0};
    std::atomic<uint64_t> connectAccept{0};
    std::atomic<uint64_t> connectClose{0};
    std::atomic<uint64_t> writeBatches{0};
    std::atomic<uint64_t> writeBatchItems{0};
    std::atomic<uint64_t> readBatches{0};
    std::atomic<uint64_t> readBatchItems{0};
    std::atomic<uint64_t> tmpFails{0};
    std::atomic<uint64_t> queuedGets{0};
    std::atomic<uint64_t> badMagic{0};
    std::atomic<uint64_t> badOpcode{0};

    std::string toJson() const;
};

// Global singleton
extern DispatcherStats gDispStats;
// Runtime read-batch cap (set from main via --max-read-batch).
extern size_t gMaxReadBatch;
// Master switch for per-op hot-path stat increments. At 1 M ops/s the cache-
// line bouncing of atomic fetch_adds across N reader threads is measurable.
// Disabled with --no-hot-stats; coarse counters (connectAccept/Close,
// badMagic, tmpFails, writeBatches) still update unconditionally.
extern bool gStatsHotPath;
// Inline-fast wrapper used at hot-path call sites.
inline void hotStatAdd(std::atomic<uint64_t>& s, uint64_t v = 1) {
    if (gStatsHotPath) {
        s.fetch_add(v, std::memory_order_relaxed);
    }
}
inline void hotStatSub(std::atomic<uint64_t>& s, uint64_t v = 1) {
    if (gStatsHotPath) {
        s.fetch_sub(v, std::memory_order_relaxed);
    }
}

// A single KV request flowing through the engine.
// Uses intrusive linked list for lock-free per-vb write queues.
struct Request {
    // Parsed from mcbp header
    uint8_t opcode{0};
    uint16_t vbucket{0};
    uint32_t opaque{0};
    uint64_t cas{0};

    // Zero-copy views into dataBuf
    Slice key;
    Slice value;
    uint32_t flags{0};
    uint32_t expiry{0};
    uint8_t datatype{0}; // request datatype (SET)
    uint8_t resultDatatype{0}; // stored datatype (GET)

    // Owns the request body data
    std::unique_ptr<folly::IOBuf> dataBuf;

    // For routing response back to connection
    Connection* conn{nullptr};
    folly::EventBase* evb{nullptr};

    // Result filled by engine thread
    Status resultStatus;
    uint64_t resultSeqno{0};
    uint32_t resultFlags{0};
    // GET result value copied from FetchBuffer. std::vector with capacity
    // preserved across reset() — the Connection-local Request pool reuses
    // this buffer, so steady-state value-size workloads incur zero allocs.
    std::vector<uint8_t> responseBuf;

    // Intrusive linked list hook for per-vb queues (write OR read, not both)
    folly::AtomicIntrusiveLinkedListHook<Request> hook;

    // Reset to default state for pool reuse. Cheap — no heap frees beyond
    // releasing dataBuf/responseBuf, which usually came from coalesce/move.
    void reset() {
        opcode = 0;
        vbucket = 0;
        opaque = 0;
        cas = 0;
        key = Slice();
        value = Slice();
        flags = 0;
        expiry = 0;
        datatype = 0;
        resultDatatype = 0;
        dataBuf.reset();
        conn = nullptr;
        evb = nullptr;
        resultStatus = Status();
        resultSeqno = 0;
        resultFlags = 0;
        responseBuf.clear(); // preserve capacity for reuse
    }
};

// Per-vbucket queue using lock-free MPSC list
struct VBQueue {
    folly::AtomicIntrusiveLinkedList<Request, &Request::hook> list;
    std::atomic<bool> scheduled{false};
};

// Tasks dispatched to writer/reader pools
struct PersistTask {
    class Shard* shard{nullptr};
    uint16_t vbid{0};
};

struct ReadTask {
    class Shard* shard{nullptr};
    uint16_t vbid{0};
};

class WriterPool;
class ReaderPool;

// One Magma instance. Owns per-vb write queues, seqno state, AND its own
// reader/writer thread pools (sharded — each shard's pool only sees tasks
// for its own vbuckets, eliminating central-MPMC contention at high reader
// counts).
class Shard {
public:
    Shard(uint16_t shardId, const std::string& path, const Magma::Config& cfg);
    ~Shard();

    // Pools are created lazily after Bucket is fully constructed (they need
    // a back-pointer for queuedBytes_).
    void CreatePools(size_t numWriters,
                     size_t numReaders,
                     size_t queueSize,
                     Bucket* bucket);

    Status Open();
    void Close();

    Magma* GetMagma() {
        return magma_.get();
    }
    uint16_t GetShardId() const {
        return shardId_;
    }
    WriterPool* GetWriterPool() {
        return writerPool_.get();
    }
    ReaderPool* GetReaderPool() {
        return readerPool_.get();
    }

    VBQueue& GetVBWriteQueue(uint16_t vbid) {
        return vbWriteQueues_[vbid];
    }
    VBQueue& GetVBReadQueue(uint16_t vbid) {
        return vbReadQueues_[vbid];
    }

    uint64_t NextSeqno(uint16_t vbid) {
        return seqnos_[vbid].fetch_add(1, std::memory_order_relaxed) + 1;
    }
    void RecoverSeqno(uint16_t vbid, uint64_t seq) {
        seqnos_[vbid].store(seq, std::memory_order_relaxed);
    }

    // KVStore creation tracking
    bool IsKVStoreCreated(uint16_t vbid) const {
        return createdBitmap_[vbid / 64].load(std::memory_order_relaxed) &
               (1ULL << (vbid % 64));
    }
    void MarkKVStoreCreated(uint16_t vbid) {
        createdBitmap_[vbid / 64].fetch_or(1ULL << (vbid % 64),
                                           std::memory_order_relaxed);
    }

    static constexpr uint16_t kMaxVBuckets = 1024;

private:
    uint16_t shardId_;
    std::unique_ptr<Magma> magma_;
    std::array<VBQueue, kMaxVBuckets> vbWriteQueues_;
    std::array<VBQueue, kMaxVBuckets> vbReadQueues_;
    std::array<std::atomic<uint64_t>, kMaxVBuckets> seqnos_{};
    std::atomic<uint64_t> createdBitmap_[kMaxVBuckets / 64]{};
    std::unique_ptr<WriterPool> writerPool_;
    std::unique_ptr<ReaderPool> readerPool_;
};

// Global writer thread pool
class WriterPool {
public:
    WriterPool(size_t numThreads, size_t queueSize, Bucket* bucket);
    ~WriterPool();

    void Submit(PersistTask task);
    void Shutdown();

private:
    void workerLoop();
    void executePersist(PersistTask& task);

    std::vector<std::thread> threads_;
    folly::MPMCQueue<PersistTask> taskQueue_;
    std::atomic<bool> shutdown_{false};
    Bucket* bucket_;
};

// Global reader thread pool — uses per-VB queues + ReadTask scheduling
class ReaderPool {
public:
    ReaderPool(size_t numThreads, size_t queueSize, Bucket* bucket);
    ~ReaderPool();

    void Submit(ReadTask task);
    void Shutdown();

private:
    void workerLoop();
    void executeRead(ReadTask& task);

    std::vector<std::thread> threads_;
    folly::MPMCQueue<ReadTask> taskQueue_;
    std::atomic<bool> shutdown_{false};
    Bucket* bucket_;
};

// Container of shards. Routes requests to the correct shard.
class Bucket {
public:
    Bucket(const std::string& name,
           const std::string& dataDir,
           uint16_t numShards,
           size_t numWriters,
           size_t numReaders,
           uint16_t numVBuckets,
           bool durable,
           size_t writeQueueMemLimit,
           size_t echoGetSize,
           const Magma::Config& baseCfg);
    ~Bucket();

    Status Open();
    void Close();

    Shard& GetShard(uint16_t vbid) {
        return *shards_[vbid % numShards_];
    }

    // Returns true if enqueued, false if over memory limit (TMPFAIL)
    bool EnqueueWrite(Request* req);
    void EnqueueRead(Request* req);
    bool IsDurable() const {
        return durable_;
    }

    const std::string& GetName() const {
        return name_;
    }
    size_t GetEchoGetSize() const {
        return echoGetSize_;
    }
    const std::string& GetEchoGetValue() const {
        return echoGetValue_;
    }
    std::string GetStatsJson();

    // Trigger full compaction of every kvstore (all shards × all vbuckets).
    // Synchronous — returns when every compaction finishes.
    void CompactAll();

    // Public for writer pool access
    std::atomic<size_t> queuedBytes_{0};

private:
    std::string name_;
    std::string dataDir_;
    uint16_t numShards_;
    uint16_t numVBuckets_;
    bool durable_;
    size_t writeQueueMemLimit_;
    size_t echoGetSize_;
    std::string echoGetValue_;
    std::vector<std::unique_ptr<Shard>> shards_;
    // Per-shard pools live inside each Shard now (sharded). Bucket holds
    // no global pools — EnqueueRead/EnqueueWrite route to the shard's own.
};

} // namespace kvserver
} // namespace magma
