#pragma once

#include "include/libmagma/magma.h"
#include "cache/doccache.h"
#include "metadata.h"
#include "protocol.h"
#include "statslot.h"
#include "tuner.h"

#include <folly/AtomicIntrusiveLinkedList.h>
#include <folly/MPMCQueue.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>

#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace magma {
namespace kvserver {

class Connection;
class Bucket;

// Per-counter sharding for the hot path.
//
// One global atomic per counter was the single most expensive thing in the GET
// path: two fetch_adds per request cost more than the parse, the cache lookup
// and the response combined, because every increment moves a cache line
// between cores - and it got worse with more threads. Spreading each counter
// over cache-line-aligned per-thread slots keeps increments local; readers sum
// the slots. Values stay exact, only the layout changes.
inline constexpr size_t kStatShards = kStatSlots;

struct alignas(64) ShardedCounter {
    std::atomic<uint64_t> v{0};
};

// One counter, sharded. Sum() is O(kStatShards) and only runs when stats are
// scraped.
struct HotCounter {
    std::array<ShardedCounter, kStatShards> slots;

    uint64_t Sum() const {
        uint64_t t = 0;
        for (const auto& s : slots) {
            t += s.v.load(std::memory_order_relaxed);
        }
        return t;
    }
    void Add(uint64_t v, size_t slot) {
        slots[slot].v.fetch_add(v, std::memory_order_relaxed);
    }
    void Sub(uint64_t v, size_t slot) {
        slots[slot].v.fetch_sub(v, std::memory_order_relaxed);
    }
};

// Server-side dispatcher counters
struct DispatcherStats {
    HotCounter cmdSet;
    HotCounter cmdGet;
    HotCounter cmdDelete;
    HotCounter cmdSetResp;
    HotCounter cmdGetResp;
    HotCounter cmdSetRespErr;
    HotCounter cmdGetRespMiss;
    std::atomic<uint64_t> connectAccept{0};
    std::atomic<uint64_t> connectClose{0};
    HotCounter writeBatches;
    HotCounter writeBatchItems;
    HotCounter readBatches;
    HotCounter readBatchItems;
    std::atomic<uint64_t> tmpFails{0};
    HotCounter queuedGets;
    std::atomic<uint64_t> badMagic{0};
    std::atomic<uint64_t> badOpcode{0};
    // Connection moves between IO threads: begun, and finished on the target.
    std::atomic<uint64_t> migrationsStarted{0};
    std::atomic<uint64_t> migrationsDone{0};
    // Why a migration check had to wait, and total time spent migrating.
    std::atomic<uint64_t> migWaitOutstanding{0};
    std::atomic<uint64_t> migWaitFlush{0};
    std::atomic<uint64_t> migWaitInflight{0};
    std::atomic<uint64_t> migTotalUs{0};

    std::string toJson() const;
};

// Global singleton
extern DispatcherStats gDispStats;
// The bucket's document cache, or null. Published by Bucket::SetCache so the
// stats endpoint can report it alongside the dispatcher counters.
extern DocCache* gDocCache;
// Runtime read-batch cap (set from main via --max-read-batch).
extern size_t gMaxReadBatch;

// Write coalescing. A vbucket whose last WriteDocs finished less than
// gWriteCoalesceNs ago and has fewer than gMinWriteBatch items queued is
// not written again until the interval has passed. Every WriteDocs carries
// a fixed cost (a log transaction, memtable and index locking, the write
// cache check), so writing whatever happens to be queued the instant a
// writer is free turns a saturated pool into one that spends its CPU on
// one- and two-item batches. 0 disables.
extern size_t gMinWriteBatch;
extern uint64_t gWriteCoalesceNs;
// Master switch for per-op hot-path stat increments. At 1 M ops/s the cache-
// line bouncing of atomic fetch_adds across N reader threads is measurable.
// Disabled with --no-hot-stats; coarse counters (connectAccept/Close,
// badMagic, tmpFails, writeBatches) still update unconditionally.
extern bool gStatsHotPath;

// Inline-fast wrappers used at hot-path call sites.
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
inline void hotStatAdd(HotCounter& c, uint64_t v = 1) {
    if (gStatsHotPath) {
        c.Add(v, statSlot());
    }
}
inline void hotStatSub(HotCounter& c, uint64_t v = 1) {
    if (gStatsHotPath) {
        c.Sub(v, statSlot());
    }
}

// Response-dispatch micro-batching.
//
// Measured 2026-09-14, networked 1 KB GETs: kvserver issued 0.93 epoll_wait and
// 0.83 sendmsg PER GET - one network syscall per request, where a well
// batched server needs ~1 per 60. Connection::scheduleFlush already coalesces responses into one
// write per event-loop iteration, but the reader thread posts ONE
// runInEventBaseThread per completed op, and each post wakes the target loop.
// One wakeup per request means one loop iteration per request, so the
// coalescing never has more than a single response to coalesce.
//
// Grouping a few completions into one post amortises both the wakeup and the
// write. We deliberately do NOT revert to dispatching at end-of-GetDocs: that
// was the intra-batch head-of-line blocking fixed in c0a9af9d8. Flushing every
// gDispatchBatch completions caps the added wait at (gDispatchBatch-1) ops
// within the same IO wave.
// 1 = previous behaviour (dispatch per completion).
extern size_t gDispatchBatch;

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

    // Owns the request body data when it did not fit inlineData.
    std::unique_ptr<folly::IOBuf> dataBuf;
    // Small SET bodies (extras + key + value) are copied here so the IOBuf
    // can be released on the IO thread that allocated it. Writers used to
    // free it, and freeing into another thread's allocator arena was a
    // third of writer CPU.
    static constexpr size_t kInlineData = 128;
    alignas(8) char inlineData[kInlineData];

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
    // Write queues only: items pushed but not yet swept, and when the last
    // WriteDocs for this vbucket finished. Both drive write coalescing.
    std::atomic<uint32_t> pending{0};
    std::atomic<uint64_t> lastWriteNs{0};
};

// Tasks dispatched to writer/reader pools
struct PersistTask {
    class Shard* shard{nullptr};
    uint16_t vbid{0};
};

struct ReadTask {
    class Shard* shard{nullptr};
    uint16_t vbid{0};
    // Stamped by Submit; the gap to dequeue is how long work waited for a
    // thread, which the tuner reports.
    uint64_t enqueuedNs{0};
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

    // Finished write Requests come back here instead of being deleted on a
    // writer thread. IO threads take from it before allocating. False when
    // full; the caller deletes.
    bool RecycleRequest(Request* req) {
        return freeRequests_.write(req);
    }
    Request* TakeRequest() {
        Request* r{nullptr};
        return freeRequests_.read(r) ? r : nullptr;
    }

private:
    uint16_t shardId_;
    std::unique_ptr<Magma> magma_;
    folly::MPMCQueue<Request*> freeRequests_;
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
    // Submit now, or after the coalescing interval if the vbucket was just
    // written and has little queued. Caller has already won `scheduled`.
    void SubmitOrDefer(PersistTask task, VBQueue& vbq);
    void Shutdown();

private:
    struct Deferred {
        uint64_t notBeforeNs{0};
        PersistTask task;
    };

    void workerLoop();
    void deferLoop();
    void executePersist(PersistTask& task);

    std::vector<std::thread> threads_;
    folly::MPMCQueue<PersistTask> taskQueue_;
    // FIFO of tasks waiting out the coalescing interval. Every entry has the
    // same delay, so arrival order is due order and one thread sleeping on
    // the head is enough.
    folly::MPMCQueue<Deferred> deferQueue_;
    std::thread deferThread_;
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

    // Run-time sizing, driven by the tuner. Grow starts threads now; Shrink
    // queues one retire sentinel per thread and the workers that pick them
    // up exit after their current task. Reap joins them.
    size_t Size() const {
        return live_.load(std::memory_order_relaxed);
    }
    void Grow(size_t n);
    void Shrink(size_t n);
    void Reap();

    // Work done since the previous call. Busy time is measured around
    // executeRead only: the idle spin in workerLoop must not count as work.
    struct Sample {
        uint64_t busyNs{0};
        uint64_t waitNs{0};
        uint64_t tasks{0};
        uint64_t items{0};
    };
    Sample TakeSample();

private:
    struct Worker {
        std::thread thread;
        std::atomic<bool> done{false};
    };
    struct alignas(64) Acct {
        std::atomic<uint64_t> busyNs{0};
        std::atomic<uint64_t> waitNs{0};
        std::atomic<uint64_t> tasks{0};
        std::atomic<uint64_t> items{0};
    };

    void spawn();
    void workerLoop(Worker* self);
    // Returns the number of requests served.
    size_t executeRead(ReadTask& task);

    std::vector<std::unique_ptr<Worker>> workers_;
    std::atomic<size_t> live_{0};
    folly::MPMCQueue<ReadTask> taskQueue_;
    std::atomic<bool> shutdown_{false};
    Bucket* bucket_;
    Acct acct_[kStatShards];
    Sample lastSample_;
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

    // Closes the bucket. Waits for acknowledged writes to reach magma before
    // tearing anything down, so a clean shutdown does not lose data.
    void Close();

    // Blocks until every acknowledged write has been persisted, or until the
    // timeout expires - in which case the shortfall is logged as an error and
    // that data is lost. Called by Close().
    void DrainWrites(
            std::chrono::milliseconds timeout = std::chrono::minutes(10));

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

    // Document cache in front of magma (--cache-size). Null when disabled.
    // Set before Open(); the IO threads read it without synchronisation.
    void SetCache(std::unique_ptr<DocCache> cache) {
        cache_ = std::move(cache);
        gDocCache = cache_.get();
    }
    DocCache* GetCache() {
        return cache_.get();
    }

    // Trigger full compaction of every kvstore (all shards × all vbuckets).
    // Synchronous — returns when every compaction finishes.
    void CompactAll();

    size_t NumShards() const {
        return numShards_;
    }
    std::vector<ReaderPool*> ReaderPools() {
        std::vector<ReaderPool*> v;
        for (auto& s : shards_) {
            v.push_back(s->GetReaderPool());
        }
        return v;
    }

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
    std::unique_ptr<DocCache> cache_;
    std::vector<std::unique_ptr<Shard>> shards_;
    // Per-shard pools live inside each Shard now (sharded). Bucket holds
    // no global pools — EnqueueRead/EnqueueWrite route to the shard's own.
};

// The per-shard reader pools presented to the tuner as one pool. Every shard
// carries the same share of vbuckets, so they are always kept the same size:
// Size() is the total and Step() is the shard count.
class ReaderPoolGroup : public ElasticPool {
public:
    explicit ReaderPoolGroup(Bucket* bucket) : pools_(bucket->ReaderPools()) {
    }
    const char* Name() const override {
        return "readers";
    }
    size_t Size() const override;
    size_t Step() const override {
        return pools_.size();
    }
    void Grow(size_t n) override;
    void Shrink(size_t n) override;
    PoolSample Sample(double wallSec) override;
    void Reap() override;

private:
    std::vector<ReaderPool*> pools_;
};

} // namespace kvserver
} // namespace magma
