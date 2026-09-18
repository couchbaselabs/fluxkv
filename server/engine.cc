#include "engine.h"

#include <pthread.h>

#include <algorithm>
#include "connection.h"
#include "iothread.h"
#include "include/libmagma/operations.h"

#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <filesystem>

namespace magma {
namespace kvserver {

DispatcherStats gDispStats;

DocCache* gDocCache = nullptr;
// Runtime-tunable read-batch cap (--max-read-batch). 128 matches the
// libaio + IOQueueDepth=16 sweet spot; lower (8-16) is better for sync
// QD=1 multi-thread parallelism.
size_t gMaxReadBatch = 128;
size_t gMinWriteBatch = 64;
BatchSort gBatchSort = BatchSort::Auto;
bool gAsyncDurable = false;
double gSortDupThreshold = 0.05;
// Per-shard cap on recycled Requests; beyond it writers delete. Sized to
// cover the write queue at a few hundred bytes per Request.
static constexpr size_t kFreeRequestCap = 1 << 18;
uint64_t gWriteCoalesceNs = 500 * 1000;

namespace {
inline uint64_t steadyNowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
}

// First 8 key bytes, big-endian, so that ordering by this value orders by
// key for keys of 8 bytes or fewer.
inline uint64_t keyPrefix(const Slice& k) {
    if (k.Len() >= 8) {
        uint64_t v;
        std::memcpy(&v, k.Data(), 8);
        return __builtin_bswap64(v);
    }
    uint64_t p = 0;
    for (size_t j = 0; j < k.Len(); j++) {
        p |= static_cast<uint64_t>(static_cast<uint8_t>(k.Data()[j]))
             << (56 - 8 * j);
    }
    return p;
}


// Fraction of a sample of the batch that repeats an earlier sampled key.
// Sampling with a stride rather than a prefix keeps it representative of
// the whole batch. Prefix collisions between different keys only make the
// estimate high, which costs a sort, never correctness: the sort's dedupe
// compares full keys.
constexpr size_t kDupSampleMax = 128;

double estimateDupFraction(const std::vector<Request*>& batch) {
    const size_t n = batch.size();
    const size_t want = std::min(n, kDupSampleMax);
    const size_t stride = std::max<size_t>(1, n / want);
    folly::F14FastSet<uint64_t> seen;
    seen.reserve(want);
    size_t sampled = 0, dups = 0;
    for (size_t i = 0; i < n && sampled < want; i += stride) {
        sampled++;
        if (!seen.insert(keyPrefix(batch[i]->key)).second) {
            dups++;
        }
    }
    return sampled > 1 ? static_cast<double>(dups) / sampled : 0.0;
}

inline bool shouldDeferWrite(const VBQueue& vbq, uint64_t now) {
    if (gWriteCoalesceNs == 0 ||
        vbq.pending.load(std::memory_order_relaxed) >= gMinWriteBatch) {
        return false;
    }
    return now - vbq.lastWriteNs.load(std::memory_order_relaxed) <
           gWriteCoalesceNs;
}
} // namespace
// Gates per-op stat increments — see engine.h.
bool gStatsHotPath = true;

std::string DispatcherStats::toJson() const {
    nlohmann::json j;
    j["cmd_set"] = cmdSet.Sum();
    j["cmd_get"] = cmdGet.Sum();
    j["cmd_delete"] = cmdDelete.Sum();
    j["cmd_set_resp"] = cmdSetResp.Sum();
    j["cmd_get_resp"] = cmdGetResp.Sum();
    j["cmd_set_resp_err"] = cmdSetRespErr.Sum();
    j["cmd_get_resp_miss"] = cmdGetRespMiss.Sum();
    j["connect_accept"] = connectAccept.load(std::memory_order_relaxed);
    j["connect_close"] = connectClose.load(std::memory_order_relaxed);
    j["write_batches"] = writeBatches.Sum();
    j["write_batch_items"] = writeBatchItems.Sum();
    j["write_dedups"] = writeDedups.Sum();
    j["write_batches_sorted"] = writeBatchesSorted.Sum();
    j["read_batches"] = readBatches.Sum();
    j["read_batch_items"] = readBatchItems.Sum();
    j["tmp_fails"] = tmpFails.load(std::memory_order_relaxed);
    j["queued_gets"] = queuedGets.Sum();
    j["bad_magic"] = badMagic.load(std::memory_order_relaxed);
    j["bad_opcode"] = badOpcode.load(std::memory_order_relaxed);
    j["migrations_started"] = migrationsStarted.load(std::memory_order_relaxed);
    j["migrations_done"] = migrationsDone.load(std::memory_order_relaxed);
    j["mig_wait_outstanding"] = migWaitOutstanding.load(std::memory_order_relaxed);
    j["mig_wait_flush"] = migWaitFlush.load(std::memory_order_relaxed);
    j["mig_wait_inflight"] = migWaitInflight.load(std::memory_order_relaxed);
    j["mig_total_us"] = migTotalUs.load(std::memory_order_relaxed);
    auto wBatches = writeBatches.Sum();
    j["avg_write_batch"] =
            wBatches > 0
                    ? (double)writeBatchItems.Sum() /
                              wBatches
                    : 0.0;
    auto rBatches = readBatches.Sum();
    j["avg_read_batch"] =
            rBatches > 0
                    ? (double)readBatchItems.Sum() /
                              rBatches
                    : 0.0;
    j["outstanding_requests"] = (int64_t)cmdSet.Sum() + cmdGet.Sum() +
                                cmdDelete.Sum() - cmdSetResp.Sum() -
                                cmdGetResp.Sum() - cmdSetRespErr.Sum() -
                                cmdGetRespMiss.Sum();
    if (gDocCache) {
        auto c = gDocCache->GetStats();
        j["cache_hits"] = c.hits;
        j["cache_misses"] = c.misses;
        j["cache_tombstone_hits"] = c.tombstoneHits;
        j["cache_puts"] = c.puts;
        j["cache_fills"] = c.fills;
        j["cache_fills_rejected"] = c.fillsRejected;
        j["cache_evictions"] = c.evictions;
        j["cache_ghost_hits"] = c.ghostHits;
        j["cache_bytes"] = c.bytes;
        j["cache_max_bytes"] = c.maxBytes;
        j["cache_items"] = c.items;
        j["cache_pending_items"] = c.pendingItems;
        uint64_t lookups = c.hits + c.misses + c.tombstoneHits;
        j["cache_hit_ratio"] =
                lookups ? (double)(c.hits + c.tombstoneHits) / lookups : 0.0;
    }
    return j.dump(2);
}

// ---- Shard ----

Shard::Shard(uint16_t shardId,
             const std::string& path,
             const Magma::Config& cfg)
    : shardId_(shardId), freeRequests_(kFreeRequestCap) {
    Magma::Config shardCfg = cfg;
    shardCfg.Path = path;
    magma_ = std::make_unique<Magma>(shardCfg);
}

Shard::~Shard() {
    if (durableThread_.joinable()) {
        durableStop_.store(true, std::memory_order_relaxed);
        durableCv_.notify_all();
        durableThread_.join();
    }
    Close();
    Request* r{nullptr};
    while (freeRequests_.read(r)) {
        delete r;
    }
}

void Shard::durableLoop() {
    pthread_setname_np(pthread_self(), "fx:durable");
    for (;;) {
        uint64_t target = 0;
        {
            std::unique_lock<std::mutex> lk(durableMu_);
            durableCv_.wait(lk, [this]() {
                return durableStop_.load(std::memory_order_relaxed) ||
                       !durableQueue_.empty();
            });
            if (durableStop_.load(std::memory_order_relaxed) &&
                durableQueue_.empty()) {
                return;
            }
            // The head has the smallest LSN, so waiting for it covers
            // every batch that can be completed in this pass.
            target = durableQueue_.front().lsn;
        }

        // One thread parks here instead of every writer. Wakes when the
        // log's flusher publishes a watermark at or past the target.
        magma_->AwaitWALDurable(target);
        const uint64_t durable = magma_->GetWALDurableLSN();

        std::vector<PendingDurable> ready;
        {
            std::lock_guard<std::mutex> g(durableMu_);
            while (!durableQueue_.empty() &&
                   durableQueue_.front().lsn <= durable) {
                ready.push_back(std::move(durableQueue_.front()));
                durableQueue_.pop_front();
            }
        }
        if (ready.empty()) {
            // Watermark did not move: the log is wedged or shutting down.
            // Do not spin on it.
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }

        // Group the responses by event base and post one task per IO
        // thread, not one per request. A completion pass covers hundreds of
        // requests spread over a handful of loops, and the per-request
        // runInEventBaseThread hop was costing more CPU on the IO threads
        // than the writers spent producing the batch.
        folly::F14FastMap<folly::EventBase*, std::vector<Request*>> byLoop;
        for (auto& p : ready) {
            const bool ok = p.status.IsOK();
            for (auto* req : p.reqs) {
                // A recycled Request is already OK; only a failure is worth
                // the string copy in Status::operator=.
                if (!ok) {
                    req->resultStatus = p.status;
                }
                byLoop[req->evb].push_back(req);
            }
            hotStatAdd(ok ? gDispStats.cmdSetResp : gDispStats.cmdSetRespErr,
                       p.reqs.size());
            // The queue budget is released only now, so it bounds writes
            // that are acknowledged-pending rather than merely unwritten.
            if (durableBucket_) {
                durableBucket_->queuedBytes_.fetch_sub(
                        p.bytes, std::memory_order_relaxed);
            }
        }

        // Each parked request holds a reservation on the loop it will be
        // answered on, so the loop cannot be retired underneath us. The
        // reservation is dropped on that loop, after the response.
        for (auto& [evb, reqs] : byLoop) {
            evb->runInEventBaseThread([v = std::move(reqs)]() {
                for (auto* req : v) {
                    req->conn->sendWriteResponse(req);
                }
            });
        }
    }
}

void Shard::AwaitDurable(PendingDurable&& p) {
    {
        std::lock_guard<std::mutex> g(durableMu_);
        durableQueue_.push_back(std::move(p));
    }
    durableCv_.notify_one();
}

void Shard::CreatePools(size_t numWriters,
                        size_t numReaders,
                        size_t queueSize,
                        Bucket* bucket) {
    if (gAsyncDurable && bucket && bucket->IsDurable()) {
        durableBucket_ = bucket;
        durableThread_ = std::thread([this]() { durableLoop(); });
    }
    writerPool_ = std::make_unique<WriterPool>(numWriters, queueSize, bucket);
    readerPool_ = std::make_unique<ReaderPool>(numReaders, queueSize, bucket);
}

Status Shard::Open() {
    return magma_->Open();
}

void Shard::Close() {
    if (magma_) {
        // Force everything buffered out to disk before closing. WriteDocs
        // returning OK only means magma accepted the write, not that it is on
        // disk, and nothing else in this server ever syncs. Without this, a
        // clean shutdown lost the most recently written data: after a 200M key
        // load, the first keys read back fine while 38% of the last ones were
        // gone, the loss rising steadily towards the end of the load.
        auto status = magma_->Sync(true /* flushAll */);
        if (!status.IsOK()) {
            spdlog::error("Sync failed during shutdown: {} - recently written "
                          "data may be lost",
                          status.String());
        }
        magma_->Close();
    }
}

// ---- WriterPool ----

// Release ownership of a per-vbucket queue and hand any work that arrived in
// the meantime to the pool. MUST be the only way a worker clears `scheduled`.
//
// The store is seq_cst on purpose. Pushers do `insertHead(); if
// (!scheduled.exchange(true)) submit`. If our store(false) were only `release`,
// x86 may reorder it after the list.empty() load below (store-buffer
// forwarding), letting us see a stale empty head while the pusher still sees
// scheduled==true - a lost wakeup that parks every worker on an empty task
// queue with requests stranded in the lists (observed live: 16,384 stranded,
// all threads in blockingRead). The recheck after the store closes the window
// in which a push landed between our sweep and our release.
template <class Q, class Task>
static inline void releaseVBQueue(VBQueue& vbq, Q& taskQueue, Task retask) {
    vbq.scheduled.store(false, std::memory_order_seq_cst);
    if (!vbq.list.empty() &&
        !vbq.scheduled.exchange(true, std::memory_order_acq_rel)) {
        taskQueue.blockingWrite(std::move(retask));
    }
}

WriterPool::WriterPool(size_t numThreads, size_t queueSize, Bucket* bucket)
    : taskQueue_(queueSize), deferQueue_(queueSize), bucket_(bucket) {
    deferThread_ = std::thread([this]() {
        pthread_setname_np(pthread_self(), "fx:coalesce");
        deferLoop();
    });
    for (size_t i = 0; i < numThreads; i++) {
        threads_.emplace_back([this]() {
            pthread_setname_np(pthread_self(), "fx:writer");
            workerLoop();
        });
    }
}

WriterPool::~WriterPool() {
    Shutdown();
}

void WriterPool::Submit(PersistTask task) {
    taskQueue_.blockingWrite(std::move(task));
}

void WriterPool::SubmitOrDefer(PersistTask task, VBQueue& vbq) {
    const uint64_t now = steadyNowNs();
    if (shouldDeferWrite(vbq, now)) {
        deferQueue_.blockingWrite(Deferred{
                vbq.lastWriteNs.load(std::memory_order_relaxed) +
                        gWriteCoalesceNs,
                task});
        return;
    }
    taskQueue_.blockingWrite(std::move(task));
}

void WriterPool::deferLoop() {
    for (;;) {
        Deferred d;
        deferQueue_.blockingRead(d);
        if (!d.task.shard) {
            // Shutdown: hand over whatever is still waiting, then exit.
            while (deferQueue_.read(d)) {
                if (d.task.shard) {
                    taskQueue_.blockingWrite(std::move(d.task));
                }
            }
            break;
        }
        const uint64_t now = steadyNowNs();
        if (d.notBeforeNs > now) {
            std::this_thread::sleep_for(
                    std::chrono::nanoseconds(d.notBeforeNs - now));
        }
        taskQueue_.blockingWrite(std::move(d.task));
    }
}

void WriterPool::Shutdown() {
    if (shutdown_.exchange(true)) {
        return;
    }
    // Flush the coalescing queue into the task queue first so nothing is
    // still waiting out its interval when the workers see their sentinels.
    deferQueue_.blockingWrite(Deferred{0, PersistTask{nullptr, 0}});
    if (deferThread_.joinable()) {
        deferThread_.join();
    }
    // The sentinels queue behind whatever work is already pending. The queue
    // is FIFO, so each worker executes every task enqueued before this point
    // and only then reads its sentinel and exits.
    for (size_t i = 0; i < threads_.size(); i++) {
        taskQueue_.blockingWrite(PersistTask{nullptr, 0});
    }
    for (auto& t : threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

size_t gDispatchBatch = 16;

namespace {
// Groups completed requests by target EventBase so one runInEventBaseThread
// carries several responses. Reused per reader thread; no per-batch alloc in
// steady state.
class DispatchAccum {
public:
    void Add(Request* req) {
        if (gDispatchBatch <= 1) {
            auto* conn = req->conn;
            req->evb->runInEventBaseThread(
                    [conn, req]() { conn->sendGetResponse(req); });
            return;
        }
        auto& vec = slotFor(req->evb);
        vec.push_back(req);
        if (vec.size() >= gDispatchBatch) {
            post(req->evb, vec);
        }
    }

    void FlushAll() {
        for (auto& g : groups_) {
            if (!g.second.empty()) {
                post(g.first, g.second);
            }
        }
        groups_.clear();
    }

private:
    // A GetDocs batch spans few distinct EventBases, so linear scan beats a map.
    std::vector<Request*>& slotFor(folly::EventBase* evb) {
        for (auto& g : groups_) {
            if (g.first == evb) {
                return g.second;
            }
        }
        groups_.emplace_back(evb, std::vector<Request*>{});
        groups_.back().second.reserve(gDispatchBatch);
        return groups_.back().second;
    }

    static void post(folly::EventBase* evb, std::vector<Request*>& vec) {
        std::vector<Request*> batch;
        batch.swap(vec);
        evb->runInEventBaseThread([batch = std::move(batch)]() mutable {
            for (auto* r : batch) {
                r->conn->sendGetResponse(r);
            }
        });
    }

    std::vector<std::pair<folly::EventBase*, std::vector<Request*>>> groups_;
};
} // namespace


void WriterPool::workerLoop() {
    // Exit on the sentinel only, never on the shutdown flag. Testing the flag
    // here let a worker that had just finished a task return immediately and
    // leave the rest of the queue unwritten, discarding writes the client had
    // already been told were stored. Callers drain first - see
    // Bucket::DrainWrites().
    for (;;) {
        PersistTask task;
        taskQueue_.blockingRead(task);
        if (!task.shard) {
            break; // sentinel
        }
        executePersist(task);
    }
}

// Items accumulated during WriteDocs. Re-arm through the queue for
// fairness with other vbs, deferred if this batch was small. Same seq_cst
// store and recheck as releaseVBQueue - see the comment there.
static inline void releaseVBQueueAfterWrite(VBQueue& vbq,
                                            Shard* shard,
                                            uint16_t vbid,
                                            WriterPool& pool) {
    vbq.scheduled.store(false, std::memory_order_seq_cst);
    if (!vbq.list.empty() &&
        !vbq.scheduled.exchange(true, std::memory_order_acq_rel)) {
        pool.SubmitOrDefer(PersistTask{shard, vbid}, vbq);
    }
}

void WriterPool::executePersist(PersistTask& task) {
    auto* shard = task.shard;
    auto& vbq = shard->GetVBWriteQueue(task.vbid);

    // Sweep all pending items for this vb
    std::vector<Request*> batch;
    batch.reserve(256);
    vbq.list.sweep([&](Request* req) { batch.push_back(req); });
    vbq.pending.fetch_sub(static_cast<uint32_t>(batch.size()),
                          std::memory_order_relaxed);

    if (batch.empty()) {
        releaseVBQueue(vbq, taskQueue_, PersistTask{shard, task.vbid});
        return;
    }
    std::reverse(batch.begin(), batch.end());

    // Sort the batch by key, arrival order within a key. Two reasons.
    // Repeats within one batch are common under skewed keys (Zipf 0.99: the
    // top key alone is ~5% of all writes) and every one costs a memtable
    // insert, log bytes and a flush and compaction pass before GC removes
    // it; sorted, the newest write per key is the last of a run and the
    // rest are dropped without hashing. And magma's skiplist has a
    // sequential-insert fast path (reuse the last insert's predecessors when
    // the new key sorts right after it), which key order feeds. Seqnos are
    // assigned in this order, so within one batch they follow key order,
    // not arrival order; nothing here exposes cross-key ordering. Dropped
    // requests are answered/recycled with the survivors below.
    std::vector<Request*> dropped;
    const bool doSort =
            batch.size() > 1 &&
            (gBatchSort == BatchSort::Always ||
             (gBatchSort == BatchSort::Auto &&
              estimateDupFraction(batch) >= gSortDupThreshold));
    if (doSort) {
        hotStatAdd(gDispStats.writeBatchesSorted);
        // Sort a compact (key prefix, length, index) array rather than the
        // pointers: comparing through the pointers touched two scattered
        // Requests per compare and cost more than the memtable insert it
        // was meant to speed up. Keys of 8 bytes or fewer are decided by
        // prefix and length alone and never dereference.
        struct SortKey {
            uint64_t prefix;
            uint32_t idx;
            uint32_t len;
        };
        std::vector<SortKey> keys(batch.size());
        for (size_t i = 0; i < batch.size(); i++) {
            const auto& k = batch[i]->key;
            keys[i] = {keyPrefix(k),
                       static_cast<uint32_t>(i),
                       static_cast<uint32_t>(k.Len())};
        }
        auto fullKey = [&](uint32_t i) {
            return std::string_view(batch[i]->key.Data(), batch[i]->key.Len());
        };
        auto sameKey = [&](const SortKey& a, const SortKey& b) {
            return a.prefix == b.prefix && a.len == b.len &&
                   (a.len <= 8 || fullKey(a.idx) == fullKey(b.idx));
        };
        std::sort(keys.begin(),
                  keys.end(),
                  [&](const SortKey& a, const SortKey& b) {
                      if (a.prefix != b.prefix) {
                          return a.prefix < b.prefix;
                      }
                      if (a.len <= 8 && b.len <= 8) {
                          return a.len != b.len ? a.len < b.len : a.idx < b.idx;
                      }
                      const auto ka = fullKey(a.idx), kb = fullKey(b.idx);
                      if (ka != kb) {
                          return ka < kb;
                      }
                      return a.idx < b.idx;
                  });
        // Newest write per key wins; it is the last of each equal run.
        // Seqnos are assigned below in this order, so within one batch they
        // follow key order rather than arrival order. Nothing here exposes
        // cross-key ordering, and magma only requires per-key increase.
        std::vector<Request*> sorted;
        sorted.reserve(batch.size());
        for (size_t i = 0; i < keys.size(); i++) {
            auto* req = batch[keys[i].idx];
            if (i + 1 < keys.size() && sameKey(keys[i], keys[i + 1])) {
                dropped.push_back(req);
            } else {
                sorted.push_back(req);
            }
        }
        batch.swap(sorted);
        if (!dropped.empty()) {
            hotStatAdd(gDispStats.writeDedups, dropped.size());
        }
    }

    // Build WriteOperation batch
    std::vector<Magma::WriteOperation> ops;
    ops.reserve(batch.size());
    size_t batchBytes = 0;
    // DocMeta is packed and written to disk as-is, so the vector's storage
    // is the encoded form: one allocation per batch, not a string per op.
    std::vector<DocMeta> metas(batch.size());
    const uint64_t cas =
            std::chrono::system_clock::now().time_since_epoch().count();

    for (size_t i = 0; i < batch.size(); i++) {
        auto* req = batch[i];
        uint64_t seqno = shard->NextSeqno(task.vbid);
        req->resultSeqno = seqno;

        DocMeta& dm = metas[i];
        dm.seqno = seqno;
        dm.cas = cas;
        dm.valueSize = req->value.Len();
        dm.flags = req->flags;
        dm.expiry = req->expiry;
        dm.datatype = req->datatype;
        dm.deleted =
                (req->opcode == static_cast<uint8_t>(Opcode::Delete)) ? 1 : 0;

        batchBytes += req->key.Len() + req->value.Len() + sizeof(Request);
        Slice meta(reinterpret_cast<const char*>(&dm), sizeof(DocMeta));

        if (req->opcode == static_cast<uint8_t>(Opcode::Delete)) {
            ops.push_back(Magma::WriteOperation::NewDocDelete(req->key, meta));
        } else {
            ops.push_back(Magma::WriteOperation::NewDocUpsert(
                    req->key, meta, req->value));
        }
    }

    // batchBytes was summed in the loop above; a separate pass over the
    // batch to add it up was another walk over cold memory.
    hotStatAdd(gDispStats.writeBatches);
    hotStatAdd(gDispStats.writeBatchItems, ops.size());

    auto status = shard->GetMagma()->WriteDocs(task.vbid, ops);
    vbq.lastWriteNs.store(steadyNowNs(), std::memory_order_relaxed);

    // Release the cache pins these writes took in handleSet/handleDelete.
    // Done whether or not WriteDocs succeeded: a failed write is reported to
    // the client (durable) or already lost (async), and keeping the entry
    // pinned would only leak cache budget.
    if (auto* cache = bucket_->GetCache()) {
        for (auto* req : batch) {
            cache->MarkPersisted(
                    task.vbid,
                    std::string_view(req->key.Data(), req->key.Len()),
                    status.IsOK() ? req->resultSeqno : 0);
        }
    }

    // Dropped duplicates are done too: same outcome as the batch. Their
    // bytes were charged in StageWrite like everyone else's, so they must
    // be returned here as well.
    for (auto* req : dropped) {
        batchBytes += req->key.Len() + req->value.Len() + sizeof(Request);
    }
    batch.insert(batch.end(), dropped.begin(), dropped.end());

    if (bucket_->IsDurable() && gAsyncDurable && status.IsOK()) {
        // Hand the batch to the shard's completion thread and move on. The
        // records are already in the memtable and staged in the log; what
        // is left is the log reaching disk, and one thread can wait for
        // that on behalf of every writer. queuedBytes_ is released there,
        // so the write-queue limit bounds acknowledged-pending bytes.
        Shard::PendingDurable p;
        p.lsn = shard->GetMagma()->GetWALTailLSN();
        p.status = status;
        p.bytes = batchBytes;
        p.reqs = std::move(batch);
        shard->AwaitDurable(std::move(p));
        batch.clear();
        releaseVBQueueAfterWrite(vbq, shard, task.vbid, *this);
        return;
    }

    bucket_->queuedBytes_.fetch_sub(batchBytes, std::memory_order_relaxed);

    if (bucket_->IsDurable()) {
        // Durable mode: send response after WriteDocs completes
        for (auto* req : batch) {
            req->resultStatus = status;
            if (status.IsOK()) {
                hotStatAdd(gDispStats.cmdSetResp);
            } else {
                hotStatAdd(gDispStats.cmdSetRespErr);
            }
            auto* conn = req->conn;
            req->evb->runInEventBaseThread(
                    [conn, req]() { conn->sendWriteResponse(req); });
        }
    } else {
        // Async mode: response already sent by IO thread. Recycle the
        // requests for the IO threads to reuse; deleting here frees memory
        // another thread allocated.
        if (status.IsOK()) {
            hotStatAdd(gDispStats.cmdSetResp, batch.size());
        } else {
            hotStatAdd(gDispStats.cmdSetRespErr, batch.size());
        }
        for (size_t i = 0; i < batch.size(); i++) {
            batch[i]->reset();
            batch[i]->hook.next = i + 1 < batch.size() ? batch[i + 1] : nullptr;
        }
        if (!shard->RecycleRequests(batch[0])) {
            for (auto* req : batch) {
                delete req;
            }
        }
    }

    releaseVBQueueAfterWrite(vbq, shard, task.vbid, *this);
}

// ---- ReaderPool ----

namespace {
// Read-miss fill. PutIfAbsent, never Put: a key with a queued write is pinned
// in the cache, so a fill can only land for keys whose disk version is
// current (see kvcache.h).
inline void fillCache(Bucket* bucket,
                      uint16_t vbid,
                      const Request* req,
                      const DocMeta& dm,
                      const Slice& value) {
    if (auto* cache = bucket->GetCache()) {
        cache->PutIfAbsent(vbid,
                           std::string_view(req->key.Data(), req->key.Len()),
                           std::string_view(value.Data(), value.Len()),
                           dm.seqno,
                           dm.flags,
                           dm.expiry,
                           dm.datatype);
    }
}
} // namespace

namespace {
inline uint64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
}
// Sentinels travel through the task queue with a null shard: vbid 0 stops a
// worker for shutdown, vbid 1 retires one worker for Shrink.
constexpr uint16_t kStopSentinel = 0;
constexpr uint16_t kRetireSentinel = 1;
} // namespace

ReaderPool::ReaderPool(size_t numThreads, size_t queueSize, Bucket* bucket)
    : taskQueue_(queueSize), bucket_(bucket) {
    for (size_t i = 0; i < numThreads; i++) {
        spawn();
    }
}

ReaderPool::~ReaderPool() {
    Shutdown();
}

void ReaderPool::Submit(ReadTask task) {
    task.enqueuedNs = nowNs();
    taskQueue_.blockingWrite(std::move(task));
}

void ReaderPool::spawn() {
    auto w = std::make_unique<Worker>();
    auto* self = w.get();
    w->thread = std::thread([this, self]() {
        pthread_setname_np(pthread_self(), "fx:reader");
        workerLoop(self);
    });
    workers_.push_back(std::move(w));
    live_.fetch_add(1, std::memory_order_relaxed);
}

void ReaderPool::Grow(size_t n) {
    for (size_t i = 0; i < n; i++) {
        spawn();
    }
}

void ReaderPool::Shrink(size_t n) {
    n = std::min(n, live_.load(std::memory_order_relaxed));
    live_.fetch_sub(n, std::memory_order_relaxed);
    for (size_t i = 0; i < n; i++) {
        taskQueue_.blockingWrite(ReadTask{nullptr, kRetireSentinel, 0});
    }
}

void ReaderPool::Reap() {
    for (auto it = workers_.begin(); it != workers_.end();) {
        if ((*it)->done.load(std::memory_order_acquire)) {
            (*it)->thread.join();
            it = workers_.erase(it);
        } else {
            ++it;
        }
    }
}

ReaderPool::Sample ReaderPool::TakeSample() {
    Sample total;
    for (const auto& a : acct_) {
        total.busyNs += a.busyNs.load(std::memory_order_relaxed);
        total.waitNs += a.waitNs.load(std::memory_order_relaxed);
        total.tasks += a.tasks.load(std::memory_order_relaxed);
        total.items += a.items.load(std::memory_order_relaxed);
    }
    Sample delta{total.busyNs - lastSample_.busyNs,
                 total.waitNs - lastSample_.waitNs,
                 total.tasks - lastSample_.tasks,
                 total.items - lastSample_.items};
    lastSample_ = total;
    return delta;
}

void ReaderPool::Shutdown() {
    if (shutdown_.exchange(true)) {
        return;
    }
    for (size_t i = 0; i < workers_.size(); i++) {
        taskQueue_.blockingWrite(ReadTask{nullptr, kStopSentinel, 0});
    }
    for (auto& w : workers_) {
        if (w->thread.joinable()) {
            w->thread.join();
        }
    }
    workers_.clear();
    live_.store(0, std::memory_order_relaxed);
}

void ReaderPool::workerLoop(Worker* self) {
    // Brief busy-spin before parking. At moderate rates (e.g. 300K ops/s with
    // 64 readers → ~3 ops/reader/ms) the inter-arrival gap is shorter than
    // the cost of waking from intel_idle (10-50µs). Spinning keeps the thread
    // hot and shaves wake-up tail latency. Set MAGMA_READER_SPIN_ITERS=0 to
    // disable. Default chosen so total spin time ~50µs on typical hw.
    static const int kSpinIters = []() {
        const char* env = std::getenv("MAGMA_READER_SPIN_ITERS");
        return env ? std::atoi(env) : 2000;
    }();
    while (!shutdown_.load(std::memory_order_relaxed)) {
        ReadTask task;
        bool got = false;
        for (int i = 0; i < kSpinIters; i++) {
            if (taskQueue_.read(task)) {
                got = true;
                break;
            }
#if defined(__x86_64__) || defined(__i386__)
            asm volatile("pause" ::: "memory");
#elif defined(__aarch64__)
            asm volatile("yield" ::: "memory");
#endif
        }
        if (!got) {
            taskQueue_.blockingRead(task);
        }
        if (!task.shard) {
            // Stop, or retire this one worker. Either way this thread ends.
            break;
        }
        auto& acct = acct_[statSlot()];
        const uint64_t t0 = nowNs();
        const size_t served = executeRead(task);
        const uint64_t t1 = nowNs();
        acct.busyNs.fetch_add(t1 - t0, std::memory_order_relaxed);
        acct.waitNs.fetch_add(t0 - task.enqueuedNs, std::memory_order_relaxed);
        acct.tasks.fetch_add(1, std::memory_order_relaxed);
        acct.items.fetch_add(served, std::memory_order_relaxed);
    }
    self->done.store(true, std::memory_order_release);
}

size_t ReaderPool::executeRead(ReadTask& task) {
    auto* shard = task.shard;
    auto& vbq = shard->GetVBReadQueue(task.vbid);
    Magma::FetchBuffer idxBuf, seqBuf;

    // Sweep pending reads for this vbucket, cap batch size to allow
    // multiple reader threads to work on the same VB concurrently.
    // Raised from 32 → 128 so a single executeRead call can hand magma::GetDocs
    // a wide batch; with magmaCfg.IOQueueDepth=16 this keeps coroutines busy
    // and pushes NVMe queue depth from aqu-sz≈37 (43%% of NVMe peak) toward
    // saturation. Excess items are pushed back and re-scheduled to a peer
    // reader thread, preserving multi-thread-per-vbucket parallelism.
    const size_t kMaxReadBatch = gMaxReadBatch;
    std::vector<Request*> batch;
    batch.reserve(kMaxReadBatch);
    vbq.list.sweep([&](Request* req) { batch.push_back(req); });

    if (batch.empty()) {
        releaseVBQueue(vbq, taskQueue_, ReadTask{shard, task.vbid, nowNs()});
        return 0;
    }
    std::reverse(batch.begin(), batch.end());

    // If we swept more than kMaxReadBatch, push the excess back onto the list.
    // Do NOT submit a second task here: that created a SECOND concurrent owner
    // of this vbucket, and the two owners race on the scheduled-flag release,
    // losing a wakeup - 16,384 requests stranded with every worker parked on an
    // empty task queue. The excess is instead left in the list and picked up by
    // the single re-submit releaseVBQueue() does at the tail of this function,
    // preserving the invariant of exactly one owner per vbucket at a time.
    if (batch.size() > kMaxReadBatch) {
        for (size_t i = kMaxReadBatch; i < batch.size(); i++) {
            vbq.list.insertHead(batch[i]);
        }
        batch.resize(kMaxReadBatch);
    }

    hotStatAdd(gDispStats.readBatches);
    hotStatAdd(gDispStats.readBatchItems, batch.size());
    hotStatSub(gDispStats.queuedGets, batch.size());

    if (batch.size() == 1) {
        // Single Get — no OperationsList overhead
        auto* req = batch[0];
        Slice meta, value;
        auto status = shard->GetMagma()->Get(
                task.vbid, req->key, idxBuf, seqBuf, meta, value);
        if (status.IsOK() && !status.IsOkDocNotFound()) {
            req->resultStatus = status;
            auto dm = DocMeta::decode(meta);
            req->resultFlags = dm.flags;
            req->resultSeqno = dm.seqno;
            req->resultDatatype = dm.datatype;
            if (value.Len() > 0) {
                req->responseBuf.assign(
                        reinterpret_cast<const uint8_t*>(value.Data()),
                        reinterpret_cast<const uint8_t*>(value.Data()) +
                                value.Len());
            }
            fillCache(bucket_, task.vbid, req, dm, value);
            hotStatAdd(gDispStats.cmdGetResp);
        } else {
            // OkDocNotFound is treated as a miss — sendGetResponse needs
            // a non-OK status to emit mcbp KeyNotFound.
            req->resultStatus =
                    status.IsOkDocNotFound()
                            ? Status(Status::Code::Internal, "key-not-found")
                            : status;
            hotStatAdd(gDispStats.cmdGetRespMiss);
        }
        auto* conn = req->conn;
        req->evb->runInEventBaseThread(
                [conn, req]() { conn->sendGetResponse(req); });
    } else {
        // Batch GetDocs
        OperationsList<Magma::GetOperation> getOps;
        for (auto* req : batch) {
            getOps.Add(Magma::GetOperation(req->key, req));
        }
        thread_local DispatchAccum accum;
        shard->GetMagma()->GetDocs(
                task.vbid,
                getOps,
                [&accum, this, &task](Status s,
                   const Magma::GetOperation& op,
                   const Slice& meta,
                   const Slice& value) {
                    auto* req = static_cast<Request*>(op.UserContext);
                    req->resultStatus = s;
                    if (s.IsOK() && !s.IsOkDocNotFound()) {
                        auto dm = DocMeta::decode(meta);
                        req->resultFlags = dm.flags;
                        req->resultSeqno = dm.seqno;
                        if (value.Len() > 0) {
                            req->responseBuf.assign(
                                    reinterpret_cast<const uint8_t*>(
                                            value.Data()),
                                    reinterpret_cast<const uint8_t*>(
                                            value.Data()) +
                                            value.Len());
                        }
                        req->resultDatatype = dm.datatype;
                        fillCache(bucket_, task.vbid, req, dm, value);
                        hotStatAdd(gDispStats.cmdGetResp);
                    } else {
                        // OkDocNotFound (legitimate miss) or other error.
                        // Force resultStatus to a non-OK so sendGetResponse
                        // builds an mcbp KeyNotFound error rather than a
                        // success-with-empty-value (which silently lies to
                        // callers and skews benchmarks).
                        if (s.IsOkDocNotFound()) {
                            req->resultStatus = Status(Status::Code::Internal,
                                                       "key-not-found");
                        }
                        hotStatAdd(gDispStats.cmdGetRespMiss);
                    }
                    // Dispatch the response as soon as THIS op completes
                    // instead of after the whole GetDocs batch returns.
                    // With batch=N and coroutine fanout=IOQueueDepth, an op
                    // finishing in the first IO wave otherwise waits
                    // ~(N/IOQueueDepth) x NVMe-latency for the last wave —
                    // pure intra-batch head-of-line blocking that dominates
                    // p99/p999 under load. Lifetime: magma does not touch a
                    // GetOperation (incl. its key Slice into req->dataBuf)
                    // after its completion callback fires, so releasing the
                    // request from the evb thread before GetDocs returns is
                    // safe.
                    accum.Add(req);
                });
        accum.FlushAll();
    }

    releaseVBQueue(vbq, taskQueue_, ReadTask{shard, task.vbid, nowNs()});
    return batch.size();
}

// ---- ReaderPoolGroup ----

size_t ReaderPoolGroup::Size() const {
    size_t n = 0;
    for (auto* p : pools_) {
        n += p->Size();
    }
    return n;
}

void ReaderPoolGroup::Grow(size_t n) {
    const size_t per = std::max<size_t>(1, n / pools_.size());
    for (auto* p : pools_) {
        p->Grow(per);
    }
}

void ReaderPoolGroup::Shrink(size_t n) {
    const size_t per = std::max<size_t>(1, n / pools_.size());
    for (auto* p : pools_) {
        p->Shrink(per);
    }
}

void ReaderPoolGroup::Reap() {
    for (auto* p : pools_) {
        p->Reap();
    }
}

PoolSample ReaderPoolGroup::Sample(double wallSec) {
    PoolSample s;
    uint64_t busyNs = 0, waitNs = 0, tasks = 0, items = 0;
    for (auto* p : pools_) {
        const auto d = p->TakeSample();
        busyNs += d.busyNs;
        waitNs += d.waitNs;
        tasks += d.tasks;
        items += d.items;
        s.size += p->Size();
        if (p->Size() > 0 && wallSec > 0) {
            s.busyMax = std::max(s.busyMax,
                                 d.busyNs / (wallSec * 1e9 * p->Size()));
        }
    }
    if (s.size > 0 && wallSec > 0) {
        s.busyMean = busyNs / (wallSec * 1e9 * s.size);
    }
    s.avgBatch = tasks > 0 ? static_cast<double>(items) / tasks : 0;
    s.waitUs = tasks > 0 ? waitNs / 1e3 / tasks : 0;
    return s;
}

// ---- Bucket ----

Bucket::Bucket(const std::string& name,
               const std::string& dataDir,
               uint16_t numShards,
               size_t numWriters,
               size_t numReaders,
               uint16_t numVBuckets,
               bool durable,
               size_t writeQueueMemLimit,
               size_t echoGetSize,
               const Magma::Config& baseCfg)
    : name_(name),
      dataDir_(dataDir),
      numShards_(numShards),
      numVBuckets_(numVBuckets),
      durable_(durable),
      writeQueueMemLimit_(writeQueueMemLimit),
      echoGetSize_(echoGetSize) {
    if (echoGetSize_ > 0) {
        echoGetValue_.assign(echoGetSize_, 'X');
    }
    for (uint16_t i = 0; i < numShards; i++) {
        auto shardPath = dataDir + "/" + name + "/shard-" + std::to_string(i);
        std::filesystem::create_directories(shardPath);
        shards_.push_back(std::make_unique<Shard>(i, shardPath, baseCfg));
    }

    // Sharded pools: divide the user-requested thread budget across shards
    // so each shard's pool only sees tasks for its own vbuckets. This kills
    // the central-MPMC contention that was the bottleneck preventing
    // aqu-sz from climbing past ~130 at high reader counts.
    // Honest thread budget: shard i gets floor(N/S) + (i < N%S ? 1 : 0), min 1.
    // The previous max(1, N/S) floored the count and dropped the remainder, so
    // with 32 shards every --readers between 16 and 63 produced the same 32
    // threads. One reader owns one vbucket at a time, so the real reader count
    // *is* the read concurrency - the flag has to mean what it says, or a
    // sweep over it measures nothing.
    size_t writersPerShard = std::max<size_t>(1, numWriters / numShards);
    size_t readersPerShard = std::max<size_t>(1, numReaders / numShards);
    size_t writersRem = numWriters > numShards ? numWriters % numShards : 0;
    size_t readersRem = numReaders > numShards ? numReaders % numShards : 0;
    // Per-shard queue size — total queue capacity stays roughly the same.
    size_t perShardQueueSize = std::max<size_t>(64, 65536 / numShards);
    size_t totalWriters = 0, totalReaders = 0;
    for (size_t i = 0; i < shards_.size(); i++) {
        size_t w = writersPerShard + (i < writersRem ? 1 : 0);
        size_t r = readersPerShard + (i < readersRem ? 1 : 0);
        totalWriters += w;
        totalReaders += r;
        shards_[i]->CreatePools(w, r, perShardQueueSize, this);
    }
    // Print the totals actually created, not the per-shard figure times the
    // shard count, so the log cannot claim a budget that was never allocated.
    spdlog::info(
            "Sharded pools: {} shards × ~{} writers + ~{} readers each "
            "(total {} writers, {} readers)",
            numShards,
            writersPerShard,
            readersPerShard,
            totalWriters,
            totalReaders);
}

Bucket::~Bucket() {
    Close();
}

Status Bucket::Open() {
    for (auto& shard : shards_) {
        auto s = shard->Open();
        if (!s.IsOK()) {
            return s;
        }
    }
    // Pre-create kvstores and recover seqnos
    for (uint16_t vbid = 0; vbid < numVBuckets_; vbid++) {
        auto& shard = GetShard(vbid);
        shard.GetMagma()->CreateKVStore(vbid);
        // Mark created regardless of CreateKVStore return — on restart it
        // returns "already exists" but the kvstore is still openable. We
        // need this bit set so admin paths (CompactAll) can iterate.
        shard.MarkKVStoreCreated(vbid);
        Magma::SeqNo seq = 0;
        shard.GetMagma()->GetMaxSeqno(vbid, seq);
        shard.RecoverSeqno(vbid, seq);
    }
    spdlog::info("Pre-created {} kvstores across {} shards",
                 numVBuckets_,
                 numShards_);
    return Status::OK();
}

void Bucket::DrainWrites(std::chrono::milliseconds timeout) {
    // A write is acknowledged to the client as soon as it is queued, so at
    // this point queuedBytes_ is data the client believes is stored but that
    // has not reached magma yet. Let the writer pools finish before anything
    // is torn down.
    size_t pending = queuedBytes_.load(std::memory_order_relaxed);
    if (pending == 0) {
        return;
    }

    spdlog::info("Draining {} bytes of acknowledged writes before shutdown",
                 pending);

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
        pending = queuedBytes_.load(std::memory_order_relaxed);
        if (pending == 0) {
            spdlog::info("Write queue drained");
            return;
        }
        if (std::chrono::steady_clock::now() >= deadline) {
            spdlog::error("Write queue did not drain within {}ms - {} bytes "
                          "of acknowledged writes will be lost",
                          timeout.count(),
                          pending);
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

void Bucket::Close() {
    // Drain before stopping the pools. Shutting them down with work still
    // queued discarded acknowledged writes: after a 200M key load the most
    // recently written ~29% of the dataset was simply absent, with nothing
    // in the log to say so.
    DrainWrites();

    for (auto& shard : shards_) {
        if (auto* w = shard->GetWriterPool()) {
            w->Shutdown();
        }
        if (auto* r = shard->GetReaderPool()) {
            r->Shutdown();
        }
    }
    // Shards are independent magma instances; closing them one after
    // another took over a second each and left the process alive 10 s past
    // SIGTERM.
    std::vector<std::thread> closers;
    closers.reserve(shards_.size());
    for (auto& shard : shards_) {
        closers.emplace_back([&shard]() { shard->Close(); });
    }
    for (auto& t : closers) {
        t.join();
    }
}

void Bucket::CompactAll() {
    // Walk every kvstore (one per vbucket) on every shard and trigger a
    // full compaction. The Magma::CompactKVStore API blocks until the
    // compaction finishes, so we run them in parallel across shards
    // (each shard already has its own compactor pool).
    std::vector<std::thread> workers;
    workers.reserve(numShards_);
    for (uint16_t s = 0; s < numShards_; s++) {
        workers.emplace_back([this, s]() {
            for (uint16_t vb = s; vb < numVBuckets_; vb += numShards_) {
                auto& shard = *shards_[s];
                if (!shard.IsKVStoreCreated(vb)) {
                    continue;
                }
                shard.GetMagma()->CompactKVStore(vb, Magma::StoreType::All);
            }
        });
    }
    for (auto& w : workers) {
        w.join();
    }
}

std::string Bucket::GetStatsJson() {
    nlohmann::json merged;
    bool first = true;
    for (auto& shard : shards_) {
        Magma::MagmaStats stats;
        shard->GetMagma()->GetStats(stats);
        nlohmann::json j = stats;
        if (first) {
            merged = j;
            first = false;
        } else {
            for (auto& [key, val] : j.items()) {
                if (val.is_number_integer()) {
                    merged[key] =
                            merged[key].get<int64_t>() + val.get<int64_t>();
                } else if (val.is_number_float()) {
                    merged[key] = merged[key].get<double>() + val.get<double>();
                }
            }
        }
    }
    merged["bucket"] = name_;
    merged["numShards"] = numShards_;
    return merged.dump(2);
}

namespace {
// Per-IO-thread staging of writes by vbucket, flushed once per socket wake.
struct StagedVB {
    Request* first{nullptr}; // newest
    Request* last{nullptr}; // oldest
    uint32_t count{0};
};
struct StagingTable {
    std::array<StagedVB, Shard::kMaxVBuckets> vbs{};
    std::vector<uint16_t> touched;
    // Bytes admitted since the last FlushStaged. queuedBytes_ is one cache
    // line shared by every IO thread and writer; charging it once per wake
    // instead of once per request took StageWrite from 34% of IO-thread
    // CPU to noise. The admission check reads a value at most one wake
    // stale, which the 4 GB limit does not notice.
    size_t bytes{0};
};
thread_local StagingTable tlsStaging;
} // namespace

bool Bucket::StageWrite(Request* req) {
    size_t itemSize = req->key.Len() + req->value.Len() + sizeof(Request);
    auto& t = tlsStaging;
    if (queuedBytes_.load(std::memory_order_relaxed) + t.bytes + itemSize >
        writeQueueMemLimit_) {
        return false; // TMPFAIL — over memory limit
    }
    t.bytes += itemSize;

    auto& st = t.vbs[req->vbucket];
    if (st.count == 0) {
        tlsStaging.touched.push_back(req->vbucket);
        st.last = req;
    }
    req->hook.next = st.first;
    st.first = req;
    st.count++;
    return true;
}

void Bucket::FlushStaged() {
    auto& t = tlsStaging;
    if (t.bytes) {
        queuedBytes_.fetch_add(t.bytes, std::memory_order_relaxed);
        t.bytes = 0;
    }
    for (uint16_t vbid : t.touched) {
        auto& st = t.vbs[vbid];
        auto& shard = GetShard(vbid);
        auto& vbq = shard.GetVBWriteQueue(vbid);
        vbq.list.insertChain(st.first, st.last);
        vbq.pending.fetch_add(st.count, std::memory_order_relaxed);
        st = StagedVB{};
        if (!vbq.scheduled.exchange(true, std::memory_order_acq_rel)) {
            shard.GetWriterPool()->SubmitOrDefer({&shard, vbid}, vbq);
        }
    }
    t.touched.clear();
}

void Bucket::EnqueueRead(Request* req) {
    hotStatAdd(gDispStats.queuedGets);
    auto& shard = GetShard(req->vbucket);
    auto& vbq = shard.GetVBReadQueue(req->vbucket);

    vbq.list.insertHead(req);

    if (!vbq.scheduled.exchange(true, std::memory_order_acq_rel)) {
        shard.GetReaderPool()->Submit({&shard, req->vbucket});
    }
}

} // namespace kvserver
} // namespace magma
