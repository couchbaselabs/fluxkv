#include "engine.h"
#include "connection.h"
#include "include/libmagma/operations.h"

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <filesystem>

namespace magma {
namespace kvserver {

DispatcherStats gDispStats;
// Runtime-tunable read-batch cap (--max-read-batch). 128 matches the
// libaio + IOQueueDepth=16 sweet spot; lower (8-16) is better for sync
// QD=1 multi-thread parallelism.
size_t gMaxReadBatch = 128;
// Gates per-op stat increments — see engine.h.
bool gStatsHotPath = true;

std::string DispatcherStats::toJson() const {
    nlohmann::json j;
    j["cmd_set"] = cmdSet.load(std::memory_order_relaxed);
    j["cmd_get"] = cmdGet.load(std::memory_order_relaxed);
    j["cmd_delete"] = cmdDelete.load(std::memory_order_relaxed);
    j["cmd_set_resp"] = cmdSetResp.load(std::memory_order_relaxed);
    j["cmd_get_resp"] = cmdGetResp.load(std::memory_order_relaxed);
    j["cmd_set_resp_err"] = cmdSetRespErr.load(std::memory_order_relaxed);
    j["cmd_get_resp_miss"] = cmdGetRespMiss.load(std::memory_order_relaxed);
    j["connect_accept"] = connectAccept.load(std::memory_order_relaxed);
    j["connect_close"] = connectClose.load(std::memory_order_relaxed);
    j["write_batches"] = writeBatches.load(std::memory_order_relaxed);
    j["write_batch_items"] = writeBatchItems.load(std::memory_order_relaxed);
    j["read_batches"] = readBatches.load(std::memory_order_relaxed);
    j["read_batch_items"] = readBatchItems.load(std::memory_order_relaxed);
    j["tmp_fails"] = tmpFails.load(std::memory_order_relaxed);
    j["queued_gets"] = queuedGets.load(std::memory_order_relaxed);
    j["bad_magic"] = badMagic.load(std::memory_order_relaxed);
    j["bad_opcode"] = badOpcode.load(std::memory_order_relaxed);
    auto wBatches = writeBatches.load(std::memory_order_relaxed);
    j["avg_write_batch"] =
            wBatches > 0
                    ? (double)writeBatchItems.load(std::memory_order_relaxed) /
                              wBatches
                    : 0.0;
    auto rBatches = readBatches.load(std::memory_order_relaxed);
    j["avg_read_batch"] =
            rBatches > 0
                    ? (double)readBatchItems.load(std::memory_order_relaxed) /
                              rBatches
                    : 0.0;
    j["outstanding_requests"] = (int64_t)cmdSet.load() + cmdGet.load() +
                                cmdDelete.load() - cmdSetResp.load() -
                                cmdGetResp.load() - cmdSetRespErr.load() -
                                cmdGetRespMiss.load();
    return j.dump(2);
}

// ---- Shard ----

Shard::Shard(uint16_t shardId,
             const std::string& path,
             const Magma::Config& cfg)
    : shardId_(shardId) {
    Magma::Config shardCfg = cfg;
    shardCfg.Path = path;
    magma_ = std::make_unique<Magma>(shardCfg);
}

Shard::~Shard() {
    Close();
}

void Shard::CreatePools(size_t numWriters,
                        size_t numReaders,
                        size_t queueSize,
                        Bucket* bucket) {
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
    : taskQueue_(queueSize), bucket_(bucket) {
    for (size_t i = 0; i < numThreads; i++) {
        threads_.emplace_back([this]() { workerLoop(); });
    }
}

WriterPool::~WriterPool() {
    Shutdown();
}

void WriterPool::Submit(PersistTask task) {
    taskQueue_.blockingWrite(std::move(task));
}

void WriterPool::Shutdown() {
    if (shutdown_.exchange(true)) {
        return;
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

void WriterPool::executePersist(PersistTask& task) {
    auto* shard = task.shard;
    auto& vbq = shard->GetVBWriteQueue(task.vbid);

    // Sweep all pending items for this vb
    std::vector<Request*> batch;
    batch.reserve(256);
    vbq.list.sweep([&](Request* req) { batch.push_back(req); });

    if (batch.empty()) {
        releaseVBQueue(vbq, taskQueue_, PersistTask{shard, task.vbid});
        return;
    }
    std::reverse(batch.begin(), batch.end());

    // Build WriteOperation batch
    std::vector<Magma::WriteOperation> ops;
    ops.reserve(batch.size());
    std::vector<std::string> metaStorage(batch.size());

    for (size_t i = 0; i < batch.size(); i++) {
        auto* req = batch[i];
        uint64_t seqno = shard->NextSeqno(task.vbid);
        req->resultSeqno = seqno;

        DocMeta dm;
        dm.seqno = seqno;
        dm.cas = std::chrono::system_clock::now().time_since_epoch().count();
        dm.valueSize = req->value.Len();
        dm.flags = req->flags;
        dm.expiry = req->expiry;
        dm.datatype = req->datatype;
        dm.deleted =
                (req->opcode == static_cast<uint8_t>(Opcode::Delete)) ? 1 : 0;

        metaStorage[i] = dm.encode();
        Slice meta(metaStorage[i]);

        if (req->opcode == static_cast<uint8_t>(Opcode::Delete)) {
            ops.push_back(Magma::WriteOperation::NewDocDelete(req->key, meta));
        } else {
            ops.push_back(Magma::WriteOperation::NewDocUpsert(
                    req->key, meta, req->value));
        }
    }

    hotStatAdd(gDispStats.writeBatches);
    hotStatAdd(gDispStats.writeBatchItems, ops.size());

    auto status = shard->GetMagma()->WriteDocs(task.vbid, ops);

    // Subtract queued bytes
    size_t batchBytes = 0;
    for (auto* req : batch) {
        batchBytes += req->key.Len() + req->value.Len() + sizeof(Request);
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
        // Async mode: response already sent by IO thread. Just free requests.
        for (auto* req : batch) {
            if (status.IsOK()) {
                hotStatAdd(gDispStats.cmdSetResp);
            } else {
                hotStatAdd(gDispStats.cmdSetRespErr);
            }
            delete req;
        }
    }

    // Re-sweep immediately — items accumulated during WriteDocs.
    // If we got more, loop back via the queue for fairness with other vbs.
    releaseVBQueue(vbq, taskQueue_, PersistTask{shard, task.vbid});
}

// ---- ReaderPool ----

ReaderPool::ReaderPool(size_t numThreads, size_t queueSize, Bucket* bucket)
    : taskQueue_(queueSize), bucket_(bucket) {
    for (size_t i = 0; i < numThreads; i++) {
        threads_.emplace_back([this]() { workerLoop(); });
    }
}

ReaderPool::~ReaderPool() {
    Shutdown();
}

void ReaderPool::Submit(ReadTask task) {
    taskQueue_.blockingWrite(std::move(task));
}

void ReaderPool::Shutdown() {
    if (shutdown_.exchange(true)) {
        return;
    }
    for (size_t i = 0; i < threads_.size(); i++) {
        taskQueue_.blockingWrite(ReadTask{nullptr, 0});
    }
    for (auto& t : threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
}

void ReaderPool::workerLoop() {
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
            break;
        }
        executeRead(task);
    }
}

void ReaderPool::executeRead(ReadTask& task) {
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
        releaseVBQueue(vbq, taskQueue_, ReadTask{shard, task.vbid});
        return;
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
        shard->GetMagma()->GetDocs(
                task.vbid,
                getOps,
                [](Status s,
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
                    auto* conn = req->conn;
                    req->evb->runInEventBaseThread(
                            [conn, req]() { conn->sendGetResponse(req); });
                });
    }

    releaseVBQueue(vbq, taskQueue_, ReadTask{shard, task.vbid});
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
    for (auto& shard : shards_) {
        shard->Close();
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

bool Bucket::EnqueueWrite(Request* req) {
    // Memory backpressure check
    size_t itemSize = req->key.Len() + req->value.Len() + sizeof(Request);
    if (queuedBytes_.load(std::memory_order_relaxed) + itemSize >
        writeQueueMemLimit_) {
        return false; // TMPFAIL — over memory limit
    }
    queuedBytes_.fetch_add(itemSize, std::memory_order_relaxed);

    auto& shard = GetShard(req->vbucket);
    auto& vbq = shard.GetVBWriteQueue(req->vbucket);

    // Lock-free push (MPSC: multiple IO threads push)
    vbq.list.insertHead(req);

    // Schedule persistence if not already scheduled — push to THIS shard's
    // own writer pool (no cross-shard MPMC contention).
    if (!vbq.scheduled.exchange(true, std::memory_order_acq_rel)) {
        shard.GetWriterPool()->Submit({&shard, req->vbucket});
    }
    return true;
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
