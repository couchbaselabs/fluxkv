#pragma once

#include "engine.h"
#include "iothread.h"
#include "protocol.h"

#include <folly/io/IOBufQueue.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/AsyncTimeout.h>

#include <chrono>
#include <list>
#include <mutex>
#include <memory>
#include <string>

namespace magma {
namespace kvserver {

class Connection : public folly::AsyncSocket::ReadCallback,
                   public folly::AsyncSocket::WriteCallback {
public:
    Connection(folly::AsyncSocket::UniquePtr socket,
               Bucket* bucket,
               const std::string& clusterConfig,
               const std::string& errorMap);
    ~Connection();

    void start(IOThread* owner);

    // Called from engine threads via evb->runInEventBaseThread
    void sendWriteResponse(Request* req);
    void sendGetResponse(Request* req);
    // --direct-send, any thread: write these GET responses to the socket
    // and finish the requests. All belong to this connection.
    void sendDirect(Request* const* reqs, size_t n);

    // Move this connection to another loop. Called on the owning loop thread.
    // Reading stops at once; once every request in flight has been answered
    // and every write has completed, the socket is detached and re-attached
    // on the target, where reading resumes with whatever was already
    // buffered. Responses to requests in flight are still posted to the old
    // loop, which is why the connection waits for them before leaving.
    // Returns false if the connection is closing or already migrating, in
    // which case the caller's reservation on the target is not consumed.
    bool migrateTo(IOThread* target);

private:
    // folly::AsyncSocket::ReadCallback
    void getReadBuffer(void** bufReturn, size_t* lenReturn) override;
    void readDataAvailable(size_t len) noexcept override;
    void readEOF() noexcept override;
    void readErr(const folly::AsyncSocketException& ex) noexcept override;

    // folly::AsyncSocket::WriteCallback
    void writeSuccess() noexcept override {
        follyWrites_.fetch_sub(1, std::memory_order_release);
        if (!inflightBufs_.empty()) {
            if (spareBufs_.size() < 2) {
                inflightBufs_.front().clear();
                spareBufs_.splice(spareBufs_.end(),
                                  inflightBufs_,
                                  inflightBufs_.begin());
            } else {
                inflightBufs_.pop_front();
            }
        }
        if (migrating_ && inflightBufs_.empty()) {
            scheduleMigrateCheck();
        }
    }
    void writeErr(size_t bytesWritten,
                  const folly::AsyncSocketException& ex) noexcept override;

    bool parseAndDispatch();
    void dispatch(McbpHeader& hdr, std::unique_ptr<folly::IOBuf> body);
    void flushPending();
    // Hands src to folly (kept alive until writeSuccess); leaves src
    // empty. Under sendMu_ with --direct-send.
    void writeOwned(std::vector<uint8_t>& src);
    void writeChainLocked(std::unique_ptr<folly::IOBuf> buf);
    void traceRead(Request* req);
    void finishDirect(Request* const* reqs, size_t n);
    // Writes the responses with sendMu_ held; true if the IO thread must
    // finish them (their tail was spilled).
    bool writeDirectLocked(Request* const* reqs, size_t n);
    // Sends reqs and whatever other senders left, then unlocks g.
    void sendHeld(std::unique_lock<std::recursive_mutex>& g,
                  Request* const* reqs,
                  size_t n);
    void finishBatch(Request* const* reqs, size_t n, bool post);
    // Drops k in-flight counts, never to zero off the loop.
    void releaseOutstanding(int k, folly::EventBase* evb);
    // Sends what others left if nobody holds the lock.
    void drainPendingSends();
    void flushPendingImpl();
    void scheduleFlush();

    // Bootstrap handlers (respond inline on IO thread)
    void handleHello(const McbpHeader& hdr, const folly::IOBuf* body);
    void handleSaslListMechs(const McbpHeader& hdr);
    void handleSaslAuth(const McbpHeader& hdr, const folly::IOBuf* body);
    void handleSaslStep(const McbpHeader& hdr, const folly::IOBuf* body);
    void handleSelectBucket(const McbpHeader& hdr);
    void handleGetClusterConfig(const McbpHeader& hdr);
    void handleGetErrorMap(const McbpHeader& hdr);
    void handleNoop(const McbpHeader& hdr);

    // Data op handlers
    void handleSet(McbpHeader& hdr, std::unique_ptr<folly::IOBuf> body);
    void handleDelete(McbpHeader& hdr, std::unique_ptr<folly::IOBuf> body);
    void handleGet(McbpHeader& hdr, std::unique_ptr<folly::IOBuf> body);

    void sendUnknownCommand(const McbpHeader& hdr);
    void destroy();

    // Migration steps; see migrateTo.
    void scheduleMigrateCheck();
    void tryFinishMigrate();
    void finishAttach(IOThread* target);
    // Called when the last request in flight has been answered.
    void onDrained();

    folly::AsyncSocket::UniquePtr socket_;
    folly::IOBufQueue readBuf_{folly::IOBufQueue::cacheChainLength()};
    Bucket* bucket_;
    const std::string& clusterConfig_;
    const std::string& errorMap_;
    IOThread* owner_{nullptr};
    IOThread* migrateTarget_{nullptr};
    std::chrono::steady_clock::time_point migrateStart_;
    bool closing_{false};
    bool migrating_{false};
    bool migrateCheckPending_{false};
    std::atomic<int> outstandingRequests_{0};

    // --direct-send: readers and this loop share the socket. Readers write
    // only while folly has nothing queued and nothing is spilled; else
    // they append whole frames (or a partial write's tail) to spill_,
    // which the loop writes before anything else. Recursive: folly can
    // call writeErr -> destroy() inside a write made under the lock.
    std::recursive_mutex sendMu_;
    bool sendClosed_{false};
    // Migrating: the loop waits for the count to reach zero.
    bool drainWatch_{false};
    bool spillPosted_{false};
    std::vector<uint8_t> spill_;
    // folly writes issued and not yet completed.
    std::atomic<int> follyWrites_{0};
    int fd_{-1};
    // Requests finished on reader threads, for acquireRequest().
    std::atomic<Request*> returned_{nullptr};
    // Responses left by senders that found sendMu_ taken (via hook.next).
    std::atomic<Request*> sendPending_{nullptr};

    // SCRAM auth state
    std::string scramServerNonce_;
    std::string scramSaltedPassword_;
    std::string scramAuthMessage_;

    // Pre-built echo GET response — avoids malloc/free per request.
    // Layout: [McbpHeader(24) | flags(4) | value(N)]
    // Only the opaque field (offset 12, 4 bytes) changes per request.
    std::vector<uint8_t> echoResponseBuf_;
    static constexpr size_t kOpaqueOffset = 12; // offset of opaque in McbpHeader

    // Coalesced response buffer: handlers append responses here; flushPending()
    // emits one socket_->write per readDataAvailable wakeup or per LoopCallback
    // (engine-thread responses). Converts N tiny sendmsg syscalls into 1.
    std::vector<uint8_t> pendingWriteBuf_;
    // Cache fast path copies the value here under the cache lock; larger
    // values take the generic path.
    uint8_t valueScratch_[4096];
    // Cached-GET lookahead: requests in [front data, pfNext_) of pfBuf_ have
    // had their index line prefetched; pfAhead_ counts those not yet served.
    // pfTok_ holds their tokens for the second-stage entry prefetch.
    static constexpr int kGetPrefetchDepth = 8;
    void prefetchAhead(DocCache* cache, const folly::IOBuf* front, size_t curLen);
    const folly::IOBuf* pfBuf_{nullptr};
    const uint8_t* pfNext_{nullptr};
    int pfAhead_{0};
    uint32_t pfSeq_{0};
    uint64_t pfTok_[kGetPrefetchDepth]{};
    // In-flight send buffers awaiting writeSuccess. AsyncSocket::write() does
    // not own the data; we keep it alive here until the callback fires.
    std::list<std::vector<uint8_t>> inflightBufs_;
    // Sent buffers kept with their capacity for the next flush.
    std::list<std::vector<uint8_t>> spareBufs_;

    // LoopCallback to flush pendingWriteBuf_ at end of current EventBase loop
    // iteration. Engine threads post sendXxxResponse via runInEventBaseThread;
    // those callbacks all run in one batch. Instead of issuing a writeChain
    // per response we accumulate into pendingWriteBuf_ and emit one write at
    // the tail of the iteration. flushScheduled_ guards re-registration.
    class FlushLoopCb : public folly::EventBase::LoopCallback {
    public:
        Connection* conn{nullptr};
        void runLoopCallback() noexcept override;
    };
    FlushLoopCb flushCb_;
    bool flushScheduled_{false};
    class MigrateLoopCb : public folly::EventBase::LoopCallback {
    public:
        Connection* conn{nullptr};
        void runLoopCallback() noexcept override;
    };
    MigrateLoopCb migrateCb_;
    // Deferred flush (MAGMA_FLUSH_DELAY_US > 0): instead of flushing at the tail
    // of every EventBase iteration (~1.6 responses per sendmsg at 1.1M GET/s,
    // O45), arm a high-res timer and let responses from many reader threads
    // coalesce into one write. Flushes early once pendingWriteBuf_ reaches
    // MAGMA_FLUSH_BYTES. Queueing latency at 16K in-flight is ~15 ms, so a
    // 50-200 us delay is invisible to the client.
    class FlushTimeout : public folly::AsyncTimeout {
    public:
        explicit FlushTimeout(folly::EventBase* evb) : folly::AsyncTimeout(evb) {}
        Connection* conn{nullptr};
        void timeoutExpired() noexcept override;
    };
    std::unique_ptr<FlushTimeout> flushTimeout_;

    // Write-queue backpressure. Acknowledging on acceptance means the client
    // never waits, so it offers far more than the engine drains and the
    // surplus used to be parsed, copied and refused: 90% of requests at
    // pipeline 128, costing 20 cores and half the disk throughput. Stop
    // reading instead and let TCP hold the client off until the queue drains.
    class ResumeTimeout : public folly::AsyncTimeout {
    public:
        explicit ResumeTimeout(folly::EventBase* evb)
            : folly::AsyncTimeout(evb) {
        }
        Connection* conn{nullptr};
        void timeoutExpired() noexcept override;
    };
    std::unique_ptr<ResumeTimeout> resumeTimeout_;
    bool readPaused_{false};
    bool stageFull_{false};
    void pauseForQueue();
    void resumeAfterQueue();

    // Per-Connection Request freelist. Requests flow IO-thread → engine-thread
    // → IO-thread; we touch the pool only from the IO thread (this Connection's
    // event-base), so no synchronization is needed. Cap prevents unbounded
    // growth when the engine is slower than the network.
    // Prefers a Request recycled by the vbucket's shard writers.
    Request* acquireRequest(uint16_t vbucket);
    std::vector<Request*> reqPool_;
    static constexpr size_t kMaxPooledReqs = 256;
    Request* acquireRequest();
    void releaseRequest(Request* req);
};

} // namespace kvserver
} // namespace magma
