#include "connection.h"

#include <cbcrypto/digest.h>
#include <folly/io/Cursor.h>
#include <platform/base64.h>
#include <spdlog/spdlog.h>

#include <random>

#include <sys/socket.h>
#include <cstdlib>

namespace {
int flushDelayUs() {
    static const int v = [] {
        const char* e = std::getenv("MAGMA_FLUSH_DELAY_US");
        return e ? std::atoi(e) : 0;
    }();
    return v;
}
size_t flushBytes() {
    static const size_t v = [] {
        const char* e = std::getenv("MAGMA_FLUSH_BYTES");
        return e ? static_cast<size_t>(std::atoll(e)) : size_t(64 * 1024);
    }();
    return v;
}
} // namespace


namespace magma {
namespace kvserver {

Connection::Connection(folly::AsyncSocket::UniquePtr socket,
                       Bucket* bucket,
                       const std::string& clusterConfig,
                       const std::string& errorMap)
    : socket_(std::move(socket)),
      bucket_(bucket),
      clusterConfig_(clusterConfig),
      errorMap_(errorMap) {
    // TCP_NODELAY: folly AsyncSocket does NOT set this by default. Without
    // it, Nagle interacts with the small-response request/response pattern
    // and adds tail latency on the order of 100s of microseconds to
    // milliseconds at moderate concurrency. Critical for low-latency.
    socket_->setNoDelay(true);

    // Refilled every loop iteration and never exceeds one flush, so reserve
    // that up front instead of reallocating on the hot path.
    pendingWriteBuf_.reserve(64 * 1024);

    // SO_BUSY_POLL: kernel busy-polls the socket for arriving data instead
    // of going through the softirq scheduler when packets land in the NIC
    // ring. Eliminates a major source of p99 wake-up jitter at moderate
    // request rates. Set MAGMA_BUSY_POLL_US to override (default 50µs).
    {
        int fd = socket_->getNetworkSocket().toFd();
        if (fd >= 0) {
            const char* env = std::getenv("MAGMA_BUSY_POLL_US");
            int us = env ? std::atoi(env) : 50;
            if (us > 0) {
                int v = us;
                ::setsockopt(fd, SOL_SOCKET, SO_BUSY_POLL, &v, sizeof(v));
            }
        }
    }

    // SO_SNDBUF: enlarge the kernel TCP send buffer so the IO thread doesn't
    // block on full buffer when bursts of pipelined responses fly out at
    // 1 GB/s+. Default kernel cap is ~200KB which fills in <1ms at peak.
    // 4 MB caps under 4ms even at sustained 1 GB/s. Requires
    // net.core.wmem_max ≥ 4194304 (sysctl) — kernel silently caps otherwise.
    // Set MAGMA_SNDBUF=0 to skip.
    {
        int fd = socket_->getNetworkSocket().toFd();
        if (fd >= 0) {
            const char* env = std::getenv("MAGMA_SNDBUF");
            int sz = env ? std::atoi(env) : (4 * 1024 * 1024);
            if (sz > 0) {
                ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sz, sizeof(sz));
            }
        }
    }
    // Pre-build echo GET response buffer if echo mode is enabled
    if (bucket_->GetEchoGetSize() > 0) {
        auto& val = bucket_->GetEchoGetValue();
        size_t bodyLen = 4 + val.size(); // 4 bytes flags + value
        size_t totalLen = kHeaderSize + bodyLen;
        echoResponseBuf_.resize(totalLen);

        McbpHeader hdr{};
        hdr.magic = kResponseMagic;
        hdr.opcode = static_cast<uint8_t>(Opcode::Get);
        hdr.keyLen = 0;
        hdr.extrasLen = 4;
        hdr.datatype = 0;
        hdr.specific = static_cast<uint16_t>(McbpStatus::Success);
        hdr.bodyLen = static_cast<uint32_t>(bodyLen);
        hdr.opaque = 0; // stamped per-request
        hdr.cas = 1;
        hdr.hton();

        auto* p = echoResponseBuf_.data();
        memcpy(p, &hdr, kHeaderSize);
        p += kHeaderSize;
        uint32_t flagsNBO = htonl(0x04000000);
        memcpy(p, &flagsNBO, 4);
        p += 4;
        memcpy(p, val.data(), val.size());
    }
}

Connection::~Connection() {
    for (auto* r : reqPool_) {
        delete r;
    }
}

namespace {
// Requests recycled by the writers, unrolled from the per-shard chains.
// One per IO thread; deleted with the thread.
// Kept as the chain the writer built, popped one at a time: each Request is
// a cache miss (a writer wrote it last), and popping is the only touch
// before handleSet fills it in anyway. Unrolling into a vector first cost a
// second miss per request.
struct RecycledRequests {
    Request* chain{nullptr};
    ~RecycledRequests() {
        while (chain) {
            Request* next = chain->hook.next;
            delete chain;
            chain = next;
        }
    }
};
thread_local RecycledRequests tlsRecycled;
} // namespace

Request* Connection::acquireRequest(uint16_t vbucket) {
    auto& c = tlsRecycled.chain;
    if (!c) {
        c = bucket_->GetShard(vbucket).TakeRequests();
    }
    if (c) {
        Request* r = c;
        c = r->hook.next;
        r->hook.next = nullptr;
        return r;
    }
    return acquireRequest();
}

Request* Connection::acquireRequest() {
    if (!reqPool_.empty()) {
        auto* r = reqPool_.back();
        reqPool_.pop_back();
        return r;
    }
    return new Request();
}

void Connection::releaseRequest(Request* req) {
    if (reqPool_.size() < kMaxPooledReqs) {
        req->reset();
        reqPool_.push_back(req);
    } else {
        delete req;
    }
}

void Connection::start(IOThread* owner) {
    gDispStats.connectAccept.fetch_add(1, std::memory_order_relaxed);
    owner_ = owner;
    if (owner_) {
        owner_->Add(this);
    }
    socket_->setReadCB(this);
}

void Connection::destroy() {
    if (closing_)
        return;
    closing_ = true;
    gDispStats.connectClose.fetch_add(1, std::memory_order_relaxed);
    auto* evb = socket_->getEventBase();
    if (flushCb_.isLoopCallbackScheduled()) {
        flushCb_.cancelLoopCallback();
    }
    if (migrateCb_.isLoopCallbackScheduled()) {
        migrateCb_.cancelLoopCallback();
    }
    if (migrating_ && migrateTarget_) {
        migrateTarget_->Unreserve();
        migrateTarget_ = nullptr;
    }
    migrating_ = false;
    flushScheduled_ = false;
    if (owner_) {
        owner_->Remove(this);
        owner_ = nullptr;
    }
    socket_->setReadCB(nullptr);
    socket_->close();
    // Defer delete to next event loop iteration to avoid use-after-free
    // when destroy() is called from within a read/write callback.
    if (outstandingRequests_.load() == 0 && evb) {
        evb->runInEventBaseThread([this]() { delete this; });
    }
}

// ---- Migration between IO threads ----

bool Connection::migrateTo(IOThread* target) {
    if (closing_ || migrating_ || target == nullptr || target == owner_) {
        return false;
    }
    migrating_ = true;
    migrateTarget_ = target;
    migrateStart_ = std::chrono::steady_clock::now();
    gDispStats.migrationsStarted.fetch_add(1, std::memory_order_relaxed);
    // No new requests from here on; the ones in flight drain first.
    socket_->setReadCB(nullptr);
    scheduleMigrateCheck();
    return true;
}

// The checks run as their own loop callback, never from inside a socket
// callback: detaching the socket from within its own write completion would
// pull the EventBase out from under AsyncSocket mid-call. A loop callback
// runs at the end of the current iteration, unlike the cross-thread queue,
// which a busy loop may not drain for a long time.
void Connection::scheduleMigrateCheck() {
    if (migrateCheckPending_ || !migrating_) {
        return;
    }
    auto* evb = socket_->getEventBase();
    if (!evb) {
        return;
    }
    migrateCheckPending_ = true;
    migrateCb_.conn = this;
    evb->runInLoop(&migrateCb_);
}

void Connection::MigrateLoopCb::runLoopCallback() noexcept {
    if (!conn) {
        return;
    }
    conn->migrateCheckPending_ = false;
    conn->tryFinishMigrate();
}

void Connection::tryFinishMigrate() {
    if (!migrating_) {
        return;
    }
    if (closing_) {
        migrating_ = false;
        if (migrateTarget_) {
            migrateTarget_->Unreserve();
            migrateTarget_ = nullptr;
        }
        return;
    }
    if (outstandingRequests_.load(std::memory_order_acquire) != 0) {
        gDispStats.migWaitOutstanding.fetch_add(1, std::memory_order_relaxed);
        return; // onDrained() schedules the next check
    }
    if (!pendingWriteBuf_.empty()) {
        gDispStats.migWaitFlush.fetch_add(1, std::memory_order_relaxed);
        if (flushTimeout_ && flushTimeout_->isScheduled()) {
            flushTimeout_->cancelTimeout();
        }
        if (flushCb_.isLoopCallbackScheduled()) {
            flushCb_.cancelLoopCallback();
        }
        flushScheduled_ = false;
        // writeSuccess() schedules the next check, whether it fires inside
        // this write() or later. Never detach in the same call as a write.
        flushPending();
        return;
    }
    if (!inflightBufs_.empty()) {
        gDispStats.migWaitInflight.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    // Quiescent: nothing in flight in either direction.
    if (flushCb_.isLoopCallbackScheduled()) {
        flushCb_.cancelLoopCallback();
    }
    if (migrateCb_.isLoopCallbackScheduled()) {
        migrateCb_.cancelLoopCallback();
    }
    migrateCheckPending_ = false;
    flushScheduled_ = false;
    flushTimeout_.reset(); // bound to the old loop; recreated lazily
    if (owner_) {
        owner_->Remove(this);
        owner_ = nullptr;
    }
    IOThread* target = migrateTarget_;
    migrateTarget_ = nullptr;
    socket_->detachEventBase();
    target->Post([this, target]() { finishAttach(target); });
}

void Connection::finishAttach(IOThread* target) {
    socket_->attachEventBase(target->evb.get());
    owner_ = target;
    owner_->Add(this);
    migrating_ = false;
    gDispStats.migrationsDone.fetch_add(1, std::memory_order_relaxed);
    gDispStats.migTotalUs.fetch_add(
            std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - migrateStart_)
                    .count(),
            std::memory_order_relaxed);
    socket_->setReadCB(this);
    // Requests that arrived before reading stopped may still be buffered
    // and the client may be waiting on them.
    while (!closing_ && parseAndDispatch()) {
    }
    bucket_->FlushStaged();
    if (flushDelayUs() > 0) {
        if (!pendingWriteBuf_.empty()) {
            scheduleFlush();
        }
    } else {
        flushPending();
    }
}

void Connection::onDrained() {
    if (closing_) {
        delete this;
        return;
    }
    if (migrating_) {
        scheduleMigrateCheck();
    }
}

// ---- ReadCallback ----

void Connection::getReadBuffer(void** bufReturn, size_t* lenReturn) {
    auto res = readBuf_.preallocate(4096, 65536);
    *bufReturn = res.first;
    *lenReturn = res.second;
}

void Connection::readDataAvailable(size_t len) noexcept {
    // Work posted to this loop from other threads; see IOThread::Post.
    if (owner_ && owner_->hasMail.load(std::memory_order_acquire)) {
        owner_->SchedulePump();
    }
    readBuf_.postallocate(len);
    while (!closing_ && parseAndDispatch()) {
    }
    bucket_->FlushStaged();
    if (flushDelayUs() > 0) {
        // Let synchronous responses ride the same deferred flush.
        if (!pendingWriteBuf_.empty()) {
            scheduleFlush();
        }
    } else {
        flushPending();
    }
}

void Connection::flushPending() {
    if (pendingWriteBuf_.empty() || closing_) {
        return;
    }
    inflightBufs_.emplace_back(std::move(pendingWriteBuf_));
    pendingWriteBuf_.clear();
    auto& buf = inflightBufs_.back();
    socket_->write(this, buf.data(), buf.size());
}

void Connection::scheduleFlush() {
    if (closing_) {
        return;
    }
    if (flushScheduled_) {
        // Timer armed but the buffer is already big enough: flush now.
        if (flushTimeout_ && flushTimeout_->isScheduled() &&
            pendingWriteBuf_.size() >= flushBytes()) {
            flushTimeout_->cancelTimeout();
            flushScheduled_ = false;
            flushPending();
        }
        return;
    }
    flushScheduled_ = true;
    if (!flushCb_.conn) {
        flushCb_.conn = this;
    }
    if (flushDelayUs() > 0 && pendingWriteBuf_.size() < flushBytes()) {
        if (!flushTimeout_) {
            flushTimeout_ = std::make_unique<FlushTimeout>(socket_->getEventBase());
            flushTimeout_->conn = this;
        }
        flushTimeout_->scheduleTimeoutHighRes(
                std::chrono::microseconds(flushDelayUs()));
        return;
    }
    socket_->getEventBase()->runInLoop(&flushCb_, /*thisIteration=*/true);
}

void Connection::FlushTimeout::timeoutExpired() noexcept {
    if (!conn) {
        return;
    }
    conn->flushScheduled_ = false;
    conn->flushPending();
}

void Connection::FlushLoopCb::runLoopCallback() noexcept {
    if (!conn) return;
    conn->flushScheduled_ = false;
    conn->flushPending();
}

void Connection::readEOF() noexcept {
    destroy();
}

void Connection::readErr(const folly::AsyncSocketException& ex) noexcept {
    destroy();
}

// ---- WriteCallback ----

void Connection::writeErr(size_t /*bytesWritten*/,
                          const folly::AsyncSocketException& /*ex*/) noexcept {
    destroy();
}

// ---- Protocol parsing ----

bool Connection::parseAndDispatch() {
    if (readBuf_.chainLength() < kHeaderSize) {
        return false;
    }

    // Peek at header to determine body length
    McbpHeader hdr;
    {
        folly::io::Cursor cursor(readBuf_.front());
        cursor.pull(&hdr, kHeaderSize);
    }

    // Safety: reject non-mcbp data. Check magic AND opcode — HTTP data
    // can have 0x80 at certain offsets, so magic alone isn't sufficient.
    if (hdr.magic != kRequestMagic) {
        gDispStats.badMagic.fetch_add(1, std::memory_order_relaxed);
        spdlog::warn("Bad magic 0x{:02x} (first bytes: 0x{:02x} 0x{:02x} "
                     "0x{:02x} 0x{:02x}), closing connection",
                     hdr.magic,
                     hdr.magic,
                     hdr.opcode,
                     (uint8_t)(hdr.keyLen >> 8),
                     (uint8_t)(hdr.keyLen & 0xff));
        readBuf_.move();
        destroy();
        return false;
    }

    auto op = static_cast<Opcode>(hdr.opcode);
    if (op != Opcode::Get && op != Opcode::Set && op != Opcode::Delete &&
        op != Opcode::Noop && op != Opcode::Hello &&
        op != Opcode::SaslListMechs && op != Opcode::SaslAuth &&
        op != Opcode::SaslStep && op != Opcode::SelectBucket &&
        op != Opcode::GetClusterConfig && op != Opcode::GetErrorMap) {
        gDispStats.badOpcode.fetch_add(1, std::memory_order_relaxed);
        spdlog::warn("Bad opcode 0x{:02x}, closing connection", hdr.opcode);
        readBuf_.move();
        destroy();
        return false;
    }

    hdr.ntoh();

    size_t totalLen = kHeaderSize + hdr.bodyLen;
    if (readBuf_.chainLength() < totalLen) {
        return false;
    }

    // Fast path: echo-mode GET — skip IOBuf allocation entirely.
    // Just consume the bytes and respond inline.
    if (op == Opcode::Get && !echoResponseBuf_.empty()) {
        readBuf_.trimStart(totalLen);
        handleGet(hdr, nullptr);
        return true;
    }

    // Fast path: a cached GET sitting contiguously in the front buffer is
    // answered with no allocation. The generic path below splits an IOBuf per
    // request just to read the key, which dominates at high GET rates.
    // Pipelined reads make the contiguous case the common one.
    if (op == Opcode::Get) {
        if (auto* cache = bucket_->GetCache()) {
            const folly::IOBuf* front = readBuf_.front();
            if (front != nullptr && front->length() >= totalLen) {
                const char* p = reinterpret_cast<const char*>(front->data());
                std::string_view key(p + kHeaderSize + hdr.extrasLen,
                                     hdr.keyLen);
                CachedDoc doc;
                size_t valueLen = 0;
                const auto res = cache->GetCopy(hdr.specific,
                                                key,
                                                &doc,
                                                valueScratch_,
                                                sizeof(valueScratch_),
                                                &valueLen);
                if (res == DocCache::GetResult::Hit) {
                    hotStatAdd(gDispStats.cmdGet);
                    hotStatAdd(gDispStats.cmdGetResp);
                    appendGetResponse(pendingWriteBuf_,
                                      hdr.opaque,
                                      doc.seqno,
                                      doc.flags,
                                      valueScratch_,
                                      valueLen,
                                      doc.datatype);
                    readBuf_.trimStart(totalLen);
                    scheduleFlush();
                    return true;
                }
                if (res == DocCache::GetResult::Tombstone) {
                    hotStatAdd(gDispStats.cmdGet);
                    hotStatAdd(gDispStats.cmdGetRespMiss);
                    appendEmptyResponse(pendingWriteBuf_,
                                        hdr.opcode,
                                        McbpStatus::KeyNotFound,
                                        hdr.opaque);
                    readBuf_.trimStart(totalLen);
                    scheduleFlush();
                    return true;
                }
                // Miss, or a value too large for the scratch buffer: fall
                // through to the generic path.
            }
        }
    }

    // Consume header
    readBuf_.trimStart(kHeaderSize);

    // Consume body as IOBuf (zero-copy split)
    std::unique_ptr<folly::IOBuf> body;
    if (hdr.bodyLen > 0) {
        body = readBuf_.split(hdr.bodyLen);
    }

    dispatch(hdr, std::move(body));
    return true;
}

void Connection::dispatch(McbpHeader& hdr, std::unique_ptr<folly::IOBuf> body) {
    switch (static_cast<Opcode>(hdr.opcode)) {
    case Opcode::Hello:
        handleHello(hdr, body.get());
        break;
    case Opcode::SaslListMechs:
        handleSaslListMechs(hdr);
        break;
    case Opcode::SaslAuth:
        handleSaslAuth(hdr, body.get());
        break;
    case Opcode::SaslStep:
        handleSaslStep(hdr, body.get());
        break;
    case Opcode::SelectBucket:
        handleSelectBucket(hdr);
        break;
    case Opcode::GetClusterConfig:
        handleGetClusterConfig(hdr);
        break;
    case Opcode::GetErrorMap:
        handleGetErrorMap(hdr);
        break;
    case Opcode::Noop:
        handleNoop(hdr);
        break;
    case Opcode::Set:
        handleSet(hdr, std::move(body));
        break;
    case Opcode::Delete:
        handleDelete(hdr, std::move(body));
        break;
    case Opcode::Get:
        handleGet(hdr, std::move(body));
        break;
    default:
        sendUnknownCommand(hdr);
        break;
    }
}

// ---- Bootstrap handlers ----

void Connection::handleHello(const McbpHeader& hdr, const folly::IOBuf* body) {
    // Filter features — only echo back safe ones.
    // Crucially do NOT echo ClustermapChangeNotification (0x0d) since
    // we never push configs, and do NOT echo Collections (0x12) to
    // avoid CID-prefixed keys.
    std::vector<uint16_t> supported;
    if (body && hdr.bodyLen > hdr.keyLen) {
        auto coalesced = body->cloneCoalesced();
        const uint8_t* fdata = coalesced->data() + hdr.keyLen;
        size_t flen = hdr.bodyLen - hdr.keyLen;
        for (size_t i = 0; i + 1 < flen; i += 2) {
            uint16_t feat =
                    ntohs(*reinterpret_cast<const uint16_t*>(fdata + i));
            switch (feat) {
            case 0x04: // MUTATION_SEQNO
            case 0x07: // XERROR
            case 0x08: // SELECT_BUCKET
            case 0x0b: // JSON
            case 0x0c: // DUPLEX
            case 0x0e: // UnorderedExecution
            case 0x0a: // SNAPPY
                supported.push_back(htons(feat));
                break;
            default:
                // Skip: Collections(0x12),
                // ClustermapChangeNotification(0x0d), etc
                break;
            }
        }
    }
    auto buf = buildResponse(hdr.opcode,
                             McbpStatus::Success,
                             hdr.opaque,
                             0,
                             nullptr,
                             0,
                             nullptr,
                             0,
                             supported.data(),
                             supported.size() * sizeof(uint16_t));
    socket_->writeChain(this, std::move(buf));
}

void Connection::handleSaslListMechs(const McbpHeader& hdr) {
    const char* mechs = "SCRAM-SHA512 SCRAM-SHA256 SCRAM-SHA1 PLAIN";
    auto buf = buildResponse(hdr.opcode,
                             McbpStatus::Success,
                             hdr.opaque,
                             0,
                             nullptr,
                             0,
                             nullptr,
                             0,
                             mechs,
                             strlen(mechs));
    socket_->writeChain(this, std::move(buf));
}

// Pre-computed PBKDF2 salted passwords — computed once, reused for all
// connections. Eliminates ~10ms PBKDF2 call per SCRAM auth.
static const std::string kSalt = "magmakvserversalt";
static const std::string kSaltB64 = cb::base64::encode(kSalt);
static const int kIterations = 4096;

struct PrecomputedScram {
    std::string sha512;
    std::string sha256;
    std::string sha1;

    PrecomputedScram() {
        sha512 = cb::crypto::PBKDF2_HMAC(
                cb::crypto::Algorithm::SHA512, "password", kSalt, kIterations);
        sha256 = cb::crypto::PBKDF2_HMAC(
                cb::crypto::Algorithm::SHA256, "password", kSalt, kIterations);
        sha1 = cb::crypto::PBKDF2_HMAC(
                cb::crypto::Algorithm::SHA1, "password", kSalt, kIterations);
    }
};

static const PrecomputedScram kScram;

static cb::crypto::Algorithm getScramAlgo(const std::string& mech) {
    if (mech.find("512") != std::string::npos)
        return cb::crypto::Algorithm::SHA512;
    if (mech.find("256") != std::string::npos)
        return cb::crypto::Algorithm::SHA256;
    return cb::crypto::Algorithm::SHA1;
}

void Connection::handleSaslAuth(const McbpHeader& hdr,
                                const folly::IOBuf* body) {
    std::string mech;
    std::string authData;
    if (body) {
        auto coalesced = body->cloneCoalesced();
        if (hdr.keyLen > 0) {
            mech.assign(reinterpret_cast<const char*>(coalesced->data()),
                        hdr.keyLen);
        }
        if (hdr.bodyLen > hdr.keyLen) {
            authData.assign(reinterpret_cast<const char*>(coalesced->data()) +
                                    hdr.keyLen,
                            hdr.bodyLen - hdr.keyLen);
        }
    }

    if (mech == "PLAIN") {
        auto buf =
                buildEmptyResponse(hdr.opcode, McbpStatus::Success, hdr.opaque);
        socket_->writeChain(this, std::move(buf));
        return;
    }

    // SCRAM step 1: parse client-first-message "n,,n=user,r=clientNonce"
    std::string clientFirstBare;
    auto nPos = authData.find("n=");
    if (nPos != std::string::npos) {
        clientFirstBare = authData.substr(nPos);
    }

    std::string clientNonce;
    auto rPos = clientFirstBare.find("r=");
    if (rPos != std::string::npos) {
        clientNonce = clientFirstBare.substr(rPos + 2);
    }

    // Server nonce
    static std::mt19937 rng(std::random_device{}());
    std::string serverNonceSuffix;
    for (int i = 0; i < 18; i++) {
        serverNonceSuffix += "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef"[rng() % 32];
    }
    scramServerNonce_ = clientNonce + serverNonceSuffix;

    // Use pre-computed salted password — no PBKDF2 per connection
    auto algo = getScramAlgo(mech);
    if (algo == cb::crypto::Algorithm::SHA512) {
        scramSaltedPassword_ = kScram.sha512;
    } else if (algo == cb::crypto::Algorithm::SHA256) {
        scramSaltedPassword_ = kScram.sha256;
    } else {
        scramSaltedPassword_ = kScram.sha1;
    }

    // Server-first-message
    std::string serverFirst = "r=" + scramServerNonce_ + ",s=" + kSaltB64 +
                              ",i=" + std::to_string(kIterations);

    // Store auth message prefix for step 2
    scramAuthMessage_ = clientFirstBare + "," + serverFirst + ",";

    auto buf = buildResponse(hdr.opcode,
                             McbpStatus::AuthContinue,
                             hdr.opaque,
                             0,
                             nullptr,
                             0,
                             nullptr,
                             0,
                             serverFirst.data(),
                             serverFirst.size());
    socket_->writeChain(this, std::move(buf));
}

void Connection::handleSaslStep(const McbpHeader& hdr,
                                const folly::IOBuf* body) {
    std::string mech;
    std::string authData;
    if (body) {
        auto coalesced = body->cloneCoalesced();
        if (hdr.keyLen > 0) {
            mech.assign(reinterpret_cast<const char*>(coalesced->data()),
                        hdr.keyLen);
        }
        if (hdr.bodyLen > hdr.keyLen) {
            authData.assign(reinterpret_cast<const char*>(coalesced->data()) +
                                    hdr.keyLen,
                            hdr.bodyLen - hdr.keyLen);
        }
    }

    // SCRAM step 2: client sends "c=biws,r=nonce,p=proof"
    auto pPos = authData.find(",p=");
    std::string clientFinalWithoutProof =
            (pPos != std::string::npos) ? authData.substr(0, pPos) : authData;

    std::string authMessage = scramAuthMessage_ + clientFinalWithoutProof;

    auto algo = getScramAlgo(mech);
    std::string serverKey =
            cb::crypto::HMAC(algo, scramSaltedPassword_, "Server Key");
    std::string serverSignature =
            cb::crypto::HMAC(algo, serverKey, authMessage);
    std::string serverFinal = "v=" + cb::base64::encode(serverSignature);

    auto buf = buildResponse(hdr.opcode,
                             McbpStatus::Success,
                             hdr.opaque,
                             0,
                             nullptr,
                             0,
                             nullptr,
                             0,
                             serverFinal.data(),
                             serverFinal.size());
    socket_->writeChain(this, std::move(buf));
}

void Connection::handleSelectBucket(const McbpHeader& hdr) {
    auto buf = buildEmptyResponse(hdr.opcode, McbpStatus::Success, hdr.opaque);
    socket_->writeChain(this, std::move(buf));
}

void Connection::handleGetClusterConfig(const McbpHeader& hdr) {
    auto buf = buildResponse(hdr.opcode,
                             McbpStatus::Success,
                             hdr.opaque,
                             0,
                             nullptr,
                             0,
                             nullptr,
                             0,
                             clusterConfig_.data(),
                             clusterConfig_.size());
    socket_->writeChain(this, std::move(buf));
}

void Connection::handleGetErrorMap(const McbpHeader& hdr) {
    auto buf = buildResponse(hdr.opcode,
                             McbpStatus::Success,
                             hdr.opaque,
                             0,
                             nullptr,
                             0,
                             nullptr,
                             0,
                             errorMap_.data(),
                             errorMap_.size());
    socket_->writeChain(this, std::move(buf));
}

void Connection::handleNoop(const McbpHeader& hdr) {
    auto buf = buildEmptyResponse(hdr.opcode, McbpStatus::Success, hdr.opaque);
    socket_->writeChain(this, std::move(buf));
}

// ---- Data op handlers ----

void Connection::handleSet(McbpHeader& hdr,
                           std::unique_ptr<folly::IOBuf> body) {
    hotStatAdd(gDispStats.cmdSet);

    auto* req = acquireRequest(hdr.specific);
    req->opcode = hdr.opcode;
    req->vbucket = hdr.specific;
    req->opaque = hdr.opaque;
    req->cas = hdr.cas;
    req->datatype = hdr.datatype;

    if (body) {
        body->coalesce();
        const char* p;
        if (body->length() <= Request::kInlineData) {
            // Copy and let the IOBuf go now, on this thread.
            memcpy(req->inlineData, body->data(), body->length());
            p = req->inlineData;
        } else {
            req->dataBuf = std::move(body);
            p = reinterpret_cast<const char*>(req->dataBuf->data());
        }

        if (hdr.extrasLen >= 4) {
            req->flags = ntohl(*reinterpret_cast<const uint32_t*>(p));
        }
        if (hdr.extrasLen >= 8) {
            req->expiry = ntohl(*reinterpret_cast<const uint32_t*>(p + 4));
        }

        req->key = Slice(p + hdr.extrasLen, hdr.keyLen);
        req->value = Slice(p + hdr.extrasLen + hdr.keyLen,
                           hdr.bodyLen - hdr.extrasLen - hdr.keyLen);
    }

    // Write-through: cache before queueing so the entry is pinned by the time
    // any reader could fill over it (see cache/doccache.h).
    auto* cache = bucket_->GetCache();
    if (cache) {
        cache->Put(req->vbucket,
                   std::string_view(req->key.Data(), req->key.Len()),
                   std::string_view(req->value.Data(), req->value.Len()),
                   req->flags,
                   req->expiry,
                   req->datatype,
                   false);
    }

    if (bucket_->IsDurable()) {
        // Durable: track outstanding, wait for WriteDocs completion
        outstandingRequests_.fetch_add(1, std::memory_order_relaxed);
        req->conn = this;
        req->evb = socket_->getEventBase();
        // Keep this loop alive until the response is sent: with
        // --async-durable the request outlives the socket wake, and the
        // tuner retires a loop once its connections have migrated off.
        req->ioOwner = owner_;
        if (owner_) {
            owner_->Reserve();
        }
        if (gTraceLatency) {
            req->tArrive = steadyNowNs();
        }
        if (!bucket_->StageWrite(req)) {
            // Refused: answer now and undo the cache entry, which would
            // otherwise stay pinned forever waiting for a MarkPersisted that
            // never comes.
            gDispStats.tmpFails.fetch_add(1, std::memory_order_relaxed);
            if (cache) {
                cache->Erase(req->vbucket,
                             std::string_view(req->key.Data(),
                                              req->key.Len()));
            }
            req->resultStatus = Status(Status::Code::Internal, "tmpfail");
            sendWriteResponse(req);
        }
    } else {
        // Async: stage and respond immediately. Append into pendingWriteBuf_
        // so the response is coalesced with any others produced in this
        // readDataAvailable wake (one socket write for many SETs). The
        // staged writes are handed to the writers in FlushStaged at the end
        // of the wake.
        if (bucket_->StageWrite(req)) {
            appendEmptyResponse(pendingWriteBuf_,
                                hdr.opcode,
                                McbpStatus::Success,
                                hdr.opaque,
                                0);
        } else {
            gDispStats.tmpFails.fetch_add(1, std::memory_order_relaxed);
            appendEmptyResponse(pendingWriteBuf_,
                                hdr.opcode,
                                McbpStatus::TmpFail,
                                hdr.opaque,
                                0);
            if (cache) {
                // The store will never see this write; do not serve it.
                cache->Erase(req->vbucket,
                             std::string_view(req->key.Data(),
                                              req->key.Len()));
            }
            releaseRequest(req);
        }
    }
}

void Connection::handleDelete(McbpHeader& hdr,
                              std::unique_ptr<folly::IOBuf> body) {
    hotStatAdd(gDispStats.cmdDelete);

    auto* req = acquireRequest();
    req->opcode = hdr.opcode;
    req->vbucket = hdr.specific;
    req->opaque = hdr.opaque;
    req->cas = hdr.cas;

    if (body) {
        body->coalesce();
        req->dataBuf = std::move(body);
        const char* p = reinterpret_cast<const char*>(req->dataBuf->data());
        req->key = Slice(p + hdr.extrasLen, hdr.keyLen);
    }

    // Write-through tombstone: a GET after this answers KeyNotFound from the
    // cache and cannot be filled with the pre-delete value from disk.
    auto* cache = bucket_->GetCache();
    if (cache) {
        cache->Put(req->vbucket,
                   std::string_view(req->key.Data(), req->key.Len()),
                   std::string_view(),
                   0,
                   0,
                   0,
                   true);
    }

    if (bucket_->IsDurable()) {
        outstandingRequests_.fetch_add(1, std::memory_order_relaxed);
        req->conn = this;
        req->evb = socket_->getEventBase();
        // Keep this loop alive until the response is sent: with
        // --async-durable the request outlives the socket wake, and the
        // tuner retires a loop once its connections have migrated off.
        req->ioOwner = owner_;
        if (owner_) {
            owner_->Reserve();
        }
        if (gTraceLatency) {
            req->tArrive = steadyNowNs();
        }
        if (!bucket_->StageWrite(req)) {
            gDispStats.tmpFails.fetch_add(1, std::memory_order_relaxed);
            if (cache) {
                cache->Erase(req->vbucket,
                             std::string_view(req->key.Data(),
                                              req->key.Len()));
            }
            req->resultStatus = Status(Status::Code::Internal, "tmpfail");
            sendWriteResponse(req);
        }
    } else {
        if (bucket_->StageWrite(req)) {
            appendEmptyResponse(pendingWriteBuf_,
                                hdr.opcode,
                                McbpStatus::Success,
                                hdr.opaque,
                                0);
        } else {
            gDispStats.tmpFails.fetch_add(1, std::memory_order_relaxed);
            appendEmptyResponse(pendingWriteBuf_,
                                hdr.opcode,
                                McbpStatus::TmpFail,
                                hdr.opaque,
                                0);
            if (cache) {
                cache->Erase(req->vbucket,
                             std::string_view(req->key.Data(),
                                              req->key.Len()));
            }
            releaseRequest(req);
        }
    }
}

void Connection::handleGet(McbpHeader& hdr,
                           std::unique_ptr<folly::IOBuf> body) {
    hotStatAdd(gDispStats.cmdGet);

    // Echo mode: append pre-built response to coalesced write buffer.
    // No malloc, no IOBuf, no per-op syscall — flushPending() emits
    // one socket write for all responses produced in this read loop.
    if (!echoResponseBuf_.empty()) {
        hotStatAdd(gDispStats.cmdGetResp);
        size_t off = pendingWriteBuf_.size();
        pendingWriteBuf_.resize(off + echoResponseBuf_.size());
        memcpy(pendingWriteBuf_.data() + off,
               echoResponseBuf_.data(),
               echoResponseBuf_.size());
        // opaque is not byte-swapped by ntoh/hton — copy as-is
        memcpy(pendingWriteBuf_.data() + off + kOpaqueOffset, &hdr.opaque, 4);
        return;
    }

    // Cache hit: answered right here on the IO thread. No Request, no reader
    // hop, no wakeup - the value is copied once, from the cache entry into
    // the coalesced write buffer, under the cache's shared lock.
    if (auto* cache = bucket_->GetCache(); cache && body) {
        body->coalesce();
        const char* p = reinterpret_cast<const char*>(body->data());
        std::string_view key(p + hdr.extrasLen, hdr.keyLen);
        bool hit = cache->Get(hdr.specific, key, [&](const CachedDoc& doc) {
            if (doc.deleted) {
                appendEmptyResponse(pendingWriteBuf_,
                                    hdr.opcode,
                                    McbpStatus::KeyNotFound,
                                    hdr.opaque);
                hotStatAdd(gDispStats.cmdGetRespMiss);
            } else {
                appendGetResponse(pendingWriteBuf_,
                                  hdr.opaque,
                                  doc.seqno,
                                  doc.flags,
                                  doc.value.data(),
                                  doc.value.size(),
                                  doc.datatype);
                hotStatAdd(gDispStats.cmdGetResp);
            }
        });
        if (hit) {
            return;
        }
    }

    outstandingRequests_.fetch_add(1, std::memory_order_relaxed);
    auto* req = acquireRequest();
    req->opcode = hdr.opcode;
    req->vbucket = hdr.specific;
    req->opaque = hdr.opaque;
    req->conn = this;
    req->evb = socket_->getEventBase();

    if (body) {
        body->coalesce();
        req->dataBuf = std::move(body);
        const char* p = reinterpret_cast<const char*>(req->dataBuf->data());
        // GET has no extras, body is just key
        req->key = Slice(p + hdr.extrasLen, hdr.keyLen);
    }

    bucket_->EnqueueRead(req);
}

void Connection::sendUnknownCommand(const McbpHeader& hdr) {
    auto buf = buildEmptyResponse(
            hdr.opcode, McbpStatus::UnknownCommand, hdr.opaque);
    socket_->writeChain(this, std::move(buf));
}

// ---- Response senders called from engine threads via EventBase ----

void Connection::sendWriteResponse(Request* req) {
    // Taken when the request was queued in durable mode; released here,
    // on the loop itself, once the response is out. reset() clears the
    // field, so read it first.
    auto* iot = req->ioOwner;
    if (gTraceLatency && req->tArrive && req->tDurable) {
        const uint64_t now = steadyNowNs();
        gStages.toWriterNs.fetch_add(req->tWriter - req->tArrive,
                                     std::memory_order_relaxed);
        gStages.writeNs.fetch_add(req->tWritten - req->tWriter,
                                  std::memory_order_relaxed);
        gStages.durableNs.fetch_add(req->tDurable - req->tWritten,
                                    std::memory_order_relaxed);
        gStages.respondNs.fetch_add(now - req->tDurable,
                                    std::memory_order_relaxed);
        gStages.count.fetch_add(1, std::memory_order_relaxed);
    }
    if (!closing_) {
        McbpStatus status = req->resultStatus.IsOK()
                                    ? McbpStatus::Success
                                    : McbpStatus::InternalError;
        appendEmptyResponse(
                pendingWriteBuf_, req->opcode, status, req->opaque, req->resultSeqno);
        scheduleFlush();
    }
    releaseRequest(req);
    if (iot) {
        iot->Unreserve();
    }
    if (outstandingRequests_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        onDrained();
    }
}

void Connection::sendGetResponse(Request* req) {
    if (!closing_) {
        if (!req->resultStatus.IsOK()) {
            appendEmptyResponse(pendingWriteBuf_,
                                req->opcode,
                                McbpStatus::KeyNotFound,
                                req->opaque);
        } else {
            const void* val = req->responseBuf.empty()
                                      ? nullptr
                                      : req->responseBuf.data();
            size_t valLen = req->responseBuf.size();
            appendGetResponse(pendingWriteBuf_,
                              req->opaque,
                              req->resultSeqno,
                              req->resultFlags,
                              val,
                              valLen,
                              req->resultDatatype);
        }
        scheduleFlush();
    }
    releaseRequest(req);
    if (outstandingRequests_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        onDrained();
    }
}

} // namespace kvserver
} // namespace magma
