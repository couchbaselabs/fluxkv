// fluxbench - a pipelined mcbp load generator for fluxkv.
//
// Deliberately dependency-free: plain sockets, threads and the standard
// library. It builds without magma or folly, so it can be compiled on a
// load-generator machine that has no Couchbase tree.
//
// One thread per connection. Each thread keeps `pipeline` requests in flight:
// it sends a batch, then reads the matching responses, timing each request
// from its own send to its own response.
//
// -pregen switches to a throughput mode that builds the request bytes once at
// startup and then only sends buffers and counts responses. The default path
// formats every request and timestamps every operation inside the loop, which
// costs more CPU on the client than the server spends answering - past a few
// million ops/s it measures the load generator, not the server.

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr uint8_t kRequestMagic = 0x80;
constexpr uint8_t kResponseMagic = 0x81;
constexpr size_t kHeaderSize = 24;
constexpr uint8_t kOpGet = 0x00;
constexpr uint8_t kOpSet = 0x01;
constexpr uint8_t kOpSaslAuth = 0x21;
constexpr uint8_t kOpSelectBucket = 0x89;

struct Options {
    std::string host = "127.0.0.1";
    uint16_t port = 11210;
    size_t conns = 8;
    size_t pipeline = 8;
    size_t keys = 1000000;
    uint16_t vbuckets = 256;
    size_t valsize = 1024;
    std::string mode = "get";
    bool randvals = false;
    uint64_t seed = 1;
    std::chrono::seconds runtime{30};
    // Paced load: total ops/s across all connections, each connection
    // sending one pipeline-sized batch per interval. 0 = closed loop.
    double rate = 0;
    // Pre-generate mode: build `pregen` request buffers per connection up
    // front, each holding `batch` requests. 0 keeps the latency-measuring path.
    size_t pregen = 0;
    size_t batch = 0; // requests per pre-generated buffer; 0 => pipeline
    // Sliding window: keep `pipeline` requests in flight and refill each
    // slot as its response lands, instead of sending a batch and waiting for
    // all of it. The batch mode's rate is set by the slowest op per batch.
    bool window = false;
    // Fixed key length. Every byte of key is a byte on the wire in both
    // directions, so a variable-length "key_123" costs throughput once the
    // link saturates: at 8-byte values the request IS mostly key and header.
    // 0 keeps the original variable-length keys.
    size_t keylen = 0;
    // Zipf skew for key selection. 0 = uniform. Higher values concentrate
    // more of the load on fewer keys.
    double zipf = 0.0;
    // SASL PLAIN credentials and bucket. Empty user skips the handshake,
    // which is what a server without authentication expects.
    std::string user;
    std::string pass;
    std::string bucket;
};

Options gOpts;

// Couchbase vbucket mapping: CRC32 of the key, high bits masked, modulo the
// vbucket count. Clients must agree with whatever wrote the data, so keep this
// identical to the standard Couchbase client algorithm.
uint32_t crc32(const void* data, size_t len) {
    static uint32_t table[256];
    static bool init = false;
    if (!init) {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int k = 0; k < 8; k++) {
                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }
            table[i] = c;
        }
        init = true;
    }
    const auto* p = static_cast<const uint8_t*>(data);
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) {
        c = table[(c ^ p[i]) & 0xFF] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}

uint16_t vbucketFor(const std::string& key, uint16_t vbuckets) {
    return static_cast<uint16_t>(((crc32(key.data(), key.size()) >> 16) &
                                  0x7FFF) %
                                 vbuckets);
}

size_t gKeyLen = 0;

// Zipf-distributed key selection over [0, n).
//
// Ranks are scrambled through a hash before use, so the hot keys spread
// across the key space instead of sitting next to each other. Without that,
// skew would also concentrate on a few vbuckets and a per-vbucket queue
// would be measured on that rather than on the skew itself.
class ZipfGen {
public:
    ZipfGen() = default;
    ZipfGen(size_t n, double theta) : n_(n) {
        zetan_ = zeta(n, theta);
        const double zeta2 = zeta(2, theta);
        alpha_ = 1.0 / (1.0 - theta);
        eta_ = (1 - std::pow(2.0 / static_cast<double>(n), 1 - theta)) /
               (1 - zeta2 / zetan_);
        half_ = std::pow(0.5, theta);
    }

    size_t Next(std::mt19937_64& rng) const {
        const double u = std::generate_canonical<double, 53>(rng);
        const double uz = u * zetan_;
        size_t rank;
        if (uz < 1.0) {
            rank = 0;
        } else if (uz < 1.0 + half_) {
            rank = 1;
        } else {
            rank = static_cast<size_t>(static_cast<double>(n_) *
                                       std::pow(eta_ * u - eta_ + 1, alpha_));
            if (rank >= n_) {
                rank = n_ - 1;
            }
        }
        return scramble(rank) % n_;
    }

private:
    static double zeta(size_t n, double theta) {
        double s = 0;
        for (size_t i = 1; i <= n; i++) {
            s += 1.0 / std::pow(static_cast<double>(i), theta);
        }
        return s;
    }
    static size_t scramble(size_t x) {
        uint64_t h = static_cast<uint64_t>(x) + 0x9e3779b97f4a7c15ULL;
        h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9ULL;
        h = (h ^ (h >> 27)) * 0x94d049bb133111ebULL;
        return static_cast<size_t>(h ^ (h >> 31));
    }

    size_t n_{1};
    double zetan_{1}, alpha_{0}, eta_{0}, half_{0};
};

// Built once when -zipf is given; shared read-only by every connection.
ZipfGen gZipf;
bool gZipfOn = false;

inline size_t pickKey(const Options& opts, std::mt19937_64& rng) {
    return gZipfOn ? gZipf.Next(rng) : (rng() % opts.keys);
}

std::string keyFor(size_t index) {
    if (gKeyLen == 0) {
        return "key_" + std::to_string(index);
    }
    // Zero-padded decimal, so every key is exactly gKeyLen bytes and distinct
    // indices stay distinct as long as the width holds the key space.
    std::string k(gKeyLen, '0');
    for (size_t i = gKeyLen; i-- > 0 && index > 0;) {
        k[i] = static_cast<char>('0' + (index % 10));
        index /= 10;
    }
    return k;
}

void putU16(uint8_t* p, uint16_t v) {
    v = htons(v);
    std::memcpy(p, &v, 2);
}

void putU32(uint8_t* p, uint32_t v) {
    v = htonl(v);
    std::memcpy(p, &v, 4);
}

uint16_t getU16(const uint8_t* p) {
    uint16_t v;
    std::memcpy(&v, p, 2);
    return ntohs(v);
}

uint32_t getU32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return ntohl(v);
}

// Appends one mcbp request. GET carries just the key; SET carries 8 bytes of
// extras (flags, expiry) followed by key and value.
void appendRequest(std::vector<uint8_t>& out,
                   uint8_t opcode,
                   const std::string& key,
                   uint16_t vbucket,
                   uint32_t opaque,
                   const std::string* value) {
    const size_t extrasLen = (opcode == kOpSet) ? 8 : 0;
    const size_t valueLen = value ? value->size() : 0;
    const size_t bodyLen = extrasLen + key.size() + valueLen;

    uint8_t h[kHeaderSize] = {};
    h[0] = kRequestMagic;
    h[1] = opcode;
    putU16(h + 2, static_cast<uint16_t>(key.size()));
    h[4] = static_cast<uint8_t>(extrasLen);
    h[5] = 0; // datatype
    putU16(h + 6, vbucket);
    putU32(h + 8, static_cast<uint32_t>(bodyLen));
    putU32(h + 12, opaque);
    // cas stays zero

    out.insert(out.end(), h, h + kHeaderSize);
    if (extrasLen) {
        uint8_t extras[8] = {}; // flags=0, expiry=0
        out.insert(out.end(), extras, extras + 8);
    }
    out.insert(out.end(), key.begin(), key.end());
    if (value) {
        out.insert(out.end(), value->begin(), value->end());
    }
}

bool writeFully(int fd, const uint8_t* data, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = ::send(fd, data + off, len - off, MSG_NOSIGNAL);
        if (n <= 0) {
            return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

bool readFully(int fd, uint8_t* data, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = ::recv(fd, data + off, len - off, 0);
        if (n <= 0) {
            return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

// One synchronous mcbp command on an otherwise idle connection. Used only
// for the pre-pipeline handshake, so it reads and discards the whole reply.
bool simpleCmd(int fd,
               uint8_t opcode,
               const std::string& key,
               const std::string& value) {
    std::vector<uint8_t> req;
    uint8_t h[kHeaderSize] = {};
    h[0] = kRequestMagic;
    h[1] = opcode;
    putU16(h + 2, static_cast<uint16_t>(key.size()));
    putU32(h + 8, static_cast<uint32_t>(key.size() + value.size()));
    req.insert(req.end(), h, h + kHeaderSize);
    req.insert(req.end(), key.begin(), key.end());
    req.insert(req.end(), value.begin(), value.end());
    if (!writeFully(fd, req.data(), req.size())) {
        return false;
    }
    uint8_t rh[kHeaderSize];
    if (!readFully(fd, rh, kHeaderSize)) {
        return false;
    }
    const uint32_t bodyLen = getU32(rh + 8);
    std::vector<uint8_t> body(bodyLen);
    if (bodyLen && !readFully(fd, body.data(), bodyLen)) {
        return false;
    }
    return (static_cast<uint16_t>(rh[6] << 8 | rh[7])) == 0;
}

// SASL PLAIN then SELECT_BUCKET. Servers that take neither leave -user unset.
bool handshake(int fd) {
    if (gOpts.user.empty()) {
        return true;
    }
    std::string plain;
    plain.push_back('\0');
    plain += gOpts.user;
    plain.push_back('\0');
    plain += gOpts.pass;
    if (!simpleCmd(fd, kOpSaslAuth, "PLAIN", plain)) {
        return false;
    }
    return gOpts.bucket.empty() ||
           simpleCmd(fd, kOpSelectBucket, gOpts.bucket, "");
}

int connectTo(const std::string& host, uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    const std::string portStr = std::to_string(port);
    if (::getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0) {
        return -1;
    }
    int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd >= 0 && ::connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(res);
    if (fd >= 0) {
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        if (!handshake(fd)) {
            ::close(fd);
            fd = -1;
        }
    }
    return fd;
}

// mcbp status codes the server can return. Knowing which one came back
// matters: TmpFail means the engine applied backpressure and the load was
// simply too heavy for the disk, while KeyNotFound means a read missed.
const char* statusName(uint16_t status) {
    switch (status) {
    case 0x0000:
        return "Success";
    case 0x0001:
        return "KeyNotFound";
    case 0x0002:
        return "KeyExists";
    case 0x0007:
        return "NotMyVbucket";
    case 0x0020:
        return "AuthError";
    case 0x0081:
        return "UnknownCommand";
    case 0x0084:
        return "InternalError";
    case 0x0086:
        return "TmpFail";
    default:
        return "Other";
    }
}

// Latency histogram: 1 us .. ~1.7 s in buckets 1% apart (log2 with 64 sub-steps),
// fixed size. The previous per-sample vector grew without bound (3.3 GB at
// 700K ops/s over 20 min) and its reallocations stalled connections for long
// enough to bend a durable run's throughput down 40% over the run.
struct LatencyHist {
    static constexpr size_t kSub = 64;
    static constexpr size_t kBuckets = 31 * kSub;
    std::vector<uint64_t> counts = std::vector<uint64_t>(kBuckets, 0);
    uint64_t total = 0;
    static size_t bucket(uint32_t us) {
        if (us < 1) {
            us = 1;
        }
        const uint32_t lg = 31 - __builtin_clz(us);
        const uint32_t sub = lg >= 6 ? (us >> (lg - 6)) & (kSub - 1)
                                     : (us << (6 - lg)) & (kSub - 1);
        const size_t b = lg * kSub + sub;
        return b < kBuckets ? b : kBuckets - 1;
    }
    static uint32_t upper(size_t b) {
        const uint32_t lg = static_cast<uint32_t>(b / kSub);
        const uint32_t sub = static_cast<uint32_t>(b % kSub) + 1;
        return lg >= 6 ? ((1u << lg) + (sub << (lg - 6)))
                       : ((1u << lg) + (sub >> (6 - lg)));
    }
    void add(uint32_t us) {
        counts[bucket(us)]++;
        total++;
    }
    void merge(const LatencyHist& o) {
        for (size_t i = 0; i < kBuckets; i++) {
            counts[i] += o.counts[i];
        }
        total += o.total;
    }
    double mean() const {
        if (total == 0) {
            return 0;
        }
        double sum = 0;
        for (size_t i = 0; i < kBuckets; i++) {
            if (counts[i]) {
                sum += static_cast<double>(counts[i]) * upper(i);
            }
        }
        return sum / static_cast<double>(total);
    }
    uint32_t max() const {
        for (size_t i = kBuckets; i-- > 0;) {
            if (counts[i]) {
                return upper(i);
            }
        }
        return 0;
    }
    uint32_t percentile(double p) const {
        if (total == 0) {
            return 0;
        }
        uint64_t want = static_cast<uint64_t>(p * static_cast<double>(total));
        if (want >= total) {
            want = total - 1;
        }
        uint64_t seen = 0;
        for (size_t i = 0; i < kBuckets; i++) {
            seen += counts[i];
            if (seen > want) {
                return upper(i);
            }
        }
        return upper(kBuckets - 1);
    }
};

struct Result {
    uint64_t ops = 0;
    uint64_t errors = 0;
    std::map<uint16_t, uint64_t> statusCounts;
    LatencyHist latencies;
};

// Streaming response counter for pre-generate mode. Walks the mcbp framing in
// whatever chunks recv() hands back, without copying or even looking at
// bodies: a GET response is header + extras + value, and all we need is the
// status and how far to skip.
class ResponseCounter {
public:
    // Returns false if the stream is not mcbp - a desync we must not paper
    // over, since a client that miscounts reports a throughput that is not
    // real.
    bool Feed(const uint8_t* p, size_t n, Result& r) {
        while (n > 0) {
            if (bodyRemaining_ > 0) {
                const size_t take = std::min(n, bodyRemaining_);
                p += take;
                n -= take;
                bodyRemaining_ -= take;
                continue;
            }
            const size_t take = std::min(n, kHeaderSize - haveHeader_);
            std::memcpy(header_ + haveHeader_, p, take);
            haveHeader_ += take;
            p += take;
            n -= take;
            if (haveHeader_ < kHeaderSize) {
                return true;
            }
            haveHeader_ = 0;
            if (header_[0] != kResponseMagic) {
                return false;
            }
            const uint16_t status = getU16(header_ + 6);
            bodyRemaining_ = getU32(header_ + 8);
            r.statusCounts[status]++;
            if (status == 0) {
                r.ops++;
            } else {
                r.errors++;
            }
            completed_++;
        }
        return true;
    }
    // Responses seen since the last call; the caller uses this to decide how
    // many more request buffers it may put on the wire.
    uint64_t TakeCompleted() {
        const uint64_t c = completed_;
        completed_ = 0;
        return c;
    }

private:
    uint8_t header_[kHeaderSize];
    size_t haveHeader_ = 0;
    size_t bodyRemaining_ = 0;
    uint64_t completed_ = 0;
};

// Builds one buffer of `batch` requests of the configured op. Keys are drawn
// up front so the run-time loop touches nothing but the socket.
std::vector<uint8_t> buildBuffer(const Options& opts,
                                 std::mt19937_64& rng,
                                 size_t batch,
                                 const std::string* value) {
    const bool doSet = value != nullptr;
    std::vector<uint8_t> buf;
    buf.reserve(batch * (kHeaderSize + 24 + (doSet ? opts.valsize : 0)));
    for (size_t i = 0; i < batch; i++) {
        const std::string key = keyFor(pickKey(opts, rng));
        appendRequest(buf,
                      doSet ? kOpSet : kOpGet,
                      key,
                      vbucketFor(key, opts.vbuckets),
                      static_cast<uint32_t>(i),
                      value);
    }
    return buf;
}

// Throughput path: send pre-built buffers, keep `pipeline` requests in
// flight, count responses. No formatting and no clock reads per operation.
void runConnectionPregen(const Options& opts,
                         size_t threadIndex,
                         std::atomic<bool>& stop,
                         Result& result) {
    int fd = connectTo(opts.host, opts.port);
    if (fd < 0) {
        result.errors++;
        return;
    }
    // Big socket buffers: at a few million ops/s the default sizes turn into
    // extra wakeups on both ends.
    int bufSize = 8 << 20;
    ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufSize, sizeof(bufSize));
    ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufSize, sizeof(bufSize));

    std::mt19937_64 rng(opts.seed + threadIndex);
    const size_t batch = opts.batch ? opts.batch : opts.pipeline;
    std::string value;
    if (opts.mode == "set") {
        value.resize(opts.valsize);
        if (opts.randvals) {
            for (auto& c : value) {
                c = static_cast<char>(rng() & 0xFF);
            }
        } else {
            std::fill(value.begin(), value.end(), 'x');
        }
    }
    std::vector<std::vector<uint8_t>> buffers;
    buffers.reserve(opts.pregen);
    for (size_t i = 0; i < opts.pregen; i++) {
        buffers.push_back(buildBuffer(
                opts, rng, batch, value.empty() ? nullptr : &value));
    }

    ResponseCounter counter;
    std::vector<uint8_t> rx(1 << 20);
    const uint64_t window = std::max<uint64_t>(opts.pipeline, batch);
    uint64_t outstanding = 0;
    size_t next = 0;

    while (!stop.load(std::memory_order_relaxed)) {
        while (outstanding < window) {
            const auto& b = buffers[next++ % buffers.size()];
            if (!writeFully(fd, b.data(), b.size())) {
                result.errors++;
                ::close(fd);
                return;
            }
            outstanding += batch;
        }
        const ssize_t n = ::recv(fd, rx.data(), rx.size(), 0);
        if (n <= 0) {
            result.errors++;
            break;
        }
        if (!counter.Feed(rx.data(), static_cast<size_t>(n), result)) {
            result.errors++;
            break;
        }
        const uint64_t done = counter.TakeCompleted();
        outstanding = (done >= outstanding) ? 0 : outstanding - done;
    }
    ::close(fd);
}

// Window path: every connection keeps `pipeline` requests outstanding and
// replaces each one as soon as it completes, so one slow op holds one slot
// rather than the whole batch. Latency is still measured per request.
void runConnectionWindow(const Options& opts,
                         size_t threadIndex,
                         std::atomic<bool>& stop,
                         Result& result) {
    int fd = connectTo(opts.host, opts.port);
    if (fd < 0) {
        result.errors++;
        return;
    }
    int bufSize = 8 << 20;
    ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufSize, sizeof(bufSize));
    ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufSize, sizeof(bufSize));

    std::mt19937_64 rng(opts.seed + threadIndex);
    const bool doSet = (opts.mode == "set");
    std::string value;
    if (doSet) {
        value.resize(opts.valsize);
        if (opts.randvals) {
            for (auto& c : value) {
                c = static_cast<char>(rng() & 0xFF);
            }
        } else {
            std::fill(value.begin(), value.end(), 'x');
        }
    }

    const size_t slots = opts.pipeline;
    std::vector<std::chrono::steady_clock::time_point> sentAt(slots);
    std::vector<uint32_t> freed;
    freed.reserve(slots);
    for (uint32_t i = 0; i < slots; i++) {
        freed.push_back(i);
    }
    std::vector<uint8_t> sendBuf;
    std::vector<uint8_t> rx(1 << 20);
    size_t have = 0;

    while (!stop.load(std::memory_order_relaxed)) {
        if (!freed.empty()) {
            sendBuf.clear();
            const auto now = std::chrono::steady_clock::now();
            for (uint32_t slot : freed) {
                const std::string key = keyFor(pickKey(opts, rng));
                appendRequest(sendBuf,
                              doSet ? kOpSet : kOpGet,
                              key,
                              vbucketFor(key, opts.vbuckets),
                              slot,
                              doSet ? &value : nullptr);
                sentAt[slot] = now;
            }
            freed.clear();
            if (!writeFully(fd, sendBuf.data(), sendBuf.size())) {
                result.errors++;
                break;
            }
        }
        if (have == rx.size()) {
            rx.resize(rx.size() * 2);
        }
        const ssize_t n = ::recv(fd, rx.data() + have, rx.size() - have, 0);
        if (n <= 0) {
            result.errors++;
            break;
        }
        have += static_cast<size_t>(n);
        const auto now = std::chrono::steady_clock::now();
        size_t p = 0;
        while (have - p >= kHeaderSize) {
            const uint8_t* h = rx.data() + p;
            const uint32_t bodyLen = getU32(h + 8);
            if (have - p < kHeaderSize + bodyLen) {
                break;
            }
            if (h[0] != kResponseMagic) {
                result.errors++;
                ::close(fd);
                return;
            }
            const uint32_t opaque = getU32(h + 12);
            const uint16_t status = getU16(h + 6);
            if (opaque < slots) {
                result.latencies.add(static_cast<uint32_t>(
                        std::chrono::duration_cast<std::chrono::microseconds>(
                                now - sentAt[opaque])
                                .count()));
                freed.push_back(opaque);
            }
            result.statusCounts[status]++;
            if (status == 0) {
                result.ops++;
            } else {
                result.errors++;
            }
            p += kHeaderSize + bodyLen;
        }
        if (p > 0) {
            std::memmove(rx.data(), rx.data() + p, have - p);
            have -= p;
        }
    }
    ::close(fd);
}

void runConnection(const Options& opts,
                   size_t threadIndex,
                   std::atomic<bool>& stop,
                   Result& result) {
    int fd = connectTo(opts.host, opts.port);
    if (fd < 0) {
        result.errors++;
        return;
    }

    std::mt19937_64 rng(opts.seed + threadIndex);
    const bool doSet = (opts.mode == "set");

    // A value buffer reused for every SET. Random bytes make it
    // incompressible, which matters when comparing compression settings.
    std::string value;
    if (doSet) {
        value.resize(opts.valsize);
        if (opts.randvals) {
            for (auto& c : value) {
                c = static_cast<char>(rng() & 0xFF);
            }
        } else {
            std::fill(value.begin(), value.end(), 'x');
        }
    }

    std::vector<uint8_t> sendBuf;
    std::vector<std::chrono::steady_clock::time_point> sentAt(opts.pipeline);
    std::vector<uint8_t> header(kHeaderSize);
    std::vector<uint8_t> body;

    // SET walks the keyspace in a stride, so one load pass writes every key
    // exactly once and a later GET pass can demand a zero-miss result.
    // Choosing keys at random instead leaves part of the keyspace unwritten,
    // and the misses that follow are cheap - which inflates the read rate.
    size_t setCursor = threadIndex;
    std::chrono::steady_clock::time_point nextSend{};

    while (!stop.load(std::memory_order_relaxed)) {
        if (opts.rate > 0) {
            const double perConn = opts.rate / static_cast<double>(opts.conns);
            const auto interval = std::chrono::duration_cast<
                    std::chrono::steady_clock::duration>(
                    std::chrono::duration<double>(
                            static_cast<double>(opts.pipeline) / perConn));
            if (nextSend == std::chrono::steady_clock::time_point{}) {
                nextSend = std::chrono::steady_clock::now();
            }
            std::this_thread::sleep_until(nextSend);
            nextSend += interval;
            // A stall longer than the interval is not made up for: catching up
            // would burst and measure the burst.
            if (nextSend < std::chrono::steady_clock::now()) {
                nextSend = std::chrono::steady_clock::now();
            }
        }
        sendBuf.clear();
        auto batch = opts.pipeline;
        if (doSet && !gZipfOn) {
            if (setCursor >= opts.keys) {
                break; // this connection's share of the keyspace is written
            }
            const size_t remaining = (opts.keys - setCursor + opts.conns - 1) /
                                     opts.conns;
            batch = std::min(batch, remaining);
        }

        for (size_t i = 0; i < batch; i++) {
            size_t keyIndex;
            if (doSet && !gZipfOn) {
                keyIndex = setCursor;
                setCursor += opts.conns;
            } else {
                keyIndex = pickKey(opts, rng);
            }
            const std::string key = keyFor(keyIndex);
            appendRequest(sendBuf,
                          doSet ? kOpSet : kOpGet,
                          key,
                          vbucketFor(key, opts.vbuckets),
                          static_cast<uint32_t>(i),
                          doSet ? &value : nullptr);
            sentAt[i] = std::chrono::steady_clock::now();
        }

        if (!writeFully(fd, sendBuf.data(), sendBuf.size())) {
            result.errors++;
            break;
        }

        bool broken = false;
        for (size_t i = 0; i < batch; i++) {
            if (!readFully(fd, header.data(), kHeaderSize)) {
                broken = true;
                break;
            }
            if (header[0] != kResponseMagic) {
                result.errors++;
                broken = true;
                break;
            }
            const uint32_t bodyLen = getU32(header.data() + 8);
            const uint32_t opaque = getU32(header.data() + 12);
            const uint16_t status = getU16(header.data() + 6);

            if (bodyLen > 0) {
                body.resize(bodyLen);
                if (!readFully(fd, body.data(), bodyLen)) {
                    broken = true;
                    break;
                }
            }

            const auto now = std::chrono::steady_clock::now();
            const size_t slot = (opaque < batch) ? opaque : i;
            const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                    now - sentAt[slot])
                                    .count();
            result.latencies.add(static_cast<uint32_t>(us));

            result.statusCounts[status]++;
            if (status == 0) {
                result.ops++;
            } else {
                result.errors++;
            }
        }
        if (broken) {
            result.errors++;
            break;
        }
    }

    ::close(fd);
}

void usage(const char* prog) {
    std::cerr
            << "Usage: " << prog << " [options]\n"
            << "  -host HOST:PORT   server address (default 127.0.0.1:11210)\n"
            << "  -conns N          connections, one thread each (default 8)\n"
            << "  -pipeline N       requests in flight per connection "
               "(default 8)\n"
            << "  -window           keep -pipeline requests in flight, refilling each "
               "as it completes (default: send a batch, wait for all of it)\n"
            << "  -pregen N         pre-generate N request buffers per "
               "connection and measure throughput only (no per-op latency); "
               "use this above ~1M ops/s or the client is what you measure\n"
            << "  -batch N          requests per pre-generated buffer "
               "(default: -pipeline)\n"
            << "  -keylen N         fixed key length, zero-padded (default: "
               "variable-length key_N). Shorter keys mean fewer bytes on the "
               "wire, which matters once the link saturates\n"
            << "  -keys N           key space size (default 1000000)\n"
            << "  -vbuckets N       vbucket count, must match the server "
               "(default 256)\n"
            << "  -valsize N        value size in bytes for set (default 1024)\n"
            << "  -mode get|set     operation (default get)\n"
            << "  -user U           SASL PLAIN user; unset skips the "
               "handshake entirely (default: unset)\n"
            << "  -pass P           SASL PLAIN password\n"
            << "  -bucket B         bucket to select after authenticating\n"
            << "  -randvals         random, incompressible values\n"
            << "  -zipf THETA       Zipf key selection (e.g. 0.99); default "
               "uniform. With -mode set this also switches SET from the "
               "one-pass stride load to sustained random writes\n"
            << "  -seed N           RNG seed (default 1)\n"
            << "  -rate N           paced load, total ops/s across connections "
               "(default 0: closed loop); use with a small -pipeline for "
               "latency at a fixed rate\n"
            << "  -runtime Ns       run duration in seconds (default 30s)\n";
}

} // namespace

int main(int argc, char** argv) {
    Options opts;

    for (int i = 1; i < argc; i++) {
        const std::string arg = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) {
                usage(argv[0]);
                std::exit(2);
            }
            return argv[++i];
        };
        if (arg == "-user") {
            opts.user = next();
        } else if (arg == "-pass") {
            opts.pass = next();
        } else if (arg == "-bucket") {
            opts.bucket = next();
        } else if (arg == "-host") {
            const std::string hp = next();
            const auto colon = hp.find(':');
            if (colon == std::string::npos) {
                opts.host = hp;
            } else {
                opts.host = hp.substr(0, colon);
                opts.port = static_cast<uint16_t>(std::stoi(hp.substr(colon + 1)));
            }
        } else if (arg == "-conns") {
            opts.conns = std::stoul(next());
        } else if (arg == "-pipeline") {
            opts.pipeline = std::stoul(next());
        } else if (arg == "-window") {
            opts.window = true;
        } else if (arg == "-pregen") {
            opts.pregen = std::stoul(next());
        } else if (arg == "-batch") {
            opts.batch = std::stoul(next());
        } else if (arg == "-keylen") {
            opts.keylen = std::stoul(next());
        } else if (arg == "-keys") {
            opts.keys = std::stoul(next());
        } else if (arg == "-vbuckets") {
            opts.vbuckets = static_cast<uint16_t>(std::stoi(next()));
        } else if (arg == "-valsize") {
            opts.valsize = std::stoul(next());
        } else if (arg == "-mode") {
            opts.mode = next();
        } else if (arg == "-zipf") {
            opts.zipf = std::stod(next());
        } else if (arg == "-randvals") {
            opts.randvals = true;
        } else if (arg == "-rate") {
            opts.rate = std::stod(next());
        } else if (arg == "-seed") {
            opts.seed = std::stoull(next());
        } else if (arg == "-runtime") {
            std::string v = next();
            if (!v.empty() && v.back() == 's') {
                v.pop_back();
            }
            opts.runtime = std::chrono::seconds(std::stoll(v));
        } else if (arg == "-h" || arg == "--help") {
            usage(argv[0]);
            return 0;
        } else {
            std::cerr << "unknown option: " << arg << "\n";
            usage(argv[0]);
            return 2;
        }
    }

    if (opts.mode != "get" && opts.mode != "set") {
        std::cerr << "-mode must be get or set\n";
        return 2;
    }
    if (opts.window && opts.rate > 0) {
        std::cerr << "-window is closed loop; it does not combine with -rate\n";
        return 1;
    }
    if (opts.conns == 0 || opts.pipeline == 0 || opts.keys == 0) {
        std::cerr << "-conns, -pipeline and -keys must be non-zero\n";
        return 2;
    }

    gOpts = opts; // connectTo() reads the credentials from here

    std::vector<Result> results(opts.conns);
    std::vector<std::thread> threads;
    gKeyLen = opts.keylen;
    if (opts.zipf > 0.0) {
        // Summing the series over the whole key space takes a few seconds at
        // 100M+ keys, so do it once here rather than per connection.
        const auto t0 = std::chrono::steady_clock::now();
        gZipf = ZipfGen(opts.keys, opts.zipf);
        gZipfOn = true;
        std::cerr << "zipf theta=" << opts.zipf << " over " << opts.keys
                  << " keys (setup "
                  << std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - t0)
                             .count()
                  << "s)\n";
    }
    std::atomic<bool> stop{false};
    std::atomic<size_t> finished{0};

    const auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < opts.conns; i++) {
        threads.emplace_back([&, i]() {
            struct Done {
                std::atomic<size_t>& n;
                ~Done() {
                    n.fetch_add(1, std::memory_order_relaxed);
                }
            } done{finished};
            if (opts.pregen > 0) {
                runConnectionPregen(opts, i, stop, results[i]);
            } else if (opts.window) {
                runConnectionWindow(opts, i, stop, results[i]);
            } else {
                runConnection(opts, i, stop, results[i]);
            }
        });
    }

    // A load pass ends when every connection has written its share; do not
    // sit out the rest of -runtime.
    const auto deadline = start + opts.runtime;
    while (std::chrono::steady_clock::now() < deadline &&
           finished.load(std::memory_order_relaxed) < opts.conns) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    stop.store(true, std::memory_order_relaxed);
    for (auto& t : threads) {
        t.join();
    }
    const auto elapsed = std::chrono::duration<double>(
                                 std::chrono::steady_clock::now() - start)
                                 .count();

    uint64_t ops = 0;
    uint64_t errors = 0;
    LatencyHist all;
    std::map<uint16_t, uint64_t> statusCounts;
    for (auto& r : results) {
        ops += r.ops;
        errors += r.errors;
        all.merge(r.latencies);
        for (const auto& [status, count] : r.statusCounts) {
            statusCounts[status] += count;
        }
    }

    const double rate = (elapsed > 0) ? static_cast<double>(ops) / elapsed : 0;

    std::cout << "DONE ops=" << ops << " errs=" << errors << " secs="
              << elapsed << " rate=" << static_cast<uint64_t>(rate) << "\n";
    std::cout << "LAT p50=" << all.percentile(0.50) << "us"
              << " p90=" << all.percentile(0.90) << "us"
              << " p99=" << all.percentile(0.99) << "us"
              << " p999=" << all.percentile(0.999) << "us"
              << " p9999=" << all.percentile(0.9999) << "us"
              << " max=" << all.max() << "us"
              << " mean=" << static_cast<uint64_t>(all.mean()) << "us\n";

    // Break the failures down by status. TmpFail dominating means the load
    // outran the disk, not that anything is wrong.
    if (errors > 0) {
        std::cout << "STATUS";
        for (const auto& [status, count] : statusCounts) {
            if (status != 0) {
                std::cout << " " << statusName(status) << "=" << count;
            }
        }
        std::cout << "\n";
    }

    return (errors > 0 && ops == 0) ? 1 : 0;
}
