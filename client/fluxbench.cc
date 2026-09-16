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
    // Pre-generate mode: build `pregen` request buffers per connection up
    // front, each holding `batch` requests. 0 keeps the latency-measuring path.
    size_t pregen = 0;
    size_t batch = 0; // requests per pre-generated buffer; 0 => pipeline
    // Fixed key length. Every byte of key is a byte on the wire in both
    // directions, so a variable-length "key_123" costs throughput once the
    // link saturates: at 8-byte values the request IS mostly key and header.
    // 0 keeps the original variable-length keys.
    size_t keylen = 0;
};

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

struct Result {
    uint64_t ops = 0;
    uint64_t errors = 0;
    std::map<uint16_t, uint64_t> statusCounts;
    std::vector<uint32_t> latenciesUs;
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

// Builds one buffer of `batch` GET requests. Keys are drawn up front so the
// run-time loop touches nothing but the socket.
std::vector<uint8_t> buildGetBuffer(const Options& opts,
                                    std::mt19937_64& rng,
                                    size_t batch) {
    std::vector<uint8_t> buf;
    buf.reserve(batch * (kHeaderSize + 24));
    for (size_t i = 0; i < batch; i++) {
        const std::string key = keyFor(rng() % opts.keys);
        appendRequest(buf,
                      kOpGet,
                      key,
                      vbucketFor(key, opts.vbuckets),
                      static_cast<uint32_t>(i),
                      nullptr);
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
    std::vector<std::vector<uint8_t>> buffers;
    buffers.reserve(opts.pregen);
    for (size_t i = 0; i < opts.pregen; i++) {
        buffers.push_back(buildGetBuffer(opts, rng, batch));
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
    result.latenciesUs.reserve(1 << 16);

    // SET walks the keyspace in a stride, so one load pass writes every key
    // exactly once and a later GET pass can demand a zero-miss result.
    // Choosing keys at random instead leaves part of the keyspace unwritten,
    // and the misses that follow are cheap - which inflates the read rate.
    size_t setCursor = threadIndex;

    while (!stop.load(std::memory_order_relaxed)) {
        sendBuf.clear();
        auto batch = opts.pipeline;
        if (doSet) {
            if (setCursor >= opts.keys) {
                break; // this connection's share of the keyspace is written
            }
            const size_t remaining = (opts.keys - setCursor + opts.conns - 1) /
                                     opts.conns;
            batch = std::min(batch, remaining);
        }

        for (size_t i = 0; i < batch; i++) {
            size_t keyIndex;
            if (doSet) {
                keyIndex = setCursor;
                setCursor += opts.conns;
            } else {
                keyIndex = rng() % opts.keys;
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
            result.latenciesUs.push_back(static_cast<uint32_t>(us));

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

uint32_t percentile(std::vector<uint32_t>& sorted, double p) {
    if (sorted.empty()) {
        return 0;
    }
    size_t idx = static_cast<size_t>(p * static_cast<double>(sorted.size()));
    if (idx >= sorted.size()) {
        idx = sorted.size() - 1;
    }
    return sorted[idx];
}

void usage(const char* prog) {
    std::cerr
            << "Usage: " << prog << " [options]\n"
            << "  -host HOST:PORT   server address (default 127.0.0.1:11210)\n"
            << "  -conns N          connections, one thread each (default 8)\n"
            << "  -pipeline N       requests in flight per connection "
               "(default 8)\n"
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
            << "  -randvals         random, incompressible values\n"
            << "  -seed N           RNG seed (default 1)\n"
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
        if (arg == "-host") {
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
        } else if (arg == "-randvals") {
            opts.randvals = true;
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
    if (opts.conns == 0 || opts.pipeline == 0 || opts.keys == 0) {
        std::cerr << "-conns, -pipeline and -keys must be non-zero\n";
        return 2;
    }

    std::vector<Result> results(opts.conns);
    std::vector<std::thread> threads;
    gKeyLen = opts.keylen;
    std::atomic<bool> stop{false};

    const auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < opts.conns; i++) {
        threads.emplace_back([&, i]() {
            if (opts.pregen > 0) {
                runConnectionPregen(opts, i, stop, results[i]);
            } else {
                runConnection(opts, i, stop, results[i]);
            }
        });
    }

    std::this_thread::sleep_for(opts.runtime);
    stop.store(true, std::memory_order_relaxed);
    for (auto& t : threads) {
        t.join();
    }
    const auto elapsed = std::chrono::duration<double>(
                                 std::chrono::steady_clock::now() - start)
                                 .count();

    uint64_t ops = 0;
    uint64_t errors = 0;
    std::vector<uint32_t> all;
    std::map<uint16_t, uint64_t> statusCounts;
    for (auto& r : results) {
        ops += r.ops;
        errors += r.errors;
        all.insert(all.end(), r.latenciesUs.begin(), r.latenciesUs.end());
        for (const auto& [status, count] : r.statusCounts) {
            statusCounts[status] += count;
        }
    }
    std::sort(all.begin(), all.end());

    const double rate = (elapsed > 0) ? static_cast<double>(ops) / elapsed : 0;

    std::cout << "DONE ops=" << ops << " errs=" << errors << " secs="
              << elapsed << " rate=" << static_cast<uint64_t>(rate) << "\n";
    std::cout << "LAT p50=" << percentile(all, 0.50) << "us"
              << " p90=" << percentile(all, 0.90) << "us"
              << " p99=" << percentile(all, 0.99) << "us"
              << " p999=" << percentile(all, 0.999) << "us\n";

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
