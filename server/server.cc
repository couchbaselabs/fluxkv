#include "server.h"
#include "connection.h"

#include <folly/io/async/AsyncSocket.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <limits>

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

namespace magma {
namespace kvserver {

Server::Server(Bucket* bucket,
               uint16_t port,
               size_t numIOThreads,
               const std::string& hostname,
               uint16_t numVBuckets,
               uint16_t statsPort)
    : bucket_(bucket), port_(port), hostname_(hostname), statsPort_(statsPort) {
    clusterConfig_ = buildClusterConfigJson(
            hostname_, port_, bucket_->GetName(), numVBuckets, statsPort);
    errorMap_ = buildErrorMapJson();

    for (size_t i = 0; i < numIOThreads; i++) {
        auto iot = std::make_unique<IOThread>();
        iot->evb = std::make_unique<folly::EventBase>();
        ioThreads_.push_back(std::move(iot));
    }
}

Server::~Server() {
    Stop();
}

void Server::EnableAutoTune(const TunerConfig& cfg,
                            ThreadTuner::Bounds ioBounds,
                            ThreadTuner::Bounds readerBounds) {
    tuner_ = std::make_unique<ThreadTuner>(cfg, []() {
        return gDispStats.cmdGet.Sum() + gDispStats.cmdSet.Sum() +
               gDispStats.cmdDelete.Sum();
    });
    ioPool_ = std::make_unique<IOPool>(this);
    readerGroup_ = std::make_unique<ReaderPoolGroup>(bucket_);
    tuner_->AddPool(ioPool_.get(), ioBounds);
    tuner_->AddPool(readerGroup_.get(), readerBounds);
}

void Server::addIOThread() {
    auto iot = std::make_unique<IOThread>();
    iot->evb = std::make_unique<folly::EventBase>();
    iot->Start();
    iot->lastCpuNs = iot->CpuNs();
    std::lock_guard<std::mutex> g(ioMu_);
    ioThreads_.push_back(std::move(iot));
}

// Least-loaded live loop. The count is taken at assignment, not when the
// connection object appears on the loop, so a burst of accepts spreads out
// instead of piling onto whichever loop looked emptiest first.
IOThread* Server::pickIOThread() {
    std::lock_guard<std::mutex> g(ioMu_);
    IOThread* best = nullptr;
    for (auto& iot : ioThreads_) {
        if (iot->retiring.load(std::memory_order_relaxed)) {
            continue;
        }
        if (!best || iot->Load() < best->Load()) {
            best = iot.get();
        }
    }
    if (best) {
        best->Reserve();
    }
    return best;
}

// ---- IOPool ----

size_t Server::IOPool::Size() const {
    std::lock_guard<std::mutex> g(s_->ioMu_);
    size_t n = 0;
    for (auto& iot : s_->ioThreads_) {
        if (!iot->retiring.load(std::memory_order_relaxed)) {
            n++;
        }
    }
    return n;
}

void Server::IOPool::Grow(size_t n) {
    for (size_t i = 0; i < n; i++) {
        s_->addIOThread();
    }
    rebalance();
}

namespace {
// Post to a loop: move one of its connections per destination. Each entry
// in dests holds a reservation; an entry that ends up unused releases it.
void postMigrations(IOThread* from, std::vector<IOThread*> dests) {
    if (dests.empty()) {
        return;
    }
    from->evb->runInEventBaseThread([from, dests = std::move(dests)]() {
        size_t next = 0;
        std::vector<Connection*> conns = from->conns;
        for (auto* c : conns) {
            if (next >= dests.size()) {
                break;
            }
            if (c->migrateTo(dests[next])) {
                next++;
            }
        }
        for (; next < dests.size(); next++) {
            dests[next]->Unreserve();
        }
    });
}

// Plan up to k moves onto the emptiest of `to`, reserving each slot so
// concurrent plans do not all pick the same loop. Stops early once every
// candidate holds at least `fillTo` connections.
std::vector<IOThread*> planMoves(int k,
                                 const std::vector<IOThread*>& to,
                                 int fillTo = std::numeric_limits<int>::max()) {
    std::vector<IOThread*> dests;
    for (int i = 0; i < k && !to.empty(); i++) {
        auto* dest = *std::min_element(
                to.begin(), to.end(), [](IOThread* a, IOThread* b) {
                    return a->Load() < b->Load();
                });
        if (dest->Load() >= fillTo) {
            break;
        }
        dest->Reserve();
        dests.push_back(dest);
    }
    return dests;
}
} // namespace

// Level connections across live loops: move one from the fullest to the
// emptiest until they are within one of each other. A pool already that
// even moves nothing.
void Server::IOPool::rebalance() {
    std::vector<IOThread*> live;
    {
        std::lock_guard<std::mutex> g(s_->ioMu_);
        for (auto& iot : s_->ioThreads_) {
            if (!iot->retiring.load(std::memory_order_relaxed)) {
                live.push_back(iot.get());
            }
        }
    }
    if (live.size() < 2) {
        return;
    }
    std::vector<int> load(live.size());
    for (size_t i = 0; i < live.size(); i++) {
        load[i] = live[i]->Load();
    }
    std::vector<std::vector<IOThread*>> dests(live.size());
    for (;;) {
        auto mx = std::max_element(load.begin(), load.end());
        auto mn = std::min_element(load.begin(), load.end());
        if (*mx - *mn <= 1) {
            break;
        }
        const size_t di = mx - load.begin();
        const size_t ri = mn - load.begin();
        (*mx)--;
        (*mn)++;
        live[ri]->Reserve();
        dests[di].push_back(live[ri]);
    }
    for (size_t i = 0; i < live.size(); i++) {
        postMigrations(live[i], std::move(dests[i]));
    }
}

void Server::IOPool::Shrink(size_t n) {
    std::vector<IOThread*> live;
    {
        std::lock_guard<std::mutex> g(s_->ioMu_);
        for (auto& iot : s_->ioThreads_) {
            if (!iot->retiring.load(std::memory_order_relaxed)) {
                live.push_back(iot.get());
            }
        }
    }
    if (live.size() <= 1) {
        return;
    }
    n = std::min(n, live.size() - 1);
    // Retire the loops with the fewest connections: least to move.
    std::sort(live.begin(), live.end(), [](IOThread* a, IOThread* b) {
        return a->Load() < b->Load();
    });
    std::vector<IOThread*> retiring(live.begin(), live.begin() + n);
    std::vector<IOThread*> remaining(live.begin() + n, live.end());
    for (auto* iot : retiring) {
        iot->retiring.store(true, std::memory_order_relaxed);
        iot->emptyTicks = 0;
    }
    for (auto* iot : retiring) {
        postMigrations(iot, planMoves(iot->Load(), remaining));
    }
}

// Stop retiring loops that have been empty for two consecutive ticks. One
// tick is not enough: an accept may have been posted to the loop just before
// it was marked retiring and not have run yet.
void Server::IOPool::Reap() {
    std::vector<std::unique_ptr<IOThread>> done;
    std::vector<IOThread*> stuck, live;
    {
        std::lock_guard<std::mutex> g(s_->ioMu_);
        for (auto it = s_->ioThreads_.begin(); it != s_->ioThreads_.end();) {
            auto& iot = *it;
            if (!iot->retiring.load(std::memory_order_relaxed)) {
                live.push_back(iot.get());
                ++it;
                continue;
            }
            if (iot->Load() > 0) {
                iot->emptyTicks = 0;
                stuck.push_back(iot.get());
                ++it;
                continue;
            }
            if (++iot->emptyTicks < 2) {
                ++it;
                continue;
            }
            done.push_back(std::move(iot));
            it = s_->ioThreads_.erase(it);
        }
    }
    for (auto& iot : done) {
        iot->evb->terminateLoopSoon();
        if (iot->thread.joinable()) {
            iot->thread.join();
        }
    }
    // A retiring loop still holding connections (a late accept, or a move
    // that was skipped) gets another sweep.
    if (live.empty()) {
        return;
    }
    for (auto* iot : stuck) {
        postMigrations(iot,
                       planMoves(iot->connCount.load(std::memory_order_relaxed),
                                 live));
    }
}

PoolSample Server::IOPool::Sample(double wallSec) {
    PoolSample s;
    std::lock_guard<std::mutex> g(s_->ioMu_);
    double sum = 0;
    for (auto& iot : s_->ioThreads_) {
        const uint64_t cpu = iot->CpuNs();
        const uint64_t d = cpu >= iot->lastCpuNs ? cpu - iot->lastCpuNs : 0;
        iot->lastCpuNs = cpu;
        if (iot->retiring.load(std::memory_order_relaxed)) {
            continue;
        }
        const double busy = wallSec > 0 ? d / (wallSec * 1e9) : 0;
        sum += busy;
        s.busyMax = std::max(s.busyMax, busy);
        s.size++;
        s.threadBusy.push_back(busy);
        s.threadLoad.push_back(iot->Load());
    }
    s.busyMean = s.size > 0 ? sum / s.size : 0;
    return s;
}

// ---- Server ----

void Server::Start() {
    // Create listen socket
    listenFd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd_ < 0) {
        spdlog::error("Failed to create socket: {}", strerror(errno));
        return;
    }

    int opt = 1;
    setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (::bind(listenFd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        spdlog::error("Failed to bind port {}: {}", port_, strerror(errno));
        ::close(listenFd_);
        return;
    }

    if (::listen(listenFd_, 1024) < 0) {
        spdlog::error("Failed to listen: {}", strerror(errno));
        ::close(listenFd_);
        return;
    }

    // Start IO threads
    {
        std::lock_guard<std::mutex> g(ioMu_);
        for (auto& iot : ioThreads_) {
            iot->Start();
            iot->lastCpuNs = iot->CpuNs();
        }
    }

    running_.store(true);

    // Start HTTP stats thread
    statsThread_ = std::thread([this]() {
        int statsFd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (statsFd < 0)
            return;
        int opt = 1;
        setsockopt(statsFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        struct sockaddr_in addr {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(statsPort_);
        if (::bind(statsFd, (struct sockaddr*)&addr, sizeof(addr)) < 0 ||
            ::listen(statsFd, 8) < 0) {
            spdlog::error("Failed to bind stats port {}", statsPort_);
            ::close(statsFd);
            return;
        }
        spdlog::info(
                "Stats HTTP endpoint on port {} fd={}", statsPort_, statsFd);
        while (running_.load()) {
            try {
                struct pollfd pfd {};
                pfd.fd = statsFd;
                pfd.events = POLLIN;
                if (::poll(&pfd, 1, 1000) <= 0)
                    continue;
                int cfd = ::accept(statsFd, nullptr, nullptr);
                if (cfd < 0)
                    continue;
                // Read HTTP request to determine path
                char buf[4096];
                ssize_t nread = ::recv(cfd, buf, sizeof(buf) - 1, 0);
                buf[nread > 0 ? nread : 0] = '\0';

                // Route: gocb REST config, stats, or magma stats
                std::string body;
                if (strstr(buf, "/pools/default/b") ||
                    strstr(buf, "/pools/default/bucket")) {
                    body = clusterConfig_ + "\n\n\n\n";
                } else if (strstr(buf, "/pools")) {
                    body = "{\"pools\":[{\"name\":\"default\","
                           "\"uri\":\"/pools/default\","
                           "\"streamingUri\":\"/poolsStreaming/"
                           "default\"}],"
                           "\"isEnterprise\":true,"
                           "\"uuid\":"
                           "\"00000000000000000000000000000000\"}";
                } else if (strstr(buf, "/stats/dispatcher")) {
                    body = gDispStats.toJson();
                } else if (strstr(buf, "/stats/tuner")) {
                    body = tuner_ ? tuner_->ToJson() : "{\"enabled\":false}";
                } else if (strstr(buf, "/stats/magma")) {
                    try {
                        body = bucket_->GetStatsJson();
                    } catch (...) {
                        body = "{\"error\":\"stats unavailable\"}";
                    }
                } else if (strstr(buf, "/compact")) {
                    auto t0 = std::chrono::steady_clock::now();
                    try {
                        bucket_->CompactAll();
                        auto sec = std::chrono::duration<double>(
                                           std::chrono::steady_clock::now() - t0)
                                           .count();
                        body = "{\"compacted\":true,\"elapsed_s\":" +
                               std::to_string(sec) + "}";
                    } catch (const std::exception& e) {
                        body = std::string("{\"error\":\"") + e.what() + "\"}";
                    }
                } else {
                    body = gDispStats.toJson();
                }
                std::string resp =
                        "HTTP/1.1 200 OK\r\n"
                        "Content-Type: application/json\r\n"
                        "Content-Length: " +
                        std::to_string(body.size()) +
                        "\r\n"
                        "Connection: close\r\n\r\n" +
                        body;
                ::send(cfd, resp.data(), resp.size(), 0);
                ::close(cfd);
            } catch (const std::exception& e) {
                spdlog::error("Stats thread error: {}", e.what());
            }
        }
        ::close(statsFd);
    });

    spdlog::info("magma-kvserver listening on {}:{} with {} IO threads",
                 hostname_,
                 port_,
                 ioThreads_.size());

    if (tuner_) {
        tuner_->Start();
    }

    // Accept loop on main thread. Use poll() not select() because
    // listenFd_ may exceed FD_SETSIZE (1024) after magma opens many files.
    while (running_.load()) {
        struct sockaddr_in clientAddr {};
        socklen_t addrLen = sizeof(clientAddr);
        struct pollfd pfd {};
        pfd.fd = listenFd_;
        pfd.events = POLLIN;
        int pollRc = ::poll(&pfd, 1, 1000);
        if (pollRc <= 0) {
            continue;
        }
        int clientFd =
                ::accept(listenFd_, (struct sockaddr*)&clientAddr, &addrLen);
        if (clientFd < 0) {
            continue;
        }

        // TCP_NODELAY: don't coalesce small sends — we coalesce ourselves
        // via pendingWriteBuf_, so kernel batching only adds latency.
        int flag = 1;
        setsockopt(clientFd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        IOThread* iot = pickIOThread();
        if (!iot) {
            ::close(clientFd);
            continue;
        }

        // Dispatch to IO thread's EventBase
        auto* evb = iot->evb.get();
        auto* bucket = bucket_;
        auto* clusterCfg = &clusterConfig_;
        auto* errMap = &errorMap_;

        evb->runInEventBaseThread(
                [iot, evb, clientFd, bucket, clusterCfg, errMap]() {
                    auto socket = folly::AsyncSocket::newSocket(
                            evb, folly::NetworkSocket::fromFd(clientFd));
                    auto* conn = new Connection(
                            std::move(socket), bucket, *clusterCfg, *errMap);
                    conn->start(iot);
                });
    }
}

void Server::Stop() {
    running_.store(false);
    if (listenFd_ >= 0) {
        ::close(listenFd_);
        listenFd_ = -1;
    }
    if (tuner_) {
        tuner_->Stop();
    }
    if (statsThread_.joinable()) {
        statsThread_.join();
    }
    std::lock_guard<std::mutex> g(ioMu_);
    for (auto& iot : ioThreads_) {
        iot->evb->terminateLoopSoon();
        if (iot->thread.joinable()) {
            iot->thread.join();
        }
    }
}

} // namespace kvserver
} // namespace magma
