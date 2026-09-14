#include "server.h"
#include "connection.h"

#include <folly/io/async/AsyncSocket.h>
#include <spdlog/spdlog.h>

#include <chrono>

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
    for (size_t i = 0; i < ioThreads_.size(); i++) {
        auto* evb = ioThreads_[i]->evb.get();
        ioThreads_[i]->thread = std::thread([evb]() { evb->loopForever(); });
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

        // Round-robin to IO threads
        auto& iot = ioThreads_[nextThread_ % ioThreads_.size()];
        nextThread_++;

        // Dispatch to IO thread's EventBase
        auto* evb = iot->evb.get();
        auto* bucket = bucket_;
        auto* clusterCfg = &clusterConfig_;
        auto* errMap = &errorMap_;

        evb->runInEventBaseThread(
                [evb, clientFd, bucket, clusterCfg, errMap]() {
                    auto socket = folly::AsyncSocket::newSocket(
                            evb, folly::NetworkSocket::fromFd(clientFd));
                    auto* conn = new Connection(
                            std::move(socket), bucket, *clusterCfg, *errMap);
                    conn->start();
                });
    }
}

void Server::Stop() {
    running_.store(false);
    if (listenFd_ >= 0) {
        ::close(listenFd_);
        listenFd_ = -1;
    }
    if (statsThread_.joinable()) {
        statsThread_.join();
    }
    for (auto& iot : ioThreads_) {
        iot->evb->terminateLoopSoon();
        if (iot->thread.joinable()) {
            iot->thread.join();
        }
    }
}

} // namespace kvserver
} // namespace magma
