#pragma once

#include "engine.h"

#include <folly/io/async/EventBase.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace magma {
namespace kvserver {

class Server {
public:
    Server(Bucket* bucket,
           uint16_t port,
           size_t numIOThreads,
           const std::string& hostname,
           uint16_t numVBuckets = 16,
           uint16_t statsPort = 80);
    ~Server();

    // Blocks on accept loop. Call Stop() from signal handler.
    void Start();
    void Stop();

private:
    struct IOThread {
        std::unique_ptr<folly::EventBase> evb;
        std::thread thread;
    };

    Bucket* bucket_;
    uint16_t port_;
    std::string hostname_;
    int listenFd_{-1};
    uint16_t statsPort_;
    std::atomic<bool> running_{false};
    std::thread statsThread_;

    // Precomputed protocol responses
    std::string clusterConfig_;
    std::string errorMap_;

    // IO thread pool
    std::vector<std::unique_ptr<IOThread>> ioThreads_;
    size_t nextThread_{0};
};

} // namespace kvserver
} // namespace magma
