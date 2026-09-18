#pragma once

#include "engine.h"
#include "iothread.h"
#include "tuner.h"

#include <folly/io/async/EventBase.h>

#include <atomic>
#include <memory>
#include <mutex>
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

    // Blocks on the accept loop until RequestStop().
    void Start();
    // Async-signal-safe: only asks Start() to return. The caller then runs
    // Stop() on its own thread.
    void RequestStop();
    // Joins every thread. Idempotent.
    void Stop();

    // Size the IO threads and the reader pools at run time from load.
    // Bounds are inclusive thread counts; see ThreadTuner.
    void EnableAutoTune(const TunerConfig& cfg,
                        ThreadTuner::Bounds ioBounds,
                        ThreadTuner::Bounds readerBounds,
                        ThreadTuner::Bounds writerBounds);

private:
    // The IO threads as one pool. Growing starts loops and spreads existing
    // connections over them; shrinking drains the loops with the fewest
    // connections and stops them once empty. Runs on the tuner thread.
    class IOPool : public ElasticPool {
    public:
        explicit IOPool(Server* s) : s_(s) {
        }
        const char* Name() const override {
            return "io";
        }
        size_t Size() const override;
        size_t Step() const override {
            return 1;
        }
        void Grow(size_t n) override;
        void Shrink(size_t n) override;
        PoolSample Sample(double wallSec) override;
        void Reap() override;
        // True when no connection move is pending and no retiring loop still
        // holds connections.
        bool Settled() const override;

    private:
        void rebalance();
        Server* s_;
    };

    IOThread* pickIOThread();
    void addIOThread();

    Bucket* bucket_;
    uint16_t port_;
    std::string hostname_;
    int listenFd_{-1};
    uint16_t statsPort_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stopped_{false};
    std::thread statsThread_;

    // Precomputed protocol responses
    std::string clusterConfig_;
    std::string errorMap_;

    // IO thread pool. ioMu_ guards the vector itself (accept loop vs tuner);
    // the IOThread objects are stable once created.
    std::mutex ioMu_;
    std::vector<std::unique_ptr<IOThread>> ioThreads_;
    // Loops that were retired and are waiting to be joined.
    std::vector<std::unique_ptr<IOThread>> retiredIOThreads_;

    std::unique_ptr<IOPool> ioPool_;
    std::unique_ptr<ReaderPoolGroup> readerGroup_;
    std::unique_ptr<WriterPoolGroup> writerGroup_;
    std::unique_ptr<ThreadTuner> tuner_;
};

} // namespace kvserver
} // namespace magma
