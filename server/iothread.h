#pragma once

#include <folly/io/async/EventBase.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include <pthread.h>
#include <time.h>

namespace magma {
namespace kvserver {

class Connection;

// One event loop and the connections pinned to it. The connection list is
// owned by the loop thread; other threads read only the atomic count and
// post work to the EventBase.
struct IOThread {
    std::unique_ptr<folly::EventBase> evb;
    std::thread thread;
    clockid_t cpuClock{};
    bool hasCpuClock{false};
    uint64_t lastCpuNs{0};
    // Set by the tuner when this loop is to be drained and stopped. No new
    // connections are assigned to it and its existing ones are moved away.
    std::atomic<bool> retiring{false};
    // Connections on this loop, and ones assigned to it (an accept or a
    // migration posted but not yet run). Assignment counts at once so a
    // burst of assignments spreads out instead of all landing on the loop
    // that looked emptiest.
    std::atomic<int> connCount{0};
    std::atomic<int> pending{0};
    // Consecutive tuner ticks seen with retiring && Load() == 0.
    int emptyTicks{0};
    std::vector<Connection*> conns; // loop thread only

    int Load() const {
        return connCount.load(std::memory_order_relaxed) +
               pending.load(std::memory_order_relaxed);
    }
    void Reserve() {
        pending.fetch_add(1, std::memory_order_relaxed);
    }
    void Unreserve() {
        pending.fetch_sub(1, std::memory_order_relaxed);
    }

    void Start() {
        auto* e = evb.get();
        thread = std::thread([e]() { e->loopForever(); });
        hasCpuClock =
                pthread_getcpuclockid(thread.native_handle(), &cpuClock) == 0;
    }

    // CPU time consumed by the loop thread so far. The loop blocks in
    // epoll_wait when idle, so this is exactly its busy time.
    uint64_t CpuNs() const {
        if (!hasCpuClock) {
            return 0;
        }
        struct timespec ts {};
        if (clock_gettime(cpuClock, &ts) != 0) {
            return lastCpuNs;
        }
        return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + ts.tv_nsec;
    }

    // Loop thread only. Every arrival was reserved first.
    void Add(Connection* c) {
        conns.push_back(c);
        connCount.fetch_add(1, std::memory_order_relaxed);
        pending.fetch_sub(1, std::memory_order_relaxed);
    }
    void Remove(Connection* c) {
        for (auto it = conns.begin(); it != conns.end(); ++it) {
            if (*it == c) {
                *it = conns.back();
                conns.pop_back();
                connCount.fetch_sub(1, std::memory_order_relaxed);
                return;
            }
        }
    }
};

} // namespace kvserver
} // namespace magma
