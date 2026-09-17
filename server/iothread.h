#pragma once

#include <folly/io/async/EventBase.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
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
        pump.owner = this;
        thread = std::thread([e]() { e->loopForever(); });
        hasCpuClock =
                pthread_getcpuclockid(thread.native_handle(), &cpuClock) == 0;
    }

    // Run fn on the loop thread, promptly even when the loop is saturated.
    //
    // EventBase::runInEventBaseThread is drained only when the loop reaches
    // its cross-thread queue, and a loop with many always-ready sockets can
    // go many seconds without doing so (measured: 18 s for a 2.6 ms
    // iteration). So work is left in a mailbox and the loop is told two
    // ways: the queue, which wakes an idle loop, and a flag the read path
    // checks on every event, which a busy loop sees within one iteration.
    // Either way the mail runs as a loop callback, a plain top-level
    // context, at the end of the current iteration.
    void Post(std::function<void()> fn) {
        {
            std::lock_guard<std::mutex> g(mailMu);
            mail.push_back(std::move(fn));
        }
        hasMail.store(true, std::memory_order_release);
        evb->runInEventBaseThread([this]() { SchedulePump(); });
    }

    // Loop thread only.
    void SchedulePump() {
        if (!pump.isLoopCallbackScheduled()) {
            evb->runInLoop(&pump);
        }
    }

    struct Pump : folly::EventBase::LoopCallback {
        IOThread* owner{nullptr};
        void runLoopCallback() noexcept override {
            std::vector<std::function<void()>> batch;
            {
                std::lock_guard<std::mutex> g(owner->mailMu);
                batch.swap(owner->mail);
                owner->hasMail.store(false, std::memory_order_release);
            }
            for (auto& fn : batch) {
                fn();
            }
        }
    };
    Pump pump;
    std::mutex mailMu;
    std::vector<std::function<void()>> mail;
    std::atomic<bool> hasMail{false};

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
