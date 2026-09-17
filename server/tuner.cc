#include "tuner.h"

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <pthread.h>
#include <algorithm>
#include <cmath>

namespace magma {
namespace kvserver {

ThreadTuner::ThreadTuner(TunerConfig cfg, std::function<uint64_t()> opsCounter)
    : cfg_(std::move(cfg)), opsCounter_(std::move(opsCounter)) {
}

ThreadTuner::~ThreadTuner() {
    Stop();
}

void ThreadTuner::AddPool(ElasticPool* pool, Bounds bounds) {
    std::lock_guard<std::mutex> g(mu_);
    PoolState ps;
    ps.pool = pool;
    ps.bounds = bounds;
    pools_.push_back(std::move(ps));
}

void ThreadTuner::Start() {
    if (running_.exchange(true)) {
        return;
    }
    lastOps_ = opsCounter_();
    thread_ = std::thread([this]() {
        pthread_setname_np(pthread_self(), "fx:tuner");
        run();
    });
}

void ThreadTuner::Stop() {
    if (!running_.exchange(false)) {
        return;
    }
    if (thread_.joinable()) {
        thread_.join();
    }
}

void ThreadTuner::run() {
    using clock = std::chrono::steady_clock;
    auto lastSample = clock::now();
    auto windowStart = lastSample;
    uint64_t windowOps = lastOps_;
    size_t samples = 0;

    while (running_.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(cfg_.sampleInterval);
        if (!running_.load(std::memory_order_relaxed)) {
            break;
        }
        const auto now = clock::now();
        const double wall =
                std::chrono::duration<double>(now - lastSample).count();
        lastSample = now;

        std::lock_guard<std::mutex> g(mu_);
        sample(wall);
        if (++samples < cfg_.samplesPerDecision) {
            continue;
        }
        samples = 0;
        const uint64_t ops = opsCounter_();
        const double sec =
                std::chrono::duration<double>(now - windowStart).count();
        lastTput_ = sec > 0 ? (ops - windowOps) / sec : 0;
        windowStart = now;
        windowOps = ops;
        decide(lastTput_);
    }
}

void ThreadTuner::sample(double wallSec) {
    for (auto& ps : pools_) {
        ps.pool->Reap();
        const PoolSample s = ps.pool->Sample(wallSec);
        ps.busySum += s.busyMean;
        ps.busyMaxSeen = std::max(ps.busyMaxSeen, s.busyMax);
        ps.batchSum += s.avgBatch;
        ps.waitSum += s.waitUs;
        ps.samples++;
        ps.last.size = s.size;
        ps.last.threadBusy = s.threadBusy;
        ps.last.threadLoad = s.threadLoad;
    }
}

size_t ThreadTuner::roundToStep(size_t n, size_t step) const {
    return step > 0 ? (n / step) * step : n;
}

double ThreadTuner::recentSteady() const {
    if (steadyRecent_.empty()) {
        return lastTput_;
    }
    double s = 0;
    for (double t : steadyRecent_) {
        s += t;
    }
    return s / steadyRecent_.size();
}

void ThreadTuner::decide(double tput) {
    bool steady = true;
    for (auto& ps : pools_) {
        steady = steady && ps.trial == Trial::None;
    }
    if (steady) {
        steadyRecent_.push_back(tput);
        if (steadyRecent_.size() > 3) {
            steadyRecent_.erase(steadyRecent_.begin());
        }
        if (tput > steadyBest_) {
            steadyBest_ = tput;
            steadyLowWindows_ = 0;
        } else if (tput < steadyBest_ * (1.0 - 2 * cfg_.minGain) &&
                   ++steadyLowWindows_ >= 3) {
            steadyBest_ = tput; // the load itself went down
            steadyLowWindows_ = 0;
        }
    }
    for (auto& ps : pools_) {
        closeWindow(ps, tput, steady);
    }

    // A trial in flight owns the window, so its effect can be attributed.
    // Measurement starts once the pool says the change has taken effect
    // (connections moved, threads started) plus the settle windows.
    for (auto& ps : pools_) {
        if (ps.trial == Trial::None) {
            continue;
        }
        if (ps.settledWindows == 0 && !ps.pool->Settled() &&
            ps.windowsSinceChange < cfg_.maxSettleWindows) {
            return;
        }
        if (++ps.settledWindows > cfg_.settleWindows) {
            ps.tputSum += tput;
            if (++ps.tputN >= ps.measure) {
                judge(ps, ps.tputSum / ps.tputN);
            }
        }
        return;
    }

    // Start nothing while throughput is still moving: the baseline for the
    // next verdict would be wrong.
    if (steadyRecent_.size() < 2) {
        return;
    }
    const double a = steadyRecent_[steadyRecent_.size() - 1];
    const double b = steadyRecent_[steadyRecent_.size() - 2];
    if (a > 0 && std::abs(a - b) / a > cfg_.minGain) {
        return;
    }

    // One change per window, pools taken in turn. A pool rests one window
    // after a kept change before moving again.
    for (size_t i = 0; i < pools_.size(); i++) {
        auto& ps = pools_[(nextPool_ + i) % pools_.size()];
        if (ps.windowsSinceChange >= 2 && startTrial(ps)) {
            nextPool_ = (nextPool_ + i + 1) % pools_.size();
            return;
        }
    }
}

void ThreadTuner::closeWindow(PoolState& ps, double tput, bool steady) {
    if (ps.samples > 0) {
        ps.last.busyMean = ps.busySum / ps.samples;
        ps.last.busyMax = ps.busyMaxSeen;
        ps.last.avgBatch = ps.batchSum / ps.samples;
        ps.last.waitUs = ps.waitSum / ps.samples;
    }
    ps.busySum = ps.busyMaxSeen = ps.batchSum = ps.waitSum = 0;
    ps.samples = 0;
    ps.windowsSinceChange++;

    // Back-offs count down, and end early when a steady window shows the
    // load is no longer what the failed change was judged in.
    for (Direction* d : {&ps.grow, &ps.shrink}) {
        if (d->backoff == 0) {
            continue;
        }
        d->backoff--;
        const bool moved =
                (d->tputAtFail > 0 &&
                 std::abs(tput - d->tputAtFail) / d->tputAtFail >
                         cfg_.loadChangeTput) ||
                std::abs(ps.last.busyMean - d->busyAtFail) > cfg_.loadChangeBusy;
        if (steady && ps.windowsSinceChange >= 2 && moved) {
            d->backoff = 0;
            d->nextBackoff = 0;
        }
    }
}

bool ThreadTuner::startTrial(PoolState& ps) {
    auto* pool = ps.pool;
    const size_t size = pool->Size();
    const size_t step = std::max<size_t>(1, pool->Step());
    const double busy = ps.last.busyMean;

    // Direction: busy threads ask for more; otherwise probe fewer. The probe
    // also runs on a busy pool once growing it has failed - the limit is
    // elsewhere, so the threads may not all be needed however busy they look.
    const bool wantGrow = busy > cfg_.highBusy && ps.grow.backoff == 0 &&
                          size < ps.bounds.max;
    const bool wantShrink = !wantGrow && ps.shrink.backoff == 0 &&
                            size > ps.bounds.min &&
                            (busy <= cfg_.highBusy || ps.grow.backoff > 0);
    if (!wantGrow && !wantShrink) {
        return false;
    }
    Direction& dir = wantGrow ? ps.grow : ps.shrink;

    // A quarter of the pool, halved after each failure, never below a step.
    size_t n = std::max(step, roundToStep(size / 4, step));
    if (dir.cap > 0) {
        n = std::min(n, dir.cap);
    }
    if (wantGrow) {
        n = std::min(n, ps.bounds.max - size);
    } else {
        n = std::min(n, size - ps.bounds.min);
        // Do not shrink into the range that would just ask to grow again,
        // unless growing has already been shown not to help.
        if (ps.grow.backoff == 0) {
            while (n > step && busy * size / (size - n) >= cfg_.highBusy) {
                n -= step;
            }
            if (busy * size / (size - n) >= cfg_.highBusy) {
                return false;
            }
        }
    }
    if (n == 0) {
        return false;
    }

    ps.trial = wantGrow ? Trial::Grow : Trial::Shrink;
    ps.sizeBefore = size;
    // A grow must beat the current steady level; a shrink must hold the best
    // steady level seen, so consecutive shrinks cannot each lose a little.
    ps.refTput = wantGrow ? recentSteady() : std::max(steadyBest_, lastTput_);
    ps.busyBefore = busy;
    ps.measure = static_cast<double>(n) / size < 2 * cfg_.minGain
                         ? cfg_.smallStepMeasureWindows
                         : cfg_.measureWindows;
    ps.tputSum = 0;
    ps.tputN = 0;
    ps.windowsSinceChange = 0;
    ps.settledWindows = 0;
    ps.changes++;
    ps.lastAction = std::string(wantGrow ? "grow " : "shrink ") +
                    std::to_string(size) + "->" +
                    std::to_string(wantGrow ? size + n : size - n);
    spdlog::info("tuner: {} busy={:.2f} tput={:.0f}/s: {}",
                 pool->Name(),
                 busy,
                 lastTput_,
                 ps.lastAction);
    if (wantGrow) {
        pool->Grow(n);
    } else {
        pool->Shrink(n);
    }
    return true;
}

void ThreadTuner::judge(PoolState& ps, double tput) {
    auto* pool = ps.pool;
    const size_t size = pool->Size();
    const size_t step = std::max<size_t>(1, pool->Step());
    const bool grew = ps.trial == Trial::Grow;
    const size_t delta = grew ? size - ps.sizeBefore : ps.sizeBefore - size;
    const double ratio = ps.refTput > 0 ? tput / ps.refTput : 1.0;

    // With no load to measure against, fewer idle threads is simply fine
    // and more is not. Otherwise throughput alone decides: a grow must have
    // raised it by more than minGain, a shrink must not have lowered
    // it by more. A grow that raised it only a little because another pool
    // is the next limit is still right to keep; the other pool's turn comes
    // next window.
    bool keep;
    if (ps.refTput == 0) {
        keep = !grew;
    } else if (grew) {
        keep = ratio >= 1.0 + cfg_.minGain;
    } else {
        keep = ratio >= 1.0 - cfg_.maxLoss;
    }

    spdlog::info("tuner: {} {} {} (tput {:.0f} -> {:.0f}/s, {:+.1f}%, "
                 "busy={:.2f})",
                 pool->Name(),
                 ps.lastAction,
                 keep ? "kept" : "reverted",
                 ps.refTput,
                 tput,
                 (ratio - 1.0) * 100.0,
                 ps.last.busyMean);
    ps.lastAction += keep ? " kept" : " reverted";

    Direction& dir = grew ? ps.grow : ps.shrink;
    if (keep) {
        dir.cap = 0;
        dir.nextBackoff = 0;
    } else {
        ps.reverts++;
        if (grew) {
            pool->Shrink(delta);
        } else {
            pool->Grow(delta);
        }
        // Retry at half the step; once the smallest step fails, back off.
        if (delta > step) {
            dir.cap = std::max(step, roundToStep(delta / 2, step));
        } else {
            dir.cap = step;
            if (dir.nextBackoff == 0) {
                dir.nextBackoff = cfg_.firstBackoffWindows;
            }
            dir.backoff = dir.nextBackoff;
            dir.nextBackoff =
                    std::min(dir.nextBackoff * 2, cfg_.maxBackoffWindows);
            dir.tputAtFail = ps.refTput;
            dir.busyAtFail = ps.busyBefore;
        }
    }
    ps.trial = Trial::None;
    ps.windowsSinceChange = 0;
}

std::string ThreadTuner::ToJson() const {
    std::lock_guard<std::mutex> g(mu_);
    nlohmann::json j;
    j["throughput"] = lastTput_;
    j["steady_best"] = steadyBest_;
    j["pools"] = nlohmann::json::array();
    for (const auto& ps : pools_) {
        nlohmann::json p;
        p["name"] = ps.pool->Name();
        p["size"] = ps.pool->Size();
        p["min"] = ps.bounds.min;
        p["max"] = ps.bounds.max;
        p["busy_mean"] = ps.last.busyMean;
        p["busy_max"] = ps.last.busyMax;
        p["avg_batch"] = ps.last.avgBatch;
        p["wait_us"] = ps.last.waitUs;
        p["trial"] = ps.trial == Trial::None
                             ? "none"
                             : (ps.trial == Trial::Grow ? "grow" : "shrink");
        p["last_action"] = ps.lastAction;
        p["changes"] = ps.changes;
        p["reverts"] = ps.reverts;
        p["grow_backoff"] = ps.grow.backoff;
        p["shrink_backoff"] = ps.shrink.backoff;
        if (!ps.last.threadBusy.empty()) {
            p["thread_busy"] = ps.last.threadBusy;
        }
        if (!ps.last.threadLoad.empty()) {
            p["thread_load"] = ps.last.threadLoad;
        }
        j["pools"].push_back(std::move(p));
    }
    return j.dump(2);
}

} // namespace kvserver
} // namespace magma
