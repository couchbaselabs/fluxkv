#include "tuner.h"

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

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
    thread_ = std::thread([this]() { run(); });
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
    size_t samples = 0;
    auto windowStart = lastSample;
    uint64_t windowOps = lastOps_;

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
        const double windowSec =
                std::chrono::duration<double>(now - windowStart).count();
        const double tput = windowSec > 0 ? (ops - windowOps) / windowSec : 0;
        windowStart = now;
        windowOps = ops;
        lastTput_ = tput;
        decide(tput);
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
    if (step == 0) {
        return n;
    }
    return (n / step) * step;
}

void ThreadTuner::decide(double tput) {
    bool anyTrial = false;
    for (auto& ps : pools_) {
        anyTrial = anyTrial || ps.trial != Trial::None;
    }
    // A window with nothing in flight is a steady reading of the load.
    const bool steady = !anyTrial;
    if (steady) {
        steadyRecent_.push_back(tput);
        if (steadyRecent_.size() > 3) {
            steadyRecent_.erase(steadyRecent_.begin());
        }
        if (tput > steadyBest_) {
            steadyBest_ = tput;
            steadyLowWindows_ = 0;
        } else if (tput < steadyBest_ * (1.0 - 2 * cfg_.maxLoss)) {
            if (++steadyLowWindows_ >= 3) {
                steadyBest_ = tput; // the load itself went down
                steadyLowWindows_ = 0;
            }
        } else {
            steadyLowWindows_ = 0;
        }
    }

    // Close out the window for every pool.
    for (auto& ps : pools_) {
        if (ps.samples > 0) {
            ps.last.busyMean = ps.busySum / ps.samples;
            ps.last.busyMax = ps.busyMaxSeen;
            ps.last.avgBatch = ps.batchSum / ps.samples;
            ps.last.waitUs = ps.waitSum / ps.samples;
        }
        ps.busySum = ps.busyMaxSeen = ps.batchSum = ps.waitSum = 0;
        ps.samples = 0;
        ps.windowsSinceChange++;
        // Back-offs count down, and end at once if the situation the failed
        // change was judged in no longer holds. Only a steady window can say
        // so: a dip caused by some other trial is not a change in load.
        auto changed = [&](double t0, double b0) {
            if (!steady || ps.windowsSinceChange < 2) {
                return false;
            }
            const double dt = t0 > 0 ? std::abs(tput - t0) / t0 : 0;
            return dt > cfg_.conditionChangeTput ||
                   std::abs(ps.last.busyMean - b0) > cfg_.conditionChangeBusy;
        };
        if (ps.growBackoff > 0) {
            ps.growBackoff--;
            if (changed(ps.growRevertTput, ps.growRevertBusy)) {
                ps.growBackoff = 0;
                ps.growBackoffBase = 0;
            }
        }
        if (ps.shrinkBackoff > 0) {
            ps.shrinkBackoff--;
            if (changed(ps.shrinkRevertTput, ps.shrinkRevertBusy)) {
                ps.shrinkBackoff = 0;
                ps.shrinkBackoffBase = 0;
            }
        }
    }

    // A trial in flight owns the window: nothing else changes until it has
    // been judged, or the effect could not be attributed. The verdict is
    // taken on the mean of several windows so one noisy reading cannot pass
    // a bad change.
    for (auto& ps : pools_) {
        if (ps.trial != Trial::None) {
            if (ps.windowsSinceChange > cfg_.settleWindows) {
                ps.trialTputSum += tput;
                ps.trialTputN++;
                if (ps.trialTputN >= std::max<size_t>(1, ps.trialMeasure)) {
                    evaluateTrial(ps, ps.trialTputSum / ps.trialTputN);
                }
            }
            return;
        }
    }

    if (pools_.empty()) {
        return;
    }
    // One change per window, pools taken in turn so neither starves.
    for (size_t i = 0; i < pools_.size(); i++) {
        auto& ps = pools_[(nextPool_ + i) % pools_.size()];
        // Give a kept change one quiet window before the same pool moves
        // again in the other direction.
        if (ps.windowsSinceChange < 2) {
            continue;
        }
        if (startTrial(ps, ps.last.busyMean)) {
            nextPool_ = (nextPool_ + i + 1) % pools_.size();
            return;
        }
    }
}

bool ThreadTuner::startTrial(PoolState& ps, double busyMean) {
    auto* pool = ps.pool;
    const size_t size = pool->Size();
    const size_t step = std::max<size_t>(1, pool->Step());

    if (busyMean > cfg_.highBusy && ps.growBackoff == 0 &&
        size < ps.bounds.max) {
        // Steps scale with the pool: a single thread added to a large pool
        // cannot move throughput enough to be judged. Saturated pools take
        // larger steps to catch up in fewer windows.
        size_t n = std::max(step, roundToStep(size / 8, step));
        if (busyMean > cfg_.saturatedBusy) {
            n = std::max(step, roundToStep(size / 4, step));
        }
        if (ps.growCap > 0) {
            n = std::min(n, ps.growCap);
        }
        n = std::min(n, ps.bounds.max - size);
        if (n == 0) {
            return false;
        }
        ps.trial = Trial::Grow;
        ps.sizeBefore = size;
        ps.tputBefore = steadyBaseline();
        ps.busyBefore = busyMean;
        ps.inShrinkRun = false;
        ps.windowsSinceChange = 0;
        ps.trialTputSum = 0;
        ps.trialTputN = 0;
        // A step too small to show minGain even if it pays in full gets a
        // longer look.
        ps.trialMeasure = static_cast<double>(n) / size < cfg_.minGain
                                  ? cfg_.smallStepMeasureWindows
                                  : cfg_.measureWindows;
        ps.changes++;
        ps.lastAction = "grow " + std::to_string(size) + "->" +
                        std::to_string(size + n);
        spdlog::info("tuner: {} busy={:.2f} tput={:.0f}/s: {}",
                     pool->Name(),
                     busyMean,
                     lastTput_,
                     ps.lastAction);
        pool->Grow(n);
        return true;
    }

    // Shrink when the threads are not all needed. Two ways to know: the
    // busy fraction says so directly, or a grow was just reverted - the
    // throughput is limited elsewhere, so the pool may be oversized however
    // busy its threads look (a pool blocked on IO with a backlog reads 100%
    // busy at any size).
    const bool idle = busyMean < cfg_.highBusy;
    if ((idle || ps.growStalled) && ps.shrinkBackoff == 0 &&
        size > ps.bounds.min) {
        size_t n = step;
        if (busyMean < cfg_.lowBusy / 2) {
            n = std::max(step, roundToStep(size / 4, step));
        } else if (busyMean < cfg_.lowBusy) {
            n = std::max(step, roundToStep(size / 8, step));
        }
        if (ps.shrinkCap > 0) {
            n = std::min(n, ps.shrinkCap);
        }
        n = std::min(n, size - ps.bounds.min);
        // Do not shrink into the range where the next window would want to
        // grow again - unless growth has already been shown not to help.
        auto predicted = [&](size_t m) {
            return busyMean * static_cast<double>(size) /
                   static_cast<double>(size - m);
        };
        while (n > step && predicted(n) >= cfg_.highBusy) {
            n -= step;
        }
        if (n == 0 || (!ps.growStalled && predicted(n) >= cfg_.highBusy)) {
            return false;
        }
        ps.trial = Trial::Shrink;
        ps.sizeBefore = size;
        // The reference only rises within a run: a shrink kept at a lower
        // throughput must not lower the bar for the next one.
        ps.shrinkRunRef = ps.inShrinkRun ? std::max(ps.shrinkRunRef, lastTput_)
                                         : lastTput_;
        ps.tputBefore = std::max(ps.shrinkRunRef, steadyBest_);
        ps.busyBefore = busyMean;
        ps.windowsSinceChange = 0;
        ps.trialTputSum = 0;
        ps.trialTputN = 0;
        ps.trialMeasure = cfg_.measureWindows;
        ps.changes++;
        ps.lastAction = "shrink " + std::to_string(size) + "->" +
                        std::to_string(size - n);
        spdlog::info("tuner: {} busy={:.2f} tput={:.0f}/s: {}",
                     pool->Name(),
                     busyMean,
                     lastTput_,
                     ps.lastAction);
        pool->Shrink(n);
        return true;
    }
    return false;
}

bool ThreadTuner::evaluateTrial(PoolState& ps, double tput) {
    auto* pool = ps.pool;
    const size_t size = pool->Size();
    const size_t step = std::max<size_t>(1, pool->Step());
    const size_t delta = size > ps.sizeBefore ? size - ps.sizeBefore
                                              : ps.sizeBefore - size;
    const double ratio = ps.tputBefore > 0 ? tput / ps.tputBefore : 1.0;
    bool keep = false;

    // A reverted step is retried at half the size before this direction
    // backs off: the right count may sit between the two sizes tried.
    auto halve = [&](size_t& cap,
                     size_t& backoff,
                     size_t& base,
                     double& revTput,
                     double& revBusy) {
        if (delta > step) {
            cap = std::max(step, roundToStep(delta / 2, step));
        } else {
            cap = step;
            if (base == 0) {
                base = cfg_.firstBackoffWindows;
            }
            backoff = base;
            base = std::min(base * 2, cfg_.maxBackoffWindows);
            revTput = ps.tputBefore;
            revBusy = ps.busyBefore;
        }
    };

    if (ps.trial == Trial::Grow) {
        // More threads must have bought throughput, or the limit is elsewhere.
        // A saturated pool returns at most the relative growth; a large step
        // has to return at least a quarter of it, a small one (which cannot
        // reach minGain even in full) at least half.
        const double rel = ps.sizeBefore > 0
                                   ? static_cast<double>(delta) / ps.sizeBefore
                                   : 0;
        const double need = rel >= cfg_.minGain
                                    ? std::max(cfg_.minGain, rel / 4)
                                    : std::max(cfg_.minSmallGain, rel / 2);
        keep = ratio >= 1.0 + need;
        if (keep) {
            ps.growBackoffBase = 0;
            ps.growCap = 0;
            ps.growStalled = false;
        } else {
            halve(ps.growCap,
                  ps.growBackoff,
                  ps.growBackoffBase,
                  ps.growRevertTput,
                  ps.growRevertBusy);
            ps.growStalled = true;
        }
    } else {
        // Fewer threads must not have cost throughput, nor newly saturated
        // the rest (a pool that was already saturated tells us nothing).
        // With no load to measure against, fewer idle threads is simply fine.
        keep = (ps.tputBefore == 0 || ratio >= 1.0 - cfg_.maxLoss) &&
               (ps.last.busyMax < cfg_.saturatedBusy ||
                ps.busyBefore >= cfg_.saturatedBusy);
        if (keep) {
            ps.shrinkBackoffBase = 0;
            ps.shrinkCap = 0;
            ps.inShrinkRun = ps.tputBefore > 0;
        } else {
            halve(ps.shrinkCap,
                  ps.shrinkBackoff,
                  ps.shrinkBackoffBase,
                  ps.shrinkRevertTput,
                  ps.shrinkRevertBusy);
            ps.inShrinkRun = false;
        }
    }

    const char* verdict = keep ? "kept" : "reverted";
    spdlog::info("tuner: {} {} {} (tput {:.0f} -> {:.0f}/s, {:+.1f}%, "
                 "busy={:.2f})",
                 pool->Name(),
                 ps.lastAction,
                 verdict,
                 ps.tputBefore,
                 tput,
                 (ratio - 1.0) * 100.0,
                 ps.last.busyMean);
    ps.lastAction += keep ? " kept" : " reverted";
    if (!keep) {
        ps.reverts++;
        if (size > ps.sizeBefore) {
            pool->Shrink(size - ps.sizeBefore);
        } else if (size < ps.sizeBefore) {
            pool->Grow(ps.sizeBefore - size);
        }
    }
    ps.trial = Trial::None;
    ps.windowsSinceChange = 0;
    return keep;
}

double ThreadTuner::steadyBaseline() const {
    if (steadyRecent_.empty()) {
        return lastTput_;
    }
    double s = 0;
    for (double t : steadyRecent_) {
        s += t;
    }
    return s / steadyRecent_.size();
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
        p["grow_backoff"] = ps.growBackoff;
        p["shrink_backoff"] = ps.shrinkBackoff;
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
