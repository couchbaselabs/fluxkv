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

double ThreadTuner::steadyLevel(size_t n) const {
    if (steadyRecent_.empty()) {
        return lastTput_;
    }
    n = std::min(std::max<size_t>(n, 1), steadyRecent_.size());
    std::vector<double> w(steadyRecent_.end() - n, steadyRecent_.end());
    std::sort(w.begin(), w.end());
    return w[w.size() / 2];
}

double ThreadTuner::cv(const std::vector<double>& w) const {
    if (w.size() < 3) {
        return 0;
    }
    double mean = 0;
    for (double t : w) {
        mean += t;
    }
    mean /= w.size();
    if (mean <= 0) {
        return 0;
    }
    double var = 0;
    for (double t : w) {
        var += (t - mean) * (t - mean);
    }
    return std::sqrt(var / w.size()) / mean;
}

double ThreadTuner::steadyNoise() const {
    // The last trial's measurement windows are consecutive and at one pool
    // configuration, so their spread is the load's own variation. The steady
    // history spans trials at different sizes and different throughput
    // levels, which read as 25% noise on a load that varied 7%; it is only
    // the fallback before any trial has been measured.
    if (measuredNoise_ >= 0) {
        return measuredNoise_;
    }
    return cv(steadyRecent_);
}

ThreadTuner::Direction& ThreadTuner::dirRef(PoolState& ps, bool grew) {
    return grew ? ps.grow : ps.shrink;
}

double ThreadTuner::tolerance(const PoolState& ps) const {
    // Averaging `measure` windows divides the noise by sqrt(measure).
    const double n = ps.measure > 0 ? static_cast<double>(ps.measure) : 1.0;
    return std::max(cfg_.minGain, ps.noise / std::sqrt(n));
}

bool ThreadTuner::rampingUp() const {
    for (const auto& ps : pools_) {
        if (ps.trial == Trial::None && ps.grow.backoff == 0 &&
            ps.grow.keptStreak >= cfg_.rampAfterKept &&
            ps.last.busyMean >= cfg_.rampBusy &&
            ps.pool->Size() < ps.bounds.max) {
            return true;
        }
    }
    return false;
}

void ThreadTuner::decide(double tput) {
    bool steady = true;
    for (auto& ps : pools_) {
        steady = steady && ps.trial == Trial::None;
    }
    if (steady) {
        // A window with no load, or a step change in load, is not noise.
        // Keep the history to windows that describe the current load, or
        // the ramp from idle would read as 100% variation. A step change
        // persists; a single deep window is a stall (a flush storm takes
        // one 2 s window to 400K under a 1.1M load) and stays in the
        // history as the noise it is.
        const bool deep = !steadyRecent_.empty() &&
                          tput < 0.5 * steadyLevel(kLevelWindows);
        stepLow_ = deep ? stepLow_ + 1 : 0;
        if (tput <= 0 || stepLow_ >= 2) {
            steadyRecent_.clear();
            stepLow_ = 0;
        }
        if (tput > 0) {
            steadyRecent_.push_back(tput);
        }
        if (steadyRecent_.size() > kSteadyHistory) {
            steadyRecent_.erase(steadyRecent_.begin());
        }
        // Track the level, not the window: see steadyBest_.
        if (steadyRecent_.size() >= kLevelWindows) {
            const double level = steadyLevel(kLevelWindows);
            if (level > steadyBest_) {
                steadyBest_ = level;
                steadyLowDecisions_ = 0;
            } else if (level < steadyBest_ * (1.0 - 2 * cfg_.minGain) &&
                       ++steadyLowDecisions_ >= 3) {
                steadyBest_ = level; // the load itself went down
                steadyLowDecisions_ = 0;
            }
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
        if (tput <= 0) {
            // No load, so nothing to judge a change against (the gap between
            // a load and the next phase read as a -100% verdict and cost a
            // backoff). Measure again once load returns; undo the change,
            // with no backoff, if it does not.
            ps.tputWindows.clear();
            ps.settledWindows = 0;
            if (ps.windowsSinceChange > cfg_.maxSettleWindows) {
                const size_t size = ps.pool->Size();
                if (ps.trial == Trial::Grow) {
                    ps.pool->Shrink(size - ps.sizeBefore);
                } else {
                    ps.pool->Grow(ps.sizeBefore - size);
                }
                spdlog::info("tuner: {} {} abandoned (no load)",
                             ps.pool->Name(),
                             ps.lastAction);
                ps.lastAction += " abandoned";
                ps.trial = Trial::None;
                ps.windowsSinceChange = 0;
            }
            return;
        }
        if (++ps.settledWindows > cfg_.settleWindows) {
            ps.tputWindows.push_back(tput);
            if (ps.tputWindows.size() >= ps.measure) {
                auto w = ps.tputWindows;
                std::sort(w.begin(), w.end());
                judge(ps, w[w.size() / 2]);
            }
        }
        return;
    }

    // Start nothing while throughput is still moving: the baseline for the
    // next verdict would be wrong. The reference is a median over
    // kLevelWindows, so that many steady windows must exist first.
    if (steadyRecent_.size() < kLevelWindows) {
        return;
    }
    const double a = steadyRecent_[steadyRecent_.size() - 1];
    const double b = steadyRecent_[steadyRecent_.size() - 2];
    if (a > 0 && std::abs(a - b) / a > std::max(cfg_.minGain, steadyNoise())) {
        // Each kept grow raises throughput, which makes the next window
        // disagree with the last, which blocks the next grow: the tuner
        // stalls itself for the whole ramp. A pegged pool that has kept
        // every recent step is exempt while throughput is rising; a load
        // that is falling or oscillating still waits for a baseline.
        if (!(a > b && rampingUp())) {
            return;
        }
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
    // While a pool keeps every step it is given, escalate: reaching the
    // working size otherwise costs a trial per quarter, and each trial pays
    // settle plus measure windows. A grow escalates only while pegged; a
    // shrink only while its last kept step raised throughput, i.e. the pool
    // is past its working size, not merely able to lose a thread. dir.cap
    // below still bounds a retry after a revert.
    if (dir.keptStreak >= cfg_.rampAfterKept &&
        (wantGrow ? busy >= cfg_.rampBusy : dir.lastGained)) {
        // A shrink stops at half the pool per step.
        const size_t mult =
                wantGrow && dir.keptStreak >= cfg_.rampAfterKept + 1 ? 4 : 2;
        n = std::max(n, roundToStep(size / 4 * mult, step));
    }
    if (dir.cap > 0) {
        n = std::min(n, dir.cap);
    }
    if (wantGrow) {
        n = std::min(n, ps.bounds.max - size);
    } else {
        n = std::min(n, size - ps.bounds.min);
        // Do not shrink into the range that would just ask to grow again,
        // unless growing has already been shown not to help, or the last
        // shrink raised throughput: then busy is not what limits this pool
        // (durable writers got busier with every step from 96 and faster
        // too). The projection is scaled by what earlier shrinks of this
        // pool actually did.
        if (ps.grow.backoff == 0 && !dir.lastGained) {
            const double b = busy * ps.shrinkScale;
            while (n > step && b * size / (size - n) >= cfg_.highBusy) {
                n -= step;
            }
            if (b * size / (size - n) >= cfg_.highBusy) {
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
    // Both references are medians over at least as many windows as the
    // verdict will use.
    ps.noise = steadyNoise();
    ps.measure = static_cast<double>(n) / size < 2 * cfg_.minGain
                         ? cfg_.smallStepMeasureWindows
                         : cfg_.measureWindows;
    if (ps.noise > cfg_.minGain) {
        auto want = static_cast<size_t>(std::ceil(ps.noise / cfg_.minGain));
        ps.measure = std::min<size_t>(8, std::max(ps.measure, want));
    }
    const double level = steadyLevel(std::max(ps.measure, kLevelWindows));
    // Both judge against the current level. A shrink used to have to hold
    // the best level ever seen, but a write phase starts high and decays as
    // compaction debt builds, so shrinks were reverted against a level the
    // load no longer offered; the streak reference still stops a run of
    // small losses from adding up.
    ps.refTput = level;
    ps.busyBefore = busy;
    ps.tputWindows.clear();
    ps.windowsSinceChange = 0;
    ps.settledWindows = 0;
    ps.changes++;
    ps.lastAction = std::string(wantGrow ? "grow " : "shrink ") +
                    std::to_string(size) + "->" +
                    std::to_string(wantGrow ? size + n : size - n);
    spdlog::info("tuner: {} busy={:.2f} tput={:.0f}/s noise={:.1f}% "
                 "measure={}: {}",
                 pool->Name(),
                 busy,
                 lastTput_,
                 ps.noise * 100.0,
                 ps.measure,
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
    if (ps.tputWindows.size() >= 3) {
        measuredNoise_ = cv(ps.tputWindows);
    }

    // With no load to measure against, fewer idle threads is simply fine
    // and more is not. Otherwise throughput alone decides: a grow must have
    // raised it by more than minGain, a shrink must not have lowered
    // it by more. A grow that raised it only a little because another pool
    // is the next limit is still right to keep; the other pool's turn comes
    // next window.
    // Under a noisy load the thresholds widen to what the measurement can
    // resolve: a grow must beat the noise, a shrink may lose up to it.
    // Shrinks stay strict: widening their allowance to the noise would let
    // a series of them each lose a few percent for real.
    const double tol = tolerance(ps);
    // A move must also hold the level the streak started from.
    const double streakRatio =
            dirRef(ps, grew).streakRef > 0
                    ? tput / dirRef(ps, grew).streakRef
                    : 1.0;
    bool keep;
    if (ps.refTput == 0) {
        keep = !grew;
    } else if (!grew && ps.busyBefore < cfg_.idleBusy) {
        // The pool was doing nothing, so global throughput cannot say
        // whether removing threads from it helped; its own idleness can.
        // Judging these on throughput produced verdicts like a reader
        // shrink "kept" on a 124% gain in a write-only load.
        keep = true;
    } else if (grew) {
        keep = ratio >= 1.0 + tol;
    } else {
        const double allow = std::max(cfg_.maxLoss, std::min(tol, 0.02));
        keep = ratio >= 1.0 - allow && streakRatio >= 1.0 - allow;
    }

    spdlog::info("tuner: {} {} {} (tput {:.0f} -> {:.0f}/s, {:+.1f}%, "
                 "tol={:.1f}%, busy={:.2f})",
                 pool->Name(),
                 ps.lastAction,
                 keep ? "kept" : "reverted",
                 ps.refTput,
                 tput,
                 (ratio - 1.0) * 100.0,
                 tol * 100.0,
                 ps.last.busyMean);
    ps.lastAction += keep ? " kept" : " reverted";

    Direction& dir = grew ? ps.grow : ps.shrink;
    if (keep && !grew && ps.busyBefore >= cfg_.idleBusy && size > 0) {
        const double projected = ps.busyBefore * ps.sizeBefore / size;
        if (projected > 0) {
            const double seen = std::clamp(ps.last.busyMean / projected, 0.1, 1.0);
            ps.shrinkScale = 0.5 * ps.shrinkScale + 0.5 * seen;
        }
    }
    dir.lastGained = keep && ratio >= 1.0 + tol;
    if (keep) {
        dir.cap = 0;
        dir.nextBackoff = 0;
        if (dir.keptStreak == 0) {
            dir.streakRef = ps.refTput;
        }
        dir.keptStreak++;
    } else {
        ps.reverts++;
        dir.keptStreak = 0;
        dir.streakRef = 0;
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
        p["noise"] = steadyNoise();
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
