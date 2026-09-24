#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace magma {
namespace kvserver {

// One interval's worth of measurements for a thread pool.
struct PoolSample {
    size_t size{0}; // live threads at the end of the interval
    double busyMean{0}; // mean fraction of the interval each thread was working
    double busyMax{0}; // the busiest thread
    double avgBatch{0}; // work items per wakeup, where the pool knows it
    double waitUs{0}; // mean time work waited for a thread, where measurable
    // Optional per-thread detail for the stats endpoint.
    std::vector<double> threadBusy;
    std::vector<int> threadLoad;
};

// A pool whose size can be changed at run time. All methods are called from
// the tuner thread only; Grow/Shrink may complete asynchronously, and Size()
// reports the target count, not the number of threads that have finished
// starting or exiting.
class ElasticPool {
public:
    virtual ~ElasticPool() = default;
    virtual const char* Name() const = 0;
    virtual size_t Size() const = 0;
    // Smallest change that makes sense for this pool (1 for a flat pool, the
    // shard count for a pool that is replicated per shard).
    virtual size_t Step() const = 0;
    virtual void Grow(size_t n) = 0;
    virtual void Shrink(size_t n) = 0;
    // Measurements since the previous call. wallSec is the interval length.
    virtual PoolSample Sample(double wallSec) = 0;
    // Finish any asynchronous Shrink (join exited threads). Called each tick.
    virtual void Reap() {}
    // False while a Grow or Shrink is still taking effect (threads starting,
    // work moving). The tuner does not judge a change before this is true.
    virtual bool Settled() const {
        return true;
    }
};

// The per-shard pools of one role presented to the tuner as a single pool.
// Every shard carries the same share of the work, so the shards are kept the
// same size: Size() is the total and Step() is one thread per shard.
// Pool must provide Size/Grow/Shrink/Reap and a TakeSample() returning
// busyNs, waitNs, tasks and items accumulated since the previous call.
template <typename Pool>
class ShardedPoolGroup : public ElasticPool {
public:
    ShardedPoolGroup(const char* name, std::vector<Pool*> pools)
        : name_(name), pools_(std::move(pools)) {
    }

    const char* Name() const override {
        return name_;
    }
    size_t Step() const override {
        return pools_.size();
    }
    size_t Size() const override {
        size_t n = 0;
        for (auto* p : pools_) {
            n += p->Size();
        }
        return n;
    }
    void Grow(size_t n) override {
        const size_t per = std::max<size_t>(1, n / pools_.size());
        for (auto* p : pools_) {
            p->Grow(per);
        }
    }
    void Shrink(size_t n) override {
        const size_t per = std::max<size_t>(1, n / pools_.size());
        for (auto* p : pools_) {
            p->Shrink(per);
        }
    }
    void Reap() override {
        for (auto* p : pools_) {
            p->Reap();
        }
    }
    PoolSample Sample(double wallSec) override {
        PoolSample s;
        uint64_t busyNs = 0, waitNs = 0, tasks = 0, items = 0;
        for (auto* p : pools_) {
            const auto d = p->TakeSample();
            busyNs += d.busyNs;
            waitNs += d.waitNs;
            tasks += d.tasks;
            items += d.items;
            s.size += p->Size();
            if (p->Size() > 0 && wallSec > 0) {
                s.busyMax = std::max(
                        s.busyMax, d.busyNs / (wallSec * 1e9 * p->Size()));
            }
        }
        if (s.size > 0 && wallSec > 0) {
            s.busyMean = busyNs / (wallSec * 1e9 * s.size);
        }
        s.avgBatch = tasks > 0 ? static_cast<double>(items) / tasks : 0;
        s.waitUs = tasks > 0 ? waitNs / 1e3 / tasks : 0;
        return s;
    }

private:
    const char* name_;
    std::vector<Pool*> pools_;
};

struct TunerConfig {
    std::chrono::milliseconds sampleInterval{250};
    // A decision window is this many samples; throughput is averaged over it.
    size_t samplesPerDecision{8};
    // Above this mean busy fraction the pool is offered more threads.
    double highBusy{0.80};
    // A grow is kept if throughput rose by more than minGain; a shrink if it
    // fell by less than maxLoss. maxLoss is the smaller so that no step can
    // be kept in both directions, which would cycle at the boundary.
    double minGain{0.01};
    double maxLoss{0.005};
    // A pool whose threads are this idle is not measurable against global
    // throughput: shrinking it is judged on the idleness alone.
    double idleBusy{0.02};
    // A pool this busy that has taken several steps in a row without a
    // revert is ramping towards its working size. Its step escalates so
    // that costs a few trials rather than dozens.
    double rampBusy{0.90};
    size_t rampAfterKept{2};
    // Windows to skip after a change, then windows to average before judging
    // it; a step too small to show the tolerance in full gets the longer look.
    size_t settleWindows{1};
    size_t measureWindows{2};
    size_t smallStepMeasureWindows{4};
    // Longest to wait for a pool to report a change has taken effect.
    size_t maxSettleWindows{15};
    // After a reverted change that direction waits this many windows before
    // trying again, doubling each time up to the cap - unless the load
    // changes first (throughput or busy fraction move by these amounts).
    size_t firstBackoffWindows{8};
    size_t maxBackoffWindows{256};
    double loadChangeTput{0.10};
    double loadChangeBusy{0.15};
};

// Sizes the registered pools by trial.
//
// Every decision window the tuner changes one pool by a quarter of its size
// - up if its threads are mostly busy, down otherwise - waits for the change
// to take effect, and keeps it only if throughput responded: up by more than
// minGain for a grow, down by less than maxLoss for a shrink. A failed
// change is undone and retried at half the step; once the smallest step
// fails, that direction backs off until the load changes. Busy fraction
// only chooses the direction to try; throughput decides. That is what lets
// it shrink a pool blocked on IO with a backlog, which reads 100% busy at
// any size: growing it fails, so shrinking is tried, and holds.
class ThreadTuner {
public:
    struct Bounds {
        size_t min{1};
        size_t max{1};
    };

    ThreadTuner(TunerConfig cfg, std::function<uint64_t()> opsCounter);
    ~ThreadTuner();

    void AddPool(ElasticPool* pool, Bounds bounds);
    void Start();
    void Stop();

    std::string ToJson() const;

private:
    enum class Trial { None, Grow, Shrink };

    // Retry state for one direction of one pool.
    struct Direction {
        size_t cap{0}; // largest step to try next; 0 = unlimited
        size_t backoff{0}; // windows left before trying again
        size_t nextBackoff{0}; // what the next failure will cost
        double tputAtFail{0};
        double busyAtFail{0};
        // Consecutive kept moves. A pool still accepting every step is
        // ramping, not searching, so the step escalates; any revert
        // resets it and the halving search takes over.
        size_t keptStreak{0};
        // The last kept move raised throughput beyond the noise.
        bool lastGained{false};
        // Throughput before the first move of the current streak. Each move
        // is judged against it as well as against the last level, so a run
        // of moves that each lose a little cannot add up to a large loss:
        // measured, readers walked 112 -> 40 and writers 80 -> 48, each step
        // "kept" inside a 1.1% tolerance, for a 10% loss overall.
        double streakRef{0};
    };

    struct PoolState {
        ElasticPool* pool{nullptr};
        Bounds bounds;
        // Window accumulators and the last completed window.
        double busySum{0}, busyMaxSeen{0}, batchSum{0}, waitSum{0};
        size_t samples{0};
        PoolSample last;
        // Trial in flight.
        Trial trial{Trial::None};
        size_t sizeBefore{0};
        double refTput{0};
        double busyBefore{0};
        // Observed over projected busy after kept shrinks. A shrink projects
        // busy * size / (size - n); pools whose work gets cheaper as it
        // concentrates (writers batch more docs per wake) come in under that,
        // and the guard learns to allow the steps it would otherwise refuse.
        double shrinkScale{1.0};
        size_t measure{0};
        double noise{0}; // steadyNoise() when the trial started
        // Per-window throughput during a trial. The verdict takes the
        // median: a single flush or compaction stall inside the window
        // otherwise decides it, and one did - a grow measured -77.7% in a
        // load whose own noise was 12%.
        std::vector<double> tputWindows;
        size_t windowsSinceChange{0};
        size_t settledWindows{0}; // windows since the pool reported Settled
        Direction grow, shrink;
        // For the stats endpoint.
        std::string lastAction{"none"};
        uint64_t changes{0};
        uint64_t reverts{0};
    };

    void run();
    void sample(double wallSec);
    void decide(double tput);
    void closeWindow(PoolState& ps, double tput, bool steady);
    bool startTrial(PoolState& ps);
    void judge(PoolState& ps, double tput);
    size_t roundToStep(size_t n, size_t step) const;
    // Median of the last `n` steady windows: the same estimator judge()
    // applies to a trial's windows, so a reference and a verdict resolve
    // the same load.
    double steadyLevel(size_t n) const;
    // True while a pegged pool is still being given steps it keeps. Such a
    // pool does not need a settled baseline to know it wants more threads.
    bool rampingUp() const;
    // Window-to-window variation of steady throughput (coefficient of
    // variation over steadyRecent_). Verdict thresholds and measurement
    // length scale with it: a load whose throughput swings 8% between
    // windows on its own cannot support a 1% verdict after two windows.
    double steadyNoise() const;
    double cv(const std::vector<double>& w) const;
    double tolerance(const PoolState& ps) const;
    static Direction& dirRef(PoolState& ps, bool grew);

    TunerConfig cfg_;
    std::function<uint64_t()> opsCounter_;
    std::vector<PoolState> pools_;
    mutable std::mutex mu_; // guards pools_ for ToJson
    std::thread thread_;
    std::atomic<bool> running_{false};
    size_t nextPool_{0};
    double lastTput_{0};
    uint64_t lastOps_{0};
    // Throughput in windows with no trial in flight. A grow is judged
    // against the median of the last few; a shrink against the best such
    // median seen, so a run of small losses cannot add up. The best follows
    // the load down after a few medians below it. Raw windows never set
    // either: a load that swings 2x within a few seconds (flush storms) puts
    // a trough in every other window, and a reference taken from one made
    // the next shrink look like a 27% gain.
    std::vector<double> steadyRecent_;
    double steadyBest_{0};
    size_t steadyLowDecisions_{0};
    size_t stepLow_{0};
    double measuredNoise_{-1};
    static constexpr size_t kSteadyHistory = 16;
    static constexpr size_t kLevelWindows = 4;
};

} // namespace kvserver
} // namespace magma
