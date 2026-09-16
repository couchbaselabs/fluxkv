#pragma once

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
};

struct TunerConfig {
    std::chrono::milliseconds sampleInterval{250};
    // Decisions are made once per this many samples; the window has to be
    // long enough for a throughput reading to be stable.
    size_t samplesPerDecision{8};
    // Above this mean busy fraction the pool is short of threads.
    double highBusy{0.80};
    // Above this the pool is saturated and grows in larger steps.
    double saturatedBusy{0.93};
    // Below this the pool has more threads than the load needs.
    double lowBusy{0.55};
    // A grow is kept only if throughput rose by at least this fraction;
    // absorbs run-to-run noise.
    double minGain{0.02};
    // A shrink is kept only if throughput stayed within this fraction of the
    // best steady value seen, so losses cannot stack across steps or pools.
    double maxLoss{0.01};
    // Windows to wait after a change before measuring it, and windows to
    // average the measurement over.
    size_t settleWindows{1};
    size_t measureWindows{2};
    // A step whose expected gain is below minGain is measured over this many
    // windows instead, since it has to be judged against a smaller margin.
    size_t smallStepMeasureWindows{4};
    // Floor on the gain a small step must show.
    double minSmallGain{0.005};
    // Back-off after a change that did not pay, in windows: starts here and
    // doubles up to the cap while the load looks the same.
    size_t firstBackoffWindows{8};
    size_t maxBackoffWindows{256};
    // A back-off ends early when throughput moves by this fraction, or the
    // busy fraction by this much, from when the change was reverted.
    double conditionChangeTput{0.10};
    double conditionChangeBusy{0.15};
};

// Sizes the registered pools from their own busy fraction and the server's
// throughput.
//
// A pool whose threads are mostly busy is short of threads; a pool whose
// threads are mostly idle has more than the load needs, and fewer threads
// batch better. Neither signal alone is safe to act on: a saturated pool
// gains nothing from growth when something else (the client, the network) is
// the limit, and a lightly loaded pool may still be at the count that carries
// the load. So every change is a trial: the tuner changes one pool by one
// step, waits, and keeps the change only if throughput responded the way the
// change predicted. A failed trial is undone and that direction backs off
// exponentially until the busy signal changes.
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

    struct PoolState {
        ElasticPool* pool{nullptr};
        Bounds bounds;
        // Window accumulators.
        double busySum{0};
        double busyMaxSeen{0};
        double batchSum{0};
        double waitSum{0};
        size_t samples{0};
        // Last completed window, for the stats endpoint.
        PoolSample last;
        // Trial in flight.
        Trial trial{Trial::None};
        size_t sizeBefore{0};
        double tputBefore{0};
        double busyBefore{0};
        size_t windowsSinceChange{0};
        double trialTputSum{0};
        size_t trialTputN{0};
        size_t trialMeasure{0}; // windows to average for this trial
        // Set when a grow was reverted: throughput no longer responds to
        // more threads, so fewer may do. Cleared when a grow is kept.
        bool growStalled{false};
        // Consecutive kept shrinks are judged against the throughput at the
        // start of the run, so a sequence of small losses cannot add up.
        bool inShrinkRun{false};
        double shrinkRunRef{0};
        // Back-off, in decision windows, per direction. A reverted change is
        // not retried while the load looks the same; the back-off is cleared
        // early when throughput or busy fraction move materially.
        size_t growBackoff{0};
        size_t shrinkBackoff{0};
        size_t growBackoffBase{0}; // 0 = cfg.firstBackoffWindows
        size_t shrinkBackoffBase{0};
        double growRevertTput{0};
        double growRevertBusy{0};
        double shrinkRevertTput{0};
        double shrinkRevertBusy{0};
        // After a reverted step the next attempt in that direction is half
        // as large; the smallest step backs off instead. 0 = no cap.
        size_t growCap{0};
        size_t shrinkCap{0};
        std::string lastAction{"none"};
        uint64_t changes{0};
        uint64_t reverts{0};
    };

    void run();
    void sample(double wallSec);
    void decide(double tput);
    bool evaluateTrial(PoolState& ps, double tput);
    bool startTrial(PoolState& ps, double busyMean);
    size_t roundToStep(size_t n, size_t step) const;

    TunerConfig cfg_;
    std::function<uint64_t()> opsCounter_;
    std::vector<PoolState> pools_;
    mutable std::mutex mu_; // guards pools_ for ToJson
    std::thread thread_;
    std::atomic<bool> running_{false};
    size_t nextPool_{0};
    double lastTput_{0};
    uint64_t lastOps_{0};
    // Best throughput seen in a window with no trial in flight. Shrinks are
    // judged against it. It follows the load down after a few steady windows
    // below it, so a lighter load does not block shrinking forever.
    double steadyBest_{0};
    size_t steadyLowWindows_{0};
    // Recent steady-window throughputs; their mean is the baseline a grow
    // is judged against, which halves the noise of a single window.
    std::vector<double> steadyRecent_;
    double steadyBaseline() const;
};

} // namespace kvserver
} // namespace magma
