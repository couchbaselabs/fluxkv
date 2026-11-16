#pragma once

// The read-path sizing controller: pure, no clocks, threads or I/O, so
// recorded windows replay through it exactly. The Autosizer shell feeds it.

#include "tuner.h"

#include <folly/json/dynamic.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <optional>
#include <vector>

namespace magma {
namespace kvserver {

enum class PoolId : uint8_t { Io, MagmaReaders };
constexpr size_t kPoolCount = 2;
const char* PoolName(PoolId id);

using AutosizeClock = std::chrono::steady_clock;

// Pinned when min == max: sampled for stats, never resized.
struct PoolBounds {
    size_t min{1};
    size_t max{1};
    size_t step{1};
};

// Everything policy, in one place. Flags override few.
struct AutosizePolicy {
    double targetUtil{0.70}; // demand law sizes to work / targetUtil
    double ioTargetUtil{0.60}; // lower: IO resize costs connection migration
    double censoredUtil{0.90}; // at or above, demand is unknown: probe
    double depthHeadroom{0.75}; // grow when in-flight/thread >= this x cap
    // A probe grow is kept iff dReads/R >= efficiency x dCpu/C ...
    double efficiency{0.15};
    // ... and dReads/R >= max(minGain, 2 standard errors).
    double minGain{0.01};
    unsigned shrinkPatienceWindows{5};
    double maxShrinkFrac{0.25};
    // Queue wait at which a busy pool is starved of threads, per PoolId:
    // IO loop time, magma submit-to-claim.
    std::array<double, kPoolCount> saturatedWaitUs{1000, 100};
    double spinUtil{0.5}; // below, idle threads park without spinning
    size_t maxSpinners{8};
    size_t settleWindows{1}; // skipped after a probe settles
    size_t maxUnsettledWindows{15};
    size_t minMeasureWindows{3};
    size_t maxMeasureWindows{16};
    double loadShift{0.15}; // reads/s move that clears a learned ceiling
    std::chrono::seconds backoffBase{60};
    std::chrono::seconds backoffMax{600};
};

// One 1 s window as the controller sees it.
struct Window {
    double readsPerSec{0}; // GETs answered, from fluxkv's own pass counter
    double processCores{0}; // CLOCK_PROCESS_CPUTIME_ID delta
    double otherCores{0}; // processCores - sum(pool workCores)
    double compactions{0}; // compaction tasks finished in the window
    std::array<PoolSample, kPoolCount> pools{};
    std::array<bool, kPoolCount> settled{true, true};
};

folly::dynamic WindowToDynamic(const Window& w);
Window WindowFromDynamic(const folly::dynamic& d);

enum class Regime : uint8_t { Demand, Probing, Ceiling };
enum class Reason : uint8_t {
    Demand,
    DepthBound,
    ProbeStart,
    ProbeKeep,
    ProbeRevert,
    ProbeAbandonBackground,
    ProbeAbandonImplausible,
    ProbeAbandonUnsettled,
    Bound, // forced into [min, max]
};
const char* RegimeName(Regime r);
const char* ReasonName(Reason r);

struct Action {
    PoolId pool;
    size_t to;
    Reason why;
};
struct SpinAction {
    PoolId pool;
    size_t spinners;
};
struct Decisions {
    std::vector<Action> resize;
    std::vector<SpinAction> spin;
};

// Last-n buffer of windows.
class WindowRing {
public:
    explicit WindowRing(size_t capacity) : capacity_(capacity) {
    }
    void Push(const Window& w) {
        if (items_.size() == capacity_) {
            items_.pop_front();
        }
        items_.push_back(w);
    }
    void Clear() {
        items_.clear();
    }
    size_t Size() const {
        return items_.size();
    }
    const std::deque<Window>& Items() const {
        return items_;
    }
    // The newest n (or fewer).
    std::vector<Window> Last(size_t n) const {
        n = std::min(n, items_.size());
        return {items_.end() - n, items_.end()};
    }

private:
    size_t capacity_;
    std::deque<Window> items_;
};

// Per-pool controller memory, owned by Decide.
struct PoolState {
    Regime regime{Regime::Demand};
    double workEwma{0};
    bool haveWork{false};
    unsigned shrinkVotes{0};
    // Learned from a reverted probe: demand may shrink below it but never
    // grow above it until the load shifts or the backoff expires.
    size_t ceiling{std::numeric_limits<size_t>::max()};
    AutosizeClock::time_point backoffUntil{};
    std::chrono::seconds backoff{0};
    double revertReads{0};
    double revertOther{0};
    size_t spinners{std::numeric_limits<size_t>::max()}; // last emitted

    struct Probe {
        size_t from{0};
        size_t to{0};
        size_t unsettled{0}; // windows waited for Settled
        size_t skipped{0}; // settle windows skipped since Settled
        size_t measure{0};
        double noise{0};
        double refOther{0};
        double refCompactions{0};
        std::vector<Window> ref;
        std::vector<Window> trial;
    } probe;

    // For stats.
    size_t want{0}; // the demand law's size at the last window it ran
    Reason lastReason{Reason::Demand};
    size_t lastFrom{0};
    size_t lastTo{0};
    bool acted{false};
    uint64_t keeps{0};
    uint64_t reverts{0};
    uint64_t inconclusive{0};
};

struct ControllerState {
    std::array<PoolState, kPoolCount> pools;
    WindowRing history{30};
    std::optional<PoolId> probing; // at most one probe process-wide
    size_t sinceResize{0}; // windows measured at the current sizes
};

// One window's decisions. Mutates only s.
Decisions Decide(const Window& w,
                 AutosizeClock::time_point now,
                 const std::array<PoolBounds, kPoolCount>& bounds,
                 const AutosizePolicy& p,
                 ControllerState& s);

// Robust coefficient of variation of reads/s over the history.
double ReadsNoise(const WindowRing& history);

} // namespace kvserver
} // namespace magma
