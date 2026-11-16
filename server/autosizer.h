#pragma once

#include "autosize.h"
#include "cpu_budget.h"

#include <folly/json/dynamic.h>

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

namespace magma {
namespace kvserver {

// Initial sizes and bounds, derived in one place from the CPU budget and
// the command line. A pool given on the command line is pinned.
struct AutosizeConfig {
    CpuBudget budget;
    AutosizePolicy policy;
    std::optional<size_t> pinIo;
    std::optional<size_t> pinMagma;

    // ioThreads / magmaReaders: the command-line value, 0 when not given.
    static AutosizeConfig For(const CpuBudget& budget,
                              size_t ioThreads,
                              size_t magmaReaders,
                              double efficiency);

    size_t InitialIo() const;
    size_t InitialMagma() const;
    // Recomputed each window: magma and IO together stay inside the budget.
    std::array<PoolBounds, kPoolCount> Bounds(const CpuBudget& b,
                                              size_t ioSize) const;
};

// The thin shell around Decide: samples the pools every 250 ms, closes a
// window every 4 ticks, applies the decisions and keeps them for stats.
class Autosizer {
public:
    struct Ports {
        ElasticPool* io;
        ElasticPool* magmaReaders;
        std::function<uint64_t()> reads; // GETs answered, cumulative
        std::function<uint64_t()> compactions; // finished, cumulative
    };

    static std::unique_ptr<Autosizer> Start(const AutosizeConfig& cfg,
                                            Ports ports);
    ~Autosizer();

    folly::dynamic Stats() const;

private:
    Autosizer(const AutosizeConfig& cfg, Ports ports);
    void run();
    void window(double wallSec, AutosizeClock::time_point now);
    void apply(const Decisions& d);
    ElasticPool* port(PoolId id) const;

    AutosizeConfig cfg_;
    Ports ports_;
    CpuBudget budget_; // written by the autosize thread under mu_
    AutosizeClock::time_point budgetReadAt_;
    ControllerState state_;
    uint64_t lastReads_{0};
    uint64_t lastCompactions_{0};
    uint64_t lastProcessNs_{0};

    mutable std::mutex mu_; // guards the stats copies below
    std::deque<folly::dynamic> windows_; // the last 60, replayable
    std::array<PoolSample, kPoolCount> lastSamples_{};
    std::array<PoolBounds, kPoolCount> lastBounds_{};
    std::array<PoolState, kPoolCount> lastStates_{};
    std::array<uint64_t, kPoolCount> changes_{};
    std::optional<PoolId> probing_;

    std::atomic<bool> running_{true};
    std::thread thread_;
};

} // namespace kvserver
} // namespace magma
