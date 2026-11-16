#include "autosizer.h"

#include <spdlog/spdlog.h>

#include <pthread.h>
#include <time.h>

#include <algorithm>
#include <chrono>

namespace magma {
namespace kvserver {

namespace {

constexpr auto kTick = std::chrono::milliseconds(250);
constexpr size_t kTicksPerWindow = 4;
constexpr size_t kKeptWindows = 60;
// A cgroup quota can change under a running process.
constexpr auto kBudgetRefresh = std::chrono::seconds(30);

uint64_t processCpuNs() {
    timespec ts{};
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
    return uint64_t(ts.tv_sec) * 1000000000ULL + uint64_t(ts.tv_nsec);
}

bool sameBudget(const CpuBudget& a, const CpuBudget& b) {
    return a.usable == b.usable && a.physicalCores == b.physicalCores &&
           a.smtWidth == b.smtWidth && a.cgroup == b.cgroup;
}

} // namespace

AutosizeConfig AutosizeConfig::For(const CpuBudget& budget,
                                   size_t ioThreads,
                                   size_t magmaReaders,
                                   double efficiency) {
    AutosizeConfig c;
    c.budget = budget;
    c.policy.efficiency = efficiency;
    if (ioThreads) {
        c.pinIo = ioThreads;
    }
    if (magmaReaders) {
        c.pinMagma = magmaReaders;
    }
    return c;
}

size_t AutosizeConfig::InitialIo() const {
    return pinIo.value_or(std::max<size_t>(2, (budget.usable + 9) / 10));
}

size_t AutosizeConfig::InitialMagma() const {
    return pinMagma.value_or(std::max<size_t>(1, budget.usable / 2));
}

std::array<PoolBounds, kPoolCount> AutosizeConfig::Bounds(const CpuBudget& b,
                                                          size_t ioSize) const {
    std::array<PoolBounds, kPoolCount> out;
    const size_t usable = b.usable;
    out[size_t(PoolId::Io)] =
            pinIo ? PoolBounds{*pinIo, *pinIo, 1}
                  : PoolBounds{2, std::max<size_t>(2, usable / 2), 1};
    out[size_t(PoolId::MagmaReaders)] =
            pinMagma ? PoolBounds{*pinMagma, *pinMagma, 1}
                     : PoolBounds{1,
                                  usable > ioSize + 2 ? usable - ioSize - 2 : 1,
                                  1};
    return out;
}

std::unique_ptr<Autosizer> Autosizer::Start(const AutosizeConfig& cfg,
                                            Ports ports) {
    return std::unique_ptr<Autosizer>(new Autosizer(cfg, std::move(ports)));
}

Autosizer::Autosizer(const AutosizeConfig& cfg, Ports ports)
    : cfg_(cfg), ports_(std::move(ports)), budget_(cfg.budget) {
    budgetReadAt_ = AutosizeClock::now();
    lastReads_ = ports_.reads();
    lastCompactions_ = ports_.compactions ? ports_.compactions() : 0;
    lastProcessNs_ = processCpuNs();
    // Baseline for the first window's deltas.
    ports_.io->Sample(0);
    ports_.magmaReaders->Sample(0);
    thread_ = std::thread([this]() {
        pthread_setname_np(pthread_self(), "fx:autosize");
        run();
    });
}

Autosizer::~Autosizer() {
    running_ = false;
    if (thread_.joinable()) {
        thread_.join();
    }
}

ElasticPool* Autosizer::port(PoolId id) const {
    return id == PoolId::Io ? ports_.io : ports_.magmaReaders;
}

void Autosizer::run() {
    auto windowStart = AutosizeClock::now();
    size_t ticks = 0;
    while (running_.load()) {
        std::this_thread::sleep_for(kTick);
        ports_.io->Reap();
        ports_.magmaReaders->Reap();
        if (++ticks < kTicksPerWindow) {
            continue;
        }
        ticks = 0;
        const auto now = AutosizeClock::now();
        window(std::chrono::duration<double>(now - windowStart).count(), now);
        windowStart = now;
    }
}

void Autosizer::window(double wallSec, AutosizeClock::time_point now) {
    if (wallSec <= 0) {
        return;
    }
    if (now - budgetReadAt_ >= kBudgetRefresh) {
        budgetReadAt_ = now;
        const auto b = ReadCpuBudget();
        if (!sameBudget(b, budget_)) {
            spdlog::info("autosize: cpu budget usable={} cores={} smt={} "
                         "cgroup={} (was usable={})",
                         b.usable, b.physicalCores, b.smtWidth, b.cgroup,
                         budget_.usable);
            std::lock_guard<std::mutex> g(mu_);
            budget_ = b;
        }
    }

    Window w;
    const uint64_t reads = ports_.reads();
    w.readsPerSec = (reads - lastReads_) / wallSec;
    lastReads_ = reads;
    const uint64_t cpu = processCpuNs();
    w.processCores = (cpu - lastProcessNs_) / (wallSec * 1e9);
    lastProcessNs_ = cpu;
    if (ports_.compactions) {
        const uint64_t c = ports_.compactions();
        w.compactions = double(c >= lastCompactions_ ? c - lastCompactions_ : 0);
        lastCompactions_ = c;
    }
    double poolCores = 0;
    for (size_t i = 0; i < kPoolCount; i++) {
        auto* p = port(static_cast<PoolId>(i));
        w.pools[i] = p->Sample(wallSec);
        w.settled[i] = p->Settled();
        poolCores += w.pools[i].workCores;
    }
    w.otherCores = std::max(0.0, w.processCores - poolCores);

    const auto bounds = cfg_.Bounds(budget_, w.pools[size_t(PoolId::Io)].size);
    const auto d = Decide(w, now, bounds, cfg_.policy, state_);
    apply(d);

    std::lock_guard<std::mutex> g(mu_);
    windows_.push_back(WindowToDynamic(w));
    if (windows_.size() > kKeptWindows) {
        windows_.pop_front();
    }
    lastSamples_ = w.pools;
    lastBounds_ = bounds;
    for (size_t i = 0; i < kPoolCount; i++) {
        lastStates_[i] = state_.pools[i];
        lastStates_[i].probe = {}; // keeps the copy small
    }
    for (const auto& a : d.resize) {
        changes_[size_t(a.pool)]++;
    }
    probing_ = state_.probing;
}

void Autosizer::apply(const Decisions& d) {
    for (const auto& a : d.resize) {
        auto* p = port(a.pool);
        const size_t size = p->Size();
        if (a.to > size) {
            p->Grow(a.to - size);
        } else if (a.to < size) {
            p->Shrink(size - a.to);
        }
        spdlog::info("autosize: {} {} -> {} ({})",
                     PoolName(a.pool), size, a.to, ReasonName(a.why));
    }
    for (const auto& s : d.spin) {
        port(s.pool)->SetIdleSpin(s.spinners);
    }
}

folly::dynamic Autosizer::Stats() const {
    std::lock_guard<std::mutex> g(mu_);
    folly::dynamic pools = folly::dynamic::array;
    for (size_t i = 0; i < kPoolCount; i++) {
        const auto& smp = lastSamples_[i];
        const auto& ps = lastStates_[i];
        const auto& b = lastBounds_[i];
        folly::dynamic pool = folly::dynamic::object
                ("name", PoolName(static_cast<PoolId>(i)))
                ("size", smp.size)
                ("min", b.min)
                ("max", b.max)
                ("pinned", b.min == b.max)
                ("want", ps.want)
                ("regime", RegimeName(ps.regime))
                ("util", smp.size > 0 ? smp.workCores / smp.size : 0.0)
                ("work_cores", smp.workCores)
                ("queue_wait_us", smp.queueWaitUs)
                ("inflight_per_thread", smp.inFlightPerThread)
                ("ceiling", ps.ceiling == std::numeric_limits<size_t>::max()
                                    ? folly::dynamic(nullptr)
                                    : folly::dynamic(ps.ceiling))
                ("spinners", ps.spinners == std::numeric_limits<size_t>::max()
                                     ? folly::dynamic(nullptr)
                                     : folly::dynamic(ps.spinners))
                ("changes", changes_[i])
                ("keeps", ps.keeps)
                ("reverts", ps.reverts)
                ("inconclusive", ps.inconclusive);
        if (ps.lastTo != 0) {
            pool["last"] = folly::dynamic::object("from", ps.lastFrom)(
                    "to", ps.lastTo)("reason", ReasonName(ps.lastReason));
        }
        pools.push_back(std::move(pool));
    }
    folly::dynamic windows = folly::dynamic::array;
    for (const auto& w : windows_) {
        windows.push_back(w);
    }
    return folly::dynamic::object
            ("enabled", true)
            ("budget", folly::dynamic::object("usable", budget_.usable)(
                               "cores", budget_.physicalCores)(
                               "smt", budget_.smtWidth)("cgroup", budget_.cgroup))
            ("probing", probing_ ? folly::dynamic(PoolName(*probing_))
                                 : folly::dynamic(nullptr))
            ("pools", std::move(pools))
            ("windows", std::move(windows));
}

} // namespace kvserver
} // namespace magma
