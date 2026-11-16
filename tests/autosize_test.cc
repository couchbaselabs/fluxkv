// Decide() against a plant fitted to the x86 lab box and against scripted
// windows. Plain asserts, as cache_test.

#include "autosize.h"

#include <folly/json/json.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <random>
#include <vector>

using namespace magma::kvserver;

#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__,      \
                         __LINE__, #cond);                                   \
            std::exit(1);                                                    \
        }                                                                    \
    } while (0)

namespace {

constexpr size_t kIo = size_t(PoolId::Io);
constexpr size_t kMagma = size_t(PoolId::MagmaReaders);
const double kInf = std::numeric_limits<double>::infinity();

double interp(const std::vector<std::pair<double, double>>& pts, double x) {
    if (x <= pts.front().first) {
        return pts.front().second;
    }
    for (size_t i = 1; i < pts.size(); i++) {
        if (x <= pts[i].first) {
            const auto [x0, y0] = pts[i - 1];
            const auto [x1, y1] = pts[i];
            return y0 + (y1 - y0) * (x - x0) / (x1 - x0);
        }
    }
    return pts.back().second;
}

// T2 on .54 with --io-reads: GET/s and process cores at saturation by magma
// reader count (24/48/64 measured; 0 and 80 extrapolated).
double capacity(size_t n) {
    return interp({{0, 0}, {24, 1.155e6}, {48, 1.565e6}, {64, 1.611e6}, {80, 1.618e6}},
                  double(n));
}
double coresAtCapacity(size_t n) {
    return interp({{0, 3}, {24, 38.4}, {48, 70.4}, {64, 72.8}, {80, 73.6}},
                  double(n));
}

struct Plant {
    double offered{kInf}; // GET/s offered; inf = closed loop
    size_t io{8};
    size_t magma{40};
    double other{3}; // background cores outside the pools
    double compactions{0};
    double readsScale{1}; // scripted disturbance on the throughput signal
    double noise{0.005};
    std::mt19937 rng{42};

    Window Next() {
        std::normal_distribution<double> jitter(0, noise);
        const double cap = capacity(magma);
        const double ioCap = io * 0.95 / 2.5e-6;
        const double t = std::min({offered, cap, ioCap});
        const double ioWork = t * 2.5e-6; // 2.5 us per GET on the IO loop
        const double perGet =
                (coresAtCapacity(magma) - other - capacity(magma) * 2.5e-6) / cap;
        const double magmaWork = t * perGet;
        Window w;
        w.readsPerSec = t * readsScale * (1 + jitter(rng));
        w.otherCores = other;
        w.compactions = compactions;
        w.processCores = magmaWork + ioWork + other;
        w.pools[kIo] = {};
        w.pools[kIo].size = io;
        w.pools[kIo].kind = BusyKind::ThreadCpu;
        w.pools[kIo].workCores = ioWork;
        w.pools[kIo].queueWaitUs = offered > ioCap && ioCap < cap ? 2000 : 200;
        w.pools[kMagma] = {};
        w.pools[kMagma].size = magma;
        w.pools[kMagma].kind = BusyKind::ThreadCpu;
        w.pools[kMagma].workCores = magmaWork;
        w.pools[kMagma].queueWaitUs = offered > cap ? 400 : 20;
        w.pools[kMagma].inFlightPerThread = 4;
        w.pools[kMagma].depthCap = 64;
        return w;
    }
};

struct Run {
    Plant plant;
    AutosizePolicy policy;
    ControllerState state;
    unsigned usable{80};
    std::array<PoolBounds, kPoolCount> pinnedBounds{};
    bool pinMagma{false};
    AutosizeClock::time_point now{AutosizeClock::time_point{} + std::chrono::hours(1)};
    std::vector<Action> log;
    std::vector<Window> windows;

    std::array<PoolBounds, kPoolCount> bounds() const {
        std::array<PoolBounds, kPoolCount> b;
        b[kIo] = {2, usable / 2, 1};
        // Magma and IO together never oversubscribe the budget.
        b[kMagma] = {1, std::max<size_t>(1, usable - plant.io - 2), 1};
        if (pinMagma) {
            b[kMagma] = {plant.magma, plant.magma, 1};
        }
        return b;
    }

    Decisions Step() {
        auto w = plant.Next();
        windows.push_back(w);
        auto d = Decide(w, now, bounds(), policy, state);
        for (const auto& a : d.resize) {
            log.push_back(a);
            (a.pool == PoolId::Io ? plant.io : plant.magma) = a.to;
        }
        now += std::chrono::seconds(1);
        return d;
    }
    void Steps(size_t n) {
        for (size_t i = 0; i < n; i++) {
            Step();
        }
    }
    size_t Count(Reason why, size_t from = 0) const {
        return std::count_if(log.begin() + from, log.end(), [why](const Action& a) {
            return a.why == why;
        });
    }
};

// Closed-loop T2 from the default start climbs by probes and stops where
// the next step costs more CPU than it buys.
void steadyT2Converges() {
    Run r;
    r.Steps(60);
    std::printf("steady: magma=%zu io=%zu keeps=%llu reverts=%zu\n",
                r.plant.magma, r.plant.io,
                (unsigned long long)r.state.pools[kMagma].keeps,
                r.Count(Reason::ProbeRevert));
    CHECK(r.plant.magma >= 58 && r.plant.magma <= 66);
    CHECK(r.state.pools[kMagma].keeps >= 2); // a keep emits no action
    CHECK(r.Count(Reason::ProbeRevert) == 1);
    CHECK(r.state.pools[kMagma].ceiling == r.plant.magma);
    // IO at ~0.5 util sits inside the shrink gap.
    CHECK(r.plant.io >= 6 && r.plant.io <= 8);

    // Held for the 60 s backoff, then one re-probe that reverts and
    // doubles it.
    const auto until = r.state.pools[kMagma].backoffUntil;
    const size_t before = r.log.size();
    while (r.now < until) {
        r.Step();
    }
    CHECK(r.log.size() == before);
    r.Steps(25);
    CHECK(r.Count(Reason::ProbeRevert) == 2);
    CHECK(r.state.pools[kMagma].backoff == std::chrono::seconds(120));
}

// At efficiency 1.0 a step must pay for itself one for one: it stops lower.
void efficiencyKnobStopsEarlier() {
    Run r;
    r.policy.efficiency = 1.0;
    r.Steps(60);
    std::printf("efficiency=1: magma=%zu\n", r.plant.magma);
    CHECK(r.plant.magma < 58);
}

// Load falls to 200K: the demand law shrinks both pools from their own CPU
// with no probes, and spinners follow the size down. Back to closed loop: probes climb again.
void stepLoadDownAndUp() {
    Run r;
    r.Steps(60);
    const size_t converged = r.plant.magma;
    const size_t mark = r.log.size();
    r.plant.offered = 200e3;
    size_t spinners = 99;
    for (size_t i = 0; i < 90; i++) {
        for (const auto& s : r.Step().spin) {
            if (s.pool == PoolId::MagmaReaders) {
                spinners = s.spinners;
            }
        }
    }
    std::printf("200K: magma=%zu io=%zu\n", r.plant.magma, r.plant.io);
    CHECK(r.plant.magma < converged / 3);
    CHECK(r.plant.io <= 3);
    CHECK(spinners <= (r.plant.magma + 7) / 8); // was 8 at T2
    CHECK(r.Count(Reason::ProbeStart, mark) == 0);
    // The shift cleared the learned ceiling.
    CHECK(r.state.pools[kMagma].ceiling == std::numeric_limits<size_t>::max());

    r.plant.offered = kInf;
    r.Steps(120);
    std::printf("back to T2: magma=%zu io=%zu\n", r.plant.magma, r.plant.io);
    CHECK(r.plant.magma >= 55);
    // IO probed back up until it no longer caps the magma readers.
    CHECK(r.plant.io * 0.95 / 2.5e-6 >= capacity(r.plant.magma));
}

// Compaction CPU appearing mid-probe: abandoned, no ceiling, no backoff.
void compactionAbandonsProbe() {
    for (int byCounter = 0; byCounter < 2; byCounter++) {
        Run r;
        for (size_t i = 0; i < 40 && !r.state.probing; i++) {
            r.Step();
        }
        CHECK(r.state.probing.has_value());
        const size_t from = r.state.pools[kMagma].probe.from;
        if (byCounter) {
            r.plant.compactions = 12;
        } else {
            r.plant.other = 14;
        }
        for (size_t i = 0; i < 20 && r.state.probing; i++) {
            r.Step();
        }
        CHECK(!r.state.probing.has_value());
        CHECK(r.log.back().why == Reason::ProbeAbandonBackground);
        CHECK(r.plant.magma == from);
        CHECK(r.state.pools[kMagma].ceiling == std::numeric_limits<size_t>::max());
        CHECK(r.state.pools[kMagma].inconclusive == 1);
    }

    // Nor does a probe start in a window where it just moved.
    Run r;
    r.Steps(3);
    r.plant.other = 14;
    r.Step();
    CHECK(!r.state.probing.has_value());
}

// A throughput jump the step cannot explain is not a verdict.
void implausibleGainAbandons() {
    Run r;
    for (size_t i = 0; i < 40 && !r.state.probing; i++) {
        r.Step();
    }
    CHECK(r.state.probing.has_value());
    r.plant.readsScale = 1.6;
    for (size_t i = 0; i < 20 && r.state.probing; i++) {
        r.Step();
    }
    CHECK(r.log.back().why == Reason::ProbeAbandonImplausible);
}

// --no-hot-stats used to zero the old tuner's throughput. With no read
// signal no probe can be judged, but the demand law still sizes from CPU.
void noReadSignalStillSizesByCpu() {
    Run r;
    r.plant.offered = 300e3;
    r.plant.readsScale = 0;
    r.Steps(90);
    CHECK(r.Count(Reason::ProbeStart) == 0);
    CHECK(r.plant.magma < 20);
    CHECK(r.plant.io <= 3);
}

// A pinned pool is sampled but never resized, however starved.
void pinnedPoolHolds() {
    Run r;
    r.pinMagma = true;
    r.Steps(60);
    CHECK(r.plant.magma == 40);
    for (const auto& a : r.log) {
        CHECK(a.pool != PoolId::MagmaReaders);
    }
}

// Scripted: a magma pool below the censored util whose threads run near
// their in-flight cap grows on depth.
void depthBoundGrows() {
    AutosizePolicy p;
    ControllerState s;
    std::array<PoolBounds, kPoolCount> b{{{8, 8, 1}, {1, 64, 1}}};
    Window w;
    w.readsPerSec = 1e6;
    w.processCores = 20;
    w.pools[kMagma].size = 16;
    w.pools[kMagma].kind = BusyKind::ThreadCpu;
    w.pools[kMagma].workCores = 8; // util 0.5: want_cpu 12
    w.pools[kMagma].inFlightPerThread = 15;
    w.pools[kMagma].depthCap = 16; // want_depth = ceil(16*15/12) = 20
    w.pools[kIo].size = 8;
    auto d = Decide(w, AutosizeClock::time_point{}, b, p, s);
    CHECK(d.resize.size() == 1);
    CHECK(d.resize[0].pool == PoolId::MagmaReaders);
    CHECK(d.resize[0].to == 20);
    CHECK(d.resize[0].why == Reason::DepthBound);
}

// After a revert the demand law may shrink below the ceiling but not grow
// past it, until the load shifts.
void ceilingHoldsDemandGrowth() {
    AutosizePolicy p;
    ControllerState s;
    s.pools[kMagma].ceiling = 20;
    s.pools[kMagma].regime = Regime::Ceiling;
    s.pools[kMagma].revertReads = 1e6;
    auto now = AutosizeClock::time_point{} + std::chrono::hours(1);
    s.pools[kMagma].backoffUntil = now + std::chrono::seconds(60);
    std::array<PoolBounds, kPoolCount> b{{{8, 8, 1}, {1, 64, 1}}};
    Window w;
    w.readsPerSec = 0.95e6; // within 15% of the revert level
    w.pools[kIo].size = 8;
    w.pools[kMagma].size = 20;
    w.pools[kMagma].kind = BusyKind::ThreadCpu;
    w.pools[kMagma].workCores = 17; // util 0.85: want_cpu 25
    for (int i = 0; i < 5; i++) {
        auto d = Decide(w, now, b, p, s);
        CHECK(d.resize.empty());
        now += std::chrono::seconds(1);
    }
    w.readsPerSec = 1.3e6;
    for (int i = 0; i < 4; i++) {
        Decide(w, now, b, p, s);
        now += std::chrono::seconds(1);
    }
    CHECK(s.pools[kMagma].ceiling == std::numeric_limits<size_t>::max());
    auto d = Decide(w, now, b, p, s);
    CHECK(d.resize.size() == 1 && d.resize[0].to > 20);
}

// A recorded window list replays to the same decisions: the /stats/autosize
// history is a fixture.
void replayFromJson() {
    Run live;
    live.Steps(40);
    folly::dynamic arr = folly::dynamic::array;
    for (const auto& w : live.windows) {
        arr.push_back(WindowToDynamic(w));
    }
    const auto parsed = folly::parseJson(folly::toJson(arr));

    ControllerState s;
    AutosizePolicy p;
    auto now = AutosizeClock::time_point{} + std::chrono::hours(1);
    std::vector<Action> replayed;
    Plant sizes; // only for io size feeding the magma bound
    for (const auto& item : parsed) {
        const auto w = WindowFromDynamic(item);
        std::array<PoolBounds, kPoolCount> b;
        b[kIo] = {2, 40, 1};
        const size_t magmaMax = std::max<size_t>(1, 80 - w.pools[kIo].size - 2);
        b[kMagma] = PoolBounds{1, magmaMax, 1};
        for (const auto& a : Decide(w, now, b, p, s).resize) {
            replayed.push_back(a);
        }
        now += std::chrono::seconds(1);
    }
    CHECK(replayed.size() == live.log.size());
    for (size_t i = 0; i < replayed.size(); i++) {
        CHECK(replayed[i].pool == live.log[i].pool);
        CHECK(replayed[i].to == live.log[i].to);
        CHECK(replayed[i].why == live.log[i].why);
    }
}

} // namespace

int main() {
    steadyT2Converges();
    efficiencyKnobStopsEarlier();
    stepLoadDownAndUp();
    compactionAbandonsProbe();
    implausibleGainAbandons();
    noReadSignalStillSizesByCpu();
    pinnedPoolHolds();
    depthBoundGrows();
    ceilingHoldsDemandGrowth();
    replayFromJson();
    std::printf("autosize_test: OK\n");
    return 0;
}
