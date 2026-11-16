#include "autosize.h"

#include <algorithm>
#include <cmath>

namespace magma {
namespace kvserver {

const char* PoolName(PoolId id) {
    switch (id) {
    case PoolId::Io:
        return "io";
    case PoolId::MagmaReaders:
        return "magma_readers";
    }
    return "?";
}

const char* RegimeName(Regime r) {
    switch (r) {
    case Regime::Demand:
        return "demand";
    case Regime::Probing:
        return "probing";
    case Regime::Ceiling:
        return "ceiling";
    }
    return "?";
}

const char* ReasonName(Reason r) {
    switch (r) {
    case Reason::Demand:
        return "demand";
    case Reason::DepthBound:
        return "depth-bound";
    case Reason::ProbeStart:
        return "probe-start";
    case Reason::ProbeKeep:
        return "probe-keep";
    case Reason::ProbeRevert:
        return "probe-revert";
    case Reason::ProbeAbandonBackground:
        return "probe-abandon(background)";
    case Reason::ProbeAbandonImplausible:
        return "probe-abandon(implausible)";
    case Reason::ProbeAbandonUnsettled:
        return "probe-abandon(unsettled)";
    case Reason::Bound:
        return "bound";
    }
    return "?";
}

folly::dynamic WindowToDynamic(const Window& w) {
    folly::dynamic pools = folly::dynamic::array;
    for (size_t i = 0; i < kPoolCount; i++) {
        const auto& p = w.pools[i];
        pools.push_back(folly::dynamic::object("size", p.size)(
                "kind", p.kind == BusyKind::ThreadCpu ? "thread_cpu" : "wall")(
                "work_cores", p.workCores)("queue_wait_us", p.queueWaitUs)(
                "inflight_per_thread", p.inFlightPerThread)(
                "depth_cap", p.depthCap)("settled", w.settled[i]));
    }
    return folly::dynamic::object("reads_per_sec", w.readsPerSec)(
            "process_cores", w.processCores)("other_cores", w.otherCores)(
            "compactions", w.compactions)("pools", std::move(pools));
}

Window WindowFromDynamic(const folly::dynamic& d) {
    Window w;
    w.readsPerSec = d["reads_per_sec"].asDouble();
    w.processCores = d["process_cores"].asDouble();
    w.otherCores = d["other_cores"].asDouble();
    w.compactions = d["compactions"].asDouble();
    for (size_t i = 0; i < kPoolCount && i < d["pools"].size(); i++) {
        const auto& p = d["pools"][i];
        auto& s = w.pools[i];
        s.size = p["size"].asInt();
        s.kind = p["kind"].asString() == "thread_cpu" ? BusyKind::ThreadCpu
                                                       : BusyKind::WallInCall;
        s.workCores = p["work_cores"].asDouble();
        s.queueWaitUs = p["queue_wait_us"].asDouble();
        s.inFlightPerThread = p["inflight_per_thread"].asDouble();
        s.depthCap = p["depth_cap"].asInt();
        w.settled[i] = p["settled"].asBool();
    }
    return w;
}

namespace {

constexpr size_t kRefWindows = 4;
// EWMA weight of a 1 s window for a 4 s time constant.
const double kWorkAlpha = 1.0 - std::exp(-1.0 / 4.0);

double median(std::vector<double> v) {
    if (v.empty()) {
        return 0;
    }
    const size_t mid = v.size() / 2;
    std::nth_element(v.begin(), v.begin() + mid, v.end());
    double m = v[mid];
    if (v.size() % 2 == 0) {
        m = (m + *std::max_element(v.begin(), v.begin() + mid)) / 2;
    }
    return m;
}

template <typename F>
double medianOf(const std::vector<Window>& ws, F field) {
    std::vector<double> v;
    v.reserve(ws.size());
    for (const auto& w : ws) {
        v.push_back(field(w));
    }
    return median(std::move(v));
}

double reads(const Window& w) {
    return w.readsPerSec;
}
double cores(const Window& w) {
    return w.processCores;
}
double other(const Window& w) {
    return w.otherCores;
}
double compactions(const Window& w) {
    return w.compactions;
}

// Background CPU (compaction, flush) moved enough to swamp a probe's step.
bool otherMoved(double now, double ref) {
    return std::abs(now - ref) > std::max(1.0, 0.3 * ref);
}
bool compactionsMoved(double now, double ref) {
    return std::abs(now - ref) > std::max(2.0, 0.3 * ref);
}

bool pinned(const PoolBounds& b) {
    return b.min == b.max;
}

void record(PoolState& ps, size_t from, size_t to, Reason why) {
    ps.lastReason = why;
    ps.lastFrom = from;
    ps.lastTo = to;
    ps.acted = true;
}

void act(Decisions& d, PoolState& ps, PoolId id, size_t from, size_t to,
         Reason why) {
    d.resize.push_back({id, to, why});
    record(ps, from, to, why);
}

void clearCeiling(PoolState& ps, bool resetBackoff) {
    ps.ceiling = std::numeric_limits<size_t>::max();
    if (resetBackoff) {
        ps.backoff = std::chrono::seconds{0};
        ps.backoffUntil = {};
    }
    if (ps.regime == Regime::Ceiling) {
        ps.regime = Regime::Demand;
    }
}

void endProbe(ControllerState& s, PoolState& ps) {
    s.probing.reset();
    ps.regime = ps.ceiling == std::numeric_limits<size_t>::max()
                        ? Regime::Demand
                        : Regime::Ceiling;
    ps.probe.ref.clear();
    ps.probe.trial.clear();
}

// Probe of pool id, one window further. Ends it with a verdict once the
// trial has its windows.
void stepProbe(const Window& w,
               AutosizeClock::time_point now,
               const AutosizePolicy& p,
               ControllerState& s,
               PoolId id,
               Decisions& d) {
    const size_t i = static_cast<size_t>(id);
    auto& ps = s.pools[i];
    auto& pr = ps.probe;
    const auto abandon = [&](Reason why) {
        act(d, ps, id, pr.to, pr.from, why);
        ps.inconclusive++;
        endProbe(s, ps);
    };

    if (pr.trial.empty() && pr.skipped == 0 && !w.settled[i]) {
        if (++pr.unsettled > p.maxUnsettledWindows) {
            abandon(Reason::ProbeAbandonUnsettled);
        }
        return;
    }
    if (pr.skipped < p.settleWindows) {
        pr.skipped++;
        return;
    }
    pr.trial.push_back(w);
    if (pr.trial.size() < pr.measure) {
        return;
    }

    const double refReads = medianOf(pr.ref, reads);
    const double refCores = medianOf(pr.ref, cores);
    const double r = refReads > 0 ? medianOf(pr.trial, reads) / refReads - 1 : 0;
    const double c = refCores > 0 ? medianOf(pr.trial, cores) / refCores - 1 : 0;

    if (otherMoved(medianOf(pr.trial, other), pr.refOther) ||
        compactionsMoved(medianOf(pr.trial, compactions), pr.refCompactions)) {
        abandon(Reason::ProbeAbandonBackground);
        return;
    }
    // n more threads out of `from` cannot move reads by much more than n/from.
    const double step = double(pr.to - pr.from) / std::max<size_t>(1, pr.from);
    if (refReads <= 0 || std::abs(r) > std::max(0.05, step)) {
        abandon(Reason::ProbeAbandonImplausible);
        return;
    }
    const double needed = std::max(
            p.minGain, 2 * pr.noise / std::sqrt(double(pr.trial.size())));
    if (r >= needed && r >= p.efficiency * std::max(c, 0.0)) {
        record(ps, pr.from, pr.to, Reason::ProbeKeep);
        ps.keeps++;
        ps.backoff = std::chrono::seconds{0};
        endProbe(s, ps);
        return;
    }
    act(d, ps, id, pr.to, pr.from, Reason::ProbeRevert);
    ps.reverts++;
    ps.ceiling = pr.from;
    ps.backoff = ps.backoff.count() == 0
                         ? p.backoffBase
                         : std::min(2 * ps.backoff, p.backoffMax);
    ps.backoffUntil = now + ps.backoff;
    ps.revertReads = refReads;
    ps.revertOther = pr.refOther;
    endProbe(s, ps);
}

} // namespace

double ReadsNoise(const WindowRing& history) {
    // On successive differences: each kept probe shifts the level, and a
    // MAD around one median would count those shifts as noise.
    const auto& items = history.Items();
    std::vector<double> levels, diffs;
    for (size_t i = 0; i < items.size(); i++) {
        levels.push_back(items[i].readsPerSec);
        if (i > 0) {
            diffs.push_back(std::abs(items[i].readsPerSec -
                                     items[i - 1].readsPerSec));
        }
    }
    const double m = median(std::move(levels));
    if (m <= 0 || diffs.empty()) {
        return 0;
    }
    // |a - b| of two N(0, s) draws has median 0.954 s.
    return median(std::move(diffs)) / 0.954 / m;
}

Decisions Decide(const Window& w,
                 AutosizeClock::time_point now,
                 const std::array<PoolBounds, kPoolCount>& bounds,
                 const AutosizePolicy& p,
                 ControllerState& s) {
    Decisions d;
    s.history.Push(w);
    const auto recent = s.history.Last(kRefWindows);
    const double recentReads = medianOf(recent, reads);
    const double recentOther = medianOf(recent, other);
    const double recentCompactions = medianOf(recent, compactions);

    for (size_t i = 0; i < kPoolCount; i++) {
        auto& ps = s.pools[i];
        ps.acted = false;
        const double work = w.pools[i].workCores;
        ps.workEwma = ps.haveWork ? ps.workEwma + kWorkAlpha * (work - ps.workEwma)
                                  : work;
        ps.haveWork = true;

        if (ps.ceiling != std::numeric_limits<size_t>::max()) {
            const bool shifted =
                    ps.revertReads > 0 &&
                    std::abs(recentReads / ps.revertReads - 1) >= p.loadShift;
            if (shifted || otherMoved(recentOther, ps.revertOther)) {
                clearCeiling(ps, true);
            } else if (now >= ps.backoffUntil) {
                // Keep the backoff length: a second revert doubles it.
                clearCeiling(ps, false);
            }
        }
    }

    if (s.probing) {
        // Other pools hold still: a move would confound the probe.
        stepProbe(w, now, p, s, *s.probing, d);
    } else {
        for (size_t i = 0; i < kPoolCount; i++) {
            const auto id = static_cast<PoolId>(i);
            const auto& b = bounds[i];
            const auto& smp = w.pools[i];
            auto& ps = s.pools[i];
            const size_t size = smp.size;
            if (size < b.min || size > b.max) {
                act(d, ps, id, size, std::clamp(size, b.min, b.max), Reason::Bound);
                ps.shrinkVotes = 0;
                continue;
            }
            if (pinned(b) || !w.settled[i] || size == 0) {
                ps.shrinkVotes = 0;
                continue;
            }

            const double util = smp.workCores / size;
            const double target =
                    id == PoolId::Io ? p.ioTargetUtil : p.targetUtil;
            const size_t wantCpu = size_t(std::ceil(ps.workEwma / target));
            const size_t wantDepth =
                    smp.depthCap > 0
                            ? size_t(std::ceil(size * smp.inFlightPerThread /
                                               (p.depthHeadroom * smp.depthCap)))
                            : 0;
            const size_t cap = std::max(b.min, std::min(b.max, ps.ceiling));
            const size_t want =
                    std::clamp(std::max(wantCpu, wantDepth), b.min, cap);
            ps.want = want;

            if (smp.kind == BusyKind::ThreadCpu && util < p.censoredUtil) {
                const double shrinkGap =
                        std::max(double(b.step), 0.2 * double(size));
                if (want > size) {
                    act(d, ps, id, size, std::min(want, 2 * size),
                        wantDepth > wantCpu ? Reason::DepthBound
                                            : Reason::Demand);
                    ps.shrinkVotes = 0;
                } else if (double(want) <= double(size) - shrinkGap) {
                    if (++ps.shrinkVotes >= p.shrinkPatienceWindows) {
                        const size_t cut = std::max(
                                b.step, size_t(double(size) * p.maxShrinkFrac));
                        const size_t to = std::max(want, size - std::min(cut, size));
                        if (to < size) {
                            act(d, ps, id, size, to, Reason::Demand);
                        }
                        ps.shrinkVotes = 0;
                    }
                } else {
                    ps.shrinkVotes = 0;
                }
                continue;
            }
            ps.shrinkVotes = 0;

            const bool saturated = smp.queueWaitUs >= p.saturatedWaitUs[i];
            // A probe starts from a window where nothing else moved.
            if (util >= p.censoredUtil && saturated && size < cap &&
                now >= ps.backoffUntil && d.resize.empty() &&
                s.history.Size() >= kRefWindows && recentReads > 0 &&
                !otherMoved(w.otherCores, recentOther) &&
                !compactionsMoved(w.compactions, recentCompactions)) {
                auto& pr = ps.probe;
                pr = {};
                pr.from = size;
                // A pegged pool far below its max (regrowing after an idle
                // shrink to 1) doubles: +25% steps from 1 took 5 min on .54.
                const size_t grow = size * 4 <= b.max ? size : size / 4;
                pr.to = std::min(cap, size + std::max(b.step, grow));
                pr.ref = recent;
                pr.refOther = recentOther;
                pr.refCompactions = recentCompactions;
                pr.noise = ReadsNoise(s.history);
                const double m = std::ceil(8 * std::pow(pr.noise / p.minGain, 2));
                pr.measure = std::clamp(size_t(m), p.minMeasureWindows,
                                        p.maxMeasureWindows);
                ps.regime = Regime::Probing;
                s.probing = id;
                act(d, ps, id, pr.from, pr.to, Reason::ProbeStart);
                break;
            }
        }
    }

    for (size_t i = 0; i < kPoolCount; i++) {
        auto& ps = s.pools[i];
        const auto& smp = w.pools[i];
        size_t size = smp.size;
        for (const auto& a : d.resize) {
            if (a.pool == static_cast<PoolId>(i)) {
                size = a.to;
            }
        }
        const double util = smp.size > 0 ? smp.workCores / smp.size : 0;
        const size_t spinners =
                util < p.spinUtil ? 0 : std::min(p.maxSpinners, (size + 7) / 8);
        if (spinners != ps.spinners) {
            ps.spinners = spinners;
            d.spin.push_back({static_cast<PoolId>(i), spinners});
        }
    }
    return d;
}

} // namespace kvserver
} // namespace magma
