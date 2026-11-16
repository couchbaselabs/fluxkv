#include "magma_reader_port.h"

#include "util/io/reader_pool.h"

#include <algorithm>

namespace magma {
namespace kvserver {

namespace {
uint64_t since(uint64_t now, uint64_t before) {
    return now >= before ? now - before : 0;
}
} // namespace

size_t MagmaReaderPort::Size() const {
    return Magma::GetNumThreads(Magma::ThreadType::Reader);
}

void MagmaReaderPort::Grow(size_t n) {
    Magma::SetNumThreads(Magma::ThreadType::Reader, Size() + n);
}

void MagmaReaderPort::Shrink(size_t n) {
    const size_t size = Size();
    Magma::SetNumThreads(Magma::ThreadType::Reader, size - std::min(n, size));
}

PoolSample MagmaReaderPort::Sample(double wallSec) {
    const auto stats = Magma::GetReaderPoolStats();
    if (last_.size() < stats.size()) {
        last_.resize(stats.size());
    }
    PoolSample s;
    s.kind = BusyKind::ThreadCpu;
    s.size = stats.size();
    s.depthCap = ReaderPool::MaxInFlightPerThread;
    uint64_t workNs = 0, jobs = 0, waitNs = 0, inFlight = 0;
    for (size_t id = 0; id < stats.size(); id++) {
        const auto& now = stats[id];
        auto& before = last_[id];
        const uint64_t cpu = since(now.CpuNs, before.CpuNs);
        const uint64_t work = since(cpu, since(now.SpinNs, before.SpinNs));
        workNs += work;
        jobs += since(now.Jobs, before.Jobs);
        waitNs += since(now.QueueWaitNs, before.QueueWaitNs);
        inFlight += now.InFlight;
        if (wallSec > 0) {
            s.busyMax = std::max(s.busyMax, work / (wallSec * 1e9));
        }
        before = now;
    }
    if (wallSec > 0) {
        s.workCores = workNs / (wallSec * 1e9);
    }
    if (s.size > 0) {
        s.busyMean = s.workCores / s.size;
        s.inFlightPerThread = double(inFlight) / s.size;
    }
    s.queueWaitUs = jobs > 0 ? waitNs / 1e3 / jobs : 0;
    s.waitUs = s.queueWaitUs;
    return s;
}

void MagmaReaderPort::SetIdleSpin(size_t spinners) {
    Magma::SetReaderPoolSpinners(spinners);
}

} // namespace kvserver
} // namespace magma
