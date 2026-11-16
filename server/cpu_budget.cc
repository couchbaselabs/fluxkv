#include "cpu_budget.h"

#include <sched.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <set>
#include <sstream>

namespace magma {
namespace kvserver {

namespace {

std::string readFirstLine(const std::string& path) {
    std::ifstream in(path);
    std::string line;
    std::getline(in, line);
    return line;
}

// Cores from cpu.max ("<quota> <period>" or "max <period>"); 0 when unlimited.
double quotaCores(const std::string& cpuMax) {
    std::istringstream in(cpuMax);
    std::string quota;
    double period = 0;
    if (!(in >> quota >> period) || quota == "max" || period <= 0) {
        return 0;
    }
    try {
        return std::stod(quota) / period;
    } catch (...) {
        return 0;
    }
}

// The cgroup v2 cpu.max governing this process, walking up to the root:
// a parent's quota binds the children too, so the smallest one wins.
std::string liveCpuMax() {
    std::ifstream in("/proc/self/cgroup");
    std::string line, path;
    while (std::getline(in, line)) {
        if (line.rfind("0::", 0) == 0) {
            path = line.substr(3);
        }
    }
    if (path.empty()) {
        return "";
    }
    std::string best;
    double bestCores = 0;
    while (true) {
        const auto cpuMax = readFirstLine("/sys/fs/cgroup" + path + "/cpu.max");
        const double cores = quotaCores(cpuMax);
        if (cores > 0 && (bestCores == 0 || cores < bestCores)) {
            best = cpuMax;
            bestCores = cores;
        }
        if (path == "/" || path.empty()) {
            break;
        }
        const auto slash = path.find_last_of('/');
        path = slash == 0 ? "/" : path.substr(0, slash);
    }
    return best;
}

} // namespace

CpuBudget ReadCpuBudget(const std::vector<unsigned>& affinity,
                        const std::string& sysCpuDir,
                        const std::string& cpuMax) {
    CpuBudget b;
    const unsigned cpus = std::max<size_t>(1, affinity.size());

    // A CPU whose siblings file is missing counts as its own core.
    std::set<std::string> cores;
    for (auto cpu : affinity) {
        auto siblings = readFirstLine(sysCpuDir + "/cpu" + std::to_string(cpu) +
                                      "/topology/thread_siblings_list");
        cores.insert(siblings.empty() ? "cpu" + std::to_string(cpu) : siblings);
    }
    b.physicalCores = std::max<size_t>(1, cores.size());
    b.smtWidth = std::max<unsigned>(
            1, unsigned(std::lround(double(cpus) / b.physicalCores)));

    b.usable = cpus;
    const double quota = quotaCores(cpuMax);
    if (quota > 0) {
        b.cgroup = cpuMax;
        b.usable = std::clamp<unsigned>(unsigned(std::ceil(quota)), 1, cpus);
    }
    return b;
}

CpuBudget ReadCpuBudget() {
    std::vector<unsigned> affinity;
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == 0) {
        for (unsigned cpu = 0; cpu < CPU_SETSIZE; cpu++) {
            if (CPU_ISSET(cpu, &set)) {
                affinity.push_back(cpu);
            }
        }
    }
    return ReadCpuBudget(affinity, "/sys/devices/system/cpu", liveCpuMax());
}

} // namespace kvserver
} // namespace magma
