#pragma once

#include <string>
#include <vector>

namespace magma {
namespace kvserver {

// The CPUs this process may use. Thread pool bounds derive from it, so a
// box with SMT, a narrowed affinity mask or a cgroup quota is sized for
// what it really has.
struct CpuBudget {
    // |sched_getaffinity| capped by the cgroup v2 cpu.max quota.
    unsigned usable{1};
    // Distinct thread_siblings_list sets among the affinity CPUs.
    unsigned physicalCores{1};
    // usable CPUs per core, rounded; 1 without SMT.
    unsigned smtWidth{1};
    // cpu.max as read ("400000 100000"), or "none".
    std::string cgroup{"none"};
};

// Reads the live process: affinity, /sys and /proc/self/cgroup.
CpuBudget ReadCpuBudget();

// The same from given sources, for tests. sysCpuDir holds cpuN/topology/
// thread_siblings_list; cpuMax is the cgroup's cpu.max contents, empty when
// there is none.
CpuBudget ReadCpuBudget(const std::vector<unsigned>& affinity,
                        const std::string& sysCpuDir,
                        const std::string& cpuMax);

} // namespace kvserver
} // namespace magma
