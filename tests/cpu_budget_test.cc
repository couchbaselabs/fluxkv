// Unit tests for CpuBudget on sysfs fixtures shaped like the lab boxes.
// Plain asserts, as cache_test.

#include "cpu_budget.h"

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <unistd.h>

using namespace magma::kvserver;
namespace fs = std::filesystem;

#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__,      \
                         __LINE__, #cond);                                   \
            std::exit(1);                                                    \
        }                                                                    \
    } while (0)

namespace {

// cpuN/topology/thread_siblings_list for cpus [0, n); with smt, cpu i and
// i + n/2 are siblings (x86 numbering).
fs::path makeSys(const std::string& name, unsigned n, bool smt) {
    auto root = fs::temp_directory_path() /
                ("fx_cpu_budget_" + name + "_" + std::to_string(getpid()));
    fs::remove_all(root);
    for (unsigned cpu = 0; cpu < n; cpu++) {
        auto dir = root / ("cpu" + std::to_string(cpu)) / "topology";
        fs::create_directories(dir);
        std::ofstream out(dir / "thread_siblings_list");
        if (smt) {
            const unsigned first = cpu % (n / 2);
            out << first << "," << first + n / 2 << "\n";
        } else {
            out << cpu << "\n";
        }
    }
    return root;
}

std::vector<unsigned> range(unsigned from, unsigned to) {
    std::vector<unsigned> v;
    for (unsigned i = from; i < to; i++) {
        v.push_back(i);
    }
    return v;
}

void x86Box() {
    auto sys = makeSys("x86", 80, true);
    auto b = ReadCpuBudget(range(0, 80), sys, "");
    CHECK(b.usable == 80);
    CHECK(b.physicalCores == 40);
    CHECK(b.smtWidth == 2);
    CHECK(b.cgroup == "none");

    // Pinned to one sibling of each core: no SMT left to share.
    b = ReadCpuBudget(range(0, 40), sys, "");
    CHECK(b.usable == 40);
    CHECK(b.physicalCores == 40);
    CHECK(b.smtWidth == 1);
    fs::remove_all(sys);
}

void gravitonBox() {
    auto sys = makeSys("i8g", 48, false);
    auto b = ReadCpuBudget(range(0, 48), sys, "max 100000");
    CHECK(b.usable == 48);
    CHECK(b.physicalCores == 48);
    CHECK(b.smtWidth == 1);
    CHECK(b.cgroup == "none");
    fs::remove_all(sys);
}

void cgroupQuotaCaps() {
    auto sys = makeSys("quota", 80, true);
    auto b = ReadCpuBudget(range(0, 80), sys, "1250000 100000");
    CHECK(b.usable == 13); // 12.5 cores rounds up
    CHECK(b.physicalCores == 40);
    CHECK(b.cgroup == "1250000 100000");

    // A quota above the affinity is no cap.
    b = ReadCpuBudget(range(0, 8), sys, "4000000 100000");
    CHECK(b.usable == 8);
    fs::remove_all(sys);
}

void missingSysfs() {
    auto b = ReadCpuBudget(range(0, 4), "/nonexistent", "garbage");
    CHECK(b.usable == 4);
    CHECK(b.physicalCores == 4);
    CHECK(b.smtWidth == 1);
}

void liveProcess() {
    auto b = ReadCpuBudget();
    CHECK(b.usable >= 1);
    CHECK(b.physicalCores >= 1);
    CHECK(b.smtWidth >= 1);
    std::printf("live: usable=%u cores=%u smt=%u cgroup=%s\n",
                b.usable,
                b.physicalCores,
                b.smtWidth,
                b.cgroup.c_str());
}

} // namespace

int main() {
    x86Box();
    gravitonBox();
    cgroupQuotaCaps();
    missingSysfs();
    liveProcess();
    std::printf("cpu_budget_test: OK\n");
    return 0;
}
