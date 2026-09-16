#include "statslot.h"

#include <array>
#include <mutex>

namespace magma {
namespace kvserver {

namespace {
std::mutex gSlotMu;
std::array<unsigned, kStatSlots> gSlotUsers{};

struct SlotLease {
    size_t slot;
    SlotLease() {
        std::lock_guard<std::mutex> g(gSlotMu);
        slot = 0;
        for (size_t i = 1; i < kStatSlots; i++) {
            if (gSlotUsers[i] < gSlotUsers[slot]) {
                slot = i;
            }
        }
        gSlotUsers[slot]++;
    }
    ~SlotLease() {
        std::lock_guard<std::mutex> g(gSlotMu);
        gSlotUsers[slot]--;
    }
};
} // namespace

size_t statSlot() {
    thread_local const SlotLease lease;
    return lease.slot;
}

} // namespace kvserver
} // namespace magma
