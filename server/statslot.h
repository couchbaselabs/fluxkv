#pragma once

#include <cstddef>

namespace magma {
namespace kvserver {

// Per-thread slots for sharded counters. A thread keeps its slot for life
// and gives it back on exit; a new thread takes the least-occupied slot. With
// pools that grow and shrink at run time, threads come and go, and a simple
// round-robin assignment soon has two busy threads bouncing the same cache
// line - the contention the sharding exists to avoid.
inline constexpr size_t kStatSlots = 128;

size_t statSlot();

} // namespace kvserver
} // namespace magma
