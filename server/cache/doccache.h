#pragma once

// Pluggable document cache in front of magma.
//
// The engine talks only to this interface; policies live in their own files
// (s3fifo_cache.h, ...) and are selected by name through CreateDocCache().
//
// Contract every implementation must honour - the engine's consistency
// argument depends on it:
//
//  * Put() is the write-through path. It replaces any existing entry and pins
//    the new one: an entry with pending > 0 MUST NOT be evicted. Each Put is
//    matched by exactly one MarkPersisted() from the writer thread once magma
//    has applied the write, which unpins it. Overwriting a pinned entry carries
//    its outstanding count forward.
//  * PutIfAbsent() is the read-miss fill. It MUST NOT replace an existing
//    entry. Because a key with a queued write is always resident, a fill can
//    only land for keys whose disk version is current - this is what closes
//    the stale-fill race between a queued write and a concurrent disk read.
//  * A Put with deleted=true installs a tombstone; Get reports it as a hit
//    with deleted set so DELETE-then-GET answers KeyNotFound without a disk
//    read.
//  * Get() runs the callback while the entry is guaranteed alive; the value
//    view is invalid after the callback returns.
//  * Budgets are in bytes of real memory (header + key + value + index slot),
//    not item counts.

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace magma {
namespace kvserver {

// What a hit hands back. value points into the cache and is valid only for
// the duration of the Get callback.
struct CachedDoc {
    uint64_t seqno{0};
    uint32_t flags{0};
    uint32_t expiry{0};
    uint8_t datatype{0};
    bool deleted{false};
    std::string_view value;
};

struct DocCacheStats {
    uint64_t hits{0};
    uint64_t misses{0};
    uint64_t tombstoneHits{0};
    uint64_t puts{0}; // write-through inserts (SET/DELETE)
    uint64_t fills{0}; // read-miss inserts that actually landed
    uint64_t fillsRejected{0}; // PutIfAbsent found the key present
    uint64_t evictions{0};
    uint64_t ghostHits{0}; // policy-specific; 0 if not applicable
    uint64_t bytes{0};
    uint64_t items{0};
    uint64_t pendingItems{0}; // items pinned by an unapplied write
    uint64_t maxBytes{0};
};

class DocCache {
public:
    virtual ~DocCache() = default;

    // Runs fn under whatever protection keeps the entry alive if the key is
    // cached (including tombstones) and returns true; false on a miss.
    virtual bool Get(uint16_t vbid,
                     std::string_view key,
                     const std::function<void(const CachedDoc&)>& fn) = 0;

    // Copy-out variant of Get for the hot path. Get()'s callback is a
    // std::function through this interface - an indirect call that cannot be
    // inlined, and measured at ~30% of server CPU at high GET rates. Copying
    // the value into a caller-supplied buffer is cheaper for small values.
    //
    // Returns Miss, Tombstone (key deleted; `out` metadata is valid, no
    // value), or Hit with the value length written to *valueLen. A value
    // larger than bufCap reports TooLarge and the caller falls back to Get().
    enum class GetResult { Miss, Hit, Tombstone, TooLarge };

    // Hints that GetCopy(vbid, key) follows soon, so pipelined GETs overlap
    // their cache misses; no effect on results. PrefetchSlot starts the index
    // load and returns a token; PrefetchEntry(token), issued once that load
    // has had time to land, starts the entry load.
    virtual uint64_t PrefetchSlot(uint16_t vbid, std::string_view key) const {
        (void)vbid;
        (void)key;
        return 0;
    }
    virtual void PrefetchEntry(uint64_t token) const {
        (void)token;
    }
    virtual GetResult GetCopy(uint16_t vbid,
                              std::string_view key,
                              CachedDoc* out,
                              void* buf,
                              size_t bufCap,
                              size_t* valueLen) = 0;

    // Write-through insert; see the contract above.
    virtual void Put(uint16_t vbid,
                     std::string_view key,
                     std::string_view value,
                     uint32_t flags,
                     uint32_t expiry,
                     uint8_t datatype,
                     bool deleted) = 0;

    // Read-fill; inserts only when the key is absent. Never pins.
    virtual void PutIfAbsent(uint16_t vbid,
                             std::string_view key,
                             std::string_view value,
                             uint64_t seqno,
                             uint32_t flags,
                             uint32_t expiry,
                             uint8_t datatype) = 0;

    // Writer-thread acknowledgement of one Put. Records the seqno magma
    // assigned (if non-zero) and releases one pin.
    virtual void MarkPersisted(uint16_t vbid,
                               std::string_view key,
                               uint64_t seqno) = 0;

    // Drops the entry outright. Used when a write that was Put is then
    // refused by the write queue, so the cache does not hold a version the
    // store will never see.
    virtual void Erase(uint16_t vbid, std::string_view key) = 0;

    virtual DocCacheStats GetStats() const = 0;
    virtual const char* Policy() const = 0;
};

// Factory. policy: "s3fifo" (default). Returns null for an unknown policy and
// puts the reason in *error.
std::unique_ptr<DocCache> CreateDocCache(const std::string& policy,
                                         size_t maxBytes,
                                         size_t numShards,
                                         std::string* error);

} // namespace kvserver
} // namespace magma
