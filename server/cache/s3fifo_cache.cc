#include "s3fifo_cache.h"
#include "statslot.h"

#include <folly/SharedMutex.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/hash/SpookyHashV2.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <deque>
#include <mutex>
#include <new>
#include <shared_mutex>
#include <string>

namespace magma {
namespace kvserver {

namespace {
// F14FastMap slot + the pointer to the entry; charged so the byte budget
// reflects memory actually consumed rather than payload alone.
constexpr size_t kMapSlotOverhead = 24;
// S3-FIFO: the small queue is 10% of the budget.
constexpr size_t kSmallPercent = 10;
constexpr uint8_t kMaxFreq = 3;
enum Queue : uint8_t { kSmall = 0, kMain = 1 };

// Read-path counters, one cache-line-aligned slot per thread. A single atomic
// per shard is still a contended RMW on every lookup; giving each thread its
// own slot keeps the line local. GetStats sums them.
struct alignas(64) ReadStatSlot {
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> misses{0};
    std::atomic<uint64_t> tombstoneHits{0};
    std::atomic<uint64_t> fillsRejected{0};
};

ReadStatSlot gReadStats[kStatSlots];

inline size_t readStatSlot() {
    return statSlot();
}
} // namespace

// One allocation holds the header, the map key (vbid || key) and the value.
struct S3FifoCache::Entry {
    Entry* prev{nullptr};
    Entry* next{nullptr};
    std::atomic<uint64_t> seqno{0};
    uint32_t flags{0};
    uint32_t expiry{0};
    uint32_t keyLen{0};
    uint32_t valLen{0};
    // Writes queued to magma but not yet applied. Non-zero pins the entry.
    std::atomic<uint32_t> pending{0};
    uint16_t vbid{0};
    uint8_t datatype{0};
    uint8_t deleted{0};
    std::atomic<uint8_t> freq{0};
    uint8_t queue{kSmall};

    char* data() {
        return reinterpret_cast<char*>(this + 1);
    }
    const char* data() const {
        return reinterpret_cast<const char*>(this + 1);
    }
    // The map is keyed by vbid || key so one shard can hold the same key for
    // different vbuckets.
    std::string_view mapKey() const {
        return {data(), sizeof(uint16_t) + keyLen};
    }
    std::string_view value() const {
        return {data() + sizeof(uint16_t) + keyLen, valLen};
    }
    size_t charge() const {
        return sizeof(Entry) + sizeof(uint16_t) + keyLen + valLen +
               kMapSlotOverhead;
    }

    static Entry* create(uint16_t vbid,
                         std::string_view key,
                         std::string_view value,
                         uint64_t seqno,
                         uint32_t flags,
                         uint32_t expiry,
                         uint8_t datatype,
                         bool deleted) {
        void* mem = ::operator new(sizeof(Entry) + sizeof(uint16_t) +
                                   key.size() + value.size());
        auto* e = new (mem) Entry();
        e->seqno.store(seqno, std::memory_order_relaxed);
        e->flags = flags;
        e->expiry = expiry;
        e->keyLen = static_cast<uint32_t>(key.size());
        e->valLen = static_cast<uint32_t>(value.size());
        e->vbid = vbid;
        e->datatype = datatype;
        e->deleted = deleted ? 1 : 0;
        std::memcpy(e->data(), &vbid, sizeof(uint16_t));
        std::memcpy(e->data() + sizeof(uint16_t), key.data(), key.size());
        std::memcpy(e->data() + sizeof(uint16_t) + key.size(),
                    value.data(),
                    value.size());
        return e;
    }
    static void destroy(Entry* e) {
        e->~Entry();
        ::operator delete(static_cast<void*>(e));
    }
};

namespace {
// Intrusive FIFO: insert at head, evict from tail, O(1) unlink of any entry
// (needed when a Put replaces or a fill is superseded).
struct Fifo {
    S3FifoCache::Entry* head{nullptr};
    S3FifoCache::Entry* tail{nullptr};
    size_t bytes{0};
    size_t count{0};

    void pushHead(S3FifoCache::Entry* e) {
        e->prev = nullptr;
        e->next = head;
        if (head) {
            head->prev = e;
        } else {
            tail = e;
        }
        head = e;
        bytes += e->charge();
        count++;
    }
    S3FifoCache::Entry* popTail() {
        auto* e = tail;
        if (e) {
            remove(e);
        }
        return e;
    }
    void remove(S3FifoCache::Entry* e) {
        if (e->prev) {
            e->prev->next = e->next;
        } else {
            head = e->next;
        }
        if (e->next) {
            e->next->prev = e->prev;
        } else {
            tail = e->prev;
        }
        e->prev = e->next = nullptr;
        bytes -= e->charge();
        count--;
    }
};
} // namespace

struct S3FifoCache::Shard {
    // Shared for Get/MarkPersisted (atomic field updates only), exclusive for
    // anything that touches the map or the lists.
    mutable folly::SharedMutex mu;
    folly::F14FastMap<std::string_view, Entry*> map;
    Fifo small;
    Fifo main;
    // Ghost queue of key hashes evicted from small. Bounded to main's item
    // count (the paper's sizing) with a floor so a cold cache still learns.
    std::deque<uint64_t> ghost;
    folly::F14FastSet<uint64_t> ghostSet;

    size_t budget{0};
    size_t smallBudget{0};

    // Under the exclusive lock.
    uint64_t puts{0};
    uint64_t fills{0};
    uint64_t evictions{0};
    uint64_t ghostHits{0};
    // Touched under the shared lock, hence atomic.
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> misses{0};
    std::atomic<uint64_t> tombstoneHits{0};
    std::atomic<uint64_t> fillsRejected{0};
    std::atomic<uint64_t> pendingItems{0};

    size_t bytes() const {
        return small.bytes + main.bytes;
    }

    void ghostAdd(uint64_t h) {
        if (!ghostSet.insert(h).second) {
            return; // already remembered; keeps deque and set in step
        }
        ghost.push_back(h);
        size_t cap = std::max<size_t>(1024, main.count);
        while (ghost.size() > cap) {
            ghostSet.erase(ghost.front());
            ghost.pop_front();
        }
    }

    void unlink(Entry* e) {
        (e->queue == kSmall ? small : main).remove(e);
    }

    void insert(Entry* e, uint64_t h) {
        if (ghostSet.count(h)) {
            e->queue = kMain;
            main.pushHead(e);
            ghostHits++;
        } else {
            e->queue = kSmall;
            small.pushHead(e);
        }
        map.emplace(e->mapKey(), e);
    }

    void free(Entry* e) {
        map.erase(e->mapKey());
        if (e->pending.load(std::memory_order_relaxed) > 0) {
            pendingItems.fetch_sub(1, std::memory_order_relaxed);
        }
        Entry::destroy(e);
    }

    // S3-FIFO eviction. Pinned entries (pending writes) are treated like a
    // hit: moved along rather than freed, so the cache can sit over budget
    // by at most the bytes of unapplied writes.
    void evict() {
        // Every pass over main lowers each survivor's freq by one, so a few
        // passes always reach a victim unless everything is pinned. The guard
        // bounds the work in that case instead of spinning.
        size_t guard = (kMaxFreq + 1) * (small.count + main.count) + 1;
        while (bytes() > budget && guard-- > 0) {
            if (small.bytes > smallBudget && small.tail) {
                evictSmall();
            } else if (main.tail) {
                evictMain();
            } else if (small.tail) {
                evictSmall();
            } else {
                break;
            }
        }
    }

    void evictSmall() {
        auto* e = small.popTail();
        bool pinned = e->pending.load(std::memory_order_relaxed) > 0;
        if (pinned || e->freq.load(std::memory_order_relaxed) > 1) {
            e->freq.store(0, std::memory_order_relaxed);
            e->queue = kMain;
            main.pushHead(e);
            return;
        }
        ghostAdd(hashKey(e->vbid,
                         std::string_view(e->data() + sizeof(uint16_t),
                                          e->keyLen)));
        evictions++;
        free(e);
    }

    void evictMain() {
        auto* e = main.popTail();
        bool pinned = e->pending.load(std::memory_order_relaxed) > 0;
        uint8_t f = e->freq.load(std::memory_order_relaxed);
        if (pinned || f > 0) {
            if (f > 0) {
                e->freq.store(f - 1, std::memory_order_relaxed);
            }
            main.pushHead(e);
            return;
        }
        evictions++;
        free(e);
    }
};

S3FifoCache::S3FifoCache(size_t maxBytes, size_t numShards) : maxBytes_(maxBytes) {
    numShards_ = 1;
    while (numShards_ < std::max<size_t>(1, numShards)) {
        numShards_ <<= 1;
    }
    shardMask_ = numShards_ - 1;
    shards_ = std::make_unique<Shard[]>(numShards_);
    size_t perShard = maxBytes / numShards_;
    for (size_t i = 0; i < numShards_; i++) {
        shards_[i].budget = perShard;
        shards_[i].smallBudget = perShard * kSmallPercent / 100;
    }
}

S3FifoCache::~S3FifoCache() {
    for (size_t i = 0; i < numShards_; i++) {
        auto& s = shards_[i];
        for (Fifo* q : {&s.small, &s.main}) {
            while (auto* e = q->popTail()) {
                Entry::destroy(e);
            }
        }
        s.map.clear();
    }
}

uint64_t S3FifoCache::hashKey(uint16_t vbid, std::string_view key) {
    return folly::hash::SpookyHashV2::Hash64(key.data(), key.size(), vbid);
}

S3FifoCache::Shard& S3FifoCache::shardFor(uint64_t hash) const {
    // The map hashes the low bits itself; pick the shard from the high ones.
    return shards_[(hash >> 32) & shardMask_];
}

namespace {
// Builds the lookup key without copying the user key: vbid || key, on stack
// for typical key sizes.
struct MapKeyBuf {
    char inlineBuf[256];
    std::string heap;
    std::string_view view;

    MapKeyBuf(uint16_t vbid, std::string_view key) {
        size_t n = sizeof(uint16_t) + key.size();
        char* p = inlineBuf;
        if (n > sizeof(inlineBuf)) {
            heap.resize(n);
            p = heap.data();
        }
        std::memcpy(p, &vbid, sizeof(uint16_t));
        std::memcpy(p + sizeof(uint16_t), key.data(), key.size());
        view = {p, n};
    }
};
} // namespace

bool S3FifoCache::Get(uint16_t vbid,
                  std::string_view key,
                  const std::function<void(const CachedDoc&)>& fn) {
    uint64_t h = hashKey(vbid, key);
    auto& s = shardFor(h);
    MapKeyBuf mk(vbid, key);

    std::shared_lock lock(s.mu);
    auto it = s.map.find(mk.view);
    if (it == s.map.end()) {
        gReadStats[readStatSlot()].misses.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    Entry* e = it->second;
    // Racy increments are fine: losing one is a rounding error in the
    // policy, and it saves a locked RMW on every hit.
    uint8_t f = e->freq.load(std::memory_order_relaxed);
    if (f < kMaxFreq) {
        e->freq.store(f + 1, std::memory_order_relaxed);
    }
    CachedDoc doc;
    doc.seqno = e->seqno.load(std::memory_order_relaxed);
    doc.flags = e->flags;
    doc.expiry = e->expiry;
    doc.datatype = e->datatype;
    doc.deleted = e->deleted != 0;
    doc.value = e->value();
    if (doc.deleted) {
        gReadStats[readStatSlot()].tombstoneHits.fetch_add(1, std::memory_order_relaxed);
    } else {
        gReadStats[readStatSlot()].hits.fetch_add(1, std::memory_order_relaxed);
    }
    fn(doc);
    return true;
}

DocCache::GetResult S3FifoCache::GetCopy(uint16_t vbid,
                                         std::string_view key,
                                         CachedDoc* out,
                                         void* buf,
                                         size_t bufCap,
                                         size_t* valueLen) {
    const uint64_t h = hashKey(vbid, key);
    auto& s = shardFor(h);
    MapKeyBuf mk(vbid, key);

    std::shared_lock lock(s.mu);
    auto it = s.map.find(mk.view);
    if (it == s.map.end()) {
        gReadStats[readStatSlot()].misses.fetch_add(1, std::memory_order_relaxed);
        return GetResult::Miss;
    }
    Entry* e = it->second;
    const uint8_t f = e->freq.load(std::memory_order_relaxed);
    if (f < kMaxFreq) {
        e->freq.store(f + 1, std::memory_order_relaxed);
    }
    out->seqno = e->seqno.load(std::memory_order_relaxed);
    out->flags = e->flags;
    out->expiry = e->expiry;
    out->datatype = e->datatype;
    out->deleted = e->deleted != 0;
    if (out->deleted) {
        gReadStats[readStatSlot()].tombstoneHits.fetch_add(1, std::memory_order_relaxed);
        *valueLen = 0;
        return GetResult::Tombstone;
    }
    if (e->valLen > bufCap) {
        // Caller's buffer is too small; it must fall back to Get().
        return GetResult::TooLarge;
    }
    std::memcpy(buf, e->value().data(), e->valLen);
    *valueLen = e->valLen;
    gReadStats[readStatSlot()].hits.fetch_add(1, std::memory_order_relaxed);
    return GetResult::Hit;
}

void S3FifoCache::Put(uint16_t vbid,
                  std::string_view key,
                  std::string_view value,
                  uint32_t flags,
                  uint32_t expiry,
                  uint8_t datatype,
                  bool deleted) {
    uint64_t h = hashKey(vbid, key);
    auto& s = shardFor(h);
    Entry* e = Entry::create(
            vbid, key, value, 0, flags, expiry, datatype, deleted);

    std::unique_lock lock(s.mu);
    uint32_t carry = 0;
    auto it = s.map.find(e->mapKey());
    if (it != s.map.end()) {
        Entry* old = it->second;
        // Writes queued for the old version are still ahead of ours in the
        // writer queue; each will call MarkPersisted, so carry them over.
        carry = old->pending.load(std::memory_order_relaxed);
        s.unlink(old);
        s.map.erase(it);
        Entry::destroy(old);
    }
    e->pending.store(carry + 1, std::memory_order_relaxed);
    if (carry == 0) {
        s.pendingItems.fetch_add(1, std::memory_order_relaxed);
    }
    s.insert(e, h);
    s.puts++;
    s.evict();
}

void S3FifoCache::PutIfAbsent(uint16_t vbid,
                          std::string_view key,
                          std::string_view value,
                          uint64_t seqno,
                          uint32_t flags,
                          uint32_t expiry,
                          uint8_t datatype) {
    uint64_t h = hashKey(vbid, key);
    auto& s = shardFor(h);
    MapKeyBuf mk(vbid, key);
    {
        // Cheap reject under the shared lock; the common case after a burst
        // of misses on the same key is that the first fill already landed.
        std::shared_lock lock(s.mu);
        if (s.map.find(mk.view) != s.map.end()) {
            gReadStats[readStatSlot()].fillsRejected.fetch_add(1, std::memory_order_relaxed);
            return;
        }
    }
    Entry* e = Entry::create(
            vbid, key, value, seqno, flags, expiry, datatype, false);
    std::unique_lock lock(s.mu);
    if (s.map.find(e->mapKey()) != s.map.end()) {
        gReadStats[readStatSlot()].fillsRejected.fetch_add(1, std::memory_order_relaxed);
        Entry::destroy(e);
        return;
    }
    s.insert(e, h);
    s.fills++;
    s.evict();
}

void S3FifoCache::MarkPersisted(uint16_t vbid,
                            std::string_view key,
                            uint64_t seqno) {
    uint64_t h = hashKey(vbid, key);
    auto& s = shardFor(h);
    MapKeyBuf mk(vbid, key);
    bool overBudget = false;
    {
        std::shared_lock lock(s.mu);
        auto it = s.map.find(mk.view);
        if (it == s.map.end()) {
            return; // replaced and freed already; nothing left to unpin
        }
        Entry* e = it->second;
        if (seqno != 0) {
            e->seqno.store(seqno, std::memory_order_relaxed);
        }
        if (e->pending.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            s.pendingItems.fetch_sub(1, std::memory_order_relaxed);
            overBudget = s.bytes() > s.budget;
        }
    }
    if (overBudget) {
        // Pinned entries may have pushed the shard past its budget; now that
        // one is released, trim.
        std::unique_lock lock(s.mu);
        s.evict();
    }
}

void S3FifoCache::Erase(uint16_t vbid, std::string_view key) {
    uint64_t h = hashKey(vbid, key);
    auto& s = shardFor(h);
    MapKeyBuf mk(vbid, key);
    std::unique_lock lock(s.mu);
    auto it = s.map.find(mk.view);
    if (it == s.map.end()) {
        return;
    }
    Entry* e = it->second;
    s.unlink(e);
    s.free(e);
}

DocCacheStats S3FifoCache::GetStats() const {
    DocCacheStats st;
    st.maxBytes = maxBytes_;
    for (const auto& slot : gReadStats) {
        st.hits += slot.hits.load(std::memory_order_relaxed);
        st.misses += slot.misses.load(std::memory_order_relaxed);
        st.tombstoneHits += slot.tombstoneHits.load(std::memory_order_relaxed);
        st.fillsRejected += slot.fillsRejected.load(std::memory_order_relaxed);
    }
    for (size_t i = 0; i < numShards_; i++) {
        auto& s = shards_[i];
        st.pendingItems += s.pendingItems.load(std::memory_order_relaxed);
        std::shared_lock lock(s.mu);
        st.puts += s.puts;
        st.fills += s.fills;
        st.evictions += s.evictions;
        st.ghostHits += s.ghostHits;
        st.bytes += s.bytes();
        st.items += s.small.count + s.main.count;
    }
    return st;
}

} // namespace kvserver
} // namespace magma
