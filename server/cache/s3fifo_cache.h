#pragma once

// S3-FIFO document cache (Yang et al., "FIFO queues are all you need for
// cache eviction", SOSP 2023).
//
// A small FIFO absorbs new inserts, a main FIFO holds what survived it, and a
// ghost FIFO of hashes remembers what the small queue evicted so a returning
// key skips straight to main. A hit only bumps a 2-bit frequency counter - it
// never moves a list node - so GETs run under a shared lock and the hot path
// has no list surgery at all. The policy is scan resistant (a cold scan can
// only churn the small queue) and in the paper's traces beats LRU, ARC and
// TinyLFU at a fraction of the per-hit cost.
//
// Keys are hashed to one of numShards independent shards, each with its own
// lock, queues and share of the byte budget.

#include "doccache.h"

#include <memory>

namespace magma {
namespace kvserver {

class S3FifoCache : public DocCache {
public:
    // maxBytes is split evenly across numShards; numShards is rounded up to
    // a power of two.
    S3FifoCache(size_t maxBytes, size_t numShards = 256);
    ~S3FifoCache() override;

    S3FifoCache(const S3FifoCache&) = delete;
    S3FifoCache& operator=(const S3FifoCache&) = delete;

    bool Get(uint16_t vbid,
             std::string_view key,
             const std::function<void(const CachedDoc&)>& fn) override;
    uint64_t PrefetchSlot(uint16_t vbid, std::string_view key) const override;
    void PrefetchEntry(uint64_t token) const override;
    GetResult GetCopy(uint16_t vbid,
                      std::string_view key,
                      CachedDoc* out,
                      void* buf,
                      size_t bufCap,
                      size_t* valueLen) override;
    void Put(uint16_t vbid,
             std::string_view key,
             std::string_view value,
             uint32_t flags,
             uint32_t expiry,
             uint8_t datatype,
             bool deleted) override;
    void PutIfAbsent(uint16_t vbid,
                     std::string_view key,
                     std::string_view value,
                     uint64_t seqno,
                     uint32_t flags,
                     uint32_t expiry,
                     uint8_t datatype) override;
    void MarkPersisted(uint16_t vbid,
                       std::string_view key,
                       uint64_t seqno) override;
    void Erase(uint16_t vbid, std::string_view key) override;
    DocCacheStats GetStats() const override;
    const char* Policy() const override {
        return "s3fifo";
    }

    struct Entry;

private:
    struct Shard;

    Shard& shardFor(uint64_t hash) const;
    static uint64_t hashKey(uint16_t vbid, std::string_view key);

    size_t maxBytes_;
    size_t numShards_;
    size_t shardMask_;
    std::unique_ptr<Shard[]> shards_;
};

} // namespace kvserver
} // namespace magma
