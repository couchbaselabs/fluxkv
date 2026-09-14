#pragma once

#include "include/libmagma/slice.h"
#include <cstdint>
#include <cstring>
#include <string>

namespace magma {
namespace kvserver {

// Simple fixed-size metadata. No versioning, no leb128, no assertions.
// Just a plain POD struct that can be memcpy'd to/from a Slice.
#pragma pack(push, 1)
struct DocMeta {
    uint64_t seqno{0};
    uint64_t cas{0};
    uint32_t valueSize{0};
    uint32_t flags{0};
    uint32_t expiry{0};
    uint8_t deleted{0}; // 0=alive, 1=deleted
    uint8_t datatype{0}; // mcbp datatype bits as received (e.g. Snappy=0x02); echoed on GET

    std::string encode() const {
        return std::string(reinterpret_cast<const char*>(this),
                           sizeof(DocMeta));
    }

    static DocMeta decode(const Slice& s) {
        DocMeta m;
        if (s.Len() >= sizeof(DocMeta)) {
            memcpy(&m, s.Data(), sizeof(DocMeta));
        }
        return m;
    }
};
#pragma pack(pop)

// Magma callbacks — decode via DocMeta::decode for safety
inline uint64_t MetaGetSeqNum(const Slice& meta) {
    return DocMeta::decode(meta).seqno;
}

inline size_t MetaGetValueSize(const Slice& meta) {
    return DocMeta::decode(meta).valueSize;
}

inline bool MetaIsTombstone(const Slice& meta) {
    return DocMeta::decode(meta).deleted != 0;
}

inline std::chrono::seconds MetaGetHistoryTimestamp(const Slice& meta) {
    return std::chrono::seconds(DocMeta::decode(meta).cas);
}

} // namespace kvserver
} // namespace magma
