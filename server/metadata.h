#pragma once

#include "include/libmagma/slice.h"
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>

namespace magma {
namespace kvserver {

// Document metadata. In memory it is this packed struct; on disk it takes one
// of two forms, told apart by length:
//  legacy   exactly kLegacySize bytes: the struct as-is.
//  compact  [hdr][seqno varint][cas 8 B][valueSize varint]
//           [flags 4 B][expiry 4 B][datatype 1 B], the last three only when
//           their hdr bit is set. hdr: bit0 flags, bit1 expiry, bit2 deleted,
//           bit3 datatype, bits 5-7 version (1). A typical document takes
//           ~14 bytes instead of 30.
// The compact encoder never produces kLegacySize bytes (it pads one byte
// instead), and readers ignore bytes past the fields they know, which leaves
// room for later optional fields.
#pragma pack(push, 1)
struct DocMeta {
    uint64_t seqno{0};
    uint64_t cas{0};
    uint32_t valueSize{0};
    uint32_t flags{0};
    uint32_t expiry{0};
    uint8_t deleted{0}; // 0=alive, 1=deleted
    uint8_t datatype{0}; // mcbp datatype bits as received (e.g. Snappy=0x02); echoed on GET

    static constexpr size_t kLegacySize = 30;
    // Header, two 10-byte varints, cas, flags, expiry, datatype and the pad.
    static constexpr size_t kMaxCompactSize = 1 + 10 + 8 + 10 + 4 + 4 + 1 + 1;

    std::string encode() const {
        return std::string(reinterpret_cast<const char*>(this),
                           sizeof(DocMeta));
    }

    // Writes the compact form to out (kMaxCompactSize bytes); returns its
    // length.
    size_t encodeCompact(char* out) const;

    // Either form; false for a meta too short or of an unknown version.
    static bool decode(const Slice& s, DocMeta* out);

    // For callers that cannot fail: an undecodable meta reads as zeros, as a
    // short legacy meta always did.
    static DocMeta decode(const Slice& s) {
        DocMeta m;
        if (!decode(s, &m)) {
            m = DocMeta{};
        }
        return m;
    }
};
#pragma pack(pop)
static_assert(sizeof(DocMeta) == DocMeta::kLegacySize,
              "legacy metas on disk are this struct's bytes");

namespace detail {
constexpr uint8_t kMetaHasFlags = 1 << 0;
constexpr uint8_t kMetaHasExpiry = 1 << 1;
constexpr uint8_t kMetaDeleted = 1 << 2;
constexpr uint8_t kMetaHasDatatype = 1 << 3;
constexpr uint8_t kMetaVersionShift = 5;
constexpr uint8_t kMetaVersion = 1;

inline size_t putVarint(char* p, uint64_t v) {
    size_t n = 0;
    while (v >= 0x80) {
        p[n++] = static_cast<char>(v | 0x80);
        v >>= 7;
    }
    p[n++] = static_cast<char>(v);
    return n;
}

inline bool getVarint(const char*& p, const char* end, uint64_t* v) {
    uint64_t r = 0;
    for (unsigned shift = 0; shift < 64 && p < end; shift += 7) {
        const auto b = static_cast<uint8_t>(*p++);
        r |= uint64_t(b & 0x7f) << shift;
        if (!(b & 0x80)) {
            *v = r;
            return true;
        }
    }
    return false;
}

template <typename T>
inline bool getFixed(const char*& p, const char* end, T* v) {
    if (static_cast<size_t>(end - p) < sizeof(T)) {
        return false;
    }
    memcpy(v, p, sizeof(T));
    p += sizeof(T);
    return true;
}
} // namespace detail

inline size_t DocMeta::encodeCompact(char* out) const {
    using namespace detail;
    uint8_t hdr = kMetaVersion << kMetaVersionShift;
    hdr |= flags ? kMetaHasFlags : 0;
    hdr |= expiry ? kMetaHasExpiry : 0;
    hdr |= deleted ? kMetaDeleted : 0;
    hdr |= datatype ? kMetaHasDatatype : 0;

    // Copies, not member addresses: the struct is packed.
    const uint64_t c = cas;
    const uint32_t f = flags, e = expiry;
    size_t n = 0;
    out[n++] = static_cast<char>(hdr);
    n += putVarint(out + n, seqno);
    memcpy(out + n, &c, sizeof(c));
    n += sizeof(c);
    n += putVarint(out + n, valueSize);
    if (f) {
        memcpy(out + n, &f, sizeof(f));
        n += sizeof(f);
    }
    if (e) {
        memcpy(out + n, &e, sizeof(e));
        n += sizeof(e);
    }
    if (datatype) {
        out[n++] = static_cast<char>(datatype);
    }
    if (n == kLegacySize) {
        out[n++] = 0;
    }
    return n;
}

inline bool DocMeta::decode(const Slice& s, DocMeta* out) {
    using namespace detail;
    if (s.Len() == kLegacySize) {
        memcpy(out, s.Data(), kLegacySize);
        return true;
    }
    if (s.Len() == 0) {
        return false;
    }
    const char* p = s.Data();
    const char* end = p + s.Len();
    const auto hdr = static_cast<uint8_t>(*p++);
    if ((hdr >> kMetaVersionShift) != kMetaVersion) {
        return false;
    }
    uint64_t seq = 0, c = 0, size = 0;
    uint32_t f = 0, e = 0;
    uint8_t dt = 0;
    if (!getVarint(p, end, &seq) || !getFixed(p, end, &c) ||
        !getVarint(p, end, &size) || size > UINT32_MAX) {
        return false;
    }
    if ((hdr & kMetaHasFlags) && !getFixed(p, end, &f)) {
        return false;
    }
    if ((hdr & kMetaHasExpiry) && !getFixed(p, end, &e)) {
        return false;
    }
    if ((hdr & kMetaHasDatatype) && !getFixed(p, end, &dt)) {
        return false;
    }
    out->seqno = seq;
    out->cas = c;
    out->valueSize = static_cast<uint32_t>(size);
    out->flags = f;
    out->expiry = e;
    out->deleted = (hdr & kMetaDeleted) ? 1 : 0;
    out->datatype = dt;
    return true;
}

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
