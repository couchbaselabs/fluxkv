// Unit test for DocMeta's on-disk forms: the compact encoding round-trips,
// legacy 30-byte metas still decode, and malformed input is rejected.

#include "metadata.h"

#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <vector>

using magma::Slice;
using magma::kvserver::DocMeta;

namespace {

int failures = 0;

#define CHECK(cond)                                                     \
    do {                                                                \
        if (!(cond)) {                                                  \
            std::fprintf(stderr, "%s:%d: CHECK(%s)\n", __FILE__, __LINE__, \
                         #cond);                                        \
            failures++;                                                 \
        }                                                               \
    } while (0)

bool same(const DocMeta& a, const DocMeta& b) {
    return a.seqno == b.seqno && a.cas == b.cas && a.valueSize == b.valueSize &&
           a.flags == b.flags && a.expiry == b.expiry &&
           a.deleted == b.deleted && a.datatype == b.datatype;
}

std::string compact(const DocMeta& m) {
    char buf[DocMeta::kMaxCompactSize];
    return std::string(buf, m.encodeCompact(buf));
}

void roundTrip() {
    std::mt19937_64 rng(1);
    const uint64_t seqnos[] = {0, 1, 127, 128, 1u << 21, 1ull << 40, UINT64_MAX};
    const uint32_t sizes[] = {0, 1, 1024, UINT32_MAX};
    for (uint64_t seqno : seqnos) {
        for (uint32_t size : sizes) {
            for (int bits = 0; bits < 16; bits++) {
                DocMeta m;
                m.seqno = seqno;
                m.cas = rng();
                m.valueSize = size;
                m.flags = (bits & 1) ? static_cast<uint32_t>(rng()) | 1 : 0;
                m.expiry = (bits & 2) ? static_cast<uint32_t>(rng()) | 1 : 0;
                m.deleted = (bits & 4) ? 1 : 0;
                m.datatype = (bits & 8) ? 0x02 : 0;
                const auto enc = compact(m);
                CHECK(enc.size() != DocMeta::kLegacySize);
                CHECK(enc.size() <= DocMeta::kMaxCompactSize);
                DocMeta got;
                CHECK(DocMeta::decode(Slice(enc), &got));
                CHECK(same(got, m));
            }
        }
    }
}

// The benchmark's documents: a per-vbucket seqno, 1 KB values, no flags.
void typicalSize() {
    DocMeta m;
    m.seqno = 780000;
    m.cas = 1790000000000000000ull;
    m.valueSize = 1024;
    CHECK(compact(m).size() == 14);
}

// A compact encoding that would be 30 bytes is padded, so it is never read
// as legacy.
void padding() {
    int padded = 0;
    for (uint64_t seqno = 1; seqno != 0 && seqno < (1ull << 63); seqno <<= 7) {
        for (uint32_t size = 1; size != 0 && size < (1u << 31); size <<= 7) {
            for (int bits = 0; bits < 8; bits++) {
                DocMeta m;
                m.seqno = seqno;
                m.valueSize = size;
                m.flags = (bits & 1) ? 7 : 0;
                m.expiry = (bits & 2) ? 9 : 0;
                m.datatype = (bits & 4) ? 1 : 0;
                const auto enc = compact(m);
                if (enc.size() == DocMeta::kLegacySize + 1) {
                    padded++;
                }
                DocMeta got;
                CHECK(DocMeta::decode(Slice(enc), &got) && same(got, m));
            }
        }
    }
    CHECK(padded > 0);
}

void legacy() {
    DocMeta m;
    m.seqno = 42;
    m.cas = 0x0102030405060708ull;
    m.valueSize = 1024;
    m.flags = 0x02000006;
    m.expiry = 1790000000;
    m.deleted = 1;
    m.datatype = 0x03;
    const auto enc = m.encode();
    CHECK(enc.size() == DocMeta::kLegacySize);
    DocMeta got;
    CHECK(DocMeta::decode(Slice(enc), &got));
    CHECK(same(got, m));
}

void malformed() {
    DocMeta m;
    m.seqno = 1ull << 40;
    m.cas = 5;
    m.valueSize = 1024;
    m.flags = 3;
    m.expiry = 4;
    m.datatype = 1;
    const auto enc = compact(m);
    DocMeta got;
    for (size_t len = 0; len < enc.size(); len++) {
        if (len == DocMeta::kLegacySize) {
            continue; // Any 30 bytes are a legacy meta by definition.
        }
        CHECK(!DocMeta::decode(Slice(enc.data(), len), &got));
    }

    std::string wrongVersion = enc;
    wrongVersion[0] = static_cast<char>((wrongVersion[0] & 0x1f) | (2 << 5));
    CHECK(!DocMeta::decode(Slice(wrongVersion), &got));

    // An undecodable meta reads as zeros through the non-failing form.
    CHECK(same(DocMeta::decode(Slice(wrongVersion)), DocMeta{}));

    // Random bytes never read out of bounds, whatever they decode to.
    std::mt19937_64 rng(2);
    std::vector<char> junk(64);
    for (int i = 0; i < 200000; i++) {
        for (auto& c : junk) {
            c = static_cast<char>(rng());
        }
        DocMeta::decode(Slice(junk.data(), rng() % junk.size()), &got);
    }
}

} // namespace

int main() {
    roundTrip();
    typicalSize();
    padding();
    legacy();
    malformed();
    if (failures) {
        std::fprintf(stderr, "metadata_test: %d failures\n", failures);
        return 1;
    }
    std::printf("metadata_test: OK\n");
    return 0;
}
