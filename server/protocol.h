#pragma once

#include <arpa/inet.h>
#include <folly/io/IOBuf.h>
#include <folly/lang/Bits.h>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

namespace magma {
namespace kvserver {

// Memcached binary protocol opcodes (subset for gocb compatibility)
enum class Opcode : uint8_t {
    Get = 0x00,
    Set = 0x01,
    Delete = 0x04,
    Noop = 0x0a,
    Hello = 0x1f,
    SaslListMechs = 0x20,
    SaslAuth = 0x21,
    SaslStep = 0x22,
    SelectBucket = 0x89,
    GetClusterConfig = 0xb5,
    GetErrorMap = 0xfe,
};

enum class McbpStatus : uint16_t {
    Success = 0x0000,
    KeyNotFound = 0x0001,
    KeyExists = 0x0002,
    NotMyVbucket = 0x0007,
    AuthError = 0x0020,
    AuthContinue = 0x0021,
    UnknownCommand = 0x0081,
    InternalError = 0x0084,
    TmpFail = 0x0086,
};

static constexpr uint8_t kRequestMagic = 0x80;
static constexpr uint8_t kResponseMagic = 0x81;
static constexpr size_t kHeaderSize = 24;

#pragma pack(push, 1)
struct McbpHeader {
    uint8_t magic;
    uint8_t opcode;
    uint16_t keyLen;
    uint8_t extrasLen;
    uint8_t datatype;
    uint16_t specific; // vbucket (req) or status (resp)
    uint32_t bodyLen;
    uint32_t opaque;
    uint64_t cas;

    void ntoh() {
        keyLen = ntohs(keyLen);
        specific = ntohs(specific);
        bodyLen = ntohl(bodyLen);
        cas = folly::Endian::big(cas);
    }

    void hton() {
        keyLen = htons(keyLen);
        specific = htons(specific);
        bodyLen = htonl(bodyLen);
        cas = folly::Endian::big(cas);
    }
};
#pragma pack(pop)

static_assert(sizeof(McbpHeader) == kHeaderSize,
              "McbpHeader must be exactly 24 bytes");

// Build a response packet as a single contiguous IOBuf
std::unique_ptr<folly::IOBuf> buildResponse(uint8_t opcode,
                                            McbpStatus status,
                                            uint32_t opaque,
                                            uint64_t cas,
                                            const void* extras,
                                            size_t extrasLen,
                                            const void* key,
                                            size_t keyLen,
                                            const void* value,
                                            size_t valueLen);

inline std::unique_ptr<folly::IOBuf> buildEmptyResponse(uint8_t opcode,
                                                        McbpStatus status,
                                                        uint32_t opaque,
                                                        uint64_t cas = 0) {
    return buildResponse(
            opcode, status, opaque, cas, nullptr, 0, nullptr, 0, nullptr, 0);
}

// Build GET response with flags in extras and value
std::unique_ptr<folly::IOBuf> buildGetResponse(uint32_t opaque,
                                               uint64_t cas,
                                               uint32_t flags,
                                               const void* value,
                                               size_t valueLen);

// Append-into-buffer variants — no IOBuf alloc. The buffer is grown as needed.
// Used by Connection's pendingWriteBuf_ coalescing path (one socket write per
// epoll wake / runInLoop iteration instead of one per response).
void appendResponse(std::vector<uint8_t>& out,
                    uint8_t opcode,
                    McbpStatus status,
                    uint32_t opaque,
                    uint64_t cas,
                    const void* extras,
                    size_t extrasLen,
                    const void* key,
                    size_t keyLen,
                    const void* value,
                    size_t valueLen);

inline void appendEmptyResponse(std::vector<uint8_t>& out,
                                uint8_t opcode,
                                McbpStatus status,
                                uint32_t opaque,
                                uint64_t cas = 0) {
    appendResponse(out,
                   opcode,
                   status,
                   opaque,
                   cas,
                   nullptr,
                   0,
                   nullptr,
                   0,
                   nullptr,
                   0);
}

void appendGetResponse(std::vector<uint8_t>& out,
                       uint32_t opaque,
                       uint64_t cas,
                       uint32_t flags,
                       const void* value,
                       size_t valueLen,
                       uint8_t datatype = 0);

// Precomputed cluster config JSON for gocb
std::string buildClusterConfigJson(const std::string& hostname,
                                   uint16_t port,
                                   const std::string& bucketName,
                                   uint16_t numVBuckets = 16,
                                   uint16_t mgmtPort = 0);

// Minimal error map JSON
std::string buildErrorMapJson();

} // namespace kvserver
} // namespace magma
