#include "protocol.h"
#include <sstream>

namespace magma {
namespace kvserver {

std::unique_ptr<folly::IOBuf> buildResponse(uint8_t opcode,
                                            McbpStatus status,
                                            uint32_t opaque,
                                            uint64_t cas,
                                            const void* extras,
                                            size_t extrasLen,
                                            const void* key,
                                            size_t keyLen,
                                            const void* value,
                                            size_t valueLen) {
    size_t bodyLen = extrasLen + keyLen + valueLen;
    auto buf = folly::IOBuf::create(kHeaderSize + bodyLen);
    buf->append(kHeaderSize + bodyLen);
    auto* p = buf->writableData();

    McbpHeader hdr{};
    hdr.magic = kResponseMagic;
    hdr.opcode = opcode;
    hdr.keyLen = static_cast<uint16_t>(keyLen);
    hdr.extrasLen = static_cast<uint8_t>(extrasLen);
    hdr.datatype = 0;
    hdr.specific = static_cast<uint16_t>(status);
    hdr.bodyLen = static_cast<uint32_t>(bodyLen);
    hdr.opaque = opaque;
    hdr.cas = cas;
    hdr.hton();

    memcpy(p, &hdr, kHeaderSize);
    p += kHeaderSize;

    if (extrasLen > 0 && extras) {
        memcpy(p, extras, extrasLen);
        p += extrasLen;
    }
    if (keyLen > 0 && key) {
        memcpy(p, key, keyLen);
        p += keyLen;
    }
    if (valueLen > 0 && value) {
        memcpy(p, value, valueLen);
    }

    return buf;
}

std::unique_ptr<folly::IOBuf> buildGetResponse(uint32_t opaque,
                                               uint64_t cas,
                                               uint32_t flags,
                                               const void* value,
                                               size_t valueLen) {
    uint32_t flagsNBO = htonl(flags);
    return buildResponse(static_cast<uint8_t>(Opcode::Get),
                         McbpStatus::Success,
                         opaque,
                         cas,
                         &flagsNBO,
                         sizeof(flagsNBO),
                         nullptr,
                         0,
                         value,
                         valueLen);
}

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
                    size_t valueLen) {
    size_t bodyLen = extrasLen + keyLen + valueLen;
    size_t total = kHeaderSize + bodyLen;
    size_t off = out.size();
    out.resize(off + total);
    auto* p = out.data() + off;

    McbpHeader hdr{};
    hdr.magic = kResponseMagic;
    hdr.opcode = opcode;
    hdr.keyLen = static_cast<uint16_t>(keyLen);
    hdr.extrasLen = static_cast<uint8_t>(extrasLen);
    hdr.datatype = 0;
    hdr.specific = static_cast<uint16_t>(status);
    hdr.bodyLen = static_cast<uint32_t>(bodyLen);
    hdr.opaque = opaque;
    hdr.cas = cas;
    hdr.hton();

    memcpy(p, &hdr, kHeaderSize);
    p += kHeaderSize;
    if (extrasLen > 0 && extras) {
        memcpy(p, extras, extrasLen);
        p += extrasLen;
    }
    if (keyLen > 0 && key) {
        memcpy(p, key, keyLen);
        p += keyLen;
    }
    if (valueLen > 0 && value) {
        memcpy(p, value, valueLen);
    }
}

void appendGetResponse(std::vector<uint8_t>& out,
                       uint32_t opaque,
                       uint64_t cas,
                       uint32_t flags,
                       const void* value,
                       size_t valueLen,
                       uint8_t datatype) {
    uint32_t flagsNBO = htonl(flags);
    const size_t hdrOff = out.size();
    appendResponse(out,
                   static_cast<uint8_t>(Opcode::Get),
                   McbpStatus::Success,
                   opaque,
                   cas,
                   &flagsNBO,
                   sizeof(flagsNBO),
                   nullptr,
                   0,
                   value,
                   valueLen);
    out[hdrOff + 5] = datatype; // mcbp header: datatype byte
}

std::string buildClusterConfigJson(const std::string& hostname,
                                   uint16_t port,
                                   const std::string& bucketName,
                                   uint16_t numVBuckets,
                                   uint16_t mgmtPort) {
    if (mgmtPort == 0) mgmtPort = port;
    std::ostringstream ss;
    ss << "{"
       << "\"rev\":1,"
       << "\"revEpoch\":1,"
       << "\"name\":\"" << bucketName << "\","
       << "\"uri\":\"/pools/default/buckets/" << bucketName << "\","
       << "\"streamingUri\":\"/pools/default/bucketsStreaming/"
       << bucketName << "\","
       << "\"nodeLocator\":\"vbucket\","
       << "\"bucketType\":\"membase\","
       << "\"uuid\":\"00000000000000000000000000000000\","
       << "\"nodes\":[{"
       << "\"hostname\":\"" << hostname << ":" << port << "\","
       << "\"ports\":{\"direct\":" << port << "},"
       << "\"status\":\"healthy\","
       << "\"version\":\"0.0.0\""
       << "}],"
       << "\"nodesExt\":[{"
       << "\"services\":{\"kv\":" << port << ",\"mgmt\":" << mgmtPort << "},"
       << "\"thisNode\":true,"
       << "\"hostname\":\"" << hostname << "\""
       << "}],"
       << "\"vBucketServerMap\":{"
       << "\"hashAlgorithm\":\"CRC\","
       << "\"numReplicas\":0,"
       << "\"serverList\":[\"" << hostname << ":" << port << "\"],"
       << "\"vBucketMap\":[";

    for (int i = 0; i < numVBuckets; i++) {
        if (i > 0)
            ss << ",";
        ss << "[0]";
    }

    ss << "]}"
       << ",\"bucketCapabilities\":[]"
       << "}";
    return ss.str();
}

std::string buildErrorMapJson() {
    return R"({"version":1,"revision":1,"errors":{}})";
}

} // namespace kvserver
} // namespace magma
