#include "doccache.h"

#include "s3fifo_cache.h"

namespace magma {
namespace kvserver {

std::unique_ptr<DocCache> CreateDocCache(const std::string& policy,
                                         size_t maxBytes,
                                         size_t numShards,
                                         std::string* error) {
    if (policy.empty() || policy == "s3fifo") {
        return std::make_unique<S3FifoCache>(maxBytes, numShards);
    }
    if (error) {
        *error = "unknown cache policy '" + policy + "' (known: s3fifo)";
    }
    return nullptr;
}

} // namespace kvserver
} // namespace magma
