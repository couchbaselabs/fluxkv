#pragma once

#include "tuner.h"

#include "include/libmagma/magma.h"

#include <vector>

namespace magma {
namespace kvserver {

// The process-wide magma reader pool as an ElasticPool. Its busy figure is
// thread CPU minus idle spin, which shows demand while below 1.0.
class MagmaReaderPort final : public ElasticPool {
public:
    const char* Name() const override {
        return "magma_readers";
    }
    size_t Size() const override;
    size_t Step() const override {
        return 1;
    }
    void Grow(size_t n) override;
    void Shrink(size_t n) override;
    PoolSample Sample(double wallSec) override;
    void SetIdleSpin(size_t spinners) override;

private:
    // Cumulative counters by thread id at the previous Sample.
    std::vector<Magma::ReaderThreadStats> last_;
};

} // namespace kvserver
} // namespace magma
