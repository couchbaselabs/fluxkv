// Unit tests for the document cache. Plain asserts, no framework: the test
// binary exits non-zero on the first failure and prints which check failed.

#include "cache/doccache.h"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace magma::kvserver;

#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__,      \
                         __LINE__, #cond);                                   \
            std::exit(1);                                                    \
        }                                                                    \
    } while (0)

namespace {

std::unique_ptr<DocCache> make(size_t bytes, size_t shards = 1) {
    std::string err;
    auto c = CreateDocCache("s3fifo", bytes, shards, &err);
    CHECK(c != nullptr);
    return c;
}

// Returns the cached value, or "<miss>" / "<deleted>".
std::string get(DocCache& c, uint16_t vb, const std::string& key) {
    std::string out = "<miss>";
    c.Get(vb, key, [&](const CachedDoc& d) {
        out = d.deleted ? "<deleted>" : std::string(d.value);
    });
    return out;
}

void putClean(DocCache& c,
              uint16_t vb,
              const std::string& k,
              const std::string& v) {
    c.Put(vb, k, v, 0, 0, 0, false);
    c.MarkPersisted(vb, k, 1);
}

void testBasic() {
    auto c = make(1 << 20);
    CHECK(get(*c, 0, "a") == "<miss>");

    c->Put(0, "a", "v1", 7, 9, 2, false);
    CHECK(get(*c, 0, "a") == "v1");
    // Same key, other vbucket is a different document.
    CHECK(get(*c, 1, "a") == "<miss>");

    bool seenMeta = false;
    c->Get(0, "a", [&](const CachedDoc& d) {
        seenMeta = d.flags == 7 && d.expiry == 9 && d.datatype == 2 &&
                   d.seqno == 0;
    });
    CHECK(seenMeta);
    c->MarkPersisted(0, "a", 42);
    c->Get(0, "a", [&](const CachedDoc& d) { CHECK(d.seqno == 42); });

    // Overwrite replaces.
    putClean(*c, 0, "a", "v2");
    CHECK(get(*c, 0, "a") == "v2");

    // Tombstone is a hit that says deleted.
    c->Put(0, "a", "", 0, 0, 0, true);
    CHECK(get(*c, 0, "a") == "<deleted>");
    // A fill cannot resurrect it.
    c->PutIfAbsent(0, "a", "stale", 5, 0, 0, 0);
    CHECK(get(*c, 0, "a") == "<deleted>");
    c->MarkPersisted(0, "a", 43);

    c->Erase(0, "a");
    CHECK(get(*c, 0, "a") == "<miss>");

    auto st = c->GetStats();
    CHECK(st.items == 0);
    CHECK(st.bytes == 0);
    CHECK(st.pendingItems == 0);
    CHECK(st.fillsRejected == 1);
    std::puts("basic: ok");
}

void testFillIsAbsentOnly() {
    auto c = make(1 << 20);
    c->PutIfAbsent(3, "k", "disk1", 10, 0, 0, 0);
    CHECK(get(*c, 3, "k") == "disk1");
    c->PutIfAbsent(3, "k", "disk0", 9, 0, 0, 0);
    CHECK(get(*c, 3, "k") == "disk1");
    // A write-through replaces a fill.
    c->Put(3, "k", "new", 0, 0, 0, false);
    CHECK(get(*c, 3, "k") == "new");
    // ... and while it is pending, a stale fill is rejected.
    c->PutIfAbsent(3, "k", "disk1", 10, 0, 0, 0);
    CHECK(get(*c, 3, "k") == "new");
    c->MarkPersisted(3, "k", 11);
    CHECK(c->GetStats().pendingItems == 0);
    std::puts("fill-if-absent: ok");
}

void testBudgetAndEviction() {
    const size_t budget = 256 * 1024;
    auto c = make(budget);
    std::string val(1000, 'x');
    for (int i = 0; i < 5000; i++) {
        putClean(*c, 0, "key-" + std::to_string(i), val);
        auto st = c->GetStats();
        CHECK(st.bytes <= budget);
    }
    auto st = c->GetStats();
    CHECK(st.evictions > 0);
    CHECK(st.items > 100); // budget / ~1.1KB, minus overheads
    CHECK(st.items < 260);
    // The most recent insert is still present, the oldest are gone.
    CHECK(get(*c, 0, "key-4999") == val);
    CHECK(get(*c, 0, "key-0") == "<miss>");
    std::printf("budget: items=%llu bytes=%llu evictions=%llu ok\n",
                (unsigned long long)st.items,
                (unsigned long long)st.bytes,
                (unsigned long long)st.evictions);
}

void testPendingPinsAgainstEviction() {
    const size_t budget = 64 * 1024;
    auto c = make(budget);
    std::string val(1000, 'p');
    // 40 pinned writes, ~44KB: fits.
    for (int i = 0; i < 40; i++) {
        c->Put(0, "pinned-" + std::to_string(i), val, 0, 0, 0, false);
    }
    // Now churn 2000 clean writes through: the pinned ones must all survive
    // even though the churn alone is 30x the budget.
    for (int i = 0; i < 2000; i++) {
        putClean(*c, 0, "churn-" + std::to_string(i), val);
    }
    for (int i = 0; i < 40; i++) {
        CHECK(get(*c, 0, "pinned-" + std::to_string(i)) == val);
    }
    CHECK(c->GetStats().pendingItems == 40);

    // Overwriting a pinned entry carries the outstanding count: two
    // MarkPersisted are needed before it becomes evictable.
    c->Put(0, "pinned-0", "v2", 0, 0, 0, false);
    CHECK(c->GetStats().pendingItems == 40);
    c->MarkPersisted(0, "pinned-0", 100);
    CHECK(c->GetStats().pendingItems == 40);
    c->MarkPersisted(0, "pinned-0", 101);
    CHECK(c->GetStats().pendingItems == 39);

    // Release everything; then a HOT churn evicts them like anything else.
    // (A cold churn would not: S3-FIFO only evicts from main once the small
    // queue is under its share, and a stream of one-hit keys keeps it over.
    // That is the scan resistance, tested separately below.) Reading each
    // churn key twice promotes it to main, which is what pushes the old
    // entries out.
    for (int i = 1; i < 40; i++) {
        c->MarkPersisted(0, "pinned-" + std::to_string(i), 200 + i);
    }
    CHECK(c->GetStats().pendingItems == 0);
    for (int i = 0; i < 2000; i++) {
        auto k = "churn2-" + std::to_string(i);
        putClean(*c, 0, k, val);
        get(*c, 0, k);
        get(*c, 0, k);
    }
    int survivors = 0;
    for (int i = 0; i < 40; i++) {
        survivors += get(*c, 0, "pinned-" + std::to_string(i)) == val;
    }
    CHECK(survivors == 0);
    CHECK(c->GetStats().bytes <= budget);
    std::puts("pending-pin: ok");
}

// S3-FIFO's reason to exist: a hot working set survives a scan of cold
// one-hit keys. With a budget of ~230 items, 50 hot keys re-read between
// bursts of 200 cold keys must stay resident; plain FIFO/LRU would evict them
// every burst.
void testScanResistance() {
    const size_t budget = 256 * 1024;
    auto c = make(budget);
    std::string val(1000, 'h');
    for (int i = 0; i < 50; i++) {
        c->PutIfAbsent(0, "hot-" + std::to_string(i), val, 1, 0, 0, 0);
    }
    // Warm the frequency counters.
    for (int r = 0; r < 3; r++) {
        for (int i = 0; i < 50; i++) {
            get(*c, 0, "hot-" + std::to_string(i));
        }
    }
    uint64_t hotMisses = 0;
    int coldId = 0;
    for (int round = 0; round < 50; round++) {
        for (int i = 0; i < 200; i++) {
            c->PutIfAbsent(
                    0, "cold-" + std::to_string(coldId++), val, 1, 0, 0, 0);
        }
        for (int i = 0; i < 50; i++) {
            auto k = "hot-" + std::to_string(i);
            if (get(*c, 0, k) == "<miss>") {
                hotMisses++;
                c->PutIfAbsent(0, k, val, 1, 0, 0, 0);
            }
        }
    }
    auto st = c->GetStats();
    std::printf("scan-resistance: hot misses=%llu of 2500 lookups, ghost "
                "hits=%llu\n",
                (unsigned long long)hotMisses,
                (unsigned long long)st.ghostHits);
    // Allow a little slack for the first round while the hot set moves from
    // small to main; a FIFO would miss all 2500.
    CHECK(hotMisses < 250);
}

void testConcurrentHammer() {
    const size_t budget = 8 * 1024 * 1024;
    auto c = make(budget, 16);
    const int nThreads = 8;
    const int keys = 4000;
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> corrupt{0};
    std::atomic<uint64_t> ops{0};

    // Value encodes its key so a hit can be verified regardless of which
    // version it is.
    auto valueFor = [](int k, int ver) {
        std::string v = "k" + std::to_string(k) + ":v" + std::to_string(ver) +
                        ":";
        v.resize(200 + (k % 300), 'z');
        return v;
    };

    std::vector<std::thread> ts;
    for (int t = 0; t < nThreads; t++) {
        ts.emplace_back([&, t]() {
            std::mt19937 rng(t);
            int ver = 0;
            while (!stop.load(std::memory_order_relaxed)) {
                int k = rng() % keys;
                std::string key = "key" + std::to_string(k);
                std::string prefix = "k" + std::to_string(k) + ":";
                switch (rng() % 10) {
                case 0:
                case 1:
                    c->Put(k % 4, key, valueFor(k, ver++), 0, 0, 0, false);
                    c->MarkPersisted(k % 4, key, ver);
                    break;
                case 2:
                    c->PutIfAbsent(k % 4, key, valueFor(k, 0), 1, 0, 0, 0);
                    break;
                case 3:
                    if (rng() % 50 == 0) {
                        c->Erase(k % 4, key);
                    }
                    break;
                default:
                    c->Get(k % 4, key, [&](const CachedDoc& d) {
                        if (!d.deleted &&
                            d.value.substr(0, prefix.size()) != prefix) {
                            corrupt.fetch_add(1);
                        }
                    });
                }
                ops.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
    stop = true;
    for (auto& th : ts) {
        th.join();
    }
    auto st = c->GetStats();
    CHECK(corrupt.load() == 0);
    CHECK(st.bytes <= budget);
    CHECK(st.pendingItems == 0);
    std::printf("concurrent: %llu ops, items=%llu bytes=%llu hits=%llu "
                "misses=%llu ok\n",
                (unsigned long long)ops.load(),
                (unsigned long long)st.items,
                (unsigned long long)st.bytes,
                (unsigned long long)st.hits,
                (unsigned long long)st.misses);
}

void testUnknownPolicy() {
    std::string err;
    auto c = CreateDocCache("lru-please", 1024, 1, &err);
    CHECK(c == nullptr);
    CHECK(!err.empty());
    std::puts("factory: ok");
}

} // namespace

int main() {
    testBasic();
    testFillIsAbsentOnly();
    testBudgetAndEviction();
    testPendingPinsAgainstEviction();
    testScanResistance();
    testConcurrentHammer();
    testUnknownPolicy();
    std::puts("PASS");
    return 0;
}
