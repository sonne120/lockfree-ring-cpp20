#include <ring/queue/queue.hpp>

#include <atomic>
#include <cstdio>
#include <set>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace ring::queue;

namespace {
int g_passed = 0, g_failed = 0;
#define REQUIRE(cond) do { if (cond) ++g_passed; else { ++g_failed; \
    std::fprintf(stderr, "  FAIL  %s:%d  %s\n", __FILE__, __LINE__, #cond); } } while (0)

void test_circular_fifo_and_wrap() {
    std::printf("test_circular_fifo_and_wrap\n");
    CircularBuffer<int, 4> b;
    REQUIRE(b.empty()); REQUIRE(!b.try_pop());
    for (int i = 1; i <= 4; ++i) REQUIRE(b.try_push(i));
    REQUIRE(b.full()); REQUIRE(!b.try_push(5));
    REQUIRE(*b.try_pop() == 1); REQUIRE(*b.try_pop() == 2);
    REQUIRE(b.try_push(5)); REQUIRE(b.try_push(6));
    REQUIRE(b.size() == 4); REQUIRE(b.front() == 3); REQUIRE(b.back() == 6);
    REQUIRE(b[1] == 4);
    for (int want = 3; want <= 6; ++want) REQUIRE(*b.try_pop() == want);
    REQUIRE(b.empty());
}

void test_circular_overwrite_and_clear() {
    std::printf("test_circular_overwrite_and_clear\n");
    CircularBuffer<std::string, 2> b;
    b.push_overwrite("a"); b.push_overwrite("b"); b.push_overwrite("c");
    REQUIRE(b.size() == 2); REQUIRE(b.front() == "b"); REQUIRE(b.back() == "c");
    b.clear(); REQUIRE(b.empty()); REQUIRE(b.try_push("d")); REQUIRE(b.front() == "d");
    for (int i = 0; i < 1000; ++i) b.push_overwrite(std::to_string(i));
    REQUIRE(b.front() == "998"); REQUIRE(b.back() == "999");
}

void test_spsc_full_empty() {
    std::printf("test_spsc_full_empty\n");
    SpscQueue<int, 2> q;
    REQUIRE(q.empty_approx()); REQUIRE(!q.try_pop());
    REQUIRE(q.try_push(1)); REQUIRE(q.try_push(2)); REQUIRE(!q.try_push(3));
    REQUIRE(*q.try_pop() == 1); REQUIRE(q.try_push(3));
    REQUIRE(*q.try_pop() == 2); REQUIRE(*q.try_pop() == 3); REQUIRE(!q.try_pop());
}

void test_spsc_ordered_across_threads() {
    std::printf("test_spsc_ordered_across_threads\n");
    constexpr int kN = 500'000;
    SpscQueue<int, 256> q;
    bool ordered = true;
    std::thread c([&] {
        for (int want = 0; want < kN;) {
            if (auto v = q.try_pop()) { ordered &= (*v == want); ++want; }
            else std::this_thread::yield();
        }
    });
    for (int i = 0; i < kN; ++i) while (!q.try_push(i)) std::this_thread::yield();
    c.join();
    REQUIRE(ordered); REQUIRE(q.empty_approx());
}

void test_mpmc_unique_values() {
    std::printf("test_mpmc_unique_values\n");
    constexpr int P = 3, C = 3, PER = 50'000;
    MpmcQueue<int, 1024> q;
    std::atomic<int> popped{0};
    std::mutex mu; std::vector<int> all;
    std::vector<std::thread> ts;
    for (int p = 0; p < P; ++p) ts.emplace_back([p, &q] {
        for (int i = 0; i < PER; ++i) while (!q.try_enqueue(p * PER + i)) std::this_thread::yield();
    });
    for (int c = 0; c < C; ++c) ts.emplace_back([&] {
        std::vector<int> local; int v = 0;
        while (popped.load(std::memory_order_acquire) < P * PER) {
            if (q.try_dequeue(v)) { local.push_back(v); popped.fetch_add(1); }
            else std::this_thread::yield();
        }
        std::lock_guard lk(mu); all.insert(all.end(), local.begin(), local.end());
    });
    for (auto& t : ts) t.join();
    std::set<int> uniq(all.begin(), all.end());
    REQUIRE(all.size() == static_cast<std::size_t>(P * PER));
    REQUIRE(uniq.size() == all.size());
    REQUIRE(*uniq.begin() == 0); REQUIRE(*uniq.rbegin() == P * PER - 1);
}
}

int main() {
    test_circular_fifo_and_wrap();
    test_circular_overwrite_and_clear();
    test_spsc_full_empty();
    test_spsc_ordered_across_threads();
    test_mpmc_unique_values();
    std::printf("\n%d passed, %d failed\n", g_passed, g_failed);
    return g_failed == 0 ? 0 : 1;
}
