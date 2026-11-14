#include <ring/queue/queue.hpp>

#include <cstdio>
#include <thread>
#include <vector>

using namespace ring::queue;

int main() {
    CircularBuffer<double, 4> last;
    for (int i = 1; i <= 6; ++i) last.push_overwrite(100.0 + i);
    std::printf("CircularBuffer : last %zu payoffs, oldest=%.0f newest=%.0f\n",
                last.size(), last.front(), last.back());

    constexpr int kN = 1'000'000;
    SpscQueue<int, 1024> spsc;
    long long spsc_sum = 0;
    std::thread consumer([&] {
        for (int n = 0; n < kN;) {
            if (auto v = spsc.try_pop()) { spsc_sum += *v; ++n; }
            else std::this_thread::yield();
        }
    });
    for (int i = 0; i < kN; ++i) while (!spsc.try_push(i)) std::this_thread::yield();
    consumer.join();
    std::printf("SpscQueue      : %d items, sum=%lld\n", kN, spsc_sum);

    constexpr int kProducers = 2, kConsumers = 2, kPer = 250'000;
    MpmcQueue<int, 1024> mpmc;
    std::atomic<int>       taken{0};
    std::atomic<long long> mpmc_sum{0};
    std::vector<std::thread> threads;
    for (int p = 0; p < kProducers; ++p) threads.emplace_back([&] {
        for (int i = 0; i < kPer; ++i) while (!mpmc.try_enqueue(1)) std::this_thread::yield();
    });
    for (int c = 0; c < kConsumers; ++c) threads.emplace_back([&] {
        int v = 0;
        while (taken.load(std::memory_order_acquire) < kProducers * kPer) {
            if (mpmc.try_dequeue(v)) { mpmc_sum.fetch_add(v); taken.fetch_add(1); }
            else std::this_thread::yield();
        }
    });
    for (auto& t : threads) t.join();
    std::printf("MpmcQueue      : %d producers, %d consumers, items=%lld\n",
                kProducers, kConsumers, mpmc_sum.load());
}
