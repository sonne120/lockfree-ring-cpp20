#include "disruptor.hpp"
#include <cassert>
#include <cstdio>
#include <thread>
#include <vector>
using namespace leasing::common::ring;

struct PathEntry {
    std::vector<double> path; 
    double payoff = 0.0;
};

template <class Claim, class Wait>
static void run_case(const char* name, int producers) {
    constexpr int  kPerProducer = 200'000;
    constexpr int  kWorkers = 3;
    const long long total = 1LL * kPerProducer * producers;

    RingBuffer<PathEntry, Claim, Wait> ring(1024, [] { return PathEntry{std::vector<double>(16), 0.0}; });

    auto work = [](int id) {
        return [id](PathEntry& e, std::int64_t seq, bool) {
            if (seq % kWorkers != id) return;            // stripe: no CAS between workers
            double s = 0; for (double x : e.path) s += x;
            e.payoff = s;                                // result written back into the entry
        };
    };
    BatchConsumer w0(ring, work(0)), w1(ring, work(1)), w2(ring, work(2));

    long long count = 0; double sum = 0; std::int64_t last = -1; bool ordered = true;
    BatchConsumer agg(ring, [&](PathEntry& e, std::int64_t seq, bool) {
        ordered &= (seq == last + 1); last = seq; ++count; sum += e.payoff;
    }, {&w0.sequence(), &w1.sequence(), &w2.sequence()});

    ring.set_gating_sequences({&agg.sequence()});

    std::thread t0([&]{ w0.run(); }), t1([&]{ w1.run(); }), t2([&]{ w2.run(); }), ta([&]{ agg.run(); });
    std::vector<std::thread> ps;
    for (int p = 0; p < producers; ++p) ps.emplace_back([&] {
        for (int i = 0; i < kPerProducer; ++i) {
            const auto seq = ring.claim();
            for (double& x : ring[seq].path) x = 1.0;    // fill in place, no allocation
            ring.publish(seq);
        }
    });
    for (auto& t : ps) t.join();
    while (agg.sequence().get_acquire() < total - 1) std::this_thread::yield();
    w0.halt(); w1.halt(); w2.halt(); t0.join(); t1.join(); t2.join();
    agg.halt(); ta.join();

    assert(count == total); assert(ordered); assert(sum == 16.0 * total);
    std::printf("ok  %-34s events=%lld\n", name, count);
}

int main() {
    run_case<SingleThreadedClaimStrategy, BusySpinWaitStrategy>("single/busy-spin", 1);
    run_case<SingleThreadedClaimStrategy, YieldingWaitStrategy>("single/yielding", 1);
    run_case<SingleThreadedClaimStrategy, BlockingWaitStrategy>("single/blocking", 1);
    run_case<MultiThreadedClaimStrategy,  YieldingWaitStrategy>("multi(3 producers)/yielding", 3);
    run_case<MultiThreadedClaimStrategy,  BlockingWaitStrategy>("multi(3 producers)/blocking", 3);
}
