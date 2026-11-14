#include <ring/disruptor/disruptor.hpp>
#include <cstdio>
#include <thread>
#include <vector>
using namespace ring;
using namespace ring::disruptor;

struct PathEntry {
    std::vector<double> path;  // pre-allocated by the factory, reused forever
    double payoff = 0.0;
};

static int g_failed = 0;

template <class Claim, class Wait>
static void run_case(const char* name, int producers) {
    constexpr int  kPerProducer = 200'000;
    constexpr int  kWorkers = 3;
    const long long total = 1LL * kPerProducer * producers;

    RingBuffer<PathEntry, Claim, Wait> rb(1024, [] { return PathEntry{std::vector<double>(16), 0.0}; });

    auto work = [](int id) {
        return [id](PathEntry& e, std::int64_t seq, bool) {
            if (seq % kWorkers != id) return;            // stripe: no CAS between workers
            double s = 0; for (double x : e.path) s += x;
            e.payoff = s;                                // result written back into the entry
        };
    };
    BatchConsumer w0(rb, work(0)), w1(rb, work(1)), w2(rb, work(2));

    long long count = 0; double sum = 0; std::int64_t last = -1; bool ordered = true;
    BatchConsumer agg(rb, [&](PathEntry& e, std::int64_t seq, bool) {
        ordered &= (seq == last + 1); last = seq; ++count; sum += e.payoff;
    }, {&w0.sequence(), &w1.sequence(), &w2.sequence()});

    rb.set_gating_sequences({&agg.sequence()});

    std::thread t0([&]{ w0.run(); }), t1([&]{ w1.run(); }), t2([&]{ w2.run(); }), ta([&]{ agg.run(); });
    std::vector<std::thread> ps;
    for (int p = 0; p < producers; ++p) ps.emplace_back([&] {
        for (int i = 0; i < kPerProducer; ++i) {
            const auto seq = rb.claim();
            for (double& x : rb[seq].path) x = 1.0;    // fill in place, no allocation
            rb.publish(seq);
        }
    });
    for (auto& t : ps) t.join();
    while (agg.sequence().get_acquire() < total - 1) std::this_thread::yield();
    w0.halt(); w1.halt(); w2.halt(); t0.join(); t1.join(); t2.join();
    agg.halt(); ta.join();

    const bool ok = count == total && ordered && sum == 16.0 * static_cast<double>(total);
    std::printf("%s  %-34s events=%lld\n", ok ? "ok  " : "FAIL", name, count);
    if (!ok) ++g_failed;
}

int main() {
    run_case<SingleThreadedClaimStrategy, BusySpinWaitStrategy>("single/busy-spin", 1);
    run_case<SingleThreadedClaimStrategy, YieldingWaitStrategy>("single/yielding", 1);
    run_case<SingleThreadedClaimStrategy, BlockingWaitStrategy>("single/blocking", 1);
    run_case<MultiThreadedClaimStrategy,  YieldingWaitStrategy>("multi(3 producers)/yielding", 3);
    run_case<MultiThreadedClaimStrategy,  BlockingWaitStrategy>("multi(3 producers)/blocking", 3);
    return g_failed == 0 ? 0 : 1;
}
