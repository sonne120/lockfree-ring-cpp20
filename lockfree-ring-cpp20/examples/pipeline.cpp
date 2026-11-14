#include <ring/disruptor/disruptor.hpp>

#include <cmath>
#include <cstdio>
#include <memory>
#include <thread>
#include <vector>

using namespace ring;
using namespace ring::disruptor;

struct PathEntry {
    std::vector<double> path;      // allocated once by the entry factory
    double              payoff = 0.0;
};

double run(int workers, int total_paths) {
    constexpr std::size_t kSteps = 64;
    RingBuffer<PathEntry> rb(1024, [] { return PathEntry{std::vector<double>(kSteps), 0.0}; });

    // Stage 1: each worker prices only its own stripe — no CAS between workers.
    auto price = [workers](int id) {
        return [workers, id](PathEntry& e, std::int64_t seq, bool) {
            if (seq % workers == id) e.payoff = e.path.back();
        };
    };
    using Worker = BatchConsumer<PathEntry, SingleThreadedClaimStrategy,
                                 YieldingWaitStrategy, decltype(price(0))>;
    std::vector<std::unique_ptr<Worker>> stage1;
    std::vector<const Sequence*>         stage1_seqs;
    for (int w = 0; w < workers; ++w) {
        stage1.push_back(std::make_unique<Worker>(rb, price(w)));
        stage1_seqs.push_back(&stage1.back()->sequence());
    }

    // Stage 2: the aggregator may only read what every worker has finished.
    double sum = 0.0;
    BatchConsumer aggregator(rb, [&](PathEntry& e, std::int64_t, bool) { sum += e.payoff; },
                             stage1_seqs);

    rb.set_gating_sequences({&aggregator.sequence()});   // last stage gates the producer

    std::vector<std::thread> threads;
    for (auto& w : stage1) threads.emplace_back([&w] { w->run(); });
    threads.emplace_back([&] { aggregator.run(); });

    for (int i = 0; i < total_paths; ++i) {
        const std::int64_t seq = rb.claim();
        auto& p = rb[seq].path;
        p[0] = 100.0;
        for (std::size_t k = 1; k < kSteps; ++k) p[k] = p[k - 1] * (1.0 + 1e-4 * std::sin(i + double(k)));
        rb.publish(seq);
    }

    // Shut down in graph order: upstream first.
    const std::int64_t last = rb.cursor();
    auto drained = [last](const Sequence& s) { while (s.get_acquire() < last) std::this_thread::yield(); };
    for (auto& w : stage1) { drained(w->sequence()); w->halt(); }
    drained(aggregator.sequence());
    aggregator.halt();
    for (auto& t : threads) t.join();

    return sum / total_paths;
}

int main() {
    const double a = run(1, 200'000), b = run(3, 200'000);
    std::printf("mean payoff: 1 worker = %.12f, 3 workers = %.12f, bit-identical: %s\n",
                a, b, a == b ? "yes" : "no");
    return a == b ? 0 : 1;
}
