// One producer -> three independent subscribers; each one sees every event.
#include <ring/disruptor/disruptor.hpp>

#include <cstdio>
#include <limits>
#include <thread>

using namespace ring;
using namespace ring::disruptor;

// The "Entry": created once per slot, overwritten by the producer.
struct PayoffEvent {
    int    path_id = 0;
    double payoff  = 0.0;
};

constexpr int kEvents = 1'000'000;

// Publish kEvents events, then wait until `slowest` has seen the last one.
template <class Ring>
void produce(Ring& rb) {
    for (int i = 0; i < kEvents; ++i) {
        const std::int64_t seq = rb.claim();     // 1. claim a slot
        rb[seq].path_id = i;                     // 2. fill it in place
        rb[seq].payoff  = 100.0 + (i % 7);
        rb.publish(seq);                         // 3. make it visible
    }
}
inline void wait_until_seen(const Sequence& s, std::int64_t last) {
    while (s.get_acquire() < last) std::this_thread::yield();
}

// Demonstrates one producer feeding three independent subscribers.
void one_to_many() {
    RingBuffer<PayoffEvent> rb(1024);

    double sum = 0.0;                                           // subscriber A: mean
    BatchConsumer mean_sub(rb, [&](PayoffEvent& e, std::int64_t, bool) {
        sum += e.payoff;
    });

    double lo = std::numeric_limits<double>::infinity(), hi = -lo;   // B: extremes
    BatchConsumer range_sub(rb, [&](PayoffEvent& e, std::int64_t, bool) {
        if (e.payoff < lo) lo = e.payoff;
        if (e.payoff > hi) hi = e.payoff;
    });

    long long batches = 0, seen = 0;                            // C: batching monitor
    BatchConsumer monitor_sub(rb, [&](PayoffEvent&, std::int64_t, bool end_of_batch) {
        ++seen;
        if (end_of_batch) ++batches;
    });

    // The producer is held back by the SLOWEST of the three.
    rb.set_gating_sequences(
        {&mean_sub.sequence(), &range_sub.sequence(), &monitor_sub.sequence()});

    std::thread ta([&] { mean_sub.run(); });
    std::thread tb([&] { range_sub.run(); });
    std::thread tc([&] { monitor_sub.run(); });

    produce(rb);
    const std::int64_t last = rb.cursor();
    wait_until_seen(mean_sub.sequence(), last);
    wait_until_seen(range_sub.sequence(), last);
    wait_until_seen(monitor_sub.sequence(), last);
    mean_sub.halt(); range_sub.halt(); monitor_sub.halt();
    ta.join(); tb.join(); tc.join();

    std::printf("1 -> 3 : mean=%.4f  min=%.1f  max=%.1f  monitor saw %lld events in %lld batches\n",
                sum / kEvents, lo, hi, seen, batches);
}

int main() { one_to_many(); }
