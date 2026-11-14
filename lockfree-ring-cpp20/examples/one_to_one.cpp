// One producer -> one subscriber.
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

void one_to_one() {
    RingBuffer<PayoffEvent> rb(1024);            // single producer, yielding wait

    double sum = 0.0;
    BatchConsumer subscriber(rb, [&](PayoffEvent& e, std::int64_t, bool) {
        sum += e.payoff;
    });

    rb.set_gating_sequences({&subscriber.sequence()});   // producer must not lap it

    std::thread t([&] { subscriber.run(); });
    produce(rb);
    wait_until_seen(subscriber.sequence(), rb.cursor());
    subscriber.halt();
    t.join();

    std::printf("1 -> 1 : events=%d  mean payoff=%.4f\n", kEvents, sum / kEvents);
}

int main() { one_to_one(); }
