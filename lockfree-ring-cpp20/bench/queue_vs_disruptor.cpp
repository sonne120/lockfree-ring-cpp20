#include <ring/queue/spsc_queue.hpp>
#include <ring/disruptor/disruptor.hpp>

#include <chrono>
#include <cstdio>
#include <memory>
#include <thread>

using namespace ring;
using namespace ring::disruptor;
constexpr std::int64_t kN = 20'000'000;

template <class F> double ms(F&& f) {
    const auto t0 = std::chrono::steady_clock::now();
    f();
    return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
}

std::int64_t run_queue() {
    auto q = std::make_unique<queue::SpscQueue<std::int64_t, 1024>>();
    std::int64_t sum = 0;
    std::thread c([&] {
        for (std::int64_t n = 0; n < kN;) {
            if (auto v = q->try_pop()) { sum += *v; ++n; } else cpu_relax();
        }
    });
    for (std::int64_t i = 0; i < kN; ++i) while (!q->try_push(i)) cpu_relax();
    c.join();
    return sum;
}

std::int64_t run_disruptor() {
    RingBuffer<std::int64_t, SingleThreadedClaimStrategy, BusySpinWaitStrategy> rb(1024);
    std::int64_t sum = 0;
    BatchConsumer consumer(rb, [&](std::int64_t& v, std::int64_t, bool) { sum += v; });
    rb.set_gating_sequences({&consumer.sequence()});
    std::thread c([&] { consumer.run(); });
    for (std::int64_t i = 0; i < kN; ++i) {
        const auto seq = rb.claim();
        rb[seq] = i;
        rb.publish(seq);
    }
    while (consumer.sequence().get_acquire() < kN - 1) cpu_relax();
    consumer.halt();
    c.join();
    return sum;
}

int main() {
    std::int64_t a = 0, b = 0;
    const double tq = ms([&] { a = run_queue(); });
    const double td = ms([&] { b = run_disruptor(); });
    std::printf("SpscQueue : %7.0f ms  %6.1f M ev/s\n", tq, kN / tq / 1000.0);
    std::printf("Disruptor : %7.0f ms  %6.1f M ev/s\n", td, kN / td / 1000.0);
    return a == b ? 0 : 1;
}
