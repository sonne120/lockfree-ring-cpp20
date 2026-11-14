#pragma once

#include "ring_buffer.hpp"

#include <cstdint>
#include <utility>
#include <vector>

namespace ring::disruptor {

// BatchConsumer: owns a Sequence and runs a handler over each available batch.
// Handler signature: void(E&, int64_t seq, bool end_of_batch)

template <typename E, ClaimStrategy Claim, WaitStrategy Wait, typename Handler>
class BatchConsumer {
public:
    BatchConsumer(RingBuffer<E, Claim, Wait>& ring, Handler handler,
                  std::vector<const Sequence*> depends_on = {})
        : barrier_(ring.new_barrier(std::move(depends_on))), handler_(std::move(handler)) {}

    const Sequence& sequence() const noexcept { return sequence_; }


    void halt() { barrier_.alert(); }

    void run() {
        std::int64_t next = sequence_.get_relaxed() + 1;
        for (;;) {
            const std::int64_t available = barrier_.wait_for(next);
            if (available < next) return;                 // alerted, nothing left
            for (; next <= available; ++next) {
                handler_(barrier_.entry(next), next, next == available);
            }
            sequence_.set_release(available);             // one store per batch
        }
    }

private:
    ConsumerBarrier<E, Claim, Wait> barrier_;
    Handler                         handler_;
    Sequence                        sequence_;
};

template <typename E, ClaimStrategy C, WaitStrategy W, typename H>
BatchConsumer(RingBuffer<E, C, W>&, H) -> BatchConsumer<E, C, W, H>;
template <typename E, ClaimStrategy C, WaitStrategy W, typename H>
BatchConsumer(RingBuffer<E, C, W>&, H, std::vector<const Sequence*>) -> BatchConsumer<E, C, W, H>;

}
