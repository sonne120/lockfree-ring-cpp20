#pragma once

#include "claim_strategy.hpp"
#include "../sequence.hpp"
#include "wait_strategy.hpp"

#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace ring::disruptor {

template <typename E, ClaimStrategy Claim, WaitStrategy Wait> class ConsumerBarrier;

template <typename E,
          ClaimStrategy Claim = SingleThreadedClaimStrategy,
          WaitStrategy  Wait  = YieldingWaitStrategy>
class RingBuffer {
public:
    // EntryFactory: every slot is built once, up front, and then only mutated.
    template <std::invocable Factory>
        requires std::convertible_to<std::invoke_result_t<Factory>, E>
    RingBuffer(std::size_t capacity, Factory&& factory) : mask_(capacity - 1) {
        if (capacity < 2 || (capacity & mask_) != 0) {
            throw std::invalid_argument("RingBuffer capacity must be a power of two");
        }
        entries_.reserve(capacity);
        for (std::size_t i = 0; i < capacity; ++i) entries_.emplace_back(factory());
    }
    explicit RingBuffer(std::size_t capacity) : RingBuffer(capacity, [] { return E{}; }) {}

    RingBuffer(const RingBuffer&)            = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;

    // Call once, before the producer starts, with the LAST consumers of the graph.
    void set_gating_sequences(std::vector<const Sequence*> last_consumers) {
        gating_ = std::move(last_consumers);
    }

    // --- ProducerBarrier side: claim -> fill in place -> publish.
    std::int64_t claim() noexcept { return claim_.claim(capacity(), gating_, cursor_); }
    void publish(std::int64_t seq) {
        claim_.publish(cursor_, seq);
        wait_.signal_all();
    }

    E&       operator[](std::int64_t seq)       noexcept { return entries_[static_cast<std::size_t>(seq) & mask_]; }
    const E& operator[](std::int64_t seq) const noexcept { return entries_[static_cast<std::size_t>(seq) & mask_]; }

    // --- ConsumerBarrier side.
    ConsumerBarrier<E, Claim, Wait> new_barrier(std::vector<const Sequence*> depends_on = {}) {
        return ConsumerBarrier<E, Claim, Wait>(*this, std::move(depends_on));
    }

    std::int64_t    cursor()   const noexcept { return cursor_.get_acquire(); }
    std::size_t     capacity() const noexcept { return mask_ + 1; }

private:
    friend class ConsumerBarrier<E, Claim, Wait>;

    std::size_t                  mask_;
    std::vector<E>               entries_;
    std::vector<const Sequence*> gating_;
    Sequence                     cursor_;      // last published sequence
    Claim                        claim_;
    Wait                         wait_;
};

// ------------------------------------------------------------- ConsumerBarrier

template <typename E, ClaimStrategy Claim, WaitStrategy Wait>
class ConsumerBarrier {
public:
    ConsumerBarrier(RingBuffer<E, Claim, Wait>& ring, std::vector<const Sequence*> deps)
        : ring_(&ring), deps_(std::move(deps)) {}

    std::int64_t wait_for(std::int64_t seq) {
        return ring_->wait_.wait_for(seq, ring_->cursor_, deps_, alerted_);
    }
    E& entry(std::int64_t seq) noexcept { return (*ring_)[seq]; }

    void alert() {
        alerted_.store(true, std::memory_order_release);
        ring_->wait_.signal_all();
    }
    bool alerted() const noexcept { return alerted_.load(std::memory_order_acquire); }

private:
    RingBuffer<E, Claim, Wait>*  ring_;
    std::vector<const Sequence*> deps_;
    std::atomic<bool>            alerted_{false};
};

}  // namespace ring::disruptor
