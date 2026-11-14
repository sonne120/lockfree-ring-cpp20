#pragma once

#include "sequence.hpp"

#include <atomic>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <span>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

namespace leasing::common::ring {

inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    _mm_pause();
#else
    std::this_thread::yield();
#endif
}

using SequenceGroup = std::span<const Sequence* const>;

// Slowest sequence in a group ("track sequence" arrows in the diagram).
inline std::int64_t min_sequence(SequenceGroup g, std::int64_t fallback) noexcept {
    std::int64_t m = g.empty() ? fallback : std::numeric_limits<std::int64_t>::max();
    for (const Sequence* s : g) {
        const std::int64_t v = s->get_acquire();
        if (v < m) m = v;
    }
    return m;
}

// WaitStrategy concept and implementations
template <typename W>
concept WaitStrategy = requires(W w, std::int64_t seq, const Sequence& cursor,
                                SequenceGroup deps, const std::atomic<bool>& alerted) {
    { w.wait_for(seq, cursor, deps, alerted) } -> std::same_as<std::int64_t>;
    { w.signal_all() };
};

namespace detail {
inline std::int64_t available(const Sequence& cursor, SequenceGroup deps) noexcept {
    return min_sequence(deps, cursor.get_acquire());
}
}

struct BusySpinWaitStrategy {
    std::int64_t wait_for(std::int64_t seq, const Sequence& cursor, SequenceGroup deps,
                          const std::atomic<bool>& alerted) const noexcept {
        std::int64_t a;
        while ((a = detail::available(cursor, deps)) < seq &&
               !alerted.load(std::memory_order_acquire)) {
            cpu_relax();
        }
        return a;
    }
    void signal_all() const noexcept {}
};

struct YieldingWaitStrategy {
    std::int64_t wait_for(std::int64_t seq, const Sequence& cursor, SequenceGroup deps,
                          const std::atomic<bool>& alerted) const noexcept {
        int spins = 100;
        std::int64_t a;
        while ((a = detail::available(cursor, deps)) < seq &&
               !alerted.load(std::memory_order_acquire)) {
            if (spins > 0) { --spins; cpu_relax(); }
            else           { std::this_thread::yield(); }
        }
        return a;
    }
    void signal_all() const noexcept {}
};

class BlockingWaitStrategy {
public:
    std::int64_t wait_for(std::int64_t seq, const Sequence& cursor, SequenceGroup deps,
                          const std::atomic<bool>& alerted) {
        {
            std::unique_lock lk(m_);
            cv_.wait(lk, [&] {
                return cursor.get_acquire() >= seq ||
                       alerted.load(std::memory_order_acquire);
            });
        }
        // Upstream consumers are not signalled through the cv — spin on them.
        std::int64_t a;
        while ((a = detail::available(cursor, deps)) < seq &&
               !alerted.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        return a;
    }
    void signal_all() {
        { std::lock_guard lk(m_); }   // close the check-then-sleep window
        cv_.notify_all();
    }
private:
    std::mutex              m_;
    std::condition_variable cv_;
};

//ClaimStrategy
template <typename C>
concept ClaimStrategy = requires(C c, std::int64_t s, std::size_t cap,
                                 SequenceGroup gating, Sequence& cursor) {
    { c.claim(cap, gating, cursor) } -> std::same_as<std::int64_t>;
    { c.publish(cursor, s) };
};

// One producer thread: plain integers, no CAS, cached gating sequence.
class SingleThreadedClaimStrategy {
public:
    std::int64_t claim(std::size_t capacity, SequenceGroup gating, const Sequence&) noexcept {
        const std::int64_t next = next_ + 1;
        const std::int64_t wrap = next - static_cast<std::int64_t>(capacity);
        if (wrap > cached_gate_) {                 // only touch consumer lines when needed
            std::int64_t min;
            while (wrap > (min = min_sequence(gating, next_))) cpu_relax();
            cached_gate_ = min;
        }
        next_ = next;
        return next;
    }
    void publish(Sequence& cursor, std::int64_t seq) noexcept { cursor.set_release(seq); }
private:
    std::int64_t next_        = Sequence::kInitial;
    std::int64_t cached_gate_ = Sequence::kInitial;
};

// Many producers: CAS on a claim sequence, then publish strictly in order.
class MultiThreadedClaimStrategy {
public:
    std::int64_t claim(std::size_t capacity, SequenceGroup gating, const Sequence&) noexcept {
        for (;;) {
            const std::int64_t cur  = claimed_.get_acquire();
            const std::int64_t next = cur + 1;
            const std::int64_t wrap = next - static_cast<std::int64_t>(capacity);
            if (wrap > min_sequence(gating, cur)) { cpu_relax(); continue; }
            if (claimed_.compare_and_set(cur, next)) return next;
        }
    }
    void publish(Sequence& cursor, std::int64_t seq) noexcept {
        // Wait for earlier claimers so consumers never see a gap.
        while (!cursor.compare_and_set(seq - 1, seq)) cpu_relax();
    }
private:
    Sequence claimed_;
};

//RingBuffer

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

    // Set the sequences of the last consumers in the graph. This allows the producer
    void set_gating_sequences(std::vector<const Sequence*> last_consumers) {
        gating_ = std::move(last_consumers);
    }

    // ProducerBarrier 
    std::int64_t claim() noexcept { return claim_.claim(capacity(), gating_, cursor_); }
    void publish(std::int64_t seq) {
        claim_.publish(cursor_, seq);
        wait_.signal_all();
    }

    E&       operator[](std::int64_t seq)       noexcept { return entries_[static_cast<std::size_t>(seq) & mask_]; }
    const E& operator[](std::int64_t seq) const noexcept { return entries_[static_cast<std::size_t>(seq) & mask_]; }
    //ConsumerBarrier 
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

// ConsumerBarrier: waits for sequences to become available, handles alerting.

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

//BatchConsumer
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
