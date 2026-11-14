#pragma once
// ClaimStrategy: how a producer claims and publishes a sequence (single / multi producer).

#include "../sequence.hpp"

#include <concepts>
#include <cstddef>
#include <cstdint>

namespace ring::disruptor {

// Concept for claim strategies: must provide claim() and publish() methods.
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

}
