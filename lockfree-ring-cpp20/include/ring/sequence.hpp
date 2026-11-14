#pragma once

#include "cache.hpp"
#include <atomic>
#include <cstdint>
#include <limits>
#include <span>
#include <thread>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

namespace ring {
class alignas(kCacheLine) Sequence {
public:
    static constexpr std::int64_t kInitial = -1;

    Sequence() noexcept : value_(kInitial) {}
    explicit Sequence(std::int64_t v) noexcept : value_(v) {}

    std::int64_t get_relaxed() const noexcept {
        return value_.load(std::memory_order_relaxed);
    }

    std::int64_t get_acquire() const noexcept {
        return value_.load(std::memory_order_acquire);
    }

    void set_relaxed(std::int64_t v) noexcept {
        value_.store(v, std::memory_order_relaxed);
    }

    void set_release(std::int64_t v) noexcept {
        value_.store(v, std::memory_order_release);
    }

    // CAS for multi-producer claim.
    bool compare_and_set(std::int64_t expected, std::int64_t desired) noexcept {
        return value_.compare_exchange_strong(
            expected, desired,
            std::memory_order_acq_rel, std::memory_order_acquire);
    }

    std::int64_t fetch_add(std::int64_t n) noexcept {
        return value_.fetch_add(n, std::memory_order_acq_rel);
    }

private:
    std::atomic<std::int64_t> value_;
    // alignas on the class takes care of head padding.
    char pad_[kCacheLine - sizeof(std::atomic<std::int64_t>)];
};

static_assert(sizeof(Sequence) == kCacheLine,
              "Sequence must be exactly one cache line");
static_assert(alignof(Sequence) == kCacheLine,
              "Sequence must be cache-line aligned");

// Spin-loop hint.
inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    _mm_pause();
#else
    std::this_thread::yield();
#endif
}

using SequenceGroup = std::span<const Sequence* const>;

// Returns the minimum sequence in a group, or the fallback if the group is empty.
inline std::int64_t min_sequence(SequenceGroup g, std::int64_t fallback) noexcept {
    std::int64_t m = g.empty() ? fallback : std::numeric_limits<std::int64_t>::max();
    for (const Sequence* s : g) {
        const std::int64_t v = s->get_acquire();
        if (v < m) m = v;
    }
    return m;
}

}
