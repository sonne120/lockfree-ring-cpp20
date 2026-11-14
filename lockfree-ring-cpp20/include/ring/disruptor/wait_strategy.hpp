#pragma once

#include "../sequence.hpp"

#include <atomic>
#include <concepts>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

namespace ring::disruptor {

// Concept for wait strategies: must provide wait_for() and signal_all() methods.
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

}
