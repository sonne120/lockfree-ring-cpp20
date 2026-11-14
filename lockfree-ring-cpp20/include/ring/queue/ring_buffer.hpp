#pragma once

#include <array>
#include <cstddef>
#include <optional>
#include <utility>

namespace ring::queue {
template <typename T, std::size_t Capacity>
class CircularBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of two");
    static_assert(Capacity >= 2, "Capacity must be at least 2");

public:
    using value_type = T;

    bool try_push(T v) {
        if (full()) return false;
        slots_[head_++ & kMask] = std::move(v);
        return true;
    }

    void push_overwrite(T v) {
        if (full()) ++tail_;
        slots_[head_++ & kMask] = std::move(v);
    }

    std::optional<T> try_pop() {
        if (empty()) return std::nullopt;
        return std::move(slots_[tail_++ & kMask]);
    }

    T&       operator[](std::size_t i)       noexcept { return slots_[(tail_ + i) & kMask]; }
    const T& operator[](std::size_t i) const noexcept { return slots_[(tail_ + i) & kMask]; }

    T&       front()       noexcept { return (*this)[0]; }
    const T& front() const noexcept { return (*this)[0]; }
    T&       back()        noexcept { return (*this)[size() - 1]; }
    const T& back()  const noexcept { return (*this)[size() - 1]; }

    std::size_t size()  const noexcept { return head_ - tail_; }
    bool        empty() const noexcept { return head_ == tail_; }
    bool        full()  const noexcept { return size() == Capacity; }
    void        clear() noexcept       { tail_ = head_; }

    static constexpr std::size_t capacity() noexcept { return Capacity; }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    std::array<T, Capacity> slots_{};
    std::size_t             head_ = 0;
    std::size_t             tail_ = 0;
};

}
