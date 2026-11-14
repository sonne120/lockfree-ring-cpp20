#pragma once

#include <cstddef>
#include <new>

namespace ring {
// Cache line size for padding 
inline constexpr std::size_t kCacheLine = 128;

template <typename T>
struct alignas(kCacheLine) CacheLinePadded {
    T value;

    char pad_[kCacheLine - (sizeof(T) % kCacheLine == 0
                            ? kCacheLine
                            : sizeof(T) % kCacheLine)];

    CacheLinePadded() = default;
    explicit CacheLinePadded(const T& v) : value(v) {}
    explicit CacheLinePadded(T&& v) : value(static_cast<T&&>(v)) {}

    T&       operator*()        { return value; }
    const T& operator*() const  { return value; }
    T*       operator->()       { return &value; }
    const T* operator->() const { return &value; }
};

} 
