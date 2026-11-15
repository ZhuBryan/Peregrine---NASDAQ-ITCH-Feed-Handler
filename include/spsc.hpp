// Bounded lock-free single-producer / single-consumer ring buffer.
// Each side keeps a cached copy of the other side's index, so the common case
// touches no cache line owned by the other thread.
#pragma once
#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>

template <class T>
class SpscQueue {
public:
    explicit SpscQueue(size_t capacity_pow2)
        : buf_(new T[capacity_pow2]), cap_(capacity_pow2), mask_(capacity_pow2 - 1) {
        assert(capacity_pow2 >= 2 && (capacity_pow2 & mask_) == 0);
    }

    bool push(const T& v) {  // producer thread only
        const size_t h = head_.load(std::memory_order_relaxed);
        if (h - tail_cache_ == cap_) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (h - tail_cache_ == cap_) return false;  // full
        }
        buf_[h & mask_] = v;
        head_.store(h + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& out) {  // consumer thread only
        const size_t t = tail_.load(std::memory_order_relaxed);
        if (t == head_cache_) {
            head_cache_ = head_.load(std::memory_order_acquire);
            if (t == head_cache_) return false;  // empty
        }
        out = buf_[t & mask_];
        tail_.store(t + 1, std::memory_order_release);
        return true;
    }

private:
    std::unique_ptr<T[]> buf_;
    const size_t cap_, mask_;
    alignas(64) std::atomic<size_t> head_{0};  // written by producer
    size_t tail_cache_ = 0;
    alignas(64) std::atomic<size_t> tail_{0};  // written by consumer
    size_t head_cache_ = 0;
};
