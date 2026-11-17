// Cycle-counter timing for latency samples. On x86 this reads the time-stamp counter. On
// CPUs with an invariant TSC (Linux lists constant_tsc and nonstop_tsc in /proc/cpuinfo)
// it ticks at a fixed rate on every core, so readings taken on two threads can be
// compared. Elsewhere it falls back to std::chrono::steady_clock.
// Samples are kept in ticks during a run and converted to nanoseconds afterwards.
#pragma once
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <vector>
#if defined(__x86_64__) || defined(__i386__)
#include <x86intrin.h>
#define PEREGRINE_HAVE_TSC 1
#endif

namespace tsc {

#ifdef PEREGRINE_HAVE_TSC
inline constexpr const char* kSource = "rdtsc";
// The fences keep the timed instructions between the two reads: lfence stops the start
// read from running early, and rdtscp followed by lfence waits for the timed work.
inline uint64_t start() {
    _mm_lfence();
    return __rdtsc();
}
inline uint64_t stop() {
    unsigned aux;
    const uint64_t t = __rdtscp(&aux);
    _mm_lfence();
    return t;
}
inline uint64_t now() {
    return __rdtsc();
}  // unfenced: a timestamp handed to another thread
#else
inline constexpr const char* kSource = "steady_clock";
inline uint64_t now() {
    using namespace std::chrono;
    return uint64_t(duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
}
inline uint64_t start() {
    return now();
}
inline uint64_t stop() {
    return now();
}
#endif

// Nanoseconds per tick, measured against steady_clock over about 20 ms.
inline double ns_per_tick() {
#ifdef PEREGRINE_HAVE_TSC
    using namespace std::chrono;
    const auto t0 = steady_clock::now();
    const uint64_t k0 = now();
    while (steady_clock::now() - t0 < milliseconds(20)) {
    }
    const auto t1 = steady_clock::now();
    const uint64_t k1 = now();
    return double(duration_cast<nanoseconds>(t1 - t0).count()) / double(k1 - k0);
#else
    return 1.0;
#endif
}

// Smallest step the counter takes, in ticks. AMD Zen parts advance the TSC in steps
// (33 ticks at 3.3 GHz, i.e. 10 ns), so this bounds the resolution of every sample.
inline uint64_t resolution_ticks() {
    uint64_t best = UINT64_MAX, prev = now();
    for (int i = 0; i < 100000; ++i) {
        const uint64_t t = now();
        if (t != prev) best = std::min(best, t - prev);
        prev = t;
    }
    return best == UINT64_MAX ? 1 : best;
}

// Median cost of an empty start()/stop() pair, in ticks: the floor under every sample.
inline uint64_t overhead_ticks() {
    std::vector<uint64_t> v(10000);
    for (auto& x : v) {
        const uint64_t a = start();
        x = stop() - a;
    }
    std::nth_element(v.begin(), v.begin() + v.size() / 2, v.end());
    return v[v.size() / 2];
}

// Converts samples from ticks to nanoseconds in place.
inline void to_ns(std::vector<uint32_t>& v, double ns_per_tick) {
    for (auto& x : v) x = uint32_t(std::min(double(x) * ns_per_tick + 0.5, 4294967295.0));
}

// A sample from two tick readings, clamped to what fits a uint32_t (about a second).
inline uint32_t sample(uint64_t from, uint64_t to) {
    return to > from ? uint32_t(std::min<uint64_t>(to - from, UINT32_MAX)) : 0;
}

}  // namespace tsc
