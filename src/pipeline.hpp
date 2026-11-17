// The two threads and what passes between them: the feed side (books + publishing) and
// the consumer side (a stand-in strategy). Shared by the replay binary and the tests.
#pragma once
#include <pthread.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "book.hpp"
#include "itch.hpp"
#include "spsc.hpp"
#include "tsc.hpp"

using Clock = std::chrono::steady_clock;
inline uint64_t now_ns() {
    return uint64_t(std::chrono::nanoseconds(Clock::now().time_since_epoch()).count());
}

inline void pin(int cpu) {
    if (cpu < 0) return;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (pthread_setaffinity_np(pthread_self(), sizeof set, &set) != 0)
        std::fprintf(stderr, "warning: could not pin to core %d\n", cpu);
}

struct BboEvent {
    uint64_t ts;        // exchange timestamp, ns since midnight
    uint64_t pub_tick;  // tsc::now() at publish on 1 in 16 events, else 0 (queue latency sampling)
    Top top;
    uint16_t locate;
};

struct Feed {
    Books books{1 << 23};  // pre-sized so the order map never rehashes mid-session
    SpscQueue<BboEvent>& out;
    bool conflate;
    std::vector<std::string> symbols = std::vector<std::string>(itch::kLocates);
    std::string watch_sym = {};  // --export symbol
    int watch_loc = -1;
    uint64_t book_msgs = 0, changes = 0, published = 0, conflated = 0, last_ts = 0;

    // Conflation state: symbols whose latest BBO has not been sent yet, in arrival order.
    std::vector<uint8_t> dirty = std::vector<uint8_t>(itch::kLocates);
    std::vector<uint16_t> backlog = {};
    size_t backlog_head = 0;

    bool try_push(uint16_t loc, uint64_t ts) {
        const BboEvent e{ts, (published & 15) == 0 ? tsc::now() : 0, books.top(loc), loc};
        if (!out.push(e)) return false;
        ++published;
        return true;
    }
    // Sends the current (latest) BBO of each backlogged symbol while the queue has room.
    void flush(uint64_t ts) {
        while (backlog_head < backlog.size()) {
            const uint16_t loc = backlog[backlog_head];
            if (!try_push(loc, ts)) return;
            dirty[loc] = 0;
            ++backlog_head;
        }
        backlog.clear();
        backlog_head = 0;
    }
    void publish(bool changed, uint16_t loc, uint64_t ts) {
        ++book_msgs;
        last_ts = ts;
        if (!backlog.empty()) flush(ts);
        if (!changed) return;
        ++changes;
        if (!conflate) {
            while (!try_push(loc, ts)) {
            }  // backpressure: stall the feed until the consumer catches up
        } else if (dirty[loc]) {
            ++conflated;  // an update for this symbol is already waiting; it will carry the newest state
        } else if (!backlog.empty() || !try_push(loc, ts)) {
            dirty[loc] = 1;
            backlog.push_back(loc);
        }
    }
    void finish() {
        while (!backlog.empty()) flush(0);
    }

    void directory(uint16_t loc, const char* s) {
        size_t n = 8;
        while (n && s[n - 1] == ' ') --n;
        symbols[loc].assign(s, n);
        if (!watch_sym.empty() && symbols[loc] == watch_sym) watch_loc = loc;
    }
    void add(uint16_t loc, uint64_t ts, uint64_t ref, char side, uint32_t sh, uint32_t px) {
        publish(books.add(loc, ref, side, sh, px), loc, ts);
    }
    void reduce(uint16_t loc, uint64_t ts, uint64_t ref, uint32_t sh) { publish(books.reduce(loc, ref, sh), loc, ts); }
    void remove(uint16_t loc, uint64_t ts, uint64_t ref) { publish(books.remove(loc, ref), loc, ts); }
    void replace(uint16_t loc, uint64_t ts, uint64_t o, uint64_t n, uint32_t sh, uint32_t px) {
        publish(books.replace(loc, o, n, sh, px), loc, ts);
    }
};

// Stands in for a strategy: keeps the latest BBO per symbol and, optionally, measures how
// well top-of-book size imbalance predicts the direction of the next mid-price move.
struct Consumer {
    static constexpr uint8_t kNoQuote = 255;

    bool signal = false;   // run the imbalance study
    uint64_t work_ns = 0;  // simulated work per update
    std::vector<Top> last = std::vector<Top>(itch::kLocates);
    std::vector<uint32_t> queue_lat = {};  // sampled publish-to-consume latency: ticks, then ns after the run
    uint64_t consumed = 0, crossed = 0;
    // Mid-price moves by imbalance decile. Columns: moves and up-moves with imbalance taken
    // when the quote was first posted, then the same with imbalance taken just before the
    // move. The second pair is partly mechanical: a queue about to be exhausted is small.
    uint64_t moves[10][4] = {};
    std::vector<uint8_t> quote_start = std::vector<uint8_t>(itch::kLocates, kNoQuote);  // decile each quote opened with

    static uint8_t decile(const Top& t) {
        return uint8_t(std::min<uint64_t>(9, 10 * t.bid_qty / (t.bid_qty + t.ask_qty)));
    }
    static bool one_cent_wide(const Top& t) { return t.ask_px - t.bid_px == 100; }

    void study(const BboEvent& e) {
        const Top& prev = last[e.locate];
        const bool was = prev.bid_px && prev.ask_px, is = e.top.bid_px && e.top.ask_px;
        // twice the mid-price change, or 0 if either state is one-sided
        const int64_t d = was && is ? int64_t(e.top.bid_px) + e.top.ask_px - prev.bid_px - prev.ask_px : 0;
        uint8_t& opened = quote_start[e.locate];
        if (d) {
            if (opened != kNoQuote) {
                ++moves[opened][0];
                moves[opened][1] += d > 0;
            }
            if (one_cent_wide(prev)) {
                ++moves[decile(prev)][2];
                moves[decile(prev)][3] += d > 0;
            }
        }
        // A new mid starts a new quote; remember the imbalance it opened with.
        if (!is)
            opened = kNoQuote;
        else if (d || !was)
            opened = one_cent_wide(e.top) ? decile(e.top) : kNoQuote;
    }

    void handle(const BboEvent& e) {
        if (e.pub_tick) queue_lat.push_back(tsc::sample(e.pub_tick, tsc::now()));
        ++consumed;
        if (signal && e.ts >= itch::kRegularOpenNs && e.ts < itch::kRegularCloseNs) study(e);
        last[e.locate] = e.top;
        if (e.top.bid_px && e.top.ask_px && e.top.bid_px >= e.top.ask_px) ++crossed;
        if (work_ns)
            for (const uint64_t until = now_ns() + work_ns; now_ns() < until;) {
            }
    }

    // Runs until `done` is set and the queue has been drained.
    void run(SpscQueue<BboEvent>& queue, const std::atomic<bool>& done, bool yield_when_idle) {
        BboEvent e;
        for (;;) {
            if (queue.pop(e)) {
                handle(e);
            } else if (done.load(std::memory_order_acquire)) {
                while (queue.pop(e)) handle(e);
                return;
            } else if (yield_when_idle) {
                std::this_thread::yield();
            }
        }
    }
};
