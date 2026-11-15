// --export: collects run statistics on the feed thread and writes them as JSON.
// The output is the data behind docs/index.html (see scripts/build_page.py).
#pragma once
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <numeric>
#include <string>
#include <vector>

#include "book.hpp"
#include "itch.hpp"

class Exporter {
    static constexpr size_t kLevels = 5;                // depth captured per side
    static constexpr uint64_t kSec = 1'000'000'000ull;  // ns
    static constexpr uint64_t kSnapEvery = 5 * kSec;
    static constexpr size_t kMinutes = 24 * 60;

    std::vector<uint64_t> type_count_ = std::vector<uint64_t>(256);
    std::vector<uint64_t> msgs_ = std::vector<uint64_t>(kMinutes), volume_ = msgs_, live_ = msgs_, bbo_ = msgs_,
                          watch_volume_ = msgs_;
    std::vector<uint32_t> sym_msgs_ = std::vector<uint32_t>(65536);
    std::vector<uint32_t> snaps_;  // flat: second-of-day, then kLevels x (price, size) for bids, then asks
    size_t minute_ = 0;
    uint64_t bbo_mark_ = 0, next_snap_ = 0;

    void close_minute(const Books& books, uint64_t bbo_changes) {
        live_[minute_] = books.live_orders();
        bbo_[minute_] = bbo_changes - bbo_mark_;
        bbo_mark_ = bbo_changes;
    }
    static uint32_t pct(const std::vector<uint32_t>& sorted, double p) {
        return sorted.empty() ? 0u : sorted[std::min(sorted.size() - 1, size_t(p * sorted.size()))];
    }

public:
    // Call for every message, before it is applied to the books.
    // watch_loc is the locate of the symbol to snapshot, or -1 if its directory entry has not arrived.
    void record(const uint8_t* m, const Books& books, int watch_loc, uint64_t bbo_changes) {
        const uint64_t ts = itch::be48(m + 5);
        const size_t minute = std::min<uint64_t>(ts / (60 * kSec), kMinutes - 1);
        const uint16_t loc = itch::be16(m + 1);
        ++type_count_[m[0]];
        ++msgs_[minute];
        ++sym_msgs_[loc];
        if (m[0] == 'E' || m[0] == 'C') {
            const uint32_t shares = itch::be32(m + 19);
            volume_[minute] += shares;
            if (loc == watch_loc) watch_volume_[minute] += shares;
        }
        if (minute != minute_) {
            close_minute(books, bbo_changes);
            minute_ = minute;
        }
        if (watch_loc >= 0 && ts >= next_snap_ && ts >= itch::kRegularOpenNs && ts <= itch::kRegularCloseNs) {
            snaps_.push_back(uint32_t(ts / kSec));
            for (char side : {'B', 'S'})
                for (size_t n = 0; n < kLevels; ++n) {
                    const Level l = books.level(uint16_t(watch_loc), side, n);
                    snaps_.push_back(uint32_t(l.key));
                    snaps_.push_back(uint32_t(l.qty));
                }
            next_snap_ = (ts / kSnapEvery + 1) * kSnapEvery;
        }
    }

    struct Summary {
        uint64_t messages, book_msgs, bbo_changes, unknown_refs, crossed, last_ts_ns;
        size_t live_orders;
        double secs;
        bool state_ok;
        const std::vector<uint32_t>& feed_ns;   // sorted latency samples
        const std::vector<uint32_t>& queue_ns;  // sorted
        const uint64_t (*signal)[4];            // 10 rows: quote-start n, up; just-before n, up
        const std::vector<std::string>& symbols;
        const std::string& watch;
    };

    bool write(const char* path, const Books& books, const Summary& s) {
        close_minute(books, s.bbo_changes);
        std::FILE* f = std::fopen(path, "w");
        if (!f) return false;
        auto array = [&](const char* name, const std::vector<uint64_t>& v, size_t from, size_t to) {
            std::fprintf(f, "\"%s\":[", name);
            for (size_t i = from; i < to; ++i) std::fprintf(f, "%s%llu", i > from ? "," : "", (unsigned long long)v[i]);
            std::fprintf(f, "],");
        };
        std::fprintf(f, "{\"messages\":%llu,\"book_msgs\":%llu,\"bbo_changes\":%llu,\"unknown_refs\":%llu,",
                     (unsigned long long)s.messages, (unsigned long long)s.book_msgs, (unsigned long long)s.bbo_changes,
                     (unsigned long long)s.unknown_refs);
        std::fprintf(f, "\"crossed\":%llu,\"live_end\":%zu,\"secs\":%.3f,\"state_ok\":%s,\"last_ts\":%llu,",
                     (unsigned long long)s.crossed, s.live_orders, s.secs, s.state_ok ? "true" : "false",
                     (unsigned long long)(s.last_ts_ns / kSec));
        std::fprintf(f, "\"feed_ns\":[%u,%u,%u],\"queue_ns\":[%u,%u,%u],", pct(s.feed_ns, 0.5), pct(s.feed_ns, 0.99),
                     pct(s.feed_ns, 0.999), pct(s.queue_ns, 0.5), pct(s.queue_ns, 0.99), pct(s.queue_ns, 0.999));

        std::fprintf(f, "\"types\":{");
        const char* sep = "";
        for (int c = 'A'; c <= 'z'; ++c)
            if (type_count_[c]) {
                std::fprintf(f, "%s\"%c\":%llu", sep, c, (unsigned long long)type_count_[c]);
                sep = ",";
            }
        std::fprintf(f, "},");

        const size_t pre_open = 4 * 60, post_close = 20 * 60;  // system hours, in minutes
        array("min_msgs", msgs_, pre_open, post_close);
        array("min_vol", volume_, pre_open, post_close);
        array("min_live", live_, pre_open, post_close);
        array("min_bbo", bbo_, pre_open, post_close);
        array("watch_vol", watch_volume_, 9 * 60 + 30, 16 * 60);

        std::vector<uint64_t> hist(201);  // 10 ns buckets up to 2 us; the last bucket holds everything slower
        for (uint32_t ns : s.feed_ns) ++hist[std::min<uint32_t>(ns / 10, 200)];
        array("lat_hist", hist, 0, hist.size());

        std::vector<uint32_t> order(sym_msgs_.size());
        std::iota(order.begin(), order.end(), 0u);
        const size_t top = 15;
        std::partial_sort(order.begin(), order.begin() + top, order.end(),
                          [&](uint32_t a, uint32_t b) { return sym_msgs_[a] > sym_msgs_[b]; });
        std::fprintf(f, "\"top\":[");
        for (size_t i = 0; i < top; ++i)
            std::fprintf(f, "%s[\"%s\",%u]", i ? "," : "", s.symbols[order[i]].c_str(), sym_msgs_[order[i]]);

        std::fprintf(f, "],\"signal\":[");
        for (int b = 0; b < 10; ++b)
            std::fprintf(f, "%s[%llu,%llu,%llu,%llu]", b ? "," : "", (unsigned long long)s.signal[b][0],
                         (unsigned long long)s.signal[b][1], (unsigned long long)s.signal[b][2],
                         (unsigned long long)s.signal[b][3]);

        std::fprintf(f, "],\"watch\":\"%s\",\"levels\":%zu,\"snaps\":[", s.watch.c_str(), kLevels);
        for (size_t i = 0; i < snaps_.size(); ++i) std::fprintf(f, "%s%u", i ? "," : "", snaps_[i]);
        std::fprintf(f, "]}\n");
        return std::fclose(f) == 0;
    }
};
