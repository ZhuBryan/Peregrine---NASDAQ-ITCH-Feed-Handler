// ITCH 5.0 feed handler. The feed thread decodes messages, maintains every order book
// and publishes top-of-book changes through a lock-free SPSC queue to a consumer
// ("strategy") thread.
//
// Usage: itchfh [options] <file.itch> [SYMBOL ...]       replay a file ("-" reads stdin)
//        itchfh [options] --udp <ip> <port> [SYMBOL ...] receive MoldUDP64 (see tools/moldsend)
// Options:
//   --conflate   when the consumer falls behind, keep only the latest BBO per symbol
//                (default: backpressure, which makes replay output deterministic)
//   --pin F,C    pin the feed thread to core F and the consumer to core C
//   --work NS    simulate NS nanoseconds of strategy work per BBO update
//   --signal     consumer measures how well top-of-book size imbalance predicts the
//                direction of the next mid-price move (regular hours, 1-cent spreads)
//   --queue N    SPSC capacity, power of two (default 65536). With --conflate keep it
//                small: a deep queue hands the consumer stale updates before conflation starts
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "book.hpp"
#include "itch.hpp"
#include "mold.hpp"
#include "spsc.hpp"

using Clock = std::chrono::steady_clock;
static uint64_t now_ns() { return uint64_t(std::chrono::nanoseconds(Clock::now().time_since_epoch()).count()); }

static void pin(int cpu) {
    if (cpu < 0) return;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (pthread_setaffinity_np(pthread_self(), sizeof set, &set) != 0) std::fprintf(stderr, "warning: could not pin to core %d\n", cpu);
}

struct BboEvent {
    uint64_t ts;      // exchange timestamp, ns since midnight
    uint64_t pub_ns;  // local publish time on 1 in 16 events, else 0 (queue latency sampling)
    Top top;
    uint16_t locate;
};

struct Feed {
    Books books{1 << 23};  // pre-sized so the order map never rehashes mid-session
    SpscQueue<BboEvent>& out;
    bool conflate;
    std::vector<std::string> symbols = std::vector<std::string>(65536);
    uint64_t book_msgs = 0, changes = 0, published = 0, conflated = 0, last_ts = 0;

    // Conflation state: symbols whose latest BBO has not been sent yet, in arrival order.
    std::vector<uint8_t> dirty = std::vector<uint8_t>(65536);
    std::vector<uint16_t> backlog = {};
    size_t backlog_head = 0;

    bool try_push(uint16_t loc, uint64_t ts) {
        const BboEvent e{ts, (published & 15) == 0 ? now_ns() : 0, books.top(loc), loc};
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
            while (!try_push(loc, ts)) {}  // backpressure: stall the feed until the consumer catches up
        } else if (dirty[loc]) {
            ++conflated;  // an update for this symbol is already waiting; it will carry the newest state
        } else if (!backlog.empty() || !try_push(loc, ts)) {
            dirty[loc] = 1;
            backlog.push_back(loc);
        }
    }
    void finish() { while (!backlog.empty()) flush(0); }

    void directory(uint16_t loc, const char* s) {
        size_t n = 8; while (n && s[n - 1] == ' ') --n;
        symbols[loc].assign(s, n);
    }
    void add(uint16_t loc, uint64_t ts, uint64_t ref, char side, uint32_t sh, uint32_t px) { publish(books.add(loc, ref, side, sh, px), loc, ts); }
    void reduce(uint16_t loc, uint64_t ts, uint64_t ref, uint32_t sh) { publish(books.reduce(loc, ref, sh), loc, ts); }
    void remove(uint16_t loc, uint64_t ts, uint64_t ref) { publish(books.remove(loc, ref), loc, ts); }
    void replace(uint16_t loc, uint64_t ts, uint64_t o, uint64_t n, uint32_t sh, uint32_t px) { publish(books.replace(loc, o, n, sh, px), loc, ts); }
};

static void print_pcts(const char* label, std::vector<uint32_t>& v, const char* note) {
    std::sort(v.begin(), v.end());
    auto pct = [&](double p) { return v.empty() ? 0u : v[std::min(v.size() - 1, size_t(p * v.size()))]; };
    std::printf("%s p50 %u  p99 %u  p99.9 %u  max %u ns%s\n", label, pct(0.50), pct(0.99), pct(0.999), v.empty() ? 0u : v.back(), note);
}

int main(int argc, char** argv) {
    const char* file = nullptr; const char* udp_ip = nullptr;
    int udp_port = 0, pin_feed = -1, pin_cons = -1;
    bool conflate = false, signal = false;
    uint64_t work_ns = 0;
    size_t queue_cap = 1 << 16;
    std::vector<std::string> want;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--conflate") conflate = true;
        else if (a == "--signal") signal = true;
        else if (a == "--pin" && i + 1 < argc) { if (std::sscanf(argv[++i], "%d,%d", &pin_feed, &pin_cons) != 2) { std::fprintf(stderr, "--pin F,C\n"); return 1; } }
        else if (a == "--work" && i + 1 < argc) work_ns = std::strtoull(argv[++i], nullptr, 10);
        else if (a == "--queue" && i + 1 < argc) queue_cap = std::strtoull(argv[++i], nullptr, 10);
        else if (a == "--udp" && i + 2 < argc) { udp_ip = argv[++i]; udp_port = std::atoi(argv[++i]); }
        else if (!file && !udp_ip) file = argv[i];
        else want.push_back(a);
    }
    if (queue_cap < 2 || (queue_cap & (queue_cap - 1))) { std::fprintf(stderr, "--queue must be a power of two\n"); return 1; }
    if (!file && !udp_ip) {
        std::fprintf(stderr, "usage: %s [--conflate] [--signal] [--pin F,C] [--work NS] [--queue N] (<file.itch> | --udp <ip> <port>) [SYMBOL ...]\n", argv[0]);
        return 1;
    }

    SpscQueue<BboEvent> queue(queue_cap);
    Feed feed{.out = queue, .conflate = conflate};

    // Consumer: stands in for a strategy. Keeps the latest BBO per symbol.
    std::atomic<bool> done{false};
    std::vector<Top> last(65536);
    std::vector<uint32_t> queue_lat;
    uint64_t consumed = 0, crossed = 0;
    // Mid-price moves and how many were up, by imbalance decile measured (a) when the quote
    // was first posted and (b) immediately before it moved. (b) is partly mechanical: a queue
    // that is about to be exhausted is small by construction.
    uint64_t sig0_n[10] = {}, sig0_up[10] = {}, sig_n[10] = {}, sig_up[10] = {};
    std::vector<uint8_t> quote_start(65536, 255);
    const uint64_t OPEN = 34200ull * 1'000'000'000, CLOSE = 57600ull * 1'000'000'000;  // 09:30, 16:00
    std::thread consumer([&] {
        pin(pin_cons);
        auto handle = [&](const BboEvent& e) {
            if (e.pub_ns) queue_lat.push_back(uint32_t(std::min<uint64_t>(now_ns() - e.pub_ns, UINT32_MAX)));
            ++consumed;
            if (signal && e.ts >= OPEN && e.ts < CLOSE) {
                const Top& p = last[e.locate];  // state just before this update
                const bool was = p.bid_px && p.ask_px, is = e.top.bid_px && e.top.ask_px;
                const int64_t d = was && is ? int64_t(e.top.bid_px) + e.top.ask_px - p.bid_px - p.ask_px : 0;  // 2 x mid change
                auto bucket = [](const Top& t) { return uint8_t(std::min<uint64_t>(9, 10 * t.bid_qty / (t.bid_qty + t.ask_qty))); };
                if (d) {
                    if (p.ask_px - p.bid_px == 100) { ++sig_n[bucket(p)]; sig_up[bucket(p)] += d > 0; }
                    if (quote_start[e.locate] != 255) { ++sig0_n[quote_start[e.locate]]; sig0_up[quote_start[e.locate]] += d > 0; }
                }
                // A new mid starts a new quote; remember the imbalance it opened with.
                if (!is) quote_start[e.locate] = 255;
                else if (d || !was) quote_start[e.locate] = e.top.ask_px - e.top.bid_px == 100 ? bucket(e.top) : 255;
            }
            last[e.locate] = e.top;
            if (e.top.bid_px && e.top.ask_px && e.top.bid_px >= e.top.ask_px) ++crossed;
            if (work_ns) for (const uint64_t until = now_ns() + work_ns; now_ns() < until;) {}
        };
        BboEvent e;
        for (;;) {
            if (queue.pop(e)) handle(e);
            else if (done.load(std::memory_order_acquire)) { while (queue.pop(e)) handle(e); break; }
        }
    });

    // Per-message latency is sampled (1 in 64) so clock reads don't dominate the run.
    pin(pin_feed);
    std::vector<uint32_t> samples;
    uint64_t msgs = 0;
    auto on_msg = [&](const uint8_t* m) {
        if ((msgs++ & 63) == 0) {
            const uint64_t a = now_ns();
            itch::decode(m, feed);
            samples.push_back(uint32_t(std::min<uint64_t>(now_ns() - a, UINT32_MAX)));
        } else {
            itch::decode(m, feed);
        }
    };

    // Look 16 and 8 messages ahead and prefetch the order slot, then the symbol's levels.
    auto replay = [&](const uint8_t* p, size_t n) {
        uint16_t loc;
        uint64_t ref;
        return itch::for_each_message_lookahead(
            p, n, 8,
            [&](const uint8_t* m) { if (itch::peek_order(m, loc, ref)) feed.books.prefetch_order(ref, loc); },
            [&](const uint8_t* m) { if (itch::peek_order(m, loc, ref)) feed.books.prefetch_levels(loc); },
            on_msg);
    };

    double secs = 0;
    if (file && std::string(file) == "-") {
        // Stream from stdin, e.g. `gunzip -c day.gz | itchfh -`, for files too big to keep
        // uncompressed. Wall time counts processing only, not time blocked on the pipe.
        std::vector<uint8_t> buf(16 << 20);
        size_t have = 0;
        for (ssize_t n; (n = read(STDIN_FILENO, buf.data() + have, buf.size() - have)) > 0;) {
            have += size_t(n);
            const auto t0 = Clock::now();
            const size_t used = replay(buf.data(), have);
            secs += std::chrono::duration<double>(Clock::now() - t0).count();
            std::memmove(buf.data(), buf.data() + used, have - used);  // keep the partial tail message
            have -= used;
        }
        feed.finish();
    } else if (file) {
        const int fd = open(file, O_RDONLY);
        struct stat st;
        if (fd < 0 || fstat(fd, &st) != 0) { std::perror(file); return 1; }
        // MAP_POPULATE pre-faults the file so disk I/O stays out of the timed region.
        void* map = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (map == MAP_FAILED) { std::perror("mmap"); return 1; }
        samples.reserve(st.st_size / 32 / 64 + 1024);
        const auto t0 = Clock::now();
        replay(static_cast<const uint8_t*>(map), st.st_size);
        feed.finish();
        secs = std::chrono::duration<double>(Clock::now() - t0).count();
    } else {
        MoldReceiver rx(udp_ip, uint16_t(udp_port));
        const auto t0 = Clock::now();
        rx.run(on_msg);
        feed.finish();
        secs = std::chrono::duration<double>(Clock::now() - t0).count();
        // stderr, so stdout stays comparable with a file replay of the same data
        std::fprintf(stderr, "udp: %llu packets, %llu retransmit requests, %llu messages recovered, %llu duplicates dropped, %llu max packets buffered\n",
                     (unsigned long long)rx.packets, (unsigned long long)rx.requests, (unsigned long long)rx.recovered,
                     (unsigned long long)rx.duplicates, (unsigned long long)rx.max_buffered);
    }
    done.store(true, std::memory_order_release);
    consumer.join();

    // End-to-end check: what the consumer believes must equal the books, for every symbol.
    size_t mismatches = 0;
    for (size_t loc = 0; loc < 65536; ++loc) mismatches += !(last[loc] == feed.books.top(uint16_t(loc)));

    std::printf("messages        %llu (%llu book-changing), feed time up to %02llu:%02llu:%02llu\n", (unsigned long long)msgs, (unsigned long long)feed.book_msgs,
                (unsigned long long)(feed.last_ts / 3'600'000'000'000ull), (unsigned long long)(feed.last_ts / 60'000'000'000ull % 60), (unsigned long long)(feed.last_ts / 1'000'000'000ull % 60));
    std::printf("bbo changes     %llu, %llu published, %llu consumed, %llu conflated\n", (unsigned long long)feed.changes,
                (unsigned long long)feed.published, (unsigned long long)consumed, (unsigned long long)feed.conflated);
    std::printf("integrity       %llu unknown order refs, %llu crossed BBOs, final consumer state %s\n",
                (unsigned long long)feed.books.unknown_refs, (unsigned long long)crossed, mismatches ? "MISMATCH" : "matches books");
    if (feed.books.oversize) std::printf("WARNING         %llu orders exceeded 2^31 shares; books are not trustworthy\n", (unsigned long long)feed.books.oversize);
    std::printf("live orders     %zu at end\n", feed.books.live_orders());
    for (const auto& sym : want) {
        const auto it = std::find(feed.symbols.begin(), feed.symbols.end(), sym);
        if (it == feed.symbols.end()) { std::printf("%-8s not in stock directory\n", sym.c_str()); continue; }
        const uint16_t loc = uint16_t(it - feed.symbols.begin());
        const Top t = last[loc];
        std::printf("%-8s bid %llu @ %.4f | ask %llu @ %.4f | depth %zu x %zu levels\n", sym.c_str(),
                    (unsigned long long)t.bid_qty, t.bid_px / 1e4, (unsigned long long)t.ask_qty, t.ask_px / 1e4,
                    feed.books.depth(loc, 'B'), feed.books.depth(loc, 'S'));
    }
    if (signal) {
        std::printf("imbalance = bid size / (bid size + ask size), 1-cent spreads, 09:30-16:00; share of next mid moves that were up\n");
        std::printf("  imbalance   at quote start        just before move\n");
        for (int b = 0; b < 10; ++b)
            std::printf("  %.1f-%.1f     %5.1f%% (%8llu)    %5.1f%% (%8llu)\n", b / 10.0, (b + 1) / 10.0,
                        sig0_n[b] ? 100.0 * sig0_up[b] / sig0_n[b] : 0.0, (unsigned long long)sig0_n[b],
                        sig_n[b] ? 100.0 * sig_up[b] / sig_n[b] : 0.0, (unsigned long long)sig_n[b]);
    }
    // Timing goes last: everything above is deterministic for a given input.
    std::printf("wall time       %.3f s\n", secs);
    std::printf("throughput      %.1f M msgs/s (%.1f ns/msg avg)\n", msgs / secs / 1e6, secs * 1e9 / msgs);
    print_pcts("feed latency   ", samples, " (decode + book + publish; includes one clock read)");
    print_pcts("queue latency  ", queue_lat, " (publish to consume; includes one clock read)");
    return mismatches ? 1 : 0;
}
