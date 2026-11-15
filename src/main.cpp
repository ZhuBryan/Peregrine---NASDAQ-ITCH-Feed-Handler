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
//   --export SYM PATH  write run statistics, per-minute activity and 5-second depth
//                snapshots of SYM (09:30-16:00) to PATH as JSON, for docs/index.html
//   --queue N    SPSC capacity, power of two (default 65536). With --conflate keep it
//                small: a deep queue hands the consumer stale updates before conflation starts
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "book.hpp"
#include "export.hpp"
#include "itch.hpp"
#include "mold.hpp"
#include "pipeline.hpp"
#include "spsc.hpp"

// `v` must be sorted.
static void print_pcts(const char* label, const std::vector<uint32_t>& v, const char* note) {
    auto pct = [&](double p) { return v.empty() ? 0u : v[std::min(v.size() - 1, size_t(p * v.size()))]; };
    std::printf("%s p50 %u  p99 %u  p99.9 %u  max %u ns%s\n", label, pct(0.50), pct(0.99), pct(0.999),
                v.empty() ? 0u : v.back(), note);
}

struct Options {
    const char* file = nullptr;  // "-" reads stdin
    const char* udp_ip = nullptr;
    const char* export_sym = nullptr;
    const char* export_path = nullptr;
    int udp_port = 0, pin_feed = -1, pin_cons = -1;
    bool conflate = false, signal = false;
    uint64_t work_ns = 0;
    size_t queue_cap = 1 << 16;
    std::vector<std::string> want;  // symbols to print at the end
};

static Options parse_args(int argc, char** argv) {
    Options opt;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        if (a == "--conflate")
            opt.conflate = true;
        else if (a == "--signal")
            opt.signal = true;
        else if (a == "--pin" && i + 1 < argc) {
            if (std::sscanf(argv[++i], "%d,%d", &opt.pin_feed, &opt.pin_cons) != 2) {
                std::fprintf(stderr, "--pin F,C\n");
                std::exit(1);
            }
        } else if (a == "--work" && i + 1 < argc)
            opt.work_ns = std::strtoull(argv[++i], nullptr, 10);
        else if (a == "--export" && i + 2 < argc) {
            opt.export_sym = argv[++i];
            opt.export_path = argv[++i];
        } else if (a == "--queue" && i + 1 < argc)
            opt.queue_cap = std::strtoull(argv[++i], nullptr, 10);
        else if (a == "--udp" && i + 2 < argc) {
            opt.udp_ip = argv[++i];
            opt.udp_port = std::atoi(argv[++i]);
        } else if (a.size() > 1 && a[0] == '-') {
            std::fprintf(stderr, "unknown or incomplete option: %s\n", a.c_str());
            std::exit(1);
        } else if (!opt.file && !opt.udp_ip)
            opt.file = argv[i];
        else
            opt.want.push_back(a);
    }
    if (opt.queue_cap < 2 || (opt.queue_cap & (opt.queue_cap - 1))) {
        std::fprintf(stderr, "--queue must be a power of two\n");
        std::exit(1);
    }
    if (!opt.file && !opt.udp_ip) {
        std::fprintf(stderr,
                     "usage: %s [--conflate] [--signal] [--pin F,C] [--work NS] [--queue N] (<file.itch> | --udp <ip> "
                     "<port>) [SYMBOL ...]\n",
                     argv[0]);
        std::exit(1);
    }

    return opt;
}

// Replays from stdin, e.g. `gunzip -c day.gz | itchfh -`, for files too big to keep
// uncompressed. The returned time counts processing only, not time blocked on the pipe.
template <class Replay>
static double replay_stdin(Replay&& replay, Feed& feed) {
    std::vector<uint8_t> buf(16 << 20);
    size_t have = 0;
    double secs = 0;
    for (ssize_t n; (n = read(STDIN_FILENO, buf.data() + have, buf.size() - have)) != 0;) {
        if (n < 0) {
            if (errno == EINTR) continue;
            std::perror("stdin");
            break;
        }
        have += size_t(n);
        const auto t0 = Clock::now();
        const size_t used = replay(buf.data(), have);
        secs += std::chrono::duration<double>(Clock::now() - t0).count();
        std::memmove(buf.data(), buf.data() + used, have - used);  // keep the partial tail message
        have -= used;
    }
    feed.finish();
    return secs;
}

template <class Replay>
static double replay_file(const char* path, Replay&& replay, Feed& feed) {
    const int fd = open(path, O_RDONLY);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) != 0) {
        std::perror(path);
        std::exit(1);
    }
    // MAP_POPULATE pre-faults the file so disk I/O stays out of the timed region.
    void* map = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (map == MAP_FAILED) {
        std::perror("mmap");
        std::exit(1);
    }
    const auto t0 = Clock::now();
    replay(static_cast<const uint8_t*>(map), st.st_size);
    feed.finish();
    return std::chrono::duration<double>(Clock::now() - t0).count();
}

struct Run {
    uint64_t msgs = 0, malformed = 0;
    bool complete = true;  // false if a UDP session ended early
    double secs = 0;
    std::vector<uint32_t> feed_ns = {};  // sampled per-message latency
    size_t mismatches = 0;               // symbols where the consumer's view differs from the books
};

static void report(const Options& opt, const Feed& feed, const Consumer& consumer, const Run& run) {
    const Books& books = feed.books;
    auto ull = [](uint64_t v) { return (unsigned long long)v; };
    std::printf("messages        %llu (%llu book-changing), feed time up to %02llu:%02llu:%02llu\n", ull(run.msgs),
                ull(feed.book_msgs), ull(feed.last_ts / 3'600'000'000'000ull),
                ull(feed.last_ts / 60'000'000'000ull % 60), ull(feed.last_ts / 1'000'000'000ull % 60));
    std::printf("bbo changes     %llu, %llu published, %llu consumed, %llu conflated\n", ull(feed.changes),
                ull(feed.published), ull(consumer.consumed), ull(feed.conflated));
    std::printf("integrity       %llu unknown order refs, %llu crossed BBOs, final consumer state %s\n",
                ull(books.unknown_refs), ull(consumer.crossed), run.mismatches ? "MISMATCH" : "matches books");
    if (run.malformed) std::printf("WARNING         %llu malformed messages skipped\n", ull(run.malformed));
    if (!run.complete) std::printf("WARNING         sender went silent before the end of the session\n");
    if (books.duplicate_refs)
        std::printf("WARNING         %llu adds reused a live order reference\n", ull(books.duplicate_refs));
    if (books.inconsistent)
        std::printf("WARNING         %llu reductions found no matching price level; books are not trustworthy\n",
                    ull(books.inconsistent));
    if (books.oversize)
        std::printf("WARNING         %llu orders exceeded 2^31 shares; books are not trustworthy\n",
                    ull(books.oversize));
    std::printf("live orders     %zu at end\n", books.live_orders());
    for (const auto& sym : opt.want) {
        const auto it = std::find(feed.symbols.begin(), feed.symbols.end(), sym);
        if (it == feed.symbols.end()) {
            std::printf("%-8s not in stock directory\n", sym.c_str());
            continue;
        }
        const uint16_t loc = uint16_t(it - feed.symbols.begin());
        const Top t = consumer.last[loc];
        std::printf("%-8s bid %llu @ %.4f | ask %llu @ %.4f | depth %zu x %zu levels\n", sym.c_str(), ull(t.bid_qty),
                    t.bid_px / 1e4, ull(t.ask_qty), t.ask_px / 1e4, books.depth(loc, 'B'), books.depth(loc, 'S'));
    }
    if (opt.signal) {
        std::printf(
            "imbalance = bid size / (bid size + ask size), 1-cent spreads, 09:30-16:00; share of next mid moves "
            "that were up\n");
        std::printf("  imbalance   at quote start        just before move\n");
        for (int b = 0; b < 10; ++b) {
            const uint64_t* m = consumer.moves[b];
            std::printf("  %.1f-%.1f     %5.1f%% (%8llu)    %5.1f%% (%8llu)\n", b / 10.0, (b + 1) / 10.0,
                        m[0] ? 100.0 * m[1] / m[0] : 0.0, ull(m[0]), m[2] ? 100.0 * m[3] / m[2] : 0.0, ull(m[2]));
        }
    }
    // Timing goes last: everything above is deterministic for a given input.
    std::printf("wall time       %.3f s\n", run.secs);
    if (run.msgs)
        std::printf("throughput      %.1f M msgs/s (%.1f ns/msg avg)\n", run.msgs / run.secs / 1e6,
                    run.secs * 1e9 / run.msgs);
    print_pcts("feed latency   ", run.feed_ns, " (decode + book + publish; includes one clock read)");
    print_pcts("queue latency  ", consumer.queue_ns, " (publish to consume; includes one clock read)");
}

int main(int argc, char** argv) {
    const Options opt = parse_args(argc, argv);
    const bool from_stdin = opt.file && std::string(opt.file) == "-";

    SpscQueue<BboEvent> queue(opt.queue_cap);
    Feed feed{.out = queue, .conflate = opt.conflate};
    if (opt.export_sym) feed.watch_sym = opt.export_sym;
    Consumer consumer{.signal = opt.signal, .work_ns = opt.work_ns};
    std::atomic<bool> done{false};
    std::thread consumer_thread([&] {
        pin(opt.pin_cons);
        consumer.run(queue, done, from_stdin);  // in stdin mode the decompressor needs a core too
    });

    pin(opt.pin_feed);
    Run run;
    Exporter exporter;
    const bool exporting = opt.export_path != nullptr;
    // Per-message latency is sampled (1 in 64) so clock reads don't dominate the run.
    auto on_msg = [&](const uint8_t* m) {
        if (exporting) exporter.record(m, feed.books, feed.watch_loc, feed.changes);
        if ((run.msgs++ & 63) == 0) {
            const uint64_t a = now_ns();
            itch::decode(m, feed);
            run.feed_ns.push_back(uint32_t(std::min<uint64_t>(now_ns() - a, UINT32_MAX)));
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
            [&](const uint8_t* m) {
                if (itch::peek_order(m, loc, ref)) feed.books.prefetch_order(ref, loc);
            },
            [&](const uint8_t* m) {
                if (itch::peek_order(m, loc, ref)) feed.books.prefetch_levels(loc);
            },
            on_msg, &run.malformed);
    };

    if (from_stdin) {
        run.secs = replay_stdin(replay, feed);
    } else if (opt.file) {
        run.secs = replay_file(opt.file, replay, feed);
    } else {
        MoldReceiver rx(opt.udp_ip, uint16_t(opt.udp_port));
        const auto t0 = Clock::now();
        run.complete = rx.run(on_msg);
        feed.finish();
        run.secs = std::chrono::duration<double>(Clock::now() - t0).count();
        run.malformed = rx.malformed;
        // stderr, so stdout stays comparable with a file replay of the same data
        std::fprintf(stderr,
                     "udp: %llu packets, %llu retransmit requests, %llu messages recovered, %llu duplicates dropped, "
                     "%llu max packets buffered\n",
                     (unsigned long long)rx.packets, (unsigned long long)rx.requests, (unsigned long long)rx.recovered,
                     (unsigned long long)rx.duplicates, (unsigned long long)rx.max_buffered);
    }
    done.store(true, std::memory_order_release);
    consumer_thread.join();
    std::sort(run.feed_ns.begin(), run.feed_ns.end());
    std::sort(consumer.queue_ns.begin(), consumer.queue_ns.end());

    // End-to-end check: what the consumer believes must equal the books, for every symbol.
    for (size_t loc = 0; loc < itch::kLocates; ++loc)
        run.mismatches += !(consumer.last[loc] == feed.books.top(uint16_t(loc)));

    report(opt, feed, consumer, run);

    if (exporting) {
        const Exporter::Summary summary{
            run.msgs,          feed.book_msgs,           feed.changes, feed.books.unknown_refs, consumer.crossed,
            feed.last_ts,      feed.books.live_orders(), run.secs,     run.mismatches == 0,     run.feed_ns,
            consumer.queue_ns, consumer.moves,           feed.symbols, feed.watch_sym};
        if (!exporter.write(opt.export_path, feed.books, summary)) {
            std::perror(opt.export_path);
            return 1;
        }
    }
    return run.mismatches || !run.complete ? 1 : 0;
}
