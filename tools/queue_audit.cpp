// Audits price-time priority in a recorded ITCH session and measures fill rates by queue
// position at entry. See src/queue_audit.hpp.
// Usage: queue_audit <file.itch | -> [--json PATH] [--resize-keeps-priority] [--priority-by-reference]
#include "queue_audit.hpp"

#include <cstdio>
#include <cstring>

#include "input.hpp"
#include "itch.hpp"

static double pct(uint64_t part, uint64_t whole) {
    return whole ? 100.0 * double(part) / double(whole) : 0.0;
}

int main(int argc, char** argv) {
    const char* path = nullptr;
    const char* json = nullptr;
    bool resize_keeps_priority = false, priority_by_reference = false;
    for (int i = 1; i < argc; ++i) {
        if (!std::strcmp(argv[i], "--json") && i + 1 < argc)
            json = argv[++i];
        else if (!std::strcmp(argv[i], "--resize-keeps-priority"))
            resize_keeps_priority = true;
        else if (!std::strcmp(argv[i], "--priority-by-reference"))
            priority_by_reference = true;
        else if (!path && (argv[i][0] != '-' || !argv[i][1]))
            path = argv[i];
        else
            path = nullptr, i = argc;  // unknown option: fall through to usage
    }
    if (!path) {
        std::fprintf(stderr,
                     "usage: %s <file.itch | -> [--json PATH] [--resize-keeps-priority] [--priority-by-reference]\n",
                     argv[0]);
        return 1;
    }

    QueueAudit audit;
    audit.resize_keeps_priority = resize_keeps_priority;
    audit.priority_by_reference = priority_by_reference;
    uint64_t msgs = 0, malformed = 0;
    const double secs = for_each_chunk(path, [&](const uint8_t* p, size_t n) {
        uint16_t loc;
        uint64_t ref;
        return itch::for_each_message_lookahead(
            p, n, 8,
            [&](const uint8_t* m) {
                if (itch::peek_order(m, loc, ref)) audit.prefetch_far(ref, loc);
            },
            [&](const uint8_t* m) {
                if (itch::peek_order(m, loc, ref)) audit.prefetch_near(loc);
            },
            [&](const uint8_t* m) {
                ++msgs;
                audit.on_message(m);
            },
            &malformed);
    });

    using QA = QueueAudit;
    const QA::Stats& s = audit.stats();
    auto ull = [](uint64_t v) { return (unsigned long long)v; };
    std::printf("model                oldest = %s; a size-only replace %s its place\n",
                priority_by_reference ? "lowest reference number" : "first Add message to arrive",
                resize_keeps_priority ? "keeps" : "loses");
    std::printf("messages             %llu in %.1f s (%llu malformed)\n", ull(msgs), secs, ull(malformed));
    std::printf("adds                 %llu, of which %llu arrived after a higher-numbered order at the same level\n",
                ull(s.adds), ull(s.adds_out_of_sequence));
    std::printf("executions           %llu\n", ull(s.executions));
    std::printf("  head of queue      %llu (%.4f%%)\n", ull(s.head_of_queue), pct(s.head_of_queue, s.executions));
    std::printf("  not at best price  %llu (%.4f%%)\n", ull(s.not_best_price), pct(s.not_best_price, s.executions));
    std::printf("  not oldest         %llu (%.4f%%); orders ahead: 1: %llu, 2-5: %llu, 6-20: %llu, 21+: %llu\n",
                ull(s.not_oldest), pct(s.not_oldest, s.executions), ull(s.jumped[0]), ull(s.jumped[1]),
                ull(s.jumped[2]), ull(s.jumped[3]));
    std::printf("exceptions by time of day, message type and stock status (price | time):\n");
    for (int c = 0; c < QA::kContexts; ++c)
        for (int k = 0; k < QA::kKinds; ++k)
            for (int st = 0; st < QA::kStatuses; ++st) {
                const uint64_t* e = s.exceptions[c][k][st];
                if (e[0] || e[1])
                    std::printf("  %-18s %-16s %-30s %9llu | %9llu\n", QA::kContextLabel[c], QA::kKindLabel[k],
                                QA::kStatusLabel[st], ull(e[0]), ull(e[1]));
            }
    std::printf("not-oldest executions by how the order entered its queue and how it executed:\n");
    for (int o = 0; o < QA::kOrigins; ++o)
        for (int p = 0; p < QA::kPrices; ++p)
            if (s.not_oldest_by[o][p])
                std::printf("  %-42s %-36s %9llu\n", QA::kOriginLabel[o], QA::kPriceLabel[p],
                            ull(s.not_oldest_by[o][p]));
    std::printf("all executions by price:\n");
    for (int p = 0; p < QA::kPrices; ++p)
        std::printf("  %-36s %9llu\n", QA::kPriceLabel[p], ull(s.executions_by_price[p]));
    std::printf("fill rate by displayed shares ahead when the order joined or improved the best price:\n");
    std::printf("  %-10s %12s %10s %16s\n", "ahead", "orders", "filled", "shares filled");
    for (int b = 0; b < QA::kBuckets; ++b) {
        const QA::Entry& e = s.entry[b];
        std::printf("  %-10s %12llu %9.2f%% %15.2f%%\n", QA::kBucketLabel[b], ull(e.orders), pct(e.filled, e.orders),
                    pct(e.shares_filled, e.shares));
    }
    const bool consistent = audit.live_orders() == audit.books().live_orders() && audit.books().unknown_refs == 0 &&
                            audit.books().inconsistent == 0;
    std::printf("consistency          %zu live orders in %zu levels at end; queues %s the price-level books\n",
                audit.live_orders(), audit.live_levels(), consistent ? "agree with" : "DISAGREE with");

    if (json) {
        std::FILE* f = std::fopen(json, "w");
        if (!f) {
            std::perror(json);
            return 1;
        }
        std::fprintf(f, "{\"adds\":%llu,\"adds_out_of_sequence\":%llu,", ull(s.adds), ull(s.adds_out_of_sequence));
        std::fprintf(f, "\"executions\":%llu,\"head_of_queue\":%llu,\"not_best_price\":%llu,\"not_oldest\":%llu,",
                     ull(s.executions), ull(s.head_of_queue), ull(s.not_best_price), ull(s.not_oldest));
        std::fprintf(f, "\"jumped\":[%llu,%llu,%llu,%llu],\"entry\":[", ull(s.jumped[0]), ull(s.jumped[1]),
                     ull(s.jumped[2]), ull(s.jumped[3]));
        for (int b = 0; b < QA::kBuckets; ++b) {
            const QA::Entry& e = s.entry[b];
            std::fprintf(f, "%s[\"%s\",%llu,%llu,%llu,%llu]", b ? "," : "", QA::kBucketLabel[b], ull(e.orders),
                         ull(e.filled), ull(e.shares), ull(e.shares_filled));
        }
        std::fprintf(f, "]}\n");
        std::fclose(f);
    }
    return consistent ? 0 : 1;
}
