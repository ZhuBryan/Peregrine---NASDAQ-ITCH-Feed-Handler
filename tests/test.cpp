// Self-checking tests, no framework. Exits non-zero on first failure.
//  1. decoder: known bytes -> known fields
//  2. differential: random ITCH stream through decoder + Books vs. a brute-force reference
//  3. SPSC queue: ordering and completeness across two threads
//  4. order map vs std::unordered_map under churn; malformed input is skipped and counted
#include <cstdio>
#include <cstdlib>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "book.hpp"
#include "itch.hpp"
#include "spsc.hpp"

#define CHECK(c)                                                              \
    do {                                                                      \
        if (!(c)) {                                                           \
            std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); \
            std::exit(1);                                                     \
        }                                                                     \
    } while (0)

using Buf = std::vector<uint8_t>;
static void put(Buf& b, uint64_t v, int bytes) {
    for (int i = bytes - 1; i >= 0; --i) b.push_back(uint8_t(v >> (8 * i)));
}
static void header(Buf& b, size_t len, char type, uint16_t loc, uint64_t ts) {
    put(b, len, 2);
    b.push_back(type);
    put(b, loc, 2);
    put(b, 0, 2);
    put(b, ts, 6);
}
static void enc_add(Buf& b, uint16_t loc, uint64_t ts, uint64_t ref, char side, uint32_t sh, uint32_t px) {
    header(b, 36, 'A', loc, ts);
    put(b, ref, 8);
    b.push_back(side);
    put(b, sh, 4);
    for (char c : {'T', 'E', 'S', 'T', ' ', ' ', ' ', ' '}) b.push_back(c);
    put(b, px, 4);
}
static void enc_exec(Buf& b, uint16_t loc, uint64_t ts, uint64_t ref, uint32_t sh) {
    header(b, 31, 'E', loc, ts);
    put(b, ref, 8);
    put(b, sh, 4);
    put(b, 0, 8);
}
static void enc_cancel(Buf& b, uint16_t loc, uint64_t ts, uint64_t ref, uint32_t sh) {
    header(b, 23, 'X', loc, ts);
    put(b, ref, 8);
    put(b, sh, 4);
}
static void enc_delete(Buf& b, uint16_t loc, uint64_t ts, uint64_t ref) {
    header(b, 19, 'D', loc, ts);
    put(b, ref, 8);
}
static void enc_replace(Buf& b, uint16_t loc, uint64_t ts, uint64_t o, uint64_t n, uint32_t sh, uint32_t px) {
    header(b, 35, 'U', loc, ts);
    put(b, o, 8);
    put(b, n, 8);
    put(b, sh, 4);
    put(b, px, 4);
}

static void test_decoder() {
    Buf b;
    enc_add(b, 7, 0x0000123456789ABCull, 0x1122334455667788ull, 'S', 300, 1234500);
    struct H {
        int calls = 0;
        void directory(uint16_t, const char*) { CHECK(false); }
        void add(uint16_t loc, uint64_t ts, uint64_t ref, char side, uint32_t sh, uint32_t px) {
            ++calls;
            CHECK(loc == 7 && ts == 0x123456789ABCull && ref == 0x1122334455667788ull);
            CHECK(side == 'S' && sh == 300 && px == 1234500);
        }
        void reduce(uint16_t, uint64_t, uint64_t, uint32_t) { CHECK(false); }
        void remove(uint16_t, uint64_t, uint64_t) { CHECK(false); }
        void replace(uint16_t, uint64_t, uint64_t, uint64_t, uint32_t, uint32_t) { CHECK(false); }
    } h;
    CHECK(itch::for_each_message(b.data(), b.size(), [&](const uint8_t* m) { itch::decode(m, h); }) == b.size());
    CHECK(h.calls == 1);
    // a truncated tail must not be consumed
    CHECK(itch::for_each_message(b.data(), b.size() - 1, [&](const uint8_t*) { CHECK(false); }) == 0);
}

// Brute-force reference: just the set of live orders; top of book by full scan.
struct Ref {
    struct O {
        uint16_t loc;
        char side;
        uint32_t px, qty;
    };
    std::map<uint64_t, O> live;
    Top top(uint16_t loc) const {
        Top t;
        for (auto& [_, o] : live) {
            if (o.loc != loc) continue;
            if (o.side == 'B') {
                if (o.px > t.bid_px) {
                    t.bid_px = o.px;
                    t.bid_qty = 0;
                }
                if (o.px == t.bid_px) t.bid_qty += o.qty;
            } else {
                if (!t.ask_px || o.px < t.ask_px) {
                    t.ask_px = o.px;
                    t.ask_qty = 0;
                }
                if (o.px == t.ask_px) t.ask_qty += o.qty;
            }
        }
        return t;
    }
};

// If `emit` is set, the generated stream is also written there (used as CI input for the UDP test).
static void test_differential(unsigned seed, int n_msgs, std::FILE* emit = nullptr) {
    std::mt19937 rng(seed);
    auto rnd = [&](uint32_t n) { return uint32_t(rng() % n); };
    Ref ref;
    Books books(1 << 4);  // tiny initial capacity to force rehashing
    uint16_t cur_loc = 0;
    bool cur_changed = false;
    struct H {
        Books& b;
        uint16_t& loc;
        bool& changed;
        void directory(uint16_t, const char*) {}
        void add(uint16_t l, uint64_t, uint64_t r, char s, uint32_t sh, uint32_t px) {
            loc = l;
            changed = b.add(l, r, s, sh, px);
        }
        void reduce(uint16_t l, uint64_t, uint64_t r, uint32_t sh) {
            loc = l;
            changed = b.reduce(l, r, sh);
        }
        void remove(uint16_t l, uint64_t, uint64_t r) {
            loc = l;
            changed = b.remove(l, r);
        }
        void replace(uint16_t l, uint64_t, uint64_t o, uint64_t n, uint32_t sh, uint32_t px) {
            loc = l;
            changed = b.replace(l, o, n, sh, px);
        }
    } h{books, cur_loc, cur_changed};

    uint64_t next_ref = 1;
    for (int i = 0; i < n_msgs; ++i) {
        Buf b;
        const uint32_t op = rnd(100);
        auto pick = [&] {
            auto it = ref.live.begin();
            std::advance(it, rnd(uint32_t(ref.live.size())));
            return it;
        };
        // Tight price band so levels are shared, emptied and re-created constantly.
        auto new_px = [&](char s) { return s == 'B' ? 9990 + rnd(10) : 10000 + rnd(10); };
        if (ref.live.empty() || op < 45) {
            const uint16_t loc = uint16_t(1 + rnd(3));
            const char s = rnd(2) ? 'B' : 'S';
            const uint32_t px = new_px(s), q = 1 + rnd(500);
            enc_add(b, loc, i, next_ref, s, q, px);
            ref.live[next_ref++] = {loc, s, px, q};
        } else if (op < 70) {  // partial or full execution / cancel
            auto it = pick();
            const uint32_t q = 1 + rnd(it->second.qty);
            (op < 58 ? enc_exec : enc_cancel)(b, it->second.loc, i, it->first, q);
            if ((it->second.qty -= q) == 0) ref.live.erase(it);
        } else if (op < 88) {
            auto it = pick();
            enc_delete(b, it->second.loc, i, it->first);
            ref.live.erase(it);
        } else {
            auto it = pick();
            Ref::O o = it->second;
            o.px = new_px(o.side);
            o.qty = 1 + rnd(500);
            enc_replace(b, o.loc, i, it->first, next_ref, o.qty, o.px);
            ref.live.erase(it);
            ref.live[next_ref++] = o;
        }
        if (emit) std::fwrite(b.data(), 1, b.size(), emit);
        Top before[4];
        for (uint16_t loc = 1; loc <= 3; ++loc) before[loc] = books.top(loc);
        itch::for_each_message(b.data(), b.size(), [&](const uint8_t* m) { itch::decode(m, h); });
        for (uint16_t loc = 1; loc <= 3; ++loc) CHECK(books.top(loc) == ref.top(loc));
        CHECK(cur_changed == !(books.top(cur_loc) == before[cur_loc]));  // "BBO changed" flag is exact
        CHECK(books.live_orders() == ref.live.size());
    }
    CHECK(books.unknown_refs == 0);
}

static void test_spsc() {
    const uint64_t N = 2'000'000;
    SpscQueue<uint64_t> q(1 << 10);  // small, so both full and empty paths are hit
    uint64_t fulls = 0;
    std::thread prod([&] {
        for (uint64_t i = 1; i <= N; ++i)
            while (!q.push(i)) ++fulls;
    });
    uint64_t expect = 1, v;
    while (expect <= N)
        if (q.pop(v)) {
            CHECK(v == expect);
            ++expect;
        }
    prod.join();
    CHECK(!q.pop(v));
}

// OrderMap against std::unordered_map under heavy insert/erase churn in a small table,
// so probe runs collide, wrap around the end of the array, and the table grows.
static void test_order_map() {
    std::mt19937_64 rng(7);
    OrderMap map(1 << 3);
    std::unordered_map<uint64_t, uint32_t> ref;
    std::vector<uint64_t> keys;
    for (int i = 0; i < 200000; ++i) {
        if (keys.empty() || rng() % 100 < 52) {
            const uint64_t k = (rng() % 4096) * 8 + 1;  // clustered keys
            if (ref.count(k)) continue;
            const uint32_t px = uint32_t(rng());
            map.insert(k, Order{px, 100, 'B'});
            ref[k] = px;
            keys.push_back(k);
        } else {
            const size_t at = rng() % keys.size();
            const uint64_t k = keys[at];
            keys[at] = keys.back();
            keys.pop_back();
            Order* o = map.find(k);
            CHECK(o && o->price == ref[k]);
            map.erase(k, o);
            ref.erase(k);
            CHECK(!map.find(k));
        }
        CHECK(map.size() == ref.size());
        if (i % 1000 == 0)
            for (const auto& [k, px] : ref) CHECK(map.find(k) && map.find(k)->price == px);
    }
}

// Messages too short for their type must be skipped and counted, never decoded.
static void test_malformed() {
    struct H {
        int adds = 0;
        void directory(uint16_t, const char*) {}
        void add(uint16_t, uint64_t, uint64_t, char, uint32_t, uint32_t) { ++adds; }
        void reduce(uint16_t, uint64_t, uint64_t, uint32_t) { CHECK(false); }
        void remove(uint16_t, uint64_t, uint64_t) { CHECK(false); }
        void replace(uint16_t, uint64_t, uint64_t, uint64_t, uint32_t, uint32_t) { CHECK(false); }
    } h;
    Buf b;
    put(b, 0, 2);  // zero-length message
    put(b, 5, 2);  // an 'E' with only 5 bytes
    for (char c : {'E', 'x', 'x', 'x', 'x'}) b.push_back(uint8_t(c));
    header(b, 12, 'D', 1, 0);  // a 'D' cut off inside its order reference
    b.push_back(0);
    enc_add(b, 1, 0, 42, 'B', 100, 10000);  // one good message after the bad ones
    for (bool lookahead : {false, true}) {
        uint64_t bad = 0;
        h.adds = 0;
        auto f = [&](const uint8_t* m) { itch::decode(m, h); };
        auto peek = [&](const uint8_t* m) {
            uint16_t loc;
            uint64_t ref;
            itch::peek_order(m, loc, ref);
        };
        const size_t used = lookahead ? itch::for_each_message_lookahead(b.data(), b.size(), 2, peek, peek, f, &bad)
                                      : itch::for_each_message(b.data(), b.size(), f, &bad);
        CHECK(used == b.size() && bad == 3 && h.adds == 1);
    }
    Books books(1 << 4);
    CHECK(!books.add(1, 0, 'B', 100, 10000) && books.unknown_refs == 1 && books.live_orders() == 0);  // ref 0
}

int main(int argc, char** argv) {
    if (argc == 3 && std::string(argv[1]) == "--emit") {  // write a synthetic ITCH file and exit
        std::FILE* f = std::fopen(argv[2], "wb");
        CHECK(f);
        test_differential(1, 50000, f);
        std::fclose(f);
        return 0;
    }
    test_decoder();
    test_order_map();
    test_malformed();
    for (unsigned seed = 1; seed <= 5; ++seed) test_differential(seed, 20000);
    test_spsc();
    std::puts("all tests passed");
}
