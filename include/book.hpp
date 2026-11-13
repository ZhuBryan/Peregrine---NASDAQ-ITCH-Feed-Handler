// Full-depth order books for every symbol on the feed, keyed by ITCH stock locate.
// Build with -DBASELINE to swap in std::unordered_map + std::map for comparison.
#pragma once
#include <cstdint>
#include <vector>
#ifdef BASELINE
#include <map>
#include <unordered_map>
#endif

// 8 bytes, so an order-map slot (key + Order) is 16 bytes and never straddles a cache line.
// Side shares a word with quantity; 31 bits of quantity is far above any real order size.
struct Order {
    uint32_t price;
    uint32_t qty : 31;
    uint32_t is_bid : 1;
    Order() = default;
    Order(uint32_t px, uint32_t shares, char side) : price(px), qty(shares), is_bid(side == 'B') {}
    char side() const { return is_bid ? 'B' : 'S'; }
};

#ifdef BASELINE
class OrderMap {
    std::unordered_map<uint64_t, Order> m_;
public:
    explicit OrderMap(size_t) {}
    Order* find(uint64_t k) { auto it = m_.find(k); return it == m_.end() ? nullptr : &it->second; }
    void insert(uint64_t k, Order o) { m_.emplace(k, o); }
    void erase(uint64_t k) { m_.erase(k); }
    void prefetch(uint64_t) const {}
    size_t size() const { return m_.size(); }
};
#else
// Open-addressing hash map, linear probing, backward-shift deletion (no tombstones).
// Key 0 marks an empty slot; ITCH order reference numbers are never 0.
class OrderMap {
    struct Slot { uint64_t key; Order val; };
    std::vector<Slot> s_;
    size_t mask_, size_ = 0;
    int shift_;
    size_t home(uint64_t k) const { return (k * 0x9E3779B97F4A7C15ull) >> shift_; }
    void init(size_t cap) { s_.assign(cap, Slot{}); mask_ = cap - 1; shift_ = 64 - __builtin_ctzll(cap); }
    void place(uint64_t k, Order o) {
        size_t i = home(k);
        while (s_[i].key) i = (i + 1) & mask_;
        s_[i] = {k, o};
    }
    void grow() {  // rehash stalls the feed thread; size the map up front to avoid it
        std::vector<Slot> old = std::move(s_);
        init(old.size() * 2);
        for (auto& x : old) if (x.key) place(x.key, x.val);
    }
    size_t find_idx(uint64_t k) const {
        for (size_t i = home(k);; i = (i + 1) & mask_) {
            if (s_[i].key == k) return i;
            if (!s_[i].key) return SIZE_MAX;
        }
    }
public:
    explicit OrderMap(size_t cap_pow2) { init(cap_pow2); }
    Order* find(uint64_t k) { size_t i = find_idx(k); return i == SIZE_MAX ? nullptr : &s_[i].val; }
    void insert(uint64_t k, Order o) {
        if ((size_ + 1) * 2 > s_.size()) grow();  // max load factor 0.5
        place(k, o);
        ++size_;
    }
    void erase(uint64_t k) {
        size_t i = find_idx(k);
        if (i == SIZE_MAX) return;
        for (size_t j = (i + 1) & mask_; s_[j].key; j = (j + 1) & mask_) {
            // s_[j] may move back into the hole at i unless its home lies in (i, j]
            if (((j - home(s_[j].key)) & mask_) >= ((j - i) & mask_)) { s_[i] = s_[j]; i = j; }
        }
        s_[i].key = 0;
        --size_;
    }
    void prefetch(uint64_t k) const { __builtin_prefetch(&s_[home(k)]); }
    size_t size() const { return size_; }
};
#endif

struct Level {
    int64_t key = 0;   // +price for bids, -price for asks, so "best" is always the max key
    uint64_t qty = 0;
    bool operator==(const Level&) const = default;
};

#ifdef BASELINE
class Side {
    std::map<int64_t, uint64_t> m_;
public:
    void add(int64_t k, uint32_t q) { m_[k] += q; }
    void sub(int64_t k, uint32_t q) {
        auto it = m_.find(k);
        if (it != m_.end() && (it->second -= q) == 0) m_.erase(it);
    }
    Level best() const { return m_.empty() ? Level{} : Level{m_.rbegin()->first, m_.rbegin()->second}; }
    void prefetch() const {}
    size_t depth() const { return m_.size(); }
};
#else
// Sorted contiguous array with the best price at the back. Nearly all activity is
// at or near the touch, so the linear scan from the back and the insert/erase
// memmove are both short.
class Side {
    std::vector<Level> v_;
    size_t pos(int64_t k) const {  // number of levels with key <= k
        size_t i = v_.size();
        while (i && v_[i - 1].key > k) --i;
        return i;
    }
public:
    void add(int64_t k, uint32_t q) {
        size_t i = pos(k);
        if (i && v_[i - 1].key == k) v_[i - 1].qty += q;
        else v_.insert(v_.begin() + i, Level{k, q});
    }
    void sub(int64_t k, uint32_t q) {
        size_t i = pos(k);
        if (i && v_[i - 1].key == k && (v_[i - 1].qty -= q) == 0) v_.erase(v_.begin() + i - 1);
    }
    Level best() const { return v_.empty() ? Level{} : v_.back(); }
    void prefetch() const { if (!v_.empty()) __builtin_prefetch(&v_.back()); }
    size_t depth() const { return v_.size(); }
};
#endif

struct Top {  // price 0 = side empty
    uint32_t bid_px = 0, ask_px = 0;
    uint64_t bid_qty = 0, ask_qty = 0;
    bool operator==(const Top&) const = default;
};

class Books {
    OrderMap orders_;
    struct Sym { Side bid, ask; };
    std::vector<Sym> syms_;  // indexed by stock locate (16-bit)

    static int64_t key(char side, uint32_t px) { return side == 'B' ? int64_t(px) : -int64_t(px); }
    Side& side(uint16_t loc, char s) { return s == 'B' ? syms_[loc].bid : syms_[loc].ask; }

public:
    uint64_t unknown_refs = 0;  // messages naming an order we never saw; 0 on a clean feed
    uint64_t oversize = 0;      // orders too large for Order::qty; 0 or the books are wrong

    explicit Books(size_t order_capacity_pow2 = 1 << 20) : orders_(order_capacity_pow2), syms_(65536) {}

    // Each mutator returns true if the symbol's best bid/offer (price or size) changed.
    bool add(uint16_t loc, uint64_t ref, char s, uint32_t shares, uint32_t px) {
        Side& sd = side(loc, s);
        const Level before = sd.best();
        oversize += shares >> 31;
        orders_.insert(ref, Order{px, shares, s});
        sd.add(key(s, px), shares);
        return !(sd.best() == before);
    }
    bool reduce(uint16_t loc, uint64_t ref, uint32_t shares) {
        Order* o = orders_.find(ref);
        if (!o) { ++unknown_refs; return false; }
        if (shares > o->qty) shares = o->qty;
        Side& sd = side(loc, o->side());
        const Level before = sd.best();
        sd.sub(key(o->side(), o->price), shares);
        if ((o->qty -= shares) == 0) orders_.erase(ref);
        return !(sd.best() == before);
    }
    bool remove(uint16_t loc, uint64_t ref) {
        Order* o = orders_.find(ref);
        if (!o) { ++unknown_refs; return false; }
        return reduce(loc, ref, o->qty);
    }
    bool replace(uint16_t loc, uint64_t old_ref, uint64_t new_ref, uint32_t shares, uint32_t px) {
        Order* o = orders_.find(old_ref);
        if (!o) { ++unknown_refs; return false; }
        const char s = o->side();  // replace keeps side, loses queue priority
        const Level before = side(loc, s).best();
        reduce(loc, old_ref, o->qty);
        add(loc, new_ref, s, shares, px);
        return !(side(loc, s).best() == before);
    }

    // Two-stage prefetch for a message that will arrive shortly (see for_each_message_lookahead).
    // Stage 1: the order's hash slot and the symbol's book header. Stage 2, once the header
    // is in cache: the top-of-book end of both level arrays.
    void prefetch_order(uint64_t ref, uint16_t loc) const { orders_.prefetch(ref); __builtin_prefetch(&syms_[loc]); }
    void prefetch_levels(uint16_t loc) const { syms_[loc].bid.prefetch(); syms_[loc].ask.prefetch(); }

    Top top(uint16_t loc) const {
        const Level b = syms_[loc].bid.best(), a = syms_[loc].ask.best();
        return Top{uint32_t(b.key), uint32_t(-a.key), b.qty, a.qty};
    }
    size_t live_orders() const { return orders_.size(); }
    size_t depth(uint16_t loc, char s) const { return s == 'B' ? syms_[loc].bid.depth() : syms_[loc].ask.depth(); }
};
