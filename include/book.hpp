// Full-depth order books for every symbol on the feed, keyed by ITCH stock locate.
// Build with -DBASELINE to swap in std::unordered_map + std::map for comparison.
#pragma once
#include <sys/mman.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <type_traits>
#include <vector>

#include "itch.hpp"
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

// Open-addressing hash map from a 64-bit key to V: linear probing, backward-shift deletion
// (no tombstones). Key 0 marks an empty slot, so it cannot be stored; ITCH order reference
// numbers are never 0. V must be trivially copyable.
template <class V>
class FlatMap {
    static_assert(std::is_trivially_copyable_v<V>);
    struct Slot {
        uint64_t key;
        V val;
    };
    Slot* s_ = nullptr;
    size_t cap_ = 0, mask_ = 0, size_ = 0;
    int shift_ = 0;
    size_t home(uint64_t k) const { return (k * 0x9E3779B97F4A7C15ull) >> shift_; }
    // Lookups land on random slots across the whole table, which at full size (128 MB for the
    // order map) misses the TLB on almost every access with 4 KiB pages. Map it directly and
    // ask for transparent huge pages. A zeroed slot is an empty slot.
    static Slot* allocate(size_t cap) {
        void* m = mmap(nullptr, cap * sizeof(Slot), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (m == MAP_FAILED) {
            std::perror("FlatMap mmap");
            std::abort();
        }
        // PEREGRINE_NO_HUGEPAGES=1 turns this off, for measuring what it is worth.
        static const bool huge = !std::getenv("PEREGRINE_NO_HUGEPAGES");
        if (huge) madvise(m, cap * sizeof(Slot), MADV_HUGEPAGE);  // advisory: ignored where unsupported
        // Write every page now. A first touch that only reads maps the shared zero page, which
        // Linux will not later back with a huge page; and page faults belong outside the run.
        std::memset(m, 0, cap * sizeof(Slot));
        return static_cast<Slot*>(m);
    }
    void init(size_t cap) {
        s_ = allocate(cap);
        cap_ = cap;
        mask_ = cap - 1;
        shift_ = 64 - __builtin_ctzll(cap);
    }
    // Returns false, leaving the table unchanged, if k is already present.
    bool place(uint64_t k, const V& v) {
        size_t i = home(k);
        for (; s_[i].key; i = (i + 1) & mask_)
            if (s_[i].key == k) return false;
        s_[i] = {k, v};
        return true;
    }
    void grow() {  // a rehash stalls the caller; size the map up front to avoid it
        Slot* old = s_;
        const size_t old_cap = cap_;
        init(old_cap * 2);
        for (size_t i = 0; i < old_cap; ++i)
            if (old[i].key) place(old[i].key, old[i].val);
        munmap(old, old_cap * sizeof(Slot));
    }
    size_t find_idx(uint64_t k) const {
        for (size_t i = home(k);; i = (i + 1) & mask_) {
            if (s_[i].key == k) return i;
            if (!s_[i].key) return SIZE_MAX;
        }
    }

public:
    explicit FlatMap(size_t cap_pow2) { init(cap_pow2); }
    ~FlatMap() { munmap(s_, cap_ * sizeof(Slot)); }
    FlatMap(const FlatMap&) = delete;
    FlatMap& operator=(const FlatMap&) = delete;
    // Pointers are invalidated by insert (growth) and by erase (entries shift).
    V* find(uint64_t k) {
        size_t i = find_idx(k);
        return i == SIZE_MAX ? nullptr : &s_[i].val;
    }
    bool insert(uint64_t k, const V& v) {    // false if k is already present
        if ((size_ + 1) * 2 > cap_) grow();  // max load factor 0.5
        if (!place(k, v)) return false;
        ++size_;
        return true;
    }
    // `v` must be the pointer find(k) returned; it locates the slot without probing again.
    void erase(uint64_t, V* v) {
        const auto* slot = reinterpret_cast<const Slot*>(reinterpret_cast<const char*>(v) - offsetof(Slot, val));
        size_t i = size_t(slot - s_);
        for (size_t j = (i + 1) & mask_; s_[j].key; j = (j + 1) & mask_) {
            // s_[j] may move back into the hole at i unless its home lies in (i, j]
            if (((j - home(s_[j].key)) & mask_) >= ((j - i) & mask_)) {
                s_[i] = s_[j];
                i = j;
            }
        }
        s_[i].key = 0;
        --size_;
    }
    void prefetch(uint64_t k) const { __builtin_prefetch(&s_[home(k)]); }
    size_t size() const { return size_; }
};

#ifdef BASELINE
class OrderMap {
    std::unordered_map<uint64_t, Order> m_;

public:
    explicit OrderMap(size_t) {}
    Order* find(uint64_t k) {
        auto it = m_.find(k);
        return it == m_.end() ? nullptr : &it->second;
    }
    bool insert(uint64_t k, Order o) { return m_.emplace(k, o).second; }  // false if k is already present
    void erase(uint64_t k, Order*) { m_.erase(k); }
    void prefetch(uint64_t) const {}
    size_t size() const { return m_.size(); }
};
#else
using OrderMap = FlatMap<Order>;
#endif

struct Level {
    int64_t key = 0;  // +price for bids, -price for asks, so "best" is always the max key
    uint64_t qty = 0;
    bool operator==(const Level&) const = default;
};

#ifdef BASELINE
class Side {
    std::map<int64_t, uint64_t> m_;

public:
    void add(int64_t k, uint32_t q) { m_[k] += q; }
    bool sub(int64_t k, uint32_t q) {  // false if the level is missing or smaller than q
        auto it = m_.find(k);
        if (it == m_.end() || it->second < q) return false;
        if ((it->second -= q) == 0) m_.erase(it);
        return true;
    }
    Level best() const { return m_.empty() ? Level{} : Level{m_.rbegin()->first, m_.rbegin()->second}; }
    void prefetch() const {}
    Level level(size_t n) const {  // n-th best, 0 = best
        if (n >= m_.size()) return Level{};
        auto it = m_.rbegin();
        std::advance(it, n);
        return Level{it->first, it->second};
    }
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
        if (i && v_[i - 1].key == k)
            v_[i - 1].qty += q;
        else
            v_.insert(v_.begin() + i, Level{k, q});
    }
    bool sub(int64_t k, uint32_t q) {  // false if the level is missing or smaller than q
        const size_t i = pos(k);
        if (!i || v_[i - 1].key != k || v_[i - 1].qty < q) return false;
        if ((v_[i - 1].qty -= q) == 0) v_.erase(v_.begin() + i - 1);
        return true;
    }
    Level best() const { return v_.empty() ? Level{} : v_.back(); }
    void prefetch() const {
        if (!v_.empty()) __builtin_prefetch(&v_.back());
    }
    Level level(size_t n) const { return n < v_.size() ? v_[v_.size() - 1 - n] : Level{}; }  // n-th best, 0 = best
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
    struct Sym {
        Side bid, ask;
    };
    std::vector<Sym> syms_;  // indexed by stock locate (16-bit)

    static int64_t key(char side, uint32_t px) { return side == 'B' ? int64_t(px) : -int64_t(px); }
    Side& side(uint16_t loc, char s) { return s == 'B' ? syms_[loc].bid : syms_[loc].ask; }

    // Removes `shares` (at most the order's size) from a live order and its price level.
    bool take(uint16_t loc, uint64_t ref, Order* o, uint32_t shares) {
        Side& sd = side(loc, o->side());
        const Level before = sd.best();
        if (!sd.sub(key(o->side(), o->price), shares)) ++inconsistent;
        if (shares == o->qty)
            orders_.erase(ref, o);
        else
            o->qty -= shares;
        return !(sd.best() == before);
    }

public:
    uint64_t unknown_refs = 0;    // messages naming an order we never saw; 0 on a clean feed
    uint64_t oversize = 0;        // orders too large for Order::qty; 0 or the books are wrong
    uint64_t duplicate_refs = 0;  // adds reusing a live reference number; ignored
    // An order's price level was missing or too small when the order was reduced. The order
    // map does not store the locate (that would grow every slot past 16 bytes), so this is
    // also how a message carrying the wrong locate for its order shows up.
    uint64_t inconsistent = 0;

    explicit Books(size_t order_capacity_pow2 = 1 << 20) : orders_(order_capacity_pow2), syms_(itch::kLocates) {}

    // Each mutator returns true if the symbol's best bid/offer (price or size) changed.
    bool add(uint16_t loc, uint64_t ref, char s, uint32_t shares, uint32_t px) {
        if (ref == 0) {  // reserved as the order map's empty-slot marker; never sent by the exchange
            ++unknown_refs;
            return false;
        }
        Side& sd = side(loc, s);
        const Level before = sd.best();
        if (!orders_.insert(ref, Order{px, shares, s})) {
            ++duplicate_refs;
            return false;
        }
        oversize += shares >> 31;
        sd.add(key(s, px), shares);
        return !(sd.best() == before);
    }
    bool reduce(uint16_t loc, uint64_t ref, uint32_t shares) {
        Order* o = orders_.find(ref);
        if (!o) {
            ++unknown_refs;
            return false;
        }
        return take(loc, ref, o, shares < o->qty ? shares : o->qty);
    }
    bool remove(uint16_t loc, uint64_t ref) {
        Order* o = orders_.find(ref);
        if (!o) {
            ++unknown_refs;
            return false;
        }
        return take(loc, ref, o, o->qty);
    }
    bool replace(uint16_t loc, uint64_t old_ref, uint64_t new_ref, uint32_t shares, uint32_t px) {
        Order* o = orders_.find(old_ref);
        if (!o) {
            ++unknown_refs;
            return false;
        }
        const char s = o->side();  // replace keeps side, loses queue priority
        const Level before = side(loc, s).best();
        take(loc, old_ref, o, o->qty);
        add(loc, new_ref, s, shares, px);
        return !(side(loc, s).best() == before);
    }

    // Two-stage prefetch for a message that will arrive shortly (see for_each_message_lookahead).
    // Stage 1: the order's hash slot and the symbol's book header. Stage 2, once the header
    // is in cache: the top-of-book end of both level arrays.
    void prefetch_order(uint64_t ref, uint16_t loc) const {
        orders_.prefetch(ref);
        __builtin_prefetch(&syms_[loc]);
    }
    void prefetch_levels(uint16_t loc) const {
        syms_[loc].bid.prefetch();
        syms_[loc].ask.prefetch();
    }

    Top top(uint16_t loc) const {
        const Level b = syms_[loc].bid.best(), a = syms_[loc].ask.best();
        return Top{uint32_t(b.key), uint32_t(-a.key), b.qty, a.qty};
    }
    // n-th best level of one side as (price, size); price 0 = no such level.
    Level level(uint16_t loc, char s, size_t n) const {
        const Level l = s == 'B' ? syms_[loc].bid.level(n) : syms_[loc].ask.level(n);
        return Level{l.key < 0 ? -l.key : l.key, l.qty};
    }
    size_t live_orders() const { return orders_.size(); }
    size_t depth(uint16_t loc, char s) const { return s == 'B' ? syms_[loc].bid.depth() : syms_[loc].ask.depth(); }
};
