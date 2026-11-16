// Order-level queues layered on the price-level books. Two uses:
//   1. Audit price-time priority: when an order executes, was it at the best displayed
//      price on its side, and was it the oldest displayed order at that price?
//   2. Measure how the size already queued ahead of an order when it joins the best price
//      relates to whether it gets filled.
// Only displayed orders are visible in ITCH, so "priority" here means priority among them.
#pragma once
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <vector>

#include "book.hpp"
#include "itch.hpp"

class QueueAudit {
public:
    static constexpr int kBuckets = 8;  // shares ahead at entry; see kBucketLabel
    static constexpr const char* kBucketLabel[kBuckets] = {"0",       "1-99",      "100-199",   "200-499",
                                                           "500-999", "1000-1999", "2000-4999", "5000+"};
    enum Context { kPreOpen, kOpenCross, kRegular, kCloseCross, kAfterHours, kContexts };
    static constexpr const char* kContextLabel[kContexts] = {"before 09:30", "09:30:00-09:30:05", "regular hours",
                                                             "16:00:00-16:00:05", "after 16:00:05"};
    enum Kind { kExec, kExecPriced, kExecCross, kKinds };  // 'E', 'C' printable, 'C' non-printable
    static constexpr const char* kKindLabel[kKinds] = {"E", "C printable", "C non-printable"};
    enum Status { kTrading, kJustResumed, kNotTrading, kStatuses };  // of the stock, per 'H' messages
    static constexpr const char* kStatusLabel[kStatuses] = {"trading", "within 1 s of a status change",
                                                            "halted, paused or quote-only"};

    // How the executed order got into its queue.
    enum Origin { kAdded, kResized, kRepriced, kOrigins };
    static constexpr const char* kOriginLabel[kOrigins] = {"new order", "replace: same price, size not increased",
                                                           "replace: price changed or size increased"};
    // Execution price against the order's displayed price ('C' messages carry their own price).
    enum Price { kAtDisplay, kPricedSame, kPricedBetter, kPricedWorse, kPrices };
    static constexpr const char* kPriceLabel[kPrices] = {"E, at displayed price", "C, at displayed price",
                                                         "C, more aggressive than displayed",
                                                         "C, less aggressive than displayed"};

    // NASDAQ's ITCH specification does not say whether a replace that only shrinks an order
    // keeps its place in the queue. Run the audit under both readings and compare.
    bool resize_keeps_priority = false;
    // What "oldest" means. Add messages normally arrive in reference-number order, but
    // not always (0.26% of adds on 30 Dec 2019, mostly just after the open).
    //   false: the order whose Add message arrived first.
    //   true:  the order with the lowest reference number.
    bool priority_by_reference = false;

    struct Entry {
        uint64_t orders = 0, filled = 0, shares = 0, shares_filled = 0;
    };
    struct Stats {
        uint64_t executions = 0;      // 'E' and 'C' messages naming a live order
        uint64_t head_of_queue = 0;   // the order was the oldest at the best price on its side
        uint64_t not_best_price = 0;  // a better-priced displayed order existed on the same side
        uint64_t not_oldest = 0;      // at the best price, but older displayed orders were waiting
        uint64_t exceptions[kContexts][kKinds][kStatuses][2] = {};  // last index: 0 = price, 1 = time
        uint64_t adds = 0, adds_out_of_sequence = 0;  // adds whose reference is below the level's newest order
        uint64_t jumped[4] = {};                      // not_oldest by orders ahead: 1, 2-5, 6-20, 21+
        // Time-priority exceptions by how the order entered the queue and how it executed.
        uint64_t executions_by_price[kPrices] = {};
        uint64_t not_oldest_by[kOrigins][kPrices] = {};
        Entry entry[kBuckets];  // orders that joined or improved the best price in continuous trading
    };

private:
    static constexpr uint8_t kUntracked = 255;
    static constexpr uint64_t kSec = 1'000'000'000ull;

    struct QOrder {
        uint32_t price, qty;
        uint16_t locate;
        uint8_t is_bid, bucket, filled, origin;
        uint64_t prev, next;  // neighbours in the level's arrival-order queue; 0 = none
    };
    struct QLevel {
        uint64_t head, tail, qty;
        uint32_t count;
    };

    FlatMap<QOrder> orders_{1 << 22};
    FlatMap<QLevel> levels_{1 << 22};
    Books books_{1 << 22};
    std::vector<char> state_ = std::vector<char>(itch::kLocates);
    std::vector<uint64_t> state_since_ = std::vector<uint64_t>(itch::kLocates);
    Stats stats_;

    static uint64_t level_key(uint16_t loc, bool is_bid, uint32_t px) {
        return ((uint64_t(loc) << 33) | (uint64_t(is_bid) << 32) | px) + 1;  // +1: key 0 is reserved
    }
    static int bucket_of(uint64_t shares_ahead) {
        static constexpr uint64_t kUpper[kBuckets - 1] = {1, 100, 200, 500, 1000, 2000, 5000};
        return int(std::upper_bound(kUpper, kUpper + kBuckets - 1, shares_ahead) - kUpper);
    }
    static Context context_of(uint64_t ts) {
        const uint64_t open = itch::kRegularOpenNs, close = itch::kRegularCloseNs;
        return ts < open               ? kPreOpen
               : ts < open + 5 * kSec  ? kOpenCross
               : ts < close            ? kRegular
               : ts < close + 5 * kSec ? kCloseCross
                                       : kAfterHours;
    }
    Status status_of(uint16_t loc, uint64_t ts) const {
        if (state_[loc] != 'T') return kNotTrading;
        return ts - state_since_[loc] <= kSec ? kJustResumed : kTrading;
    }

    void add(uint16_t loc, uint64_t ts, uint64_t ref, char side, uint32_t shares, uint32_t px, Origin origin = kAdded) {
        if (ref == 0 || orders_.find(ref)) return;
        const bool is_bid = side == 'B';
        const uint64_t key = level_key(loc, is_bid, px);
        QLevel* lvl = levels_.find(key);
        if (!lvl) {
            levels_.insert(key, QLevel{});
            lvl = levels_.find(key);
        }
        // Find the order this one queues behind: the tail, or, when ranking by reference
        // number, the newest order with a lower reference.
        ++stats_.adds;
        uint64_t prev = lvl->tail, next = 0, ahead = lvl->qty;
        if (prev && prev > ref) {
            ++stats_.adds_out_of_sequence;
            if (priority_by_reference)
                while (prev && prev > ref) {
                    const QOrder* later = orders_.find(prev);
                    ahead -= later->qty;
                    next = prev;
                    prev = later->prev;
                }
        }
        // Track fill outcomes for orders entering at the touch in continuous trading.
        const Top top = books_.top(loc);
        const bool at_touch = is_bid ? px >= top.bid_px : (top.ask_px == 0 || px <= top.ask_px);
        uint8_t bucket = kUntracked;
        if (at_touch && context_of(ts) == kRegular && status_of(loc, ts) == kTrading) {
            bucket = uint8_t(bucket_of(ahead));
            ++stats_.entry[bucket].orders;
            stats_.entry[bucket].shares += shares;
        }
        if (prev)
            orders_.find(prev)->next = ref;
        else
            lvl->head = ref;
        if (next)
            orders_.find(next)->prev = ref;
        else
            lvl->tail = ref;
        orders_.insert(ref, QOrder{px, shares, loc, is_bid, bucket, 0, uint8_t(origin), prev, next});
        lvl->qty += shares;
        ++lvl->count;
        books_.add(loc, ref, side, shares, px);
    }

    // Takes the order out of its level's queue and out of the order map.
    void unlink(uint64_t ref, QOrder* o) {
        const uint64_t key = level_key(o->locate, o->is_bid, o->price);
        const uint64_t prev = o->prev, next = o->next, qty = o->qty;
        orders_.erase(ref, o);  // invalidates o
        QLevel* lvl = levels_.find(key);
        if (prev)
            orders_.find(prev)->next = next;
        else
            lvl->head = next;
        if (next)
            orders_.find(next)->prev = prev;
        else
            lvl->tail = prev;
        lvl->qty -= qty;
        if (--lvl->count == 0) levels_.erase(key, lvl);
    }

    void execute(uint16_t loc, uint64_t ts, uint64_t ref, uint32_t shares, Kind kind, uint32_t exec_px = 0) {
        QOrder* o = orders_.find(ref);
        if (!o) return;
        ++stats_.executions;
        const Top top = books_.top(loc);
        QLevel* lvl = levels_.find(level_key(loc, o->is_bid, o->price));
        const bool best_price = o->price == (o->is_bid ? top.bid_px : top.ask_px);
        const Price price = kind == kExec                             ? kAtDisplay
                            : exec_px == o->price                     ? kPricedSame
                            : (exec_px > o->price) == bool(o->is_bid) ? kPricedBetter
                                                                      : kPricedWorse;
        ++stats_.executions_by_price[price];
        if (best_price && lvl->head == ref) {
            ++stats_.head_of_queue;
        } else {
            ++stats_.exceptions[context_of(ts)][kind][status_of(loc, ts)][best_price];
            if (!best_price) {
                ++stats_.not_best_price;
            } else {
                ++stats_.not_oldest;
                ++stats_.not_oldest_by[o->origin][price];
                int ahead = 0;
                for (uint64_t r = lvl->head; r != ref && ahead < 21; r = orders_.find(r)->next) ++ahead;
                ++stats_.jumped[ahead <= 1 ? 0 : ahead <= 5 ? 1 : ahead <= 20 ? 2 : 3];
            }
        }
        shares = std::min(shares, o->qty);
        if (o->bucket != kUntracked) {
            stats_.entry[o->bucket].filled += !o->filled;
            stats_.entry[o->bucket].shares_filled += shares;
        }
        if (shares == o->qty) {
            unlink(ref, o);
        } else {
            o->qty -= shares;
            o->filled = 1;
            lvl->qty -= shares;
        }
        books_.reduce(loc, ref, shares);
    }

    void cancel(uint16_t loc, uint64_t ref, uint32_t shares) {  // a partial cancel keeps its place in the queue
        QOrder* o = orders_.find(ref);
        if (!o) return;
        if (shares >= o->qty) {
            unlink(ref, o);
        } else {
            o->qty -= shares;
            levels_.find(level_key(loc, o->is_bid, o->price))->qty -= shares;
        }
        books_.reduce(loc, ref, shares);
    }

    void remove(uint16_t loc, uint64_t ref) {
        if (QOrder* o = orders_.find(ref)) unlink(ref, o);
        books_.remove(loc, ref);
    }

    void replace(uint16_t loc, uint64_t ts, uint64_t old_ref, uint64_t new_ref, uint32_t shares, uint32_t px) {
        QOrder* o = orders_.find(old_ref);
        if (!o || new_ref == 0 || orders_.find(new_ref)) return;
        const char side = o->is_bid ? 'B' : 'S';
        const bool resize = px == o->price && shares <= o->qty;
        if (!resize || !resize_keeps_priority) {  // a new order at the back of its level's queue
            remove(loc, old_ref);
            add(loc, ts, new_ref, side, shares, px, resize ? kResized : kRepriced);
            return;
        }
        // Same queue position under a new reference number.
        QOrder q = *o;
        QLevel* lvl = levels_.find(level_key(loc, q.is_bid, q.price));
        lvl->qty -= q.qty - shares;
        q.qty = shares;
        q.origin = kResized;
        orders_.erase(old_ref, o);
        orders_.insert(new_ref, q);
        if (q.prev)
            orders_.find(q.prev)->next = new_ref;
        else
            lvl->head = new_ref;
        if (q.next)
            orders_.find(q.next)->prev = new_ref;
        else
            lvl->tail = new_ref;
        books_.replace(loc, old_ref, new_ref, shares, px);
    }

public:
    void on_message(const uint8_t* m) {
        using namespace itch;
        const uint16_t loc = be16(m + 1);
        const uint64_t ts = be48(m + 5);
        switch (m[0]) {
            case 'H':  // trading status
                state_[loc] = char(m[19]);
                state_since_[loc] = ts;
                break;
            case 'A':
            case 'F':
                add(loc, ts, be64(m + 11), char(m[19]), be32(m + 20), be32(m + 32));
                break;
            case 'E':
                execute(loc, ts, be64(m + 11), be32(m + 19), kExec);
                break;
            case 'C':
                execute(loc, ts, be64(m + 11), be32(m + 19), m[31] == 'Y' ? kExecPriced : kExecCross, be32(m + 32));
                break;
            case 'X':
                cancel(loc, be64(m + 11), be32(m + 19));
                break;
            case 'D':
                remove(loc, be64(m + 11));
                break;
            case 'U':
                replace(loc, ts, be64(m + 11), be64(m + 19), be32(m + 27), be32(m + 31));
                break;
            default:
                break;
        }
    }

    void prefetch_far(uint64_t ref, uint16_t loc) const {
        orders_.prefetch(ref);
        books_.prefetch_order(ref, loc);
    }
    void prefetch_near(uint16_t loc) const { books_.prefetch_levels(loc); }

    const Stats& stats() const { return stats_; }
    const Books& books() const { return books_; }
    size_t live_orders() const { return orders_.size(); }
    size_t live_levels() const { return levels_.size(); }
};
