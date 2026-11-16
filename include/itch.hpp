// Zero-copy decoder for NASDAQ TotalView-ITCH 5.0.
// Fields are read straight out of the wire buffer (big-endian) and handed to a
// handler; no intermediate message structs are built.
#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace itch {

// Regular trading session, in nanoseconds since midnight (the unit of ITCH timestamps).
inline constexpr uint64_t kRegularOpenNs = 34200ull * 1'000'000'000;   // 09:30
inline constexpr uint64_t kRegularCloseNs = 57600ull * 1'000'000'000;  // 16:00

inline constexpr size_t kLocates = 65536;  // stock locate is a 16-bit index

inline uint16_t be16(const uint8_t* p) {
    uint16_t v;
    std::memcpy(&v, p, 2);
    return __builtin_bswap16(v);
}
inline uint32_t be32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return __builtin_bswap32(v);
}
inline uint64_t be64(const uint8_t* p) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    return __builtin_bswap64(v);
}
inline uint64_t be48(const uint8_t* p) {
    return (uint64_t(be16(p)) << 32) | be32(p + 2);
}

// Every message starts: type(1) locate(2) tracking(2) timestamp(6, ns since midnight).
// Handler must provide:
//   directory(locate, const char sym[8])
//   add(locate, ts, ref, side, shares, price)      side is 'B' or 'S', price is 1/10000 USD
//   reduce(locate, ts, ref, shares)                execution or partial cancel
//   remove(locate, ts, ref)
//   replace(locate, ts, old_ref, new_ref, shares, price)
// Messages that do not change the displayed book are ignored.
template <class Handler>
inline void decode(const uint8_t* m, Handler& h) {
    const uint16_t loc = be16(m + 1);
    switch (m[0]) {
        case 'A':
        case 'F':  // add order (F carries an MPID we don't need)
            h.add(loc, be48(m + 5), be64(m + 11), char(m[19]), be32(m + 20), be32(m + 32));
            break;
        case 'E':
        case 'C':  // executed (C = at a different price; book impact is the same)
        case 'X':  // partial cancel
            h.reduce(loc, be48(m + 5), be64(m + 11), be32(m + 19));
            break;
        case 'D':
            h.remove(loc, be48(m + 5), be64(m + 11));
            break;
        case 'U':
            h.replace(loc, be48(m + 5), be64(m + 11), be64(m + 19), be32(m + 27), be32(m + 31));
            break;
        case 'R':
            h.directory(loc, reinterpret_cast<const char*>(m + 11));
            break;
        default:
            break;
    }
}

// Shortest valid length for a message of this type. Every message carries the 11-byte
// header, and the types this project reads have fixed layouts.
inline size_t min_length(uint8_t type) {
    switch (type) {
        case 'A':
            return 36;
        case 'F':
            return 40;
        case 'E':
            return 31;
        case 'C':
            return 36;
        case 'X':
            return 23;
        case 'D':
            return 19;
        case 'U':
            return 35;
        case 'R':
            return 39;
        case 'H':
            return 25;
        default:
            return 11;
    }
}
// decode() and peek_order() read fixed offsets, so a message must pass this first.
inline bool well_formed(const uint8_t* m, size_t len) {
    return len >= 11 && len >= min_length(m[0]);
}

// File framing (NASDAQ BinaryFILE): each message is prefixed by a 2-byte big-endian length.
// Calls f(msg) per complete, well-formed message; returns bytes consumed. Messages too
// short for their type are skipped and counted in *malformed.
template <class F>
inline size_t for_each_message(const uint8_t* p, size_t n, F&& f, uint64_t* malformed = nullptr) {
    size_t off = 0;
    while (off + 2 <= n) {
        const size_t len = be16(p + off);
        if (off + 2 + len > n) break;  // truncated tail
        if (well_formed(p + off + 2, len))
            f(p + off + 2);
        else if (malformed)
            ++*malformed;
        off += 2 + len;
    }
    return off;
}

// True for messages that touch an order; yields the order reference and locate without a full decode.
inline bool peek_order(const uint8_t* m, uint16_t& loc, uint64_t& ref) {
    switch (m[0]) {
        case 'A':
        case 'F':
        case 'E':
        case 'C':
        case 'X':
        case 'D':
        case 'U':
            loc = be16(m + 1);
            ref = be64(m + 11);
            return true;
        default:
            return false;
    }
}

// Same as for_each_message, but also runs two cursors ahead of the one being processed:
// far(msg) is called `2 * dist` messages early and near(msg) `dist` messages early, so the
// memory a message will touch can be prefetched while earlier messages are still being applied.
template <class Far, class Near, class F>
inline size_t for_each_message_lookahead(const uint8_t* p, size_t n, size_t dist, Far&& far, Near&& near, F&& f,
                                         uint64_t* malformed = nullptr) {
    const size_t none = SIZE_MAX;
    // Length of the complete message at `off`, or `none` if the buffer ends first.
    auto length_at = [&](size_t off) -> size_t {
        if (off + 2 > n) return none;
        const size_t len = be16(p + off);
        return off + 2 + len <= n ? len : none;
    };
    auto skip = [&](size_t off, size_t count) {
        for (size_t len; count && (len = length_at(off)) != none; --count) off += 2 + len;
        return off;
    };
    size_t off = 0, near_off = skip(0, dist), far_off = skip(near_off, dist);
    for (size_t len; (len = length_at(off)) != none; off += 2 + len) {
        if (const size_t l = length_at(far_off); l != none) {
            if (well_formed(p + far_off + 2, l)) far(p + far_off + 2);
            far_off += 2 + l;
        }
        if (const size_t l = length_at(near_off); l != none) {
            if (well_formed(p + near_off + 2, l)) near(p + near_off + 2);
            near_off += 2 + l;
        }
        if (well_formed(p + off + 2, len))
            f(p + off + 2);
        else if (malformed)
            ++*malformed;
    }
    return off;
}

}  // namespace itch
