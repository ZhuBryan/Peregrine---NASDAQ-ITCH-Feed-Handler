// MoldUDP64 receiver: sequenced UDP market data with gap detection and recovery.
//   downstream packet: session[10] sequence[8] count[2], then count x (length[2] message)
//   count 0 = heartbeat, 0xFFFF = end of session
//   retransmit request: session[10] sequence[8] count[2]
// Packets that arrive ahead of a gap are buffered, the missing range is re-requested,
// and messages are always delivered to the callback exactly once and in order.
#pragma once
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "itch.hpp"

class MoldReceiver {
    int fd_;
    uint64_t next_ = 1;  // next sequence number we need
    std::map<uint64_t, std::vector<uint8_t>> pending_;  // packets received ahead of a gap, by first seq
    // ponytail: unbounded buffer. A production handler caps it and falls back to a
    // snapshot (GLIMPSE) when a gap is too large to replay.
    sockaddr_in src_{};
    bool have_src_ = false;
    char session_[10] = {};

    std::chrono::steady_clock::time_point last_request_{};

    void request(uint64_t end_seq) {
        if (!have_src_) return;
        last_request_ = std::chrono::steady_clock::now();
        uint64_t want = !pending_.empty() ? pending_.begin()->first - next_ : end_seq ? end_seq - next_ : 1;
        if (want > 0xFFFE) want = 0xFFFE;
        uint8_t r[20];
        std::memcpy(r, session_, 10);
        for (int i = 0; i < 8; ++i) r[10 + i] = uint8_t(next_ >> (8 * (7 - i)));
        r[18] = uint8_t(want >> 8); r[19] = uint8_t(want);
        // Real MoldUDP64 uses a separate request server; here the sender answers on its own socket.
        sendto(fd_, r, sizeof r, 0, reinterpret_cast<sockaddr*>(&src_), sizeof src_);
        ++requests;
    }

    template <class F>
    void deliver(const uint8_t* buf, size_t n, bool recovering, F& on_msg) {
        const uint64_t seq = itch::be64(buf + 10);
        const uint16_t count = itch::be16(buf + 18);
        size_t off = 20;
        for (uint16_t i = 0; i < count && off + 2 <= n; ++i) {
            const size_t len = itch::be16(buf + off);
            if (off + 2 + len > n) break;
            if (seq + i == next_) { on_msg(buf + off + 2); ++next_; recovered += recovering; }
            else ++duplicates;  // already delivered
            off += 2 + len;
        }
    }

public:
    uint64_t packets = 0, requests = 0, recovered = 0, duplicates = 0, max_buffered = 0;

    MoldReceiver(const char* ip, uint16_t port) {
        fd_ = socket(AF_INET, SOCK_DGRAM, 0);
        int one = 1, rcvbuf = 16 << 20;
        setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
        setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof rcvbuf);
        timeval tv{0, 20000};  // 20 ms: retransmit-request retry interval
        setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        sockaddr_in a{};
        a.sin_family = AF_INET; a.sin_port = htons(port); a.sin_addr.s_addr = htonl(INADDR_ANY);
        if (fd_ < 0 || bind(fd_, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0) { std::perror("udp bind"); std::exit(1); }
        in_addr group{};
        inet_pton(AF_INET, ip, &group);
        if ((ntohl(group.s_addr) >> 28) == 0xE) {  // 224.0.0.0/4: join the multicast group
            ip_mreq mreq{group, {htonl(INADDR_ANY)}};
            if (setsockopt(fd_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof mreq) != 0) { std::perror("multicast join"); std::exit(1); }
        }
    }
    ~MoldReceiver() { close(fd_); }

    // Blocks until the end-of-session packet has been seen and every message delivered.
    template <class F>
    void run(F&& on_msg) {
        std::vector<uint8_t> buf(65536);
        uint64_t end_seq = 0;  // sequence number one past the last message, once known
        bool gap = false;
        for (;;) {
            sockaddr_in from{};
            socklen_t fl = sizeof from;
            const ssize_t n = recvfrom(fd_, buf.data(), buf.size(), 0, reinterpret_cast<sockaddr*>(&from), &fl);
            if (n < 0) {  // timeout: the request or its reply may itself have been lost
                if (gap) request(end_seq);
                continue;
            }
            if (n < 20) continue;
            if (!have_src_) { src_ = from; have_src_ = true; std::memcpy(session_, buf.data(), 10); }
            const uint64_t seq = itch::be64(buf.data() + 10);
            const uint16_t count = itch::be16(buf.data() + 18);
            if (count == 0xFFFF) {
                end_seq = seq;
            } else if (count) {
                ++packets;
                if (seq > next_) {  // something before this packet is missing
                    pending_.emplace(seq, std::vector<uint8_t>(buf.begin(), buf.begin() + n));
                    if (pending_.size() > max_buffered) max_buffered = pending_.size();
                    // Re-request if the hole is still open after 20 ms: the request or reply was lost.
                    if (!gap || std::chrono::steady_clock::now() - last_request_ > std::chrono::milliseconds(20)) { gap = true; request(end_seq); }
                } else {
                    deliver(buf.data(), size_t(n), gap, on_msg);
                    while (!pending_.empty() && pending_.begin()->first <= next_) {
                        deliver(pending_.begin()->second.data(), pending_.begin()->second.size(), false, on_msg);
                        pending_.erase(pending_.begin());
                    }
                    gap = !pending_.empty();
                    if (gap) request(end_seq);  // next hole
                }
            }
            if (end_seq && next_ >= end_seq) return;
            if (end_seq && !gap) { gap = true; request(end_seq); }  // tail of the session was lost
        }
    }
};
