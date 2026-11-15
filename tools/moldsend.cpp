// Replays an ITCH file as a MoldUDP64 stream and answers retransmit requests.
// Usage: moldsend <file.itch> <ip> <port> [--pps N] [--drop N]
//   --pps N   packets per second (default 20000)
//   --drop N  fault injection: silently skip every Nth packet
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "itch.hpp"

using Clock = std::chrono::steady_clock;
static const char SESSION[11] = "ITCHREPLAY";
static const size_t MTU = 1400, STRIDE = 256;

static void put_be(uint8_t* p, uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) p[i] = uint8_t(v >> (8 * (bytes - 1 - i)));
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::fprintf(stderr, "usage: %s <file.itch> <ip> <port> [--pps N] [--drop N]\n", argv[0]);
        return 1;
    }
    uint64_t pps = 20000, drop = 0;
    for (int i = 4; i + 1 < argc; i += 2) {
        if (!std::strcmp(argv[i], "--pps"))
            pps = std::strtoull(argv[i + 1], nullptr, 10);
        else if (!std::strcmp(argv[i], "--drop"))
            drop = std::strtoull(argv[i + 1], nullptr, 10);
    }
    const int fd = open(argv[1], O_RDONLY);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) != 0) {
        std::perror(argv[1]);
        return 1;
    }
    const auto* data = static_cast<const uint8_t*>(mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        std::perror("mmap");
        return 1;
    }

    // Sparse index (file offset of every 256th message) so any sequence number can be found for retransmit.
    std::vector<uint64_t> index;
    uint64_t total = 0;
    const size_t used = itch::for_each_message(data, st.st_size, [&](const uint8_t* m) {
        if (total++ % STRIDE == 0) index.push_back(uint64_t(m - 2 - data));
    });

    const int sock = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in dst{};
    dst.sin_family = AF_INET;
    dst.sin_port = htons(uint16_t(std::atoi(argv[3])));
    if (inet_pton(AF_INET, argv[2], &dst.sin_addr) != 1) {
        std::fprintf(stderr, "bad ip\n");
        return 1;
    }

    // Builds one packet starting at message `seq` (1-based) with at most max_msgs; returns messages packed.
    uint8_t pkt[MTU];
    size_t pkt_len = 0;
    auto build = [&](uint64_t seq, uint64_t off, uint64_t max_msgs) {
        std::memcpy(pkt, SESSION, 10);
        put_be(pkt + 10, seq, 8);
        pkt_len = 20;
        uint16_t count = 0;
        while (count < max_msgs && off + 2 <= used) {
            const size_t len = 2 + itch::be16(data + off);
            if (pkt_len + len > MTU) break;
            std::memcpy(pkt + pkt_len, data + off, len);
            pkt_len += len;
            off += len;
            ++count;
        }
        put_be(pkt + 18, count, 2);
        return count;
    };
    uint64_t retransmits = 0;
    auto serve_requests = [&] {
        uint8_t req[64];
        sockaddr_in from{};
        socklen_t fl = sizeof from;
        bool any = false;
        while (recvfrom(sock, req, sizeof req, MSG_DONTWAIT, reinterpret_cast<sockaddr*>(&from), &fl) >= 20) {
            const uint64_t seq = itch::be64(req + 10);
            const uint16_t want = itch::be16(req + 18);
            any = true;
            if (seq < 1 || seq > total) continue;
            uint64_t off = index[(seq - 1) / STRIDE];
            for (uint64_t s = (seq - 1) / STRIDE * STRIDE + 1; s < seq; ++s) off += 2 + itch::be16(data + off);
            build(seq, off, want);
            sendto(sock, pkt, pkt_len, 0, reinterpret_cast<sockaddr*>(&from), sizeof from);
            ++retransmits;
        }
        return any;
    };

    const auto interval = std::chrono::nanoseconds(1'000'000'000 / pps);
    auto due = Clock::now();
    uint64_t seq = 1, off = 0, sent = 0, dropped = 0;
    while (seq <= total) {
        // Serve retransmits while waiting for the next send slot (serving reuses pkt, so build after).
        while (Clock::now() < due) serve_requests();
        due += interval;
        const uint16_t count = build(seq, off, 0xFFFE);
        if (drop && (sent + dropped + 1) % drop == 0)
            ++dropped;
        else {
            sendto(sock, pkt, pkt_len, 0, reinterpret_cast<sockaddr*>(&dst), sizeof dst);
            ++sent;
        }
        for (uint16_t i = 0; i < count; ++i) off += 2 + itch::be16(data + off);
        seq += count;
    }
    // End of session: repeat until no retransmit request has arrived for one second.
    auto last_req = Clock::now();
    while (Clock::now() - last_req < std::chrono::seconds(1)) {
        std::memcpy(pkt, SESSION, 10);
        put_be(pkt + 10, total + 1, 8);
        put_be(pkt + 18, 0xFFFF, 2);
        sendto(sock, pkt, 20, 0, reinterpret_cast<sockaddr*>(&dst), sizeof dst);
        const auto until = Clock::now() + std::chrono::milliseconds(20);
        while (Clock::now() < until)
            if (serve_requests()) last_req = Clock::now();
    }
    std::fprintf(stderr, "moldsend: %llu messages, %llu packets sent, %llu dropped on purpose, %llu retransmits\n",
                 (unsigned long long)total, (unsigned long long)sent, (unsigned long long)dropped,
                 (unsigned long long)retransmits);
    return 0;
}
