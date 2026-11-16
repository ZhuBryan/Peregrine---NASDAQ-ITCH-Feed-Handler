// Reads a recorded ITCH stream from a file (memory-mapped) or from stdin ("-").
#pragma once
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

// Calls fn(data, size), which returns how many bytes it consumed. A file is passed whole;
// stdin (e.g. `gunzip -c day.gz | tool -`) arrives in chunks, with any partial message
// carried over to the next call. Returns the seconds spent inside fn, so time blocked on
// the pipe or on disk is not counted. Exits if the file cannot be opened.
template <class Fn>
inline double for_each_chunk(const char* path, Fn&& fn) {
    using Clock = std::chrono::steady_clock;
    auto timed = [&](const uint8_t* p, size_t n, size_t& used) {
        const auto t0 = Clock::now();
        used = fn(p, n);
        return std::chrono::duration<double>(Clock::now() - t0).count();
    };
    double secs = 0;
    size_t used = 0;
    if (std::string(path) == "-") {
        std::vector<uint8_t> buf(16 << 20);
        size_t have = 0;
        for (ssize_t n; (n = read(STDIN_FILENO, buf.data() + have, buf.size() - have)) != 0;) {
            if (n < 0) {
                if (errno == EINTR) continue;
                std::perror("stdin");
                break;
            }
            have += size_t(n);
            secs += timed(buf.data(), have, used);
            std::memmove(buf.data(), buf.data() + used, have - used);  // keep the partial tail message
            have -= used;
        }
        return secs;
    }
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
    return timed(static_cast<const uint8_t*>(map), st.st_size, used);
}
