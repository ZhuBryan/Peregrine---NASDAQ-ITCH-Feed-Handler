# Peregrine: NASDAQ ITCH 5.0 feed handler in C++20

Decodes real NASDAQ TotalView-ITCH 5.0 market data, rebuilds the full-depth order
book for every listed symbol, and publishes top-of-book changes to a consumer
thread over a lock-free SPSC queue. Input is a recorded file or a live MoldUDP64
stream with gap recovery. Header-only core, no dependencies.

```
file (mmap) ----\
                 >-- [feed thread: decode -> order map -> price levels] --SPSC--> [consumer: BBO per symbol, signal study]
MoldUDP64 (UDP) /         gap detect + retransmit                  backpressure or conflation
```

![Throughput and median latency by build](docs/bench.svg)

## Results

### Full trading day

The complete 30 Dec 2019 file (3.5 GB compressed, 8.25 GB raw, 268,744,780
messages, 04:00 to 20:00), streamed with `gunzip -c | itchfh -`:

- 0 messages referencing an unknown order across 263.2M book-changing messages.
- 124,767,189 BBO changes published and consumed; consumer state matched the books.
- 0 live orders left at the close: every order added during the day was removed.
- 4.9M msg/s (p50 120 ns, p99 852 ns) while sharing two vCPUs with `gunzip`.
- 905 crossed BBOs over the day (not all inspected; see below for the pre-open ones).

### Benchmarks

Benchmark numbers: first 2 GB of NASDAQ's 30 Dec 2019 file (64.5M messages, 04:00 to
10:23:36, 8,906 symbols), 2-vCPU Linux VM on a Ryzen 7 PRO 7840U, threads
pinned. Re-run on bare metal before quoting.

| Build | Throughput | Avg | p50 | p99 | p99.9 |
|---|---|---|---|---|---|
| Baseline: `std::unordered_map` + `std::map` | 1.5M msg/s | 654 ns | 561 ns | 2.3 us | 17.7 us |
| v1: flat hash map + sorted level arrays | 4.4-4.5M msg/s | 222-226 ns | 280-291 ns | 1.05 us | 3.1-3.2 us |
| v2: v1 + lookahead prefetch + 16-byte slots | 5.1-5.5M msg/s | 183-197 ns | 120 ns | 0.67-0.73 us | 2.3 us |

Percentiles are per-message (decode + book update + publish), sampled 1 in 64,
and each sample includes one clock read.

Correctness on the same data:
- 0 messages referencing an unknown order across 63.0M book-changing messages.
- 28,760,401 BBO changes published and consumed; the consumer's final BBO equals
  the book for all 65,536 locate slots (checked on every run, exit code 1 if not).
- 110 crossed BBOs in this slice. The ones inspected were halted names (MKD, PNRL, SVA), NASDAQ
  test symbols (ZJZZT, ZWZZT) and the 09:30 opening cross.

### Where the time went

`perf` was unavailable in the VM, so each stage was timed in isolation:

| Stage | ns/msg |
|---|---|
| Framing + decode only | 11 |
| + order map | 66-69 |
| + price levels (v1 feed thread) | 211 |
| + two-stage prefetch | 124-127 |
| + 16-byte order slots (v2 feed thread) | 115-124 |

Price levels cost about twice what the order map did. Instrumenting the level
arrays showed the scan itself was short (55% of lookups land at most one level
from the touch, average 13 levels, average shift on insert/erase 20 levels), so
the cost was cache misses hopping between 8,906 symbols, not the algorithm.

The fix: the replay loop runs two cursors ahead of the message being applied.
Sixteen messages early it prefetches the order's hash slot and the symbol's book
header; eight messages early, with the header now cached, it prefetches the
top-of-book end of both level arrays. Distance 8 was the best of 1 to 32.

Tried and dropped: backing the order map with transparent huge pages. No
measurable change here (the VM reported no huge pages in use); untested on bare metal.

### Slow consumers: backpressure vs conflation

50 MB sample, consumer doing 2 us of simulated work per update:

| Mode | Feed thread wall time | Updates delivered | Queue latency p50 |
|---|---|---|---|
| Backpressure, 65,536-slot queue | 1.31 s | 640,403 of 640,403 | 147 ms |
| `--conflate`, 65,536-slot queue | 0.13 s | 118,921 | 122 ms |
| `--conflate --queue 64` | 0.19 s | 80,788 | 138 us |

Conflation alone keeps the feed thread fast but the consumer still reads stale
data, because nothing is conflated until the deep queue fills. A shallow queue
plus conflation is what bounds staleness. In all three the final consumer state
matched the books.

### MoldUDP64 gap recovery

`tools/moldsend` replays a file as MoldUDP64 and can drop every Nth packet on
purpose. `make udptest` drops 2% over loopback and requires output identical to
a file replay: 732 packets dropped, 732 retransmit requests, 34,295 messages
recovered, identical output. The same test passes over multicast (239.1.1.1),
where the VM also loses packets by itself.

### Signal study: order-book imbalance

`--signal` makes the consumer do something a strategy would. For symbols quoted
one cent wide during regular hours (09:30 to 16:00, full day, 2.87M mid-price
moves), share of moves that were up, by bid size / (bid size + ask size):

![Next mid-price move direction by order-book imbalance](docs/imbalance.svg)

| Imbalance | Measured when the quote was first posted | Measured just before the move |
|---|---|---|
| 0.0-0.1 | 27.5% (389,473 moves) | 6.6% |
| 0.1-0.2 | 27.8% | 6.2% |
| 0.2-0.3 | 32.5% | 9.6% |
| 0.3-0.4 | 38.1% | 16.5% |
| 0.4-0.5 | 41.1% | 23.4% |
| 0.5-0.6 | 52.7% | 56.5% |
| 0.6-0.7 | 62.6% | 85.2% |
| 0.7-0.8 | 66.9% | 90.3% |
| 0.8-0.9 | 71.4% | 93.6% |
| 0.9-1.0 | 72.3% (395,535 moves) | 93.7% |

The right-hand column is largely mechanical: a queue about to be exhausted is
small by construction. The left-hand column is the meaningful one. Neither is a
trading result: it is one day, direction only, and ignores the spread, fees
and queue position. Raw output: `docs/signal_run.txt`.

## Design

- **Zero-copy decode** (`include/itch.hpp`): fields are byte-swapped straight out
  of the buffer into handler calls. No message structs, no allocation.
- **Order map** (`include/book.hpp`): open addressing, linear probing,
  backward-shift deletion (no tombstones under constant add/delete churn),
  16-byte slots, pre-sized so it never rehashes mid-session.
- **Price levels**: per-side sorted array with the best price at the back.
- **SPSC queue** (`include/spsc.hpp`): head and tail on separate cache lines, each
  side caching the other's index so the fast path reads no shared line.
- **MoldUDP64** (`include/mold.hpp`): packets ahead of a gap are buffered, the
  missing range is requested (and re-requested after 20 ms), and messages reach
  the book exactly once, in order.
- **Conflation**: a per-symbol dirty flag plus a FIFO of dirty symbols; each
  flushed update carries the newest state.

## Build and run

Linux or WSL, g++ 11+.

```
make test                                    # decoder, differential and SPSC tests, both builds
make udptest                                 # loopback UDP with injected loss vs file replay
make
./build/itchfh data/sample_50MB.itch AAPL    # small real sample
./scripts/get_data.sh                        # full day, 3.5 GB download
./scripts/get_data.sh 2000000000             # or the 2 GB slice used for the benchmarks
./build/itchfh --pin 0,1 --signal data/12302019.itch AAPL SPY MSFT
./build/itchfh_baseline --pin 0,1 data/12302019.itch
./build/itchfh --conflate --queue 64 --work 2000 data/sample_50MB.itch
gunzip -c day.gz | ./build/itchfh --signal - AAPL > docs/signal_run.txt   # stream without unpacking to disk
python3 scripts/plot.py docs/signal_run.txt  # regenerate the charts
```

## Tests

`tests/test.cpp` encodes a random ITCH stream (adds, executions, cancels,
deletes, replaces in a tight price band), runs it through the decoder and book,
and after every message compares each symbol's BBO against a brute-force
reference that rescans all live orders. It also checks the "BBO changed" flag is
exact and that the SPSC queue delivers 2M items in order across two threads.

## Not done

- Bare-metal numbers: everything above was measured in a 2-vCPU VM.
- Lookahead prefetch on the UDP path (file replay only today).
- Bounded gap buffer with snapshot fallback for very large gaps.
- Signal study at fixed lead times and across multiple days.

CI (`.github/workflows/ci.yml`) runs the unit tests and the UDP loss test on a
synthetic stream, since NASDAQ's data is not checked in.
