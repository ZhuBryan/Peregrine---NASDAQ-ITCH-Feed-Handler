# Peregrine: NASDAQ ITCH 5.0 feed handler in C++20

Decodes real NASDAQ TotalView-ITCH 5.0 market data, rebuilds the full-depth order
book for every listed symbol, and publishes top-of-book changes to a consumer
thread over a lock-free SPSC queue. Input is a recorded file or a live MoldUDP64
stream with gap recovery. Header-only core, no dependencies.

![Architecture: inputs, feed thread, queue, strategy thread](docs/architecture.svg)

![Throughput and median latency by build](docs/bench.svg)

![Terminal recording: tests, UDP loss test, and a replay of the sample](docs/run.svg)

**Demo page:** `docs/index.html` is a self-contained page with an order-book replay and
charts of the full session. Open it in a browser, or serve it with GitHub Pages from `/docs`.
Rebuild it with `python3 scripts/build_page.py` after re-exporting `docs/data/day.json`.

## Results

### Full trading day

The complete 30 Dec 2019 file (3.5 GB compressed, 8.25 GB raw, 268,744,780
messages, 04:00 to 20:00), streamed with `gunzip -c | itchfh -`:

- 0 messages referencing an unknown order across 263.2M book-changing messages.
- 124,767,189 BBO changes published and consumed; consumer state matched the books.
- 0 live orders left at the close: every order added during the day was removed.
- 4.9M msg/s (p50 120 ns, p99 852 ns) while sharing two vCPUs with `gunzip`.
- No duplicate order references, no reduction without a matching price level, and no
  malformed messages.
- 905 crossed BBOs over the day, in 15 stocks, all accounted for: 433 while the stock
  was paused, 53 while halted, 2 in a quotation-only period, and 417 within 1 ms of the
  stock reopening, when the reopening auction is about to execute.

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

A later change made every execute, cancel, delete and replace do one hash lookup
where it had done up to three. In three back-to-back paired runs on the same slice
it gave 29% to 39% more throughput than version 2 (4.2 to 6.2M against 3.1 to 4.8M
msg/s). The VM was much noisier that day, so the table above is left as measured;
`scripts/bench.sh` re-measures both builds on your own machine.

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

## Example run

The full session, streamed from the compressed file (2-vCPU VM, sharing cores with `gunzip`):

```
$ gunzip -c 12302019.NASDAQ_ITCH50.gz | ./build/itchfh --signal --export AAPL docs/data/day.json - AAPL
messages        268744780 (263241937 book-changing), feed time up to 20:00:00
bbo changes     124767189, 124767189 published, 124767189 consumed, 0 conflated
integrity       0 unknown order refs, 905 crossed BBOs, final consumer state matches books
live orders     0 at end
AAPL     bid 0 @ 0.0000 | ask 0 @ 0.0000 | depth 0 x 0 levels
imbalance = bid size / (bid size + ask size), 1-cent spreads, 09:30-16:00; share of next mid moves that were up
  imbalance   at quote start        just before move
  0.0-0.1      27.5% (  389473)      6.6% (  532062)
  0.1-0.2      27.8% (  294855)      6.2% (  303010)
  0.2-0.3      32.5% (  246731)      9.6% (  220914)
  0.3-0.4      38.1% (  226004)     16.5% (  179645)
  0.4-0.5      41.1% (  115755)     23.4% (   79796)
  0.5-0.6      52.7% (  430742)     56.5% (  326549)
  0.6-0.7      62.6% (  242314)     85.2% (  190101)
  0.7-0.8      66.9% (  186958)     90.3% (  165782)
  0.8-0.9      71.4% (  341674)     93.6% (  342170)
  0.9-1.0      72.3% (  395535)     93.7% (  530132)
wall time       54.534 s
throughput      4.9 M msgs/s (202.9 ns/msg avg)
feed latency    p50 91  p99 812  p99.9 2164  max 3186414 ns (decode + book + publish; includes one clock read)
queue latency   p50 1172  p99 1347450  p99.9 2269596  max 29607715 ns (publish to consume; includes one clock read)
```

The AAPL line is empty because the book is empty at 20:00.

## Design

- **Zero-copy decode** (`include/itch.hpp`): fields are byte-swapped straight out
  of the buffer into handler calls. No message structs, no allocation. A message
  shorter than its type's layout is skipped and counted, never decoded.
- **Order map** (`include/book.hpp`): open addressing, linear probing,
  backward-shift deletion (no tombstones under constant add/delete churn),
  16-byte slots, pre-sized so it never rehashes mid-session.
- **Price levels**: per-side sorted array with the best price at the back.
- **SPSC queue** (`include/spsc.hpp`): head and tail on separate cache lines, each
  side caching the other's index so the fast path reads no shared line.
- **MoldUDP64** (`include/mold.hpp`): packets ahead of a gap are buffered, the
  missing range is requested (and re-requested after 20 ms), and messages reach
  the book exactly once, in order. Packets from another session are ignored, and
  the receiver gives up with an error if the sender is silent for 5 seconds.
- **Pipeline** (`src/pipeline.hpp`): the feed side (books, publishing, conflation) and the
  consumer side (latest BBO, imbalance study), shared by the binary and the tests.
- **Export** (`src/export.hpp`): per-minute activity, message mix and depth snapshots as JSON
  for the demo page.
- **Conflation**: a per-symbol dirty flag plus a FIFO of dirty symbols; each
  flushed update carries the newest state.

## Build and run

Linux or WSL, g++ 11+.

```
make test                                    # decoder, differential, order-map and SPSC tests, both builds
make sanitize                                # tests under ASan + UBSan, pipeline under TSan
make udptest                                 # loopback UDP with injected loss vs file replay
make
./build/itchfh data/sample_50MB.itch AAPL    # small real sample
./scripts/get_data.sh                        # full day, 3.5 GB download
./scripts/get_data.sh 2000000000             # or the 2 GB slice used for the benchmarks
./build/itchfh --pin 0,1 --signal data/12302019.itch AAPL SPY MSFT
./build/itchfh_baseline --pin 0,1 data/12302019.itch
./scripts/bench.sh                           # both builds, 3 pinned runs each, as a table
./build/itchfh --conflate --queue 64 --work 2000 data/sample_50MB.itch
gunzip -c day.gz | ./build/itchfh --signal - AAPL > docs/signal_run.txt   # stream without unpacking to disk
python3 scripts/plot.py docs/signal_run.txt  # regenerate the charts
gunzip -c day.gz | ./build/itchfh --signal --export AAPL docs/data/day.json -   # data for the demo page
python3 scripts/build_page.py                # rebuild docs/index.html
```

## Tests

`tests/test.cpp` encodes a random ITCH stream (adds, executions, cancels,
deletes, replaces in a tight price band), runs it through the decoder and book,
and after every message compares each symbol's BBO against a brute-force
reference that rescans all live orders. It also checks the "BBO changed" flag is
exact and that the SPSC queue delivers 2M items in order across two threads.

The order map is checked against `std::unordered_map` through 200,000 inserts and
erases in a small table, so probe runs collide, wrap and trigger growth. Truncated
and zero-length messages must be skipped and counted. `make sanitize` runs the
tests under AddressSanitizer and UBSan and the two-thread pipeline under
ThreadSanitizer; all three are clean.

Further tests cover the `F` and `C` message layouts, the integrity counters (a reused
order reference, a reduction against the wrong stock), conflation through a 4-slot
queue, and the JSON export.

## Not done

- Bare-metal numbers: everything above was measured in a 2-vCPU VM.
- Lookahead prefetch on the UDP path (file replay only today).
- Bounded gap buffer with snapshot fallback for very large gaps.
- Signal study at fixed lead times and across multiple days.

CI (`.github/workflows/ci.yml`) runs the unit tests and the UDP loss test on a
synthetic stream, since NASDAQ's data is not checked in.
