# Burst-Resilient Feed Handler & Order-Book Engine

[![CI](https://github.com/Aryan4443/Burst-Resilient-Feed-Handler-Order-Book-Engine/actions/workflows/ci.yml/badge.svg)](https://github.com/Aryan4443/Burst-Resilient-Feed-Handler-Order-Book-Engine/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)

A high-performance market-data pipeline in modern **C++20**. It sits between a raw
market-data source and any price consumer, turning a firehose of order events into a
**live, accurate order book** that stays correct under extreme load.

```
FeedSource → Parser → Sequencer/Gap Detector → [lock-free SPSC ring] → Order Book → Publisher
```

Priorities, in order: **correctness → low latency (no hot-path allocation) → measurability.**

## Components

| Component | What it does |
|---|---|
| `FeedSource` | Pluggable adapter. ITCH 5.0 file replayer **and** a live Binance WebSocket adapter (opt-in, see below). Emits framed bytes / events + sequence numbers. |
| `Parser` | Zero-copy binary decoder. Unpacks ITCH fixed layouts into a POD `MarketEvent`. No per-message heap allocation. |
| `Sequencer / GapDetector` | Tracks per-stream sequence numbers; on a gap marks the book stale and drives recovery, then resyncs. |
| `SpscRingBuffer` | Lock-free single-producer/single-consumer bounded queue. Power-of-two capacity, cache-line-padded head/tail, explicit backpressure (block vs counted-drop). |
| `OrderBook` | Tick-indexed price levels; each level a FIFO intrusive list; `unordered_map<order_ref,node*>` for O(1) cancel/modify; cached best-bid/ask. |
| `Publisher` | Emits L1/L2 deltas downstream. |
| Instrumentation | HdrHistogram-style latency, throughput, queue depth, drop counters. |
| Stress harness | Nx replay, gap/burst injection, saturation ramp, markdown benchmark report. |

## Build

```bash
# CMake + Ninja (GoogleTest is fetched automatically)
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

Useful options: `-DFH_ASAN=ON` (sanitizers), `-DFH_BUILD_BENCH=OFF`, `-DFH_BUILD_TESTS=OFF`.

## Run

```bash
# 1. Generate a synthetic ITCH 5.0 sample file (real wire layout, length-framed)
./build/itch_gen --out data/sample.itch --orders 500000

# 2. Replay it end-to-end through the two-thread pipeline
./build/feed_handler --file data/sample.itch              # max speed
./build/feed_handler --file data/sample.itch --speed 1.0  # paced to original timing
./build/feed_handler --file data/sample.itch --drop       # Drop backpressure policy

# 3. Single-thread microbenchmarks (parser and parser+book ceilings)
./build/benchmarks/microbench --file data/sample.itch

# 4. Stress harness: saturation sweep, gap-recovery, backpressure -> markdown report
./build/benchmarks/bench_harness --file data/sample.itch --report reports/benchmark.md
```

See [`reports/benchmark.md`](reports/benchmark.md) for a sample report (sustained throughput,
latency-vs-load curve, gap-recovery correctness + time, and Block-vs-Drop backpressure).

## Live market data (real-time, no API key)

The same engine can run on a **live exchange feed** instead of a recorded file. The adapter is
opt-in (it fetches a WebSocket/TLS client + JSON parser via CMake), so the default build, tests,
and CI stay dependency-free:

```bash
cmake -S . -B build-live -G Ninja -DCMAKE_BUILD_TYPE=Release -DFH_BUILD_LIVE=ON
cmake --build build-live --target live_feed

# Stream Binance's public diff-depth (L2) order book through the engine — no credentials needed.
./build-live/live_feed --symbol BTCUSDT --duration 30

# US / region-restricted (HTTP 451)? point at the binance.us endpoints (auto-suggested on failure):
./build-live/live_feed --rest-host https://api.binance.us \
    --ws-url wss://stream.binance.us:9443/ws/btcusdt@depth@100ms
```

It bootstraps from a REST depth snapshot, then drives the book from the live diff stream through
the **same lock-free ring**, printing top-of-book / spread / depth / latency / gap-recovery stats
once a second:

```
BTCUSDT  bid 60313.84 x0.001 | ask 60313.85 x0.034 | spread 0.01 | levels 707 | 17 op/s |
         p50 382.98 us p99 1.40 ms | drops 0 | gaps 0 resync 1 | oob 7282 cross 0 | streaming
```

Binance publishes **aggregated price-level (L2)** updates, so the book is driven via
`OrderBook::set_level()` (absolute size per level) and the resync runs on Binance's update-id
window. See [DESIGN_NOTES.md](DESIGN_NOTES.md#live-market-data-adapter-binance-l2) for how the L2
path and update-id resync map onto the engine.

## Layout

```
include/feedhandler/   public headers (POD types, ring buffer, book, parser, ...)
src/                   library implementations
tests/                 GoogleTest unit + stress tests
benchmarks/            microbenchmarks + stress harness + benchmark reporter
tools/                 itch_gen (synthetic ITCH 5.0 generator)
apps/                  feed_handler (file replay) + live_feed (live Binance L2, opt-in)
DESIGN_NOTES.md        why each choice was made (interview prep)
```

See [`DESIGN_NOTES.md`](DESIGN_NOTES.md) for the rationale behind every data structure and
the latency / recovery design.

## Continuous integration

Every push and PR runs [`.github/workflows/ci.yml`](.github/workflows/ci.yml):

- **build & test** on Linux (GCC/libstdc++) and macOS (clang/libc++) in Release, full
  `ctest` suite, plus an end-to-end smoke test (generate → replay → microbench).
- **AddressSanitizer + UBSan** and **ThreadSanitizer** builds — the latter is the headline
  check for the lock-free SPSC ring buffer (zero data races).
- **live adapter build** — compiles the opt-in Binance adapter (`-DFH_BUILD_LIVE=ON`, deps via
  `FetchContent`) so it stays buildable, without opening a live socket in CI.

## License

[MIT](LICENSE) © 2026 Aryan Lakhani.
