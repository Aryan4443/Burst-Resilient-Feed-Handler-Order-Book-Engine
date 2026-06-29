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
| `FeedSource` | Pluggable adapter. ITCH 5.0 file replayer first; crypto WebSocket later. Emits framed bytes + sequence numbers. |
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

## Layout

```
include/feedhandler/   public headers (POD types, ring buffer, book, parser, ...)
src/                   library implementations
tests/                 GoogleTest unit + stress tests
benchmarks/            microbenchmarks + stress harness + benchmark reporter
tools/                 itch_gen (synthetic ITCH 5.0 generator)
apps/                  feed_handler end-to-end runner
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

## License

[MIT](LICENSE) © 2026 Aryan Lakhani.
