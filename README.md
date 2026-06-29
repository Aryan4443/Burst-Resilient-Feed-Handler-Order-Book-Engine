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

---

## Resume & portfolio reference

Use this section when writing bullet points, a project blurb, or interview talking points.
All throughput/latency numbers below come from the reproducible harness in
[`reports/benchmark.md`](reports/benchmark.md) (500k-message ITCH 5.0 dataset, Release build).

### One-line summary (for resume header or LinkedIn)

> Built a C++20 lock-free market-data feed handler and order-book engine processing **20M+ msg/s**
> with sub-µs p50 latency, sequence-gap recovery, and a live Binance L2 adapter with real-time
> dashboard.

### Suggested resume bullets

Pick 2–4 and trim to your format. Each is written in impact-first style; swap verbs as needed.

**Core engine**

- Designed and implemented a **two-thread C++20 market-data pipeline** (ITCH 5.0 ingest →
  zero-copy parser → gap detector → lock-free SPSC ring → order book → publisher) prioritizing
  correctness, then latency, then observability.
- Achieved **20.08 M msg/s sustained throughput** and **2.4 µs p50 / 2.8 ms p99 end-to-end
  latency** at sustainable load on a 500k-message synthetic NASDAQ ITCH replay
  ([benchmark report](reports/benchmark.md)).
- Built a **lock-free single-producer/single-consumer ring buffer** (cache-line-padded head/tail,
  cached cross-core indices, power-of-two masking) with explicit **Block vs Drop backpressure**
  policies; verified **zero data races** under ThreadSanitizer in CI.
- Implemented an **O(1) tick-indexed order book** with intrusive FIFO per price level,
  order-ref hash map for cancel/modify, object pool (no per-order heap alloc), and cached
  best-bid/ask — supporting both order-by-order (ITCH) and aggregated L2 (crypto) apply paths.
- Engineered **sequence-gap detection and delta replay**: buffers out-of-order messages, replays
  in order once the gap fills, and falls back to snapshot reload on overflow — recovered the book
  to a **byte-identical state in 127 µs** under injected gap stress tests.

**Live market data & dashboard**

- Integrated a **live Binance L2 WebSocket adapter** (REST snapshot bootstrap + diff-depth
  stream + update-id resync) driving the same ring/book stack as file replay — no API key, dual
  region support (`binance.com` global + `binance.us`).
- Built an embedded **real-time web dashboard** (HTTP snapshot polling, self-contained HTML/JS,
  no CDN) exposing engine-only telemetry — p50/p99 latency, gap/re-sync counts, ring drops,
  crossed-book detection, and 15-level depth — not available from raw exchange APIs.
- Added **side-by-side Global vs US compare view**, live **@trade tape**, hot **symbol switching**
  (BTCUSDT, ETHUSDT, SOLUSDT, …), and an **alert banner** for gaps, re-syncs, crossed books,
  and wide spreads.

**Quality & tooling**

- Authored **51 GoogleTest unit tests** covering parser byte-level decoding, book FIFO/time
  priority, sequencer gap replay, ring-buffer drop accounting, HDR latency histograms, and L2
  event-boundary crossed detection.
- Set up **multi-platform CI** (Linux + macOS Release builds, ASan/UBSan/TSan sanitizer matrix,
  end-to-end smoke: generate → replay → microbench → markdown report).
- Built a **synthetic ITCH 5.0 generator** and stress harness with offered-load sweeps, gap
  injection, and overload backpressure experiments producing reproducible markdown benchmark
  reports.

### Problem → solution → impact (interview framing)

| Problem | What you built | Measurable outcome |
|---|---|---|
| Exchange feeds arrive faster than a book can update | Lock-free SPSC ring + two-thread pipeline | 20M+ msg/s sustained; p50 ≈ 2.4 µs at 2M msg/s offered |
| Sequence gaps corrupt books silently | Sequencer with delta replay + snapshot fallback | Book identical to clean run after gap; 127 µs recovery |
| Overload must be handled explicitly, not ignored | Block (lossless) vs Drop (counted) backpressure | 0 drops (Block) or exact drop count (Drop) — never silent loss |
| Live crypto feeds use aggregated L2, not order refs | `set_level()` path + Binance update-id resync | Live BTCUSDT streaming with `cross 0`, gap-driven re-snapshot |
| Engine telemetry invisible to consumers | Embedded dashboard fed by consumer thread | Real-time p50/p99, gaps, resyncs, drops, crossed — same-origin JSON |

### Technical skills & keywords (ATS / skills section)

**Languages & standards:** C++20, CMake, Ninja

**Systems & concurrency:** lock-free data structures, SPSC ring buffer, memory ordering
(acquire/release), cache-line alignment, false-sharing avoidance, two-thread producer/consumer
pipelines, backpressure policies

**Market data domain:** NASDAQ ITCH 5.0, order book reconstruction, time priority / FIFO,
top-of-book (L1), market-by-price (L2), sequence-gap recovery, snapshot resync, Binance
diff-depth + update-id windows

**Performance engineering:** zero-copy parsing, mmap file ingest, object pooling, HDR-style
latency histograms, throughput/latency sweeps, microbenchmarks, offered-load stress testing

**Networking & live feeds:** WebSocket/TLS (IXWebSocket), REST snapshot bootstrap, JSON event
decode (nlohmann/json), multi-region endpoint handling (HTTP 451 fallback)

**Testing & CI:** GoogleTest, AddressSanitizer, UndefinedBehaviorSanitizer, ThreadSanitizer,
GitHub Actions, cross-platform builds (Linux GCC, macOS clang)

**Tools:** `itch_gen` (synthetic feed), `feed_handler` (replay), `bench_harness` (markdown
reports), `live_feed` (Binance adapter + dashboard)

### Verified performance metrics

From [`reports/benchmark.md`](reports/benchmark.md) — Apple Silicon laptop, Release `-O3`,
500,001 ITCH 5.0 messages, 65,536-slot ring:

| Metric | Value |
|---|---|
| Peak sustained throughput (Block policy) | **20.08 M msg/s** |
| p50 latency @ 2 M msg/s offered | **2.42 µs** |
| p99 latency @ 2 M msg/s offered | **2.76 ms** |
| Gap recovery time (250k-seq injection) | **127 µs** |
| Book state after gap recovery | **Identical to clean run** |
| Drop policy under overload | **434,463 drops counted exactly** (86.9%) |
| Unit tests | **51** (parser, book, sequencer, ring, instrumentation, L2) |
| CI platforms | **Linux + macOS**; ASan, UBSan, TSan |

Live adapter (typical BTCUSDT session): sub-ms p50 end-to-end ingest→book, `gaps 0` in steady
state, automatic re-snapshot on update-id discontinuity.

### Architecture at a glance

```
┌─────────────┐    ┌────────┐    ┌───────────┐    ┌─────────────────┐    ┌───────────┐    ┌───────────┐
│ FeedSource  │───▶│ Parser │───▶│ Sequencer │───▶│ SPSC ring buffer│───▶│ OrderBook │───▶│ Publisher │
│ ITCH file / │    │ zero-  │    │ gap detect│    │ lock-free,      │    │ tick-index│    │ L1/L2     │
│ Binance WS  │    │ copy   │    │ delta     │    │ Block/Drop BP   │    │ FIFO pool │    │ deltas    │
└─────────────┘    └────────┘    └───────────┘    └─────────────────┘    └───────────┘    └───────────┘
     Thread A          Thread A       Thread B            A→B                    Thread B         Thread B
```

**Thread A (ingest):** read frame → parse → stamp receive time → push to ring.
**Thread B (book):** pop → sequence/gap-check → apply to book → publish → record latency.
**Design doc:** [`DESIGN_NOTES.md`](DESIGN_NOTES.md) — rationale for every data structure choice.

---

## Components

| Component | What it does |
|---|---|
| `FeedSource` | Pluggable adapter. ITCH 5.0 file replayer **and** a live Binance WebSocket adapter (opt-in, see below). Emits framed bytes / events + sequence numbers. |
| `Parser` | Zero-copy binary decoder. Unpacks ITCH fixed layouts into a 56-byte POD `MarketEvent`. No per-message heap allocation. |
| `Sequencer / GapDetector` | Tracks per-stream sequence numbers; on a gap marks the book stale and drives recovery, then resyncs. |
| `SpscRingBuffer` | Lock-free single-producer/single-consumer bounded queue. Power-of-two capacity, cache-line-padded head/tail, explicit backpressure (block vs counted-drop). |
| `OrderBook` | Tick-indexed price levels; each level a FIFO intrusive list; `unordered_map<order_ref,node*>` for O(1) cancel/modify; cached best-bid/ask. Dual path: order-by-order (`apply`) and aggregated L2 (`set_level`). |
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

# Web dashboard with Global / US market filter (runs both feeds in parallel):
./build-live/live_feed --symbol BTCUSDT --serve 8765 --market both
# -> open http://localhost:8765

# Single market only (lighter — no compare toggle):
./build-live/live_feed --symbol BTCUSDT --serve 8765 --market global   # binance.com via data-api.binance.vision
./build-live/live_feed --symbol BTCUSDT --serve 8765 --market us       # binance.us
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

### Web dashboard

Add `--serve PORT` to expose a built-in web dashboard. With `--serve`, **both** Global and US
markets run by default:

```bash
./build-live/live_feed --symbol BTCUSDT --serve 8765 --market both
# -> http://localhost:8765
```

**HTTP endpoints**

| Endpoint | Purpose |
|---|---|
| `/` | Embedded dashboard (HTML/CSS/vanilla JS, no CDN) |
| `/snapshot.json?market=global\|us` | Single-market book + telemetry + trades |
| `/compare.json` | Both markets side-by-side (Global + US) |
| `/markets.json` | Available market ids and labels |
| `/symbols.json` | Active symbol + picker list |
| `/symbol?set=ETHUSDT` (POST) | Hot-switch symbol without restarting the process |

**Dashboard features**

- **Compare / Single view** — side-by-side Global vs US top-of-book, depth ladder, and stats
- **Trade tape** — last 40 trades from Binance `@trade` stream (price, qty, side, time)
- **Alert banner** — flags new sequence gaps, book re-syncs, crossed books, wide spreads (>0.5% of mid)
- **Symbol picker** — BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT, XRPUSDT (hot-restart feeds)
- **Engine telemetry** — updates/s, p50/p99 latency, gaps, re-syncs, ring drops, out-of-band,
  crossed — metrics only available because events flow through the engine, not raw from the exchange

The consumer thread publishes JSON snapshots ~5×/sec; a minimal HTTP server (IXWebSocket) serves
them same-origin. The hot path (WebSocket ingest → ring → book) is untouched by the dashboard.

## Layout

```
include/feedhandler/   public headers (POD types, ring buffer, book, parser, ...)
src/                   library implementations
tests/                 GoogleTest unit + stress tests (51 cases)
benchmarks/            microbenchmarks + stress harness + benchmark reporter
tools/                 itch_gen (synthetic ITCH 5.0 generator)
apps/                  feed_handler (file replay) + live_feed (live Binance L2 + web dashboard, opt-in)
DESIGN_NOTES.md        why each choice was made (interview prep)
reports/               reproducible benchmark output (benchmark.md)
.github/workflows/     CI: build/test, sanitizers, live adapter compile check
```

See [`DESIGN_NOTES.md`](DESIGN_NOTES.md) for the rationale behind every data structure and
the latency / recovery design.

## Continuous integration

Every push and PR runs [`.github/workflows/ci.yml`](.github/workflows/ci.yml):

- **build & test** on Linux (GCC/libstdc++) and macOS (clang/libc++) in Release, full
  `ctest` suite (51 tests), plus an end-to-end smoke test (generate → replay → microbench).
- **AddressSanitizer + UBSan** and **ThreadSanitizer** builds — the latter is the headline
  check for the lock-free SPSC ring buffer (zero data races).
- **live adapter build** — compiles the opt-in Binance adapter (`-DFH_BUILD_LIVE=ON`, deps via
  `FetchContent`) so it stays buildable, without opening a live socket in CI.

## License

[MIT](LICENSE) © 2026 Aryan Lakhani.
