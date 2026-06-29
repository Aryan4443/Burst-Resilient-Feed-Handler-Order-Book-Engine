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

## What this is for

Exchanges and data vendors do not send a clean “current price.” They send a **high-speed stream
of small messages** — add order, cancel, trade, replace — often millions per second at the open.
Downstream systems need a **reconstructed order book**: the live bid/ask ladder and depth at
each price.

This project is the **middle layer** that turns that raw stream into a trustworthy book:

```
Exchange / recorded file          Your engine                    Consumers
(raw ITCH or WebSocket)    →   feed handler + order book   →   strategy, UI, risk, logs
```

It is **not** a trading app (no orders, no P&amp;L). It is the infrastructure a trading stack,
research pipeline, or monitoring tool sits on top of.

### Who uses something like this

| Role | Why they need a feed handler + book |
|---|---|
| **Market makers / HFT firms** | Quote prices and react to book changes in microseconds |
| **Prop / quant trading desks** | Feed live or replayed prices into strategies and simulators |
| **Exchanges & ECNs** | Maintain the official book that clients trade against |
| **Brokerages & retail platforms** | Power bid/ask displays and depth charts |
| **Risk & compliance** | Detect stale feeds, crossed markets, or missing data before bad decisions |
| **Market-data engineers** | Validate a new feed adapter, recovery logic, or performance before production |
| **Students & researchers** | Learn how real market-data systems are structured end-to-end |

This repo is a **working reference implementation** of that layer — same architectural patterns
as production systems, runnable on a laptop without exchange credentials or colocation.

### Where it sits in a real stack

```
┌──────────────────┐
│ Binance / NASDAQ │  raw messages (WebSocket, multicast, or file)
└────────┬─────────┘
         ▼
┌──────────────────┐
│  Feed handler    │  parse, sequence-check, buffer bursts  ← this project
│  + order book    │  reconstruct L1/L2, recover from gaps
└────────┬─────────┘
         ▼
┌──────────────────┐
│ Strategy / UI /  │  pricing, charts, alerts, backtests
│ risk / storage   │
└──────────────────┘
```

Production deployments add colocation, kernel bypass, and thousands of symbols. The **ideas** —
lock-free ingest, gap recovery, tick-indexed book — are the same.

---

## Use cases

### 1. Replay recorded market data (research & backtesting)

Generate or load an ITCH 5.0 file and replay it through the full pipeline.

```bash
./build/itch_gen --out data/sample.itch --orders 500000
./build/feed_handler --file data/sample.itch
```

**When:** You want to verify book correctness on historical or synthetic data, prototype a
consumer that reads top-of-book, or regression-test after a code change — without a live
exchange connection.

**Who:** Quant researchers, strategy developers, infra engineers validating feed parsing.

---

### 2. Stress-test throughput, latency, and recovery

Run the benchmark harness to measure how the engine behaves under load, gaps, and overload.

```bash
./build/benchmarks/bench_harness --file data/sample.itch --report reports/benchmark.md
```

**When:** Before trusting a feed handler in production — e.g. “Can we sustain 20M msg/s?”, “Does
gap recovery restore an identical book?”, “What happens when the consumer falls behind?”

**Who:** Low-latency / market-data engineers, anyone sizing ring buffers or backpressure policy.

Sample results ([`reports/benchmark.md`](reports/benchmark.md)): **20.08 M msg/s** sustained,
**2.4 µs p50** latency at 2M msg/s offered, **127 µs** gap recovery to a byte-identical book.

---

### 3. Stream a live exchange book (monitoring & demo)

Connect to Binance’s public L2 diff-depth feed — no API key required.

```bash
./build-live/live_feed --symbol BTCUSDT --duration 30
```

**When:** You need a **live** reconstructed book with engine telemetry (latency, gaps,
re-syncs, crossed detection) rather than trusting the exchange UI alone.

**Who:** Crypto traders comparing venues, engineers testing WebSocket + snapshot resync logic.

---

### 4. Monitor Global vs US with a live dashboard

Run both Binance regions in parallel and open the built-in web UI.

```bash
./build-live/live_feed --symbol BTCUSDT --serve 8765 --market both
# → http://localhost:8765
```

**When:** You want to **watch** live depth, compare Global vs US pricing on the same symbol,
see the trade tape, and get **alerts** when the feed gaps, re-syncs, or shows a crossed book.

**Who:** Traders monitoring cross-region spreads, demo viewers, anyone validating that recovery
logic works on a real feed.

**Dashboard endpoints:** `/snapshot.json`, `/compare.json`, `/symbols.json` — see
[Web dashboard](#web-dashboard) below.

---

### 5. Learn market-data systems architecture

Read the code and [`DESIGN_NOTES.md`](DESIGN_NOTES.md) to see *why* each piece exists: POD
events through a lock-free ring, ITCH zero-copy parsing, sequencer gap replay, tick-indexed
FIFO book, Binance update-id resync, Block vs Drop backpressure.

**When:** Studying for infra / HFT / market-data interviews, or building a similar system from
scratch.

**Who:** CS students, career switchers, engineers moving into finance tech.

---

## Architecture

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

See [`DESIGN_NOTES.md`](DESIGN_NOTES.md) for the rationale behind every data structure choice.

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
| `/health` | Liveness probe (`{"ok":true}`) for Docker / Fly.io |
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

For cloud deployment, bind on all interfaces: `--serve 8080 --bind 0.0.0.0`, or set the `PORT`
environment variable (see [Deploy](#deploy) below).

## Deploy

Ship the live dashboard as a Docker container. The image builds `live_feed` with the Binance
adapter and listens on `0.0.0.0` when the `PORT` env var is set (standard for Fly.io, Railway,
Render, etc.).

### Docker (local or any host)

```bash
# Build
docker build -t order-book-live .

# Run — dashboard at http://localhost:8080
docker run --rm -p 8080:8080 \
  -e PORT=8080 \
  -e SYMBOL=BTCUSDT \
  -e MARKET=both \
  order-book-live

# Or with docker compose
docker compose up --build
```

**Environment variables**

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `8080` | HTTP port; starts dashboard when set (binds `0.0.0.0`) |
| `SYMBOL` | `BTCUSDT` | Trading pair |
| `MARKET` | `both` | `global`, `us`, or `both` |
| `FH_BIND` | `0.0.0.0` (when `PORT` set) | Override listen address |

**Health check:** `GET /health` → `{"ok":true}`

### Fly.io (recommended — free tier, HTTPS included)

Requires [flyctl](https://fly.io/docs/hands-on/install-flyctl/):

```bash
fly auth login
fly launch --no-deploy    # use existing fly.toml; pick a unique app name + region
fly deploy
fly open                  # https://<your-app>.fly.dev
```

Edit `app = "order-book-live"` in [`fly.toml`](fly.toml) to a unique name before launching.
The app runs continuously (`auto_stop_machines = off`) so WebSocket feeds stay connected.

### Manual server (VPS)

```bash
cmake -S . -B build-live -G Ninja -DCMAKE_BUILD_TYPE=Release -DFH_BUILD_LIVE=ON
cmake --build build-live --target live_feed

PORT=8080 SYMBOL=BTCUSDT MARKET=both ./build-live/live_feed
# or: ./build-live/live_feed --serve 8080 --bind 0.0.0.0 --market both
```

Put nginx or Caddy in front for HTTPS if exposing publicly.

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
