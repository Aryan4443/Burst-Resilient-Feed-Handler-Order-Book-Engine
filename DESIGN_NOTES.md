# Design Notes

> Running log of *why* each design choice was made. This is the interview-prep artifact:
> every data structure, latency claim, and recovery decision should be defensible from here.

Priorities, in strict order (from the project brief):
1. **Correctness** — never silently corrupt or lose data.
2. **Low latency on the hot path** — no per-message heap allocation, no locks between threads.
3. **Measurability** — everything that matters is counted and timed.

---

## Architecture

```
FeedSource ──raw frames──▶ Parser ──MarketEvent──▶ Sequencer/GapDetector ──┐
(ITCH file /                (zero-copy decode)       (per-stream seq, gaps)  │
 crypto WS)                                                                  │ push
                                                                             ▼
                                                              [ SPSC lock-free ring buffer ]
                                                                             │ pop
                                                                             ▼
                                              OrderBook engine ──L1/L2 deltas──▶ Publisher
                                              (tick-indexed levels)
        Stress harness drives load (Nx replay, gap/burst injection)
        Instrumentation taps latency / throughput / queue depth / drops
```

**Thread A (ingest):** read frame → parse → sequence/gap-check → push to ring buffer.
**Thread B (book):** pop ring buffer → apply to book → publish.
**Thread C (optional):** metrics aggregation / periodic snapshot.

The lock-free SPSC ring buffer between A and B is the headline concurrency artifact.

---

## Core types (`market_event.hpp`, `feed_source.hpp`)

**Why a flat POD `MarketEvent` instead of a class hierarchy / variant?**
- It must travel through the SPSC ring buffer by value with a plain `memcpy` — no vtables,
  no heap ownership, no destructors running on the hot path. `static_assert`s enforce
  `trivially_copyable` + `trivially_destructible`.
- A `std::variant` per message type would add a discriminant + alignment to the largest
  member anyway, and would complicate the branch-free copy. A single struct with an
  `EventType` tag is simpler and just as compact.
- Field ordering puts all 8-byte members first to eliminate internal padding → `sizeof == 56`.

**Why collapse the ITCH type alphabet into a small `EventType`?**
- Decouples downstream components from the wire format. The book only knows
  Add/Cancel/Delete/Execute/Replace/Trade; swapping in a crypto adapter later means
  writing a new parser, not touching the book.

**Why carry both `stock_locate` and `symbol`?**
- `stock_locate` is the feed's compact 2-byte instrument id → cheap book key / array index.
- `symbol` is an 8-byte inline copy for human-facing output. Copying 8 bytes inline keeps
  the event self-contained (it must outlive the decode buffer) without a heap string.

**Why does `RawFrame` hand out a borrowed pointer?**
- The pointer aliases the FeedSource's own buffer (mmap'd file / socket buffer), valid only
  until the next `next_frame()`. This is what makes the *read* path zero-copy: the parser
  decodes straight out of that memory; the only copy is decoded fields into the POD event.

---

## Parser (`itch_parser.hpp/.cpp`, `detail/byte_read.hpp`)

**What "zero-copy" actually means here.** The parser never duplicates the input buffer and
never allocates. It reads fields straight out of the borrowed `RawFrame` memory and writes
decoded values into a caller-owned `MarketEvent`. The only bytes copied are (a) the 8-byte
inline `symbol` and (b) the scalar fields — both unavoidable because the event must outlive
the decode buffer to travel through the ring buffer. There is no `malloc`/`new` on this path.

**Unaligned big-endian loads (`byte_read.hpp`).** ITCH integers are big-endian and not
guaranteed aligned within a frame, so casting a `const uint32_t*` at an arbitrary offset is
UB. We assemble values byte-by-byte with shifts; clang recognises the idiom and emits a
single load + `rev`/`movbe`. Correct *and* fast.

**Why reset the event (`out = {}`) each message?** A `MarketEvent` is reused across millions
of calls. Clearing 56 bytes (a couple of vector stores) guarantees a `Delete` never inherits
the previous `Add`'s symbol/price — correctness beats the negligible cost. Verified by the
`ReusedEventHasNoStaleFields` test.

**Length table covers all of ITCH 5.0, decode covers the book-relevant subset.**
`message_length()` knows every 5.0 type (so a framing-less reader can stride correctly and a
framed reader can validate), while `parse()` fully decodes A/F/E/C/X/D/U/P/S and reports
other valid types as `Ok` + `EventType::Unknown`. `parse()` returns a 3-state status
(`Ok`/`ShortBuffer`/`UnknownType`) instead of throwing — exceptions have no place on the hot
path, and the sequencer/harness need to *count* malformed frames, not unwind.

**Replace loses time priority — on purpose.** ITCH `U` is semantically cancel-original +
add-new; the new order goes to the back of the FIFO at its (possibly new) price. The parser
surfaces both refs; the book enforces the priority loss.

*Tests:* 14 cases incl. a fully hand-spelled Add Order byte sequence asserting every field,
one test per decoded type, reuse/stale-field, short-buffer, unknown-type, empty-buffer.

## Order book (`order_book.hpp/.cpp`)

**The three structures and why each exists:**
- **Tick-indexed level array** (`std::vector<PriceLevel>`): `index = (price - min)/tick`
  gives **O(1)** access to any price level — no tree, no hash, no comparisons. Trade-off:
  memory is proportional to the configured price band, and the band must bound the
  instrument's range. The alternative `std::map<price,level>` is O(log n) per op and
  pointer-chases cache-unfriendly nodes; a hash map is O(1) avg but loses the *ordered*
  iteration we need for depth and has a worse constant. For a single instrument the band is
  cheap (default 2M levels) and the array wins on the hot path.
- **Intrusive FIFO doubly-linked list per level**: preserves **time priority** within a
  price. Intrusive (the node *is* the order) means add/remove are pointer splices with zero
  allocation and no node/payload indirection.
- **`unordered_map<order_ref, Order*>`**: cancel/execute/delete/replace arrive by order id,
  not price. The map turns "find this order" into **O(1)** average, then the intrusive
  pointers make the unlink O(1).

**Cached best-bid/ask indices** give O(1) top-of-book. On an add that improves the top we
just move the index. When the best level empties we **rescan toward the next populated
price** — amortised O(1), worst-case a scan across empty ticks. That scan is the documented
price of the array design; in practice the top is dense so it's tiny.

**Object pool (free list).** Orders come from a chunked pool; chunks are heap arrays that
never move, so every `Order*` in the map / lists stays valid. `alloc`/`release` are O(1)
list pushes — **no per-message `new`/`delete`** in steady state, only when the pool grows.

**Operation complexity:** add O(1), cancel/execute O(1), delete O(1) amortised, replace O(1)
(= delete + add; ITCH `U` intentionally loses time priority). `depth(n)` is O(n + empty
ticks skipped).

**Per-level `side` field** makes best-pointer rescans robust even if the feed momentarily
crosses: we only ever pick a Buy level as best-bid. A clean displayed ITCH book never
crosses, so `is_crossed()` / `crossed_observed` are a *diagnostic* for parse/sequence errors.

**Correctness counters** (`unknown_ref`, `rejected_out_of_band`, `crossed_observed`): a
cancel/delete for an unknown ref (e.g. after a dropped Add during a gap) is **counted, not
crashed** — this is exactly the signal the sequencer/recovery layer reacts to. `reset()`
clears the book (for snapshot reload) but **preserves cumulative stats**.

*Tests:* 14 cases — build-up with top-of-book checked after each event, ordered+bounded
depth, partial/full cancel & execute, delete advancing best, FIFO survival, replace
(price+ref move, old ref becomes unknown), unknown-ref counting, out-of-band rejection,
crossed detection, reset-then-reuse, per-type stat counts.

## SPSC ring buffer (`spsc_ring_buffer.hpp`) — the headline concurrency artifact

**Why SPSC + lock-free is enough.** Exactly one producer (Thread A) and one consumer
(Thread B). With that restriction we never need a CAS or a lock — only paired
acquire/release on two indices. No kernel involvement on the hot path.

**Memory ordering (be ready to whiteboard this):**
- Producer: write `buffer_[tail & mask]`, **then** `tail_.store(release)`. The release
  publishes the payload write.
- Consumer: `tail_.load(acquire)` **before** reading the slot. The acquire pairs with the
  producer's release, so the payload write *happens-before* the read — the consumer can never
  observe a half-written slot.
- Symmetrically, the consumer's `head_.store(release)` pairs with the producer's
  `head_.load(acquire)`, so the producer never overwrites a slot the consumer hasn't finished
  reading (slot reuse safety).
- Loads of a thread's *own* index are `relaxed` — it's the sole writer.

**Monotonic 64-bit counters, not wrapped indices.** `size == tail - head`, so full vs empty
is unambiguous and we use the *whole* capacity (no sacrificial empty slot). 64-bit counters
don't realistically wrap, sidestepping ABA on the indices.

**Cache-line isolation + index caching (the two perf tricks):**
- `head_` and `tail_` are each `alignas(cacheline)` so the producer's writes to `tail_` don't
  invalidate the consumer's `head_` line (no false sharing).
- Each side caches the *other* side's index and only reloads the real atomic when its cache
  says full/empty. This removes a cross-core atomic load from the common-case fast path — the
  Disruptor / folly trick — dramatically cutting cache-line bounces under load.

**Backpressure is explicit and counted (`SpscChannel`).** `Block` spins with a `cpu_relax()`
hint and never loses data; `Drop` discards and increments an **exact** counter. The
invariant *offered == accepted + dropped* is the project's "never silently corrupt" guarantee.

**Validation:** ThreadSanitizer reports **zero data races** across the blocking (500k msgs)
and drop-stress runs; AddressSanitizer+UBSan clean. A deterministic overflow test proves the
drop counter is exact; the concurrent drop-stress proves *accepted == consumed* and
*accepted + dropped == offered* while drops actually occur.

## Sequencer / gap detection / recovery (`sequencer.hpp/.cpp`)

**Classification per message** (O(1) average): `seq < expected` → duplicate/old → **drop**;
`seq == expected` → **apply** and advance; `seq > expected` → a hole exists → **buffer** the
future message and mark **stale**.

**Two recovery modes — exactly what production feeds do:**
- **Delta replay (small/transient gap).** Out-of-order or briefly-lost messages are buffered
  in a bounded reorder map; when the missing `expected` seq finally arrives we apply it and
  then *drain the contiguous run* (`pop_ready`), filling the hole and clearing stale. This
  models UDP **A/B line arbitration** (two identical multicast feeds; the second often
  supplies the packet the first dropped) and a **retransmit request** to a replay server.
- **Snapshot reload (large/unrecoverable gap).** If the reorder buffer overflows, replay
  can't catch us up, so we signal `needs_snapshot()`, reload a book **snapshot** at some
  sequence, set `expected = snapshot_seq + 1`, and resume on the delta stream.

**Why "stale" matters.** While a hole is open the book may be wrong, so the consumer should
suppress publishing until we resync and the flag clears — never emit a book you can't vouch
for.

**How real exchanges handle this (interview answer):**
- **NASDAQ ITCH over MoldUDP64:** sequenced UDP; each packet carries a sequence number.
  Recovery = the **SoupBinTCP/retransmission** request server for missed packets, plus
  **Glimpse**, a TCP snapshot service that streams the current book so a late joiner / gapped
  consumer can rebuild then follow the live multicast.
- **CME MDP 3.0:** two redundant multicast feeds (**A/B**) you arbitrate by sequence; a
  separate **recovery/replay feed**; and **snapshot feeds** (`MDP snapshot`, plus
  instrument-level "natural refresh") that periodically publish full book state so a consumer
  can resynchronise without a point-to-point request.

Our `Sequencer` captures the essence of both: bounded in-line replay buffer for small gaps,
snapshot resync for big ones, with a stale flag gating output and exact counters
(`gap_episodes`, `missing_total`, `reorder_overflow`, `snapshot_recoveries`).

*Tests:* in-order baseline, duplicate drop, **gap-filled-by-replay recovers book to the exact
in-order state**, **snapshot-reload recovers book**, reorder-overflow → snapshot requested.

## Instrumentation (`instrumentation.hpp`)

**End-to-end latency = `book_updated_ns - received_ns`.** Thread A stamps `received_ns` once
when it takes a frame; Thread B stamps again right after `book.apply()` and records the diff.

**How we time without adding latency (interview answer):**
- `steady_clock::now()` is a **user-space** counter read (`mach_absolute_time` on Apple arm64;
  no syscall), ~20 ns. We read it **twice per message**, off the actual parse/apply compute.
- The histogram `record()` is **allocation-free, O(1)**: a `__builtin_clzll` to find the
  bucket, then one relaxed atomic increment. No locks, no `new`, ~ns.
- The snapshot/percentile read is **relaxed-atomic** over the bucket array, so a reporter
  thread (Thread C / dashboard) can read p50/p99/p99.9 at any time **without perturbing** the
  single writer and without a consistent-snapshot lock (metrics tolerate being a hair stale).

**Why HdrHistogram, not a fixed-bucket histogram or storing all samples?** Latency spans
many orders of magnitude (sub-µs to ms under bursts). HDR buckets by power-of-two octave with
a fixed number of linear sub-buckets per octave → **constant relative error** (~0.1% at 3 sig
figs) across the whole range in **bounded memory** (~kBs), and never allocates while
recording. Storing every sample would blow memory and cache; a linear histogram either loses
tail resolution or needs millions of buckets.

**Throughput + occupancy + drops.** A relaxed atomic message counter feeds a `RateSampler`
(Δcount/Δt) for msg/s; ring `size_approx()` gives occupancy; the channel's exact `dropped()`
gives loss. All folded into one `PipelineMetricsSnapshot`.

*Tests:* empty, uniform-distribution exact percentiles (values in the linear octave),
single-value, large-value relative-error bound (<0.1%), clamp-above-highest, reset, counter,
rate sign.

## Integration: file source, publisher, pipeline (`itch_file_source`, `publisher`, `pipeline`)

**`ItchFileSource` (zero-copy ingest).** `mmap`s the file and hands each frame up as a
borrowed pointer into the mapping (`MADV_SEQUENTIAL` for read-ahead). Framing is the public
BinaryFILE convention: 2-byte big-endian length + payload. Message index = feed sequence
number (the simple file format carries no MoldUDP sequence).

**`Publisher` (L1/L2 deltas).** Detects top-of-book (L1) changes on each update and
counts/retains them; it deliberately does *not* walk full depth per update (an L2 snapshot is
available on demand via `depth()`), keeping the hot path cheap.

**`run_pipeline` — the two-thread wiring.**
- **Thread A:** `next_frame` → `itch::parse` → stamp `recv_ns` → push `PipelineMsg` to the
  `SpscChannel` (applies the backpressure policy).
- **Thread B:** pop → `Sequencer::offer` → on `Apply`, `book.apply` + drain replayed buffered
  events + `publisher.on_update` + record latency; on `Overflow`, snapshot-reload.
- **Why gap detection lives on Thread B (with the book), not on A as the diagram suggests:**
  recovery has to manipulate the book, which lives on B. We push the *raw sequence number*
  across the ring so the detector still sees original stream order. This keeps recovery
  local and lock-free; the choice is called out as a deliberate deviation.

## Stress harness & benchmark report (`benchmarks/`)

**What it measures (and how to read it):**
1. **Peak sustained throughput** — producer flat-out, Block policy: the consumer is the
   bottleneck so this is the saturation throughput (~10–18 M msg/s here, machine-dependent).
2. **Latency vs offered load** — sweeps a *fixed offered rate* with uniform spacing and reads
   p50/p99/p99.9. Below saturation p50 is single-digit µs; the ms-scale p99 tail is OS
   scheduling jitter on a non-isolated laptop, not engine cost (the parser does 100+ M/s and
   the book ~25 M/s single-threaded in the microbench).
3. **Gap recovery** — injects a sequence gap (reorder window < reorder capacity), confirms
   the final book is **identical to the clean run**, and reports recovery time.
4. **Backpressure under overload** — with a deliberately slowed consumer so the producer
   reliably outruns it: **Block** saturates the ring losslessly (latency grows, 0 drops);
   **Drop** discards ~84% with an *exact* count and falls back to snapshot reloads. Two
   principled overload responses.

**Load-generation honesty.** A flat-out busy-spin producer starves the consumer on a laptop
(both spinning), so offered-rate pacing releases small bursts then **sleeps** the accumulated
interval (yields the core). This keeps achieved≈offered without core contention; the residual
burst is why p50 is a few µs rather than the ~40 ns empty-queue floor.

**Synthetic ITCH (`itch_gen`).** Emits wire-valid ITCH 5.0 (A/E/X/D/U/P/S) and tracks a live
order set so cancel/execute/delete/replace reference real orders → the book stays consistent
(0 unknown-ref) and non-crossing (disjoint bid/ask bands around a fixed mid). Lets the whole
engine run end-to-end with no external download, while parsing the real binary layout.

## Live market-data adapter (Binance L2)

The engine isn't tied to files — `FeedSource` is the seam, and `apps/live_feed.cpp` (opt-in
`-DFH_BUILD_LIVE=ON`) drives the **same** book/ring/latency stack from a live Binance feed. Kept
behind a flag because it pulls a WebSocket/TLS client (IXWebSocket) + JSON (nlohmann/json); the
core, tests, and CI stay dependency-free.

**Why Binance.** NASDAQ TotalView-ITCH is a paid colocated multicast feed; Coinbase's full/level3
channels now require authentication. Binance's diff-depth stream is genuinely public (no key),
high-volume, and carries a snapshot + update-id model — the recovery story the project is about.

**L2 vs the order-by-order book.** Binance publishes *aggregated price-level* (market-by-price)
updates — `[price, total_qty]`, not individual orders. So the book grew a second apply path,
`OrderBook::set_level(side, price, total)` (absolute size; 0 removes the level), sharing the same
tick-indexed array and best-of-book pointers as the order-by-order `apply()`. `set_level()` does
**not** increment `crossed_observed`; the live consumer calls `observe_event_boundary()` once per
Binance event (when the update-id window `U` changes) so transient mid-event crosses from
multi-level diffs are not counted. `live_orders()` then counts populated price levels.

**Threading is unchanged.** Thread A is IXWebSocket's callback: decode each level change to an
`L2SetLevel` event carrying the diff event's update-id window `U..u`, stamp `recv_ns`, push to the
SPSC ring. Thread A is deliberately *dumb* — it never touches the book or decides ordering. Thread
B pops, runs the resync state machine, and applies via `set_level`.

**Resync = the same gap story in Binance's id space.** Binance update ids advance by a *range* per
event (not +1), so the ITCH `Sequencer` doesn't fit; the consumer implements Binance's documented
rule directly: snapshot gives `lastUpdateId L`; drop events with `u <= L`; the first applied event
must straddle `L+1` (`U <= L+1 <= u`); thereafter each event must satisfy `U == prev_u + 1`, else
it is an unrecoverable gap → **re-snapshot** (book reset + reload + re-seek). Same
detect-discontinuity-then-reload recovery as the file pipeline, expressed on update ids.

**Honest adaptations (called out, not hidden):**
- **Price band from the snapshot mid (±%).** The tick-indexed array is sized to a band around the
  live mid; deep limit orders outside it are counted as `out_of_band`, not crashed (top-of-book is
  exact). Crypto's wide absolute prices would otherwise need an enormous array.
- **Fixed-point scaling.** Quote/size arrive as decimal strings; prices → integer ticks (`×100`,
  cents, for BTCUSDT), sizes → scaled units (`×1e6`, micro-BTC). A per-level size exceeding the
  uint32 ring field is clamped and *counted* (effectively never on BTCUSDT).
- **String order ids** (used by the order-by-order Coinbase variant) hash to the engine's `uint64`
  order ref via FNV-1a.
- **Region restriction:** `api.binance.com` returns HTTP 451 in some regions; the tool detects it
  and suggests the `binance.us` endpoints (`--rest-host` / `--ws-url`).

*Verified live:* reconstructs BTCUSDT top-of-book with a correct spread, `cross 0` (never an
inverted book), `gaps 0` once streaming, p50 ≈ sub-ms end-to-end. Unit tests cover the `set_level`
path (build / absolute-resize / zero-removes-and-advances / no-op / out-of-band).

### Web dashboard (`--serve PORT`)

The dashboard visualises the **engine's** state, not the exchange's. Putting the view on the
consumer side is the whole point: things like end-to-end p50/p99 latency, ring `drops`, `gaps`,
`re-syncs`, `out_of_band`, and `crossed` only exist *because* events flowed through the ring and
the resync state machine — a browser pointed straight at Binance could render a price ladder but
none of that telemetry.

- **Transport: HTTP snapshot polling, not a push socket.** The consumer thread serialises a small
  JSON snapshot (top-of-book, 15 levels/side, the stats line) ~5×/sec into a mutex-guarded string;
  a minimal `ix::HttpServer` (reusing the IXWebSocket dependency the live build already pulls —
  no new packages, no CDN) serves it at `/snapshot.json`, and the page polls every 250 ms. Polling
  was chosen over a WebSocket push because it's *pull-throttled* (a slow/background tab simply asks
  less often, never backs up an unbounded send queue), trivially same-origin (no CORS, no
  handshake), and easy to verify with `curl`. At a few Hz the cost is irrelevant.
- **No coupling to the hot path.** The publish is off the critical section: the snapshot is built
  from the book between ring drains on the *consumer* thread, and the HTTP handler (a separate
  server thread) only ever reads the last finished string under a short lock. Thread A and the SPSC
  ring are untouched; `--serve` adds nothing measurable to ingest latency.
- **Self-contained & opt-in.** The page (HTML/CSS/vanilla JS, hand-drawn `<canvas>` sparkline, no
  libraries) is embedded as a string in the binary, so there are no asset paths to ship and the
  default headless build is unaffected — the server only starts when `--serve` is passed.

## Build / toolchain

- C++20, CMake (+ Ninja), GoogleTest via `FetchContent`.
- `-O3 -DNDEBUG` for Release; `-Wall -Wextra -Wpedantic -Wshadow -Wconversion`.
- `FH_ASAN=ON` adds ASan+UBSan for the correctness-focused test builds.
- Apple clang 17 / arm64 verified locally.
