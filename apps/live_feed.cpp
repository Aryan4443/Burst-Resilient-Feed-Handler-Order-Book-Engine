// Live Binance feed adapter (opt-in: configure with -DFH_BUILD_LIVE=ON).
//
// Drives the SAME engine as the file replayer -- tick-indexed order book, lock-free SPSC ring,
// latency histogram -- but ingests Binance's *public* diff-depth stream (no API key needed)
// instead of a recorded ITCH file. Binance publishes aggregated price-level (L2 / market-by-
// price) updates, so the book is driven through OrderBook::set_level() (absolute size per
// price level) rather than the order-by-order apply() path used for ITCH/Coinbase.
//
// Threading (faithful to the rest of the project):
//   * Thread A = IXWebSocket's callback thread: receive JSON -> decode each level change to an
//     L2SetLevel MarketEvent (carrying the event's update-id window U..u) -> stamp recv time
//     -> push into the SPSC ring (backpressure policy applied here). The producer is "dumb":
//     it never touches the book or decides ordering.
//   * Thread B = consumer: pop -> run Binance's snapshot/resync state machine on the update ids
//     (the gap-detection + snapshot-recovery story, adapted to Binance's range-id semantics) ->
//     OrderBook::set_level -> record end-to-end latency.
//
// Bootstrap / resync (Binance's documented procedure):
//   1. Open the diff stream; events queue in the ring.
//   2. REST snapshot -> lastUpdateId L; size the tick band around the mid; preload levels.
//   3. Drop queued events with u <= L; the first applied event must straddle L+1 (U <= L+1 <= u);
//      thereafter each event must satisfy U == prev_u + 1, else it's an unrecoverable gap and we
//      re-snapshot. This is the same detect-discontinuity-then-reload recovery the file pipeline
//      does with the Sequencer, expressed in Binance's update-id space.

#include <ixwebsocket/IXHttpClient.h>
#include <ixwebsocket/IXNetSystem.h>
#include <ixwebsocket/IXWebSocket.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "feedhandler/instrumentation.hpp"
#include "feedhandler/market_event.hpp"
#include "feedhandler/order_book.hpp"
#include "feedhandler/pipeline.hpp"  // PipelineMsg, kRingCapacity
#include "feedhandler/spsc_ring_buffer.hpp"

using json = nlohmann::json;
using namespace fh;

namespace {

std::atomic<bool> g_stop{false};
std::atomic<std::uint64_t> g_saturated{0};  // level sizes that hit the uint32 transport ceiling
void on_signal(int) { g_stop.store(true); }

struct Config {
  std::string symbol = "BTCUSDT";        // Binance symbol (upper case)
  std::int64_t price_scale = 100;        // engine ticks per 1.0 quote unit (BTCUSDT step $0.01)
  std::int64_t size_scale = 1'000'000;   // engine size units per 1.0 base unit (micro-BTC)
  double band_pct = 10.0;                // book covers snapshot mid +/- this percent
  bool drop = false;                     // Drop vs Block backpressure
  int duration_s = 0;                    // 0 => run until Ctrl-C
  int depth_ms = 100;                    // diff stream cadence: 100 or 1000
  std::string ws_url;                    // overrides the derived stream URL when set
  std::string rest_host = "https://api.binance.com";
};

std::string lower(std::string s) {
  for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

std::int64_t px_to_ticks(const std::string& s, std::int64_t scale) {
  try {
    return static_cast<std::int64_t>(std::llround(std::stod(s) * static_cast<double>(scale)));
  } catch (...) {
    return -1;
  }
}

// Scaled quantity, clamped to the uint32 the ring's MarketEvent.size carries (a single BTCUSDT
// level virtually never exceeds ~4290 BTC at 1e6 scale; clamps are counted for honesty).
std::uint64_t scaled_qty(const std::string& s, std::int64_t scale) {
  double v = 0.0;
  try {
    v = std::stod(s);
  } catch (...) {
    return 0;
  }
  const long long q = std::llround(v * static_cast<double>(scale));
  if (q <= 0) return 0;
  if (q > static_cast<long long>(UINT32_MAX)) {
    g_saturated.fetch_add(1, std::memory_order_relaxed);
    return UINT32_MAX;
  }
  return static_cast<std::uint64_t>(q);
}

std::string ns_str(std::uint64_t ns) {
  char b[48];
  if (ns < 1'000)
    std::snprintf(b, sizeof(b), "%llu ns", static_cast<unsigned long long>(ns));
  else if (ns < 1'000'000)
    std::snprintf(b, sizeof(b), "%.2f us", static_cast<double>(ns) / 1e3);
  else
    std::snprintf(b, sizeof(b), "%.2f ms", static_cast<double>(ns) / 1e6);
  return b;
}

struct L2Level {
  Side side;
  std::int64_t price_ticks;
  std::uint64_t size;
};

struct Snapshot {
  bool ok = false;
  std::uint64_t last_update_id = 0;
  std::int64_t best_bid = -1;
  std::int64_t best_ask = -1;
  std::vector<L2Level> levels;
};

Snapshot fetch_snapshot(const Config& cfg) {
  Snapshot s;
  ix::HttpClient http(/*async=*/false);
  ix::SocketTLSOptions tls;
  tls.caFile = "SYSTEM";
  http.setTLSOptions(tls);
  auto args = http.createRequest();
  args->connectTimeout = 10000;
  args->transferTimeout = 20000;
  args->extraHeaders["User-Agent"] = "feedhandler-live/0.1";
  const std::string url =
      cfg.rest_host + "/api/v3/depth?symbol=" + cfg.symbol + "&limit=5000";
  auto resp = http.get(url, args);
  if (!resp || resp->statusCode != 200) {
    std::cerr << "snapshot HTTP failed (status " << (resp ? resp->statusCode : -1) << ")";
    if (resp && (resp->statusCode == 451 || resp->statusCode == 403))
      std::cerr << " -- region-restricted? try --rest-host https://api.binance.us "
                   "--ws-url wss://stream.binance.us:9443/ws/"
                << lower(cfg.symbol) << "@depth@100ms";
    std::cerr << "\n";
    return s;
  }
  json j;
  try {
    j = json::parse(resp->body);
  } catch (...) {
    std::cerr << "snapshot JSON parse failed\n";
    return s;
  }
  if (!j.contains("lastUpdateId")) return s;
  s.last_update_id = j["lastUpdateId"].get<std::uint64_t>();
  auto load = [&](const char* key, Side side, bool is_bid) {
    if (!j.contains(key)) return;
    for (const auto& lvl : j[key]) {
      if (lvl.size() < 2) continue;  // [price, qty]
      const std::int64_t px = px_to_ticks(lvl[0].get<std::string>(), cfg.price_scale);
      if (px < 0) continue;
      const std::uint64_t qty = scaled_qty(lvl[1].get<std::string>(), cfg.size_scale);
      if (qty == 0) continue;
      if (is_bid && (s.best_bid < 0 || px > s.best_bid)) s.best_bid = px;
      if (!is_bid && (s.best_ask < 0 || px < s.best_ask)) s.best_ask = px;
      s.levels.push_back({side, px, qty});
    }
  };
  load("bids", Side::Buy, true);
  load("asks", Side::Sell, false);
  s.ok = true;
  return s;
}

Config parse_args(int argc, char** argv) {
  Config c;
  for (int i = 1; i < argc; ++i) {
    const std::string k = argv[i];
    auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : std::string{}; };
    if (k == "--symbol") c.symbol = next();
    else if (k == "--price-scale") c.price_scale = std::stoll(next());
    else if (k == "--size-scale") c.size_scale = std::stoll(next());
    else if (k == "--band-pct") c.band_pct = std::stod(next());
    else if (k == "--drop") c.drop = true;
    else if (k == "--duration") c.duration_s = std::stoi(next());
    else if (k == "--depth-ms") c.depth_ms = std::stoi(next());
    else if (k == "--ws-url") c.ws_url = next();
    else if (k == "--rest-host") c.rest_host = next();
    else if (k == "--help") {
      std::cout << "usage: live_feed [--symbol BTCUSDT] [--price-scale 100] "
                   "[--size-scale 1000000]\n"
                   "                 [--band-pct 10] [--depth-ms 100|1000] [--drop] "
                   "[--duration SECONDS]\n"
                   "                 [--rest-host URL] [--ws-url URL]\n"
                   "Streams Binance's public diff-depth (L2) feed through the engine. "
                   "No API key required.\n";
      std::exit(0);
    }
  }
  return c;
}

}  // namespace

int main(int argc, char** argv) {
  const Config cfg = parse_args(argc, argv);
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);
  ix::initNetSystem();

  const double price_div = static_cast<double>(cfg.price_scale);
  const double size_div = static_cast<double>(cfg.size_scale);
  const std::string ws_url =
      !cfg.ws_url.empty()
          ? cfg.ws_url
          : "wss://stream.binance.com:9443/ws/" + lower(cfg.symbol) + "@depth@" +
                std::to_string(cfg.depth_ms) + "ms";

  SpscChannel<PipelineMsg, kRingCapacity> channel(cfg.drop ? Backpressure::Drop
                                                           : Backpressure::Block);
  std::atomic<std::uint64_t> ws_ops{0};

  // ----------------------- Thread A: WebSocket ingest -> decode -> push -----------------------
  ix::WebSocket ws;
  ws.setUrl(ws_url);
  ix::SocketTLSOptions tls;
  tls.caFile = "SYSTEM";
  ws.setTLSOptions(tls);
  ws.setOnMessageCallback([&](const ix::WebSocketMessagePtr& msg) {
    switch (msg->type) {
      case ix::WebSocketMessageType::Open:
        std::cout << "ws: connected " << ws_url << "\n";
        return;
      case ix::WebSocketMessageType::Error:
        std::cerr << "ws error: " << msg->errorInfo.reason << "\n";
        return;
      case ix::WebSocketMessageType::Message:
        break;
      default:
        return;
    }
    json j;
    try {
      j = json::parse(msg->str);
    } catch (...) {
      return;
    }
    if (j.value("e", std::string{}) != "depthUpdate") return;  // skip non-diff frames
    const std::uint64_t U = j.value("U", std::uint64_t{0});
    const std::uint64_t u = j.value("u", std::uint64_t{0});
    const std::uint64_t recv = now_ns();

    auto push_op = [&](Side side, const json& lvl) {
      if (lvl.size() < 2) return;
      const std::int64_t px = px_to_ticks(lvl[0].get<std::string>(), cfg.price_scale);
      if (px < 0) return;
      MarketEvent e{};
      e.type = EventType::L2SetLevel;
      e.side = side;
      e.price_ticks = px;
      e.size = static_cast<std::uint32_t>(scaled_qty(lvl[1].get<std::string>(), cfg.size_scale));
      e.order_ref = U;        // event update-id window: first id
      e.match_number = u;     // event update-id window: last id
      channel.send(PipelineMsg{e, recv, u});
      ws_ops.fetch_add(1, std::memory_order_relaxed);
    };

    bool any = false;
    if (auto it = j.find("b"); it != j.end())
      for (const auto& lvl : *it) { push_op(Side::Buy, lvl); any = true; }
    if (auto it = j.find("a"); it != j.end())
      for (const auto& lvl : *it) { push_op(Side::Sell, lvl); any = true; }
    if (!any) {
      // Empty event: still push a header so the consumer's contiguity check advances prev_u.
      MarketEvent e{};
      e.type = EventType::L2EventHeader;
      e.order_ref = U;
      e.match_number = u;
      channel.send(PipelineMsg{e, recv, u});
    }
  });
  ws.start();

  // ----------------------- Thread B: snapshot/resync -> set_level -> publish -----------------
  std::thread consumer([&] {
    // Let the WS connect and start buffering diff events before we snapshot, so the snapshot's
    // lastUpdateId is older than the queued stream (Binance's subscribe-then-snapshot order).
    std::this_thread::sleep_for(std::chrono::milliseconds(600));

    // Bootstrap: keep trying the REST snapshot until it succeeds (ring buffers the live stream).
    Snapshot s0;
    while (!g_stop.load(std::memory_order_acquire)) {
      std::cout << "fetching depth snapshot for " << cfg.symbol << " ...\n";
      s0 = fetch_snapshot(cfg);
      if (s0.ok && s0.best_bid >= 0 && s0.best_ask >= 0) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(800));
    }
    if (g_stop.load(std::memory_order_acquire)) return;

    const std::int64_t mid = (s0.best_bid + s0.best_ask) / 2;
    const std::int64_t half =
        static_cast<std::int64_t>(static_cast<double>(mid) * cfg.band_pct / 100.0);
    OrderBookConfig bc;
    bc.tick = 1;
    bc.min_price_ticks = std::max<std::int64_t>(1, mid - half);
    bc.max_price_ticks = mid + half;
    const std::uint64_t nlevels =
        static_cast<std::uint64_t>(bc.max_price_ticks - bc.min_price_ticks) + 1;
    if (nlevels > 60'000'000ull) {
      std::cerr << "book band would need " << nlevels
                << " levels; lower --band-pct or --price-scale\n";
      g_stop.store(true, std::memory_order_release);
      return;
    }

    OrderBook book(bc);
    LatencyHistogram hist;
    std::uint64_t L = s0.last_update_id;
    for (const auto& lv : s0.levels) book.set_level(lv.side, lv.price_ticks, lv.size);

    std::printf(
        "snapshot lastUpdateId %llu: preloaded %llu levels, band [%.2f, %.2f], top %.2f / %.2f\n",
        static_cast<unsigned long long>(L),
        static_cast<unsigned long long>(book.live_orders()),
        static_cast<double>(bc.min_price_ticks) / price_div,
        static_cast<double>(bc.max_price_ticks) / price_div,
        static_cast<double>(book.best_bid().price_ticks) / price_div,
        static_cast<double>(book.best_ask().price_ticks) / price_div);

    enum class Mode { Seeking, Streaming };
    Mode mode = Mode::Seeking;
    std::uint64_t prev_u = 0;
    bool have_event = false;
    std::uint64_t cur_U = 0;
    bool cur_apply = false;
    std::uint64_t gaps = 0, resyncs = 0;

    auto re_snapshot = [&] {
      ++resyncs;
      const Snapshot s2 = fetch_snapshot(cfg);
      if (!s2.ok) {
        std::cerr << "[recovery] snapshot failed; will retry on next event\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        have_event = false;
        return;
      }
      book.reset();
      for (const auto& lv : s2.levels) book.set_level(lv.side, lv.price_ticks, lv.size);
      L = s2.last_update_id;
      prev_u = 0;
      mode = Mode::Seeking;
      have_event = false;
      std::cerr << "[recovery] re-snapshot lastUpdateId " << L << "\n";
    };

    auto last_print = std::chrono::steady_clock::now();
    std::uint64_t last_ops = 0;
    PipelineMsg m;
    while (!g_stop.load(std::memory_order_acquire)) {
      bool drained = false;
      while (channel.receive(m)) {
        drained = true;
        const MarketEvent& e = m.event;
        const std::uint64_t U = e.order_ref;
        const std::uint64_t u = e.match_number;

        if (!have_event || U != cur_U) {  // new event boundary -> run the contiguity gate
          have_event = true;
          cur_U = U;
          cur_apply = false;
          if (mode == Mode::Seeking) {
            if (u < L + 1) {
              cur_apply = false;  // entirely older than the snapshot: drop
            } else if (U <= L + 1) {
              mode = Mode::Streaming;  // first event straddling L+1: aligned
              prev_u = u;
              cur_apply = true;
            } else {
              re_snapshot();  // snapshot older than the stream start
              continue;
            }
          } else {  // Streaming
            if (u <= prev_u) {
              cur_apply = false;  // stale / duplicate
            } else if (U == prev_u + 1) {
              prev_u = u;
              cur_apply = true;
            } else {
              ++gaps;  // U > prev_u+1: unrecoverable gap
              re_snapshot();
              continue;
            }
          }
        }

        if (cur_apply && e.type == EventType::L2SetLevel) {
          book.set_level(e.side, e.price_ticks, e.size);
          hist.record(now_ns() - m.recv_ns);
        }
      }

      const auto nowc = std::chrono::steady_clock::now();
      if (nowc - last_print >= std::chrono::seconds(1)) {
        const std::uint64_t ops = ws_ops.load(std::memory_order_relaxed);
        const double rate = static_cast<double>(ops - last_ops);
        last_ops = ops;
        last_print = nowc;
        const BookLevel b = book.best_bid();
        const BookLevel a = book.best_ask();
        const LatencySnapshot ls = hist.snapshot();
        const auto& bs = book.stats();
        std::printf(
            "%-8s bid %.2f x%.3f | ask %.2f x%.3f | spread %.2f | levels %llu | %.0f op/s | "
            "p50 %s p99 %s | drops %llu | gaps %llu resync %llu | oob %llu cross %llu | %s\n",
            cfg.symbol.c_str(),
            static_cast<double>(b.price_ticks) / price_div,
            static_cast<double>(b.aggregate_size) / size_div,
            static_cast<double>(a.price_ticks) / price_div,
            static_cast<double>(a.aggregate_size) / size_div,
            static_cast<double>(a.price_ticks - b.price_ticks) / price_div,
            static_cast<unsigned long long>(book.live_orders()), rate, ns_str(ls.p50_ns).c_str(),
            ns_str(ls.p99_ns).c_str(), static_cast<unsigned long long>(channel.dropped()),
            static_cast<unsigned long long>(gaps), static_cast<unsigned long long>(resyncs),
            static_cast<unsigned long long>(bs.rejected_out_of_band),
            static_cast<unsigned long long>(bs.crossed_observed),
            mode == Mode::Streaming ? "streaming" : "SEEKING");
      }
      if (!drained) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });

  // ----------------------- Main: run until duration / Ctrl-C -----------------------
  const auto start = std::chrono::steady_clock::now();
  while (!g_stop.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (cfg.duration_s > 0 &&
        std::chrono::steady_clock::now() - start >= std::chrono::seconds(cfg.duration_s))
      break;
  }
  g_stop.store(true, std::memory_order_release);
  ws.stop();
  consumer.join();
  ix::uninitNetSystem();

  const std::uint64_t sat = g_saturated.load(std::memory_order_relaxed);
  if (sat) std::cout << "note: " << sat << " level sizes saturated the uint32 transport field\n";
  std::cout << "stopped.\n";
  return 0;
}
