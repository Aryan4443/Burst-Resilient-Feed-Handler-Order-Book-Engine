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
#include <ixwebsocket/IXHttpServer.h>
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
#include <memory>
#include <mutex>
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

enum class MarketMode { Auto, Global, Us, Both };

struct FeedEndpoints {
  std::string id;          // "global" | "us"  (snapshot.json ?market=)
  std::string label;       // human label for the dashboard
  std::string rest_host;
  std::string ws_host;     // stream host without path
};

struct Config {
  std::string symbol = "BTCUSDT";        // Binance symbol (upper case)
  std::int64_t price_scale = 100;        // engine ticks per 1.0 quote unit (BTCUSDT step $0.01)
  std::int64_t size_scale = 1'000'000;   // engine size units per 1.0 base unit (micro-BTC)
  double band_pct = 10.0;                // book covers snapshot mid +/- this percent
  bool drop = false;                     // Drop vs Block backpressure
  int duration_s = 0;                    // 0 => run until Ctrl-C
  int depth_ms = 100;                    // diff stream cadence: 100 or 1000
  int serve_port = 0;                    // >0 => serve the web dashboard on this port
  MarketMode market = MarketMode::Auto;   // global | us | both (Auto => both when --serve)
  std::string ws_url;                    // overrides stream URL (single-feed mode only)
  std::string rest_host = "https://api.binance.com";
  bool rest_host_set = false;
  bool ws_url_set = false;
};

std::string lower(std::string s) {
  for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

std::string upper(std::string s) {
  for (char& c : s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  return s;
}

const std::vector<std::string> kSymbolCatalog = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
                                                 "XRPUSDT"};

struct AppState {
  std::mutex sym_mu;
  std::string symbol = "BTCUSDT";
};

bool catalog_has(const std::string& sym) {
  return std::find(kSymbolCatalog.begin(), kSymbolCatalog.end(), sym) != kSymbolCatalog.end();
}

FeedEndpoints endpoints_global() {
  return {"global", "Global (binance.com)", "https://data-api.binance.vision",
          "wss://data-stream.binance.vision"};
}

FeedEndpoints endpoints_us() {
  return {"us", "US (binance.us)", "https://api.binance.us", "wss://stream.binance.us:9443"};
}

std::string ws_url_for(const Config& cfg, const FeedEndpoints& ep) {
  if (cfg.ws_url_set && !cfg.ws_url.empty()) return cfg.ws_url;
  return ep.ws_host + "/ws/" + lower(cfg.symbol) + "@depth@" + std::to_string(cfg.depth_ms) + "ms";
}

Config cfg_for_market(const Config& base, const FeedEndpoints& ep) {
  Config c = base;
  if (!base.rest_host_set) c.rest_host = ep.rest_host;
  c.ws_url.clear();
  c.ws_url_set = false;
  return c;
}

MarketMode resolve_market_mode(const Config& cfg) {
  if (cfg.market != MarketMode::Auto) return cfg.market;
  return cfg.serve_port > 0 ? MarketMode::Both : MarketMode::Global;
}

std::vector<FeedEndpoints> feeds_for_mode(MarketMode mode) {
  if (mode == MarketMode::Us) return {endpoints_us()};
  if (mode == MarketMode::Both) return {endpoints_global(), endpoints_us()};
  return {endpoints_global()};
}

std::string query_param(const std::string& uri, const std::string& key) {
  const auto q = uri.find('?');
  if (q == std::string::npos) return {};
  const std::string qs = uri.substr(q + 1);
  const std::string needle = key + "=";
  const auto pos = qs.find(needle);
  if (pos == std::string::npos) return {};
  const auto start = pos + needle.size();
  const auto end = qs.find('&', start);
  return qs.substr(start, end == std::string::npos ? std::string::npos : end - start);
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
      std::cerr << " -- region-restricted? try --market us (or --market global for "
                   "data-api.binance.vision)";
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
    if (k == "--symbol") c.symbol = upper(next());
    else if (k == "--price-scale") c.price_scale = std::stoll(next());
    else if (k == "--size-scale") c.size_scale = std::stoll(next());
    else if (k == "--band-pct") c.band_pct = std::stod(next());
    else if (k == "--drop") c.drop = true;
    else if (k == "--duration") c.duration_s = std::stoi(next());
    else if (k == "--depth-ms") c.depth_ms = std::stoi(next());
    else if (k == "--serve") c.serve_port = std::stoi(next());
    else if (k == "--market") {
      const std::string m = lower(next());
      if (m == "global") c.market = MarketMode::Global;
      else if (m == "us") c.market = MarketMode::Us;
      else if (m == "both") c.market = MarketMode::Both;
      else {
        std::cerr << "unknown --market " << m << " (use global, us, or both)\n";
        std::exit(1);
      }
    }
    else if (k == "--ws-url") { c.ws_url = next(); c.ws_url_set = true; }
    else if (k == "--rest-host") { c.rest_host = next(); c.rest_host_set = true; }
    else if (k == "--help") {
      std::cout << "usage: live_feed [--symbol BTCUSDT] [--price-scale 100] "
                   "[--size-scale 1000000]\n"
                   "                 [--band-pct 10] [--depth-ms 100|1000] [--drop] "
                   "[--duration SECONDS]\n"
                   "                 [--serve PORT] [--market global|us|both]\n"
                   "                 [--rest-host URL] [--ws-url URL]\n"
                   "Streams Binance's public diff-depth (L2) feed through the engine. "
                   "No API key required.\n"
                   "  --serve PORT     web dashboard at http://localhost:PORT\n"
                   "  --market         global = binance.com data (data-api.binance.vision)\n"
                   "                   us = binance.us | both = run both (default with --serve)\n";
      std::exit(0);
    }
  }
  return c;
}

// Self-contained dashboard page (no external assets/CDNs). It polls /snapshot.json (same origin,
// so no CORS) ~4x/sec and renders top-of-book, a depth ladder, a mid-price line, and the engine's
// own telemetry (latency, gaps, re-syncs, drops, out-of-band, crossed) -- numbers only the C++
// engine produces.
const char* const kDashboardHtml = R"DASH(<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Order Book - live</title>
<style>
  :root{--bg:#0d1117;--panel:#161b22;--text:#c9d1d9;--muted:#8b949e;--border:#30363d;
        --green:#3fb950;--red:#f85149;--accent:#58a6ff;--warn:#d29922}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);
       font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
  header{display:flex;align-items:center;gap:14px;padding:12px 18px;border-bottom:1px solid var(--border)}
  header .sym{font-size:20px;font-weight:600;letter-spacing:.5px}
  header .mode{padding:2px 8px;border:1px solid var(--border);border-radius:4px;color:var(--muted)}
  header .mode.streaming{color:var(--green);border-color:var(--green)}
  header .marketWrap{display:flex;align-items:center;gap:6px;color:var(--muted);font-size:11px;
                    text-transform:uppercase;letter-spacing:.5px}
  header .pickSel{background:var(--panel);color:var(--accent);border:1px solid var(--accent);
                  border-radius:4px;padding:4px 8px;font:inherit;font-size:12px;
                  text-transform:none;letter-spacing:0;cursor:pointer}
  header .status{margin-left:auto;color:var(--muted)}
  .dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--red);
       margin-right:6px;vertical-align:middle}
  .dot.up{background:var(--green)}
  main{display:grid;grid-template-columns:1fr 1fr;gap:14px;padding:14px 18px;max-width:1080px}
  .panel{background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:14px}
  .panel h2{margin:0 0 10px;font-size:11px;text-transform:uppercase;letter-spacing:1px;
            color:var(--muted);font-weight:600}
  .hero{grid-column:1/-1;display:grid;grid-template-columns:1fr auto 1fr;align-items:center;gap:10px}
  .px{font-size:30px;font-weight:600}
  .bid .px{color:var(--green)} .ask{text-align:right} .ask .px{color:var(--red)}
  .sz{color:var(--muted);font-size:12px;margin-top:2px}
  .mid{text-align:center} .mid .v{font-size:18px} .mid .sp{color:var(--muted);font-size:12px;margin-top:2px}
  .ladder{display:grid;grid-template-columns:1fr 1fr;gap:0 18px}
  .col .row{position:relative;display:flex;justify-content:space-between;padding:2px 6px}
  .col .bar{position:absolute;top:0;bottom:0;z-index:0;opacity:.16;border-radius:3px}
  .col .p,.col .s{position:relative;z-index:1;font-variant-numeric:tabular-nums}
  .bids .bar{right:0;background:var(--green)} .bids .p{color:var(--green)}
  .asks .bar{left:0;background:var(--red)} .asks .p{color:var(--red)}
  .col .s{color:var(--muted)}
  .stats{grid-column:1/-1;display:grid;grid-template-columns:repeat(5,1fr);gap:10px}
  .stat{background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:10px 12px}
  .stat .k{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:1px}
  .stat .v{font-size:18px;margin-top:4px;font-variant-numeric:tabular-nums}
  .stat.bad .v{color:var(--warn)}
  canvas{width:100%;height:160px;display:block}
  .cap{color:var(--muted);font-size:11px;margin-top:8px}
</style></head>
<body>
<header>
  <span class="sym" id="sym">-</span>
  <label class="marketWrap" id="symbolWrap">Symbol
    <select class="pickSel" id="symbolSel"></select>
  </label>
  <label class="marketWrap" id="marketWrap" hidden>Market
    <select class="pickSel" id="marketSel"></select>
  </label>
  <span class="mode" id="mode">...</span>
  <span class="status"><span class="dot" id="dot"></span><span id="statusTxt">connecting...</span></span>
</header>
<main>
  <section class="panel hero">
    <div class="bid"><div class="px" id="bid">-</div><div class="sz" id="bidSz"></div></div>
    <div class="mid"><div class="v" id="mid">-</div><div class="sp" id="spread"></div></div>
    <div class="ask"><div class="px" id="ask">-</div><div class="sz" id="askSz"></div></div>
  </section>
  <section class="panel">
    <h2>Mid price (this session)</h2>
    <canvas id="chart"></canvas>
    <div class="cap">Source: live C++ engine - mid = (bid+ask)/2</div>
  </section>
  <section class="panel">
    <h2>Depth - top 15 levels per side</h2>
    <div class="ladder"><div class="col bids" id="bids"></div><div class="col asks" id="asks"></div></div>
  </section>
  <section class="stats" id="stats"></section>
</main>
<script>
const $=i=>document.getElementById(i);
const fmt=(n,d=2)=>(n==null||Number.isNaN(n))?'-':Number(n).toLocaleString(undefined,{minimumFractionDigits:d,maximumFractionDigits:d});
const base=s=>(s||'').replace(/USDT$|USDC$|USD$/,'');
const histByMarket={},MAXP=600;
let market=localStorage.getItem('fhMarket')||'global';
let symbol='';
function clearCharts(){Object.keys(histByMarket).forEach(k=>delete histByMarket[k]);drawChart();}
async function loadSymbols(){
  try{
    const r=await fetch('/symbols.json',{cache:'no-store'});const j=await r.json();
    const sel=$('symbolSel');sel.innerHTML='';
    (j.symbols||[]).forEach(s=>{
      const o=document.createElement('option');o.value=s;o.textContent=s;sel.appendChild(o);
    });
    symbol=j.active||(j.symbols||[])[0]||'BTCUSDT';
    sel.value=symbol;
    sel.onchange=async()=>{
      const s=sel.value;
      await fetch('/symbol?set='+encodeURIComponent(s),{method:'POST'});
      symbol=s;localStorage.setItem('fhSymbol',s);clearCharts();tick();
    };
  }catch(e){}
}
function drawChart(){
  const c=$('chart'),dpr=devicePixelRatio||1,w=c.clientWidth,h=c.clientHeight;
  c.width=w*dpr;c.height=h*dpr;const x=c.getContext('2d');x.scale(dpr,dpr);x.clearRect(0,0,w,h);
  const hist=histByMarket[market]||[];
  if(hist.length<2)return;
  let lo=Math.min(...hist),hi=Math.max(...hist);if(lo===hi){lo-=1;hi+=1;}
  const pad=6,px=i=>pad+i*(w-2*pad)/(hist.length-1),py=v=>pad+(hi-v)*(h-2*pad)/(hi-lo);
  x.strokeStyle='#58a6ff';x.lineWidth=1.5;x.beginPath();
  hist.forEach((v,i)=>i?x.lineTo(px(i),py(v)):x.moveTo(px(i),py(v)));x.stroke();
  x.fillStyle='#8b949e';x.font='10px monospace';x.fillText(fmt(hi),4,12);x.fillText(fmt(lo),4,h-4);
}
function ladder(el,arr){
  if(!arr||!arr.length){el.innerHTML='';return;}
  const max=Math.max(...arr.map(r=>r[1]),1e-9);
  el.innerHTML=arr.map(r=>`<div class="row"><span class="bar" style="width:${(r[1]/max*100).toFixed(1)}%"></span><span class="p">${fmt(r[0])}</span><span class="s">${fmt(r[1],3)}</span></div>`).join('');
}
const card=(k,v,bad)=>`<div class="stat ${bad?'bad':''}"><div class="k">${k}</div><div class="v">${v}</div></div>`;
function render(d){
  $('sym').textContent=d.symbol||'-';
  const m=$('mode');m.textContent=d.mode||'...';m.className='mode '+(d.mode==='streaming'?'streaming':'');
  const b=base(d.symbol);
  $('bid').textContent=fmt(d.bid);$('bidSz').textContent=d.bid!=null?fmt(d.bidSz,3)+' '+b:'';
  $('ask').textContent=fmt(d.ask);$('askSz').textContent=d.ask!=null?fmt(d.askSz,3)+' '+b:'';
  $('mid').textContent=fmt(d.mid);$('spread').textContent=d.spread!=null?'spread '+fmt(d.spread):'';
  ladder($('bids'),d.bids);ladder($('asks'),d.asks);
  $('stats').innerHTML=[
    card('updates/s',fmt(d.ops,0)),
    card('p50 latency',d.p50us!=null?fmt(d.p50us,0)+' us':'-'),
    card('p99 latency',d.p99us!=null?fmt(d.p99us,0)+' us':'-'),
    card('price levels',fmt(d.levels,0)),
    card('mode',d.mode||'-',d.mode!=='streaming'),
    card('gaps',fmt(d.gaps,0),d.gaps>0),
    card('re-syncs',fmt(d.resync,0),d.resync>0),
    card('ring drops',fmt(d.drops,0),d.drops>0),
    card('out-of-band',fmt(d.oob,0)),
    card('crossed',fmt(d.cross,0),d.cross>0),
  ].join('');
  if(d.mid!=null){
    if(!histByMarket[market])histByMarket[market]=[];
    const hist=histByMarket[market];
    hist.push(d.mid);if(hist.length>MAXP)hist.shift();drawChart();
  }
}
async function loadMarkets(){
  try{
    const r=await fetch('/markets.json',{cache:'no-store'});const j=await r.json();
    const sel=$('marketSel');const wrap=$('marketWrap');sel.innerHTML='';
    (j.markets||[]).forEach(m=>{
      const o=document.createElement('option');o.value=m.id;o.textContent=m.label;sel.appendChild(o);
    });
    if((j.markets||[]).length<=1){wrap.hidden=true;return;}
    wrap.hidden=false;
    if(!(j.markets||[]).some(m=>m.id===market))market=j.markets[0].id;
    sel.value=market;
    sel.onchange=()=>{market=sel.value;localStorage.setItem('fhMarket',market);clearCharts();tick();};
  }catch(e){}
}
async function tick(){
  try{const r=await fetch('/snapshot.json?market='+encodeURIComponent(market),{cache:'no-store'});
    const d=await r.json();
    $('dot').className='dot up';$('statusTxt').textContent='live';render(d);}
  catch(e){$('dot').className='dot';$('statusTxt').textContent='disconnected - is live_feed --serve running?';}
}
loadSymbols();loadMarkets();setInterval(tick,250);tick();addEventListener('resize',drawChart);
</script>
</body></html>)DASH";

struct FeedContext {
  FeedEndpoints ep;
  Config cfg;
  SpscChannel<PipelineMsg, kRingCapacity> channel;
  ix::WebSocket ws;
  std::atomic<std::uint64_t> ws_ops{0};
  std::atomic<bool> restart{false};
  std::string pending_symbol;
  std::mutex snap_mu;
  std::string snap_json = "{}";
  std::thread consumer;
  std::thread supervisor;
  bool publish_snapshots = false;

  FeedContext(FeedEndpoints ep_in, Config cfg_in, Backpressure bp)
      : ep(std::move(ep_in)), cfg(std::move(cfg_in)), channel(bp) {}
};

void start_websocket(FeedContext& f, const std::string& ws_url) {
  f.ws.setUrl(ws_url);
  ix::SocketTLSOptions tls;
  tls.caFile = "SYSTEM";
  f.ws.setTLSOptions(tls);
  f.ws.setOnMessageCallback([&f, ws_url](const ix::WebSocketMessagePtr& msg) {
    switch (msg->type) {
      case ix::WebSocketMessageType::Open:
        std::cout << "[" << f.ep.id << "] ws: connected " << ws_url << "\n";
        return;
      case ix::WebSocketMessageType::Error:
        std::cerr << "[" << f.ep.id << "] ws error: " << msg->errorInfo.reason << "\n";
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
    if (j.value("e", std::string{}) != "depthUpdate") return;
    const std::uint64_t U = j.value("U", std::uint64_t{0});
    const std::uint64_t u = j.value("u", std::uint64_t{0});
    const std::uint64_t recv = now_ns();

    auto push_op = [&](Side side, const json& lvl) {
      if (lvl.size() < 2) return;
      const std::int64_t px = px_to_ticks(lvl[0].get<std::string>(), f.cfg.price_scale);
      if (px < 0) return;
      MarketEvent e{};
      e.type = EventType::L2SetLevel;
      e.side = side;
      e.price_ticks = px;
      e.size = static_cast<std::uint32_t>(scaled_qty(lvl[1].get<std::string>(), f.cfg.size_scale));
      e.order_ref = U;
      e.match_number = u;
      f.channel.send(PipelineMsg{e, recv, u});
      f.ws_ops.fetch_add(1, std::memory_order_relaxed);
    };

    bool any = false;
    if (auto it = j.find("b"); it != j.end())
      for (const auto& lvl : *it) {
        push_op(Side::Buy, lvl);
        any = true;
      }
    if (auto it = j.find("a"); it != j.end())
      for (const auto& lvl : *it) {
        push_op(Side::Sell, lvl);
        any = true;
      }
    if (!any) {
      MarketEvent e{};
      e.type = EventType::L2EventHeader;
      e.order_ref = U;
      e.match_number = u;
      f.channel.send(PipelineMsg{e, recv, u});
    }
  });
  f.ws.start();
}

void start_consumer(FeedContext& f, double price_div, double size_div) {
  f.consumer = std::thread([&f, price_div, size_div] {
    std::this_thread::sleep_for(std::chrono::milliseconds(600));

    Snapshot s0;
    while (!g_stop.load(std::memory_order_acquire)) {
      std::cout << "[" << f.ep.id << "] fetching depth snapshot for " << f.cfg.symbol << " ...\n";
      s0 = fetch_snapshot(f.cfg);
      if (s0.ok && s0.best_bid >= 0 && s0.best_ask >= 0) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(800));
    }
    if (g_stop.load(std::memory_order_acquire)) return;

    const std::int64_t mid = (s0.best_bid + s0.best_ask) / 2;
    const std::int64_t half =
        static_cast<std::int64_t>(static_cast<double>(mid) * f.cfg.band_pct / 100.0);
    OrderBookConfig bc;
    bc.tick = 1;
    bc.min_price_ticks = std::max<std::int64_t>(1, mid - half);
    bc.max_price_ticks = mid + half;
    const std::uint64_t nlevels =
        static_cast<std::uint64_t>(bc.max_price_ticks - bc.min_price_ticks) + 1;
    if (nlevels > 60'000'000ull) {
      std::cerr << "[" << f.ep.id << "] book band would need " << nlevels
                << " levels; lower --band-pct or --price-scale\n";
      g_stop.store(true, std::memory_order_release);
      return;
    }

    OrderBook book(bc);
    LatencyHistogram hist;
    std::uint64_t L = s0.last_update_id;
    for (const auto& lv : s0.levels) book.set_level(lv.side, lv.price_ticks, lv.size);
    book.observe_event_boundary();

    std::printf(
        "[%s] snapshot lastUpdateId %llu: preloaded %llu levels, band [%.2f, %.2f], top %.2f / %.2f\n",
        f.ep.id.c_str(), static_cast<unsigned long long>(L),
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
      const Snapshot s2 = fetch_snapshot(f.cfg);
      if (!s2.ok) {
        std::cerr << "[" << f.ep.id << "] [recovery] snapshot failed; will retry on next event\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        have_event = false;
        return;
      }
      book.reset();
      for (const auto& lv : s2.levels) book.set_level(lv.side, lv.price_ticks, lv.size);
      book.observe_event_boundary();
      L = s2.last_update_id;
      prev_u = 0;
      mode = Mode::Seeking;
      have_event = false;
      std::cerr << "[" << f.ep.id << "] [recovery] re-snapshot lastUpdateId " << L << "\n";
    };

    auto last_print = std::chrono::steady_clock::now();
    auto last_snap = last_print;
    std::uint64_t last_ops = 0;
    double ops_rate = 0.0;

    auto build_snapshot = [&]() -> std::string {
      const BookLevel b = book.best_bid();
      const BookLevel a = book.best_ask();
      const LatencySnapshot ls = hist.snapshot();
      const auto& bs = book.stats();
      BookLevel buf[15];
      json s;
      s["market"] = f.ep.id;
      s["marketLabel"] = f.ep.label;
      s["symbol"] = f.cfg.symbol;
      s["mode"] = (mode == Mode::Streaming) ? "streaming" : "seeking";
      s["levels"] = book.live_orders();
      s["ops"] = ops_rate;
      if (b.valid) {
        s["bid"] = static_cast<double>(b.price_ticks) / price_div;
        s["bidSz"] = static_cast<double>(b.aggregate_size) / size_div;
      }
      if (a.valid) {
        s["ask"] = static_cast<double>(a.price_ticks) / price_div;
        s["askSz"] = static_cast<double>(a.aggregate_size) / size_div;
      }
      if (b.valid && a.valid) {
        s["spread"] = static_cast<double>(a.price_ticks - b.price_ticks) / price_div;
        s["mid"] = static_cast<double>(a.price_ticks + b.price_ticks) / (2.0 * price_div);
      }
      json jb = json::array();
      for (std::size_t i = 0, n = book.depth_bids(buf, 15); i < n; ++i)
        jb.push_back({static_cast<double>(buf[i].price_ticks) / price_div,
                      static_cast<double>(buf[i].aggregate_size) / size_div});
      json ja = json::array();
      for (std::size_t i = 0, n = book.depth_asks(buf, 15); i < n; ++i)
        ja.push_back({static_cast<double>(buf[i].price_ticks) / price_div,
                      static_cast<double>(buf[i].aggregate_size) / size_div});
      s["bids"] = jb;
      s["asks"] = ja;
      s["p50us"] = static_cast<double>(ls.p50_ns) / 1000.0;
      s["p99us"] = static_cast<double>(ls.p99_ns) / 1000.0;
      s["drops"] = f.channel.dropped();
      s["gaps"] = gaps;
      s["resync"] = resyncs;
      s["oob"] = bs.rejected_out_of_band;
      s["cross"] = bs.crossed_observed;
      return s.dump();
    };

    PipelineMsg m;
    while (!g_stop.load(std::memory_order_acquire) && !f.restart.load(std::memory_order_acquire)) {
      bool drained = false;
      while (f.channel.receive(m)) {
        drained = true;
        const MarketEvent& e = m.event;
        const std::uint64_t U = e.order_ref;
        const std::uint64_t u = e.match_number;

        if (!have_event || U != cur_U) {
          if (have_event && cur_apply) book.observe_event_boundary();
          have_event = true;
          cur_U = U;
          cur_apply = false;
          if (mode == Mode::Seeking) {
            if (u < L + 1) {
              cur_apply = false;
            } else if (U <= L + 1) {
              mode = Mode::Streaming;
              prev_u = u;
              cur_apply = true;
            } else {
              re_snapshot();
              continue;
            }
          } else {
            if (u <= prev_u) {
              cur_apply = false;
            } else if (U == prev_u + 1) {
              prev_u = u;
              cur_apply = true;
            } else {
              ++gaps;
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
      if (f.publish_snapshots && nowc - last_snap >= std::chrono::milliseconds(200)) {
        last_snap = nowc;
        std::string js = build_snapshot();
        std::lock_guard<std::mutex> lk(f.snap_mu);
        f.snap_json.swap(js);
      }
      if (nowc - last_print >= std::chrono::seconds(1)) {
        const std::uint64_t ops = f.ws_ops.load(std::memory_order_relaxed);
        const double rate = static_cast<double>(ops - last_ops);
        ops_rate = rate;
        last_ops = ops;
        last_print = nowc;
        const BookLevel b = book.best_bid();
        const BookLevel a = book.best_ask();
        const LatencySnapshot ls = hist.snapshot();
        const auto& bs = book.stats();
        std::printf(
            "[%-7s] %-8s bid %.2f x%.3f | ask %.2f x%.3f | spread %.2f | levels %llu | %.0f op/s | "
            "p50 %s p99 %s | drops %llu | gaps %llu resync %llu | oob %llu cross %llu | %s\n",
            f.ep.id.c_str(), f.cfg.symbol.c_str(),
            static_cast<double>(b.price_ticks) / price_div,
            static_cast<double>(b.aggregate_size) / size_div,
            static_cast<double>(a.price_ticks) / price_div,
            static_cast<double>(a.aggregate_size) / size_div,
            static_cast<double>(a.price_ticks - b.price_ticks) / price_div,
            static_cast<unsigned long long>(book.live_orders()), rate, ns_str(ls.p50_ns).c_str(),
            ns_str(ls.p99_ns).c_str(), static_cast<unsigned long long>(f.channel.dropped()),
            static_cast<unsigned long long>(gaps), static_cast<unsigned long long>(resyncs),
            static_cast<unsigned long long>(bs.rejected_out_of_band),
            static_cast<unsigned long long>(bs.crossed_observed),
            mode == Mode::Streaming ? "streaming" : "SEEKING");
      }
      if (!drained) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });
}

void supervise_feed(FeedContext& f, const Config& base_cfg, double price_div, double size_div) {
  while (!g_stop.load(std::memory_order_acquire)) {
    const std::string url = ws_url_for(base_cfg, f.ep);
    start_websocket(f, url);
    start_consumer(f, price_div, size_div);
    while (!g_stop.load(std::memory_order_acquire) &&
           !f.restart.load(std::memory_order_acquire))
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    f.ws.stop();
    if (f.consumer.joinable()) f.consumer.join();
    if (g_stop.load(std::memory_order_acquire)) break;
    if (!f.restart.load(std::memory_order_acquire)) break;
    f.cfg.symbol = f.pending_symbol;
    f.restart.store(false, std::memory_order_release);
    f.ws_ops.store(0, std::memory_order_relaxed);
    PipelineMsg junk;
    while (f.channel.receive(junk)) {}
    {
      std::lock_guard<std::mutex> lk(f.snap_mu);
      f.snap_json = "{}";
    }
    std::cout << "[" << f.ep.id << "] switched to " << f.cfg.symbol << "\n";
  }
}

}  // namespace

int main(int argc, char** argv) {
  const Config base_cfg = parse_args(argc, argv);
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);
  ix::initNetSystem();

  const MarketMode mode = resolve_market_mode(base_cfg);
  const auto endpoints = feeds_for_mode(mode);
  const double price_div = static_cast<double>(base_cfg.price_scale);
  const double size_div = static_cast<double>(base_cfg.size_scale);
  const bool publish_snapshots = base_cfg.serve_port > 0;

  auto app_state = std::make_shared<AppState>();
  app_state->symbol = upper(base_cfg.symbol);

  std::vector<std::unique_ptr<FeedContext>> feeds;
  feeds.reserve(endpoints.size());
  for (const auto& ep : endpoints) {
    auto f = std::make_unique<FeedContext>(
        ep, cfg_for_market(base_cfg, ep),
        base_cfg.drop ? Backpressure::Drop : Backpressure::Block);
    f->cfg.symbol = app_state->symbol;
    f->pending_symbol = app_state->symbol;
    f->publish_snapshots = publish_snapshots;
    f->supervisor =
        std::thread(supervise_feed, std::ref(*f), std::cref(base_cfg), price_div, size_div);
    feeds.push_back(std::move(f));
    if (endpoints.size() > 1)
      std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }

  if (mode == MarketMode::Both)
    std::cout << "market filter: global (binance.com) + us (binance.us) — use the dashboard "
                 "dropdown or --market global|us\n";
  else
    std::cout << "market: " << feeds.front()->ep.label << "\n";
  std::cout << "symbol: " << app_state->symbol
            << " (dashboard picker: BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT, XRPUSDT)\n";

  // ----------------------- Optional web dashboard (HTTP, same-origin JSON polling) ----------
  std::unique_ptr<ix::HttpServer> http_server;
  if (base_cfg.serve_port > 0) {
    http_server = std::make_unique<ix::HttpServer>(base_cfg.serve_port, "127.0.0.1");
    http_server->setOnConnectionCallback(
        [&feeds, app_state](ix::HttpRequestPtr req,
                            std::shared_ptr<ix::ConnectionState>) -> ix::HttpResponsePtr {
          const ix::WebSocketHttpHeaders json_h{{"Content-Type", "application/json"},
                                                {"Access-Control-Allow-Origin", "*"},
                                                {"Cache-Control", "no-store"}};
          if (req->uri.find("/symbols.json") != std::string::npos) {
            json j;
            j["symbols"] = kSymbolCatalog;
            {
              std::lock_guard<std::mutex> lk(app_state->sym_mu);
              j["active"] = app_state->symbol;
            }
            return std::make_shared<ix::HttpResponse>(200, "OK", ix::HttpErrorCode::Ok, json_h,
                                                      j.dump());
          }
          if (req->uri.find("/symbol") != std::string::npos &&
              req->uri.find("set=") != std::string::npos) {
            const std::string want = upper(query_param(req->uri, "set"));
            if (!catalog_has(want)) {
              return std::make_shared<ix::HttpResponse>(
                  400, "Bad Request", ix::HttpErrorCode::Invalid, json_h,
                  std::string(R"({"error":"unknown symbol"})"));
            }
            {
              std::lock_guard<std::mutex> lk(app_state->sym_mu);
              app_state->symbol = want;
            }
            for (const auto& f : feeds) {
              f->pending_symbol = want;
              f->restart.store(true, std::memory_order_release);
            }
            json ok;
            ok["ok"] = true;
            ok["symbol"] = want;
            return std::make_shared<ix::HttpResponse>(200, "OK", ix::HttpErrorCode::Ok, json_h,
                                                      ok.dump());
          }
          if (req->uri.find("/markets.json") != std::string::npos) {
            json j;
            json arr = json::array();
            for (const auto& f : feeds) {
              arr.push_back({{"id", f->ep.id}, {"label", f->ep.label}});
            }
            j["markets"] = arr;
            return std::make_shared<ix::HttpResponse>(200, "OK", ix::HttpErrorCode::Ok, json_h,
                                                      j.dump());
          }
          if (req->uri.find("/snapshot.json") != std::string::npos) {
            std::string want = query_param(req->uri, "market");
            if (want.empty() && !feeds.empty()) want = feeds.front()->ep.id;
            std::string body = "{}";
            for (const auto& f : feeds) {
              if (f->ep.id == want) {
                std::lock_guard<std::mutex> lk(f->snap_mu);
                body = f->snap_json;
                break;
              }
            }
            return std::make_shared<ix::HttpResponse>(200, "OK", ix::HttpErrorCode::Ok, json_h,
                                                      body);
          }
          const ix::WebSocketHttpHeaders html_h{{"Content-Type", "text/html; charset=utf-8"}};
          return std::make_shared<ix::HttpResponse>(200, "OK", ix::HttpErrorCode::Ok, html_h,
                                                    std::string(kDashboardHtml));
        });
    const auto res = http_server->listen();
    if (!res.first) {
      std::cerr << "dashboard: cannot listen on port " << base_cfg.serve_port << ": " << res.second
                << "\n";
      http_server.reset();
    } else {
      http_server->start();
      std::cout << "dashboard: open http://localhost:" << base_cfg.serve_port << " in your browser";
      if (mode == MarketMode::Both) std::cout << " (Market + Symbol dropdowns in header)";
      std::cout << "\n";
    }
  }

  // ----------------------- Main: run until duration / Ctrl-C -----------------------
  const auto start = std::chrono::steady_clock::now();
  while (!g_stop.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (base_cfg.duration_s > 0 &&
        std::chrono::steady_clock::now() - start >= std::chrono::seconds(base_cfg.duration_s))
      break;
  }
  g_stop.store(true, std::memory_order_release);
  for (auto& f : feeds) {
    f->restart.store(false, std::memory_order_release);
    f->ws.stop();
    if (f->supervisor.joinable()) f->supervisor.join();
  }
  if (http_server) http_server->stop();
  ix::uninitNetSystem();

  const std::uint64_t sat = g_saturated.load(std::memory_order_relaxed);
  if (sat) std::cout << "note: " << sat << " level sizes saturated the uint32 transport field\n";
  std::cout << "stopped.\n";
  return 0;
}
