#include "feedhandler/pipeline.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>

#include "feedhandler/detail/cpu.hpp"
#include "feedhandler/itch_parser.hpp"
#include "feedhandler/publisher.hpp"

namespace fh {

PipelineResult run_pipeline(FeedSource& src, const PipelineConfig& cfg) {
  SpscChannel<PipelineMsg, kRingCapacity> channel(cfg.backpressure);
  std::atomic<bool> producer_done{false};
  std::atomic<std::uint64_t> parse_errors{0};
  std::atomic<std::uint64_t> peak_queue{0};

  // ---------------------- Thread A: ingest -> parse -> push ----------------------
  std::thread producer([&] {
    const bool rate_pace = cfg.offered_rate > 0.0;
    const bool ts_pace = !rate_pace && cfg.speed_multiplier > 0.0;
    const double rate_interval_ns = rate_pace ? (1e9 / cfg.offered_rate) : 0.0;
    // Release messages in bursts and sleep for the accumulated interval at each burst
    // boundary. Sleeping (vs busy-spinning between every message) yields the core to the
    // consumer, so a sub-saturation offered rate does not starve it. Burst sized so each
    // sleep is ~200us (efficient) and stays well under the ring capacity.
    const std::uint64_t rate_batch =
        rate_pace ? std::clamp<std::uint64_t>(
                        static_cast<std::uint64_t>(64'000.0 / rate_interval_ns), 1, 4096)
                  : 1;
    bool have_base = false;
    std::uint64_t base_event_ts = 0;
    std::uint64_t base_wall = 0;
    std::uint64_t produced = 0;
    auto wait_until = [&](std::uint64_t target) {
      for (;;) {
        const std::uint64_t n = now_ns();
        if (n >= target) break;
        const std::uint64_t rem = target - n;
        if (rem > 50'000) std::this_thread::sleep_for(std::chrono::nanoseconds(rem - 50'000));
        else detail::cpu_relax();
      }
    };
    auto maybe_pace = [&](std::uint64_t ev_ts) {
      if (rate_pace) {
        if (!have_base) { base_wall = now_ns(); have_base = true; }
        if (produced % rate_batch == 0) {
          wait_until(base_wall +
                     static_cast<std::uint64_t>(static_cast<double>(produced) * rate_interval_ns));
        }
        return;
      }
      if (!ts_pace) return;
      if (!have_base) { base_event_ts = ev_ts; base_wall = now_ns(); have_base = true; return; }
      if (ev_ts < base_event_ts) return;
      const double virt = static_cast<double>(ev_ts - base_event_ts) / cfg.speed_multiplier;
      wait_until(base_wall + static_cast<std::uint64_t>(virt));
    };

    bool holding = false;
    PipelineMsg held{};
    std::uint64_t release_at = 0;
    std::uint64_t local_peak = 0;

    RawFrame f;
    while (src.next_frame(f)) {
      MarketEvent ev;
      if (itch::parse(f.data, f.len, ev) != itch::ParseStatus::Ok) {
        parse_errors.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      maybe_pace(ev.timestamp_ns);
      ++produced;

      const PipelineMsg m{ev, now_ns(), f.sequence};

      // Gap injection: hold one frame back to open a sequence gap at the consumer.
      if (cfg.inject_gap_at != 0 && cfg.gap_reorder_distance != 0 && !holding &&
          f.sequence == cfg.inject_gap_at) {
        holding = true;
        held = m;
        release_at = f.sequence + cfg.gap_reorder_distance;
        continue;
      }

      channel.send(m);
      if (holding && f.sequence >= release_at) {
        channel.send(held);  // re-inject the delayed frame -> sequencer fills the gap
        holding = false;
      }

      const std::uint64_t occ = channel.size_approx();
      if (occ > local_peak) local_peak = occ;
    }
    if (holding) channel.send(held);

    peak_queue.store(local_peak, std::memory_order_relaxed);
    producer_done.store(true, std::memory_order_release);
  });

  // ---------------------- Thread B: pop -> sequence -> book -> publish ----------------------
  OrderBook book(cfg.book);
  Sequencer seq;
  Publisher pub;
  LatencyHistogram hist;

  std::uint64_t msgs = 0;
  std::uint64_t recovery_max = 0;
  std::uint64_t recovery_last = 0;
  std::uint64_t gap_open_ns = 0;
  bool was_stale = false;

  auto apply_ev = [&](const MarketEvent& e) {
    book.apply(e);
    pub.on_update(book);
  };

  auto handle = [&](const PipelineMsg& m) {
    ++msgs;
    const Disposition d = seq.offer(m.sequence, m.event);
    if (d == Disposition::Apply) {
      apply_ev(m.event);
      MarketEvent r;
      while (seq.pop_ready(r)) apply_ev(r);
      hist.record(now_ns() - m.recv_ns);  // end-to-end: received -> book updated
    } else if (d == Disposition::Overflow) {
      // Replay can't catch us up: snapshot reload. In a pure file replay we rebuild forward.
      book.reset();
      seq.resync_to_snapshot(m.sequence);
      apply_ev(m.event);
    }
    // Buffered / Drop: nothing to apply right now.

    const bool s = seq.stale();
    if (s && !was_stale) gap_open_ns = now_ns();
    if (!s && was_stale) {
      recovery_last = now_ns() - gap_open_ns;
      if (recovery_last > recovery_max) recovery_max = recovery_last;
    }
    was_stale = s;

    if (cfg.consumer_spin != 0) {  // simulate a slower downstream consumer
      volatile std::uint32_t sink = 0;
      for (std::uint32_t k = 0; k < cfg.consumer_spin; ++k) sink += k;
    }
  };

  const auto t0 = std::chrono::steady_clock::now();
  PipelineMsg m;
  while (!producer_done.load(std::memory_order_acquire)) {
    bool any = false;
    while (channel.receive(m)) { handle(m); any = true; }
    if (!any) detail::cpu_relax();
  }
  while (channel.receive(m)) handle(m);  // final drain
  const auto t1 = std::chrono::steady_clock::now();

  producer.join();

  // ---------------------- Assemble results ----------------------
  PipelineResult res;
  res.book_stats = book.stats();
  res.seq_stats = seq.stats();
  res.latency = hist.snapshot();
  res.messages = msgs;
  res.parse_errors = parse_errors.load(std::memory_order_relaxed);
  res.drops = channel.dropped();
  res.peak_queue = peak_queue.load(std::memory_order_relaxed);
  res.l1_deltas = pub.l1_deltas();
  res.elapsed_s = std::chrono::duration<double>(t1 - t0).count();
  res.throughput_msgs_per_s =
      res.elapsed_s > 0.0 ? static_cast<double>(msgs) / res.elapsed_s : 0.0;
  res.recovery_max_ns = recovery_max;
  res.recovery_last_ns = recovery_last;
  res.final_bid = book.best_bid();
  res.final_ask = book.best_ask();
  res.final_live_orders = book.live_orders();
  res.n_bids = book.depth_bids(res.top_bids, PipelineResult::kDepthSample);
  res.n_asks = book.depth_asks(res.top_asks, PipelineResult::kDepthSample);
  return res;
}

}  // namespace fh
