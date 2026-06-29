// End-to-end runner: replay a length-framed ITCH 5.0 file through the two-thread pipeline and
// print final book state, throughput and latency percentiles.
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string>

#include "feedhandler/itch_file_source.hpp"
#include "feedhandler/market_event.hpp"
#include "feedhandler/pipeline.hpp"

namespace {

std::string ns_str(std::uint64_t ns) {
  char b[64];
  if (ns < 1'000) std::snprintf(b, sizeof(b), "%llu ns", static_cast<unsigned long long>(ns));
  else if (ns < 1'000'000) std::snprintf(b, sizeof(b), "%.2f us", static_cast<double>(ns) / 1e3);
  else std::snprintf(b, sizeof(b), "%.2f ms", static_cast<double>(ns) / 1e6);
  return b;
}

void print_level(const char* label, const fh::BookLevel& l) {
  if (!l.valid) { std::cout << "  " << label << ": (empty)\n"; return; }
  std::printf("  %s: %.4f  x %llu  (%u orders)\n", label,
              fh::price_ticks_to_double(l.price_ticks),
              static_cast<unsigned long long>(l.aggregate_size), l.order_count);
}

}  // namespace

int main(int argc, char** argv) {
  std::string file = "data/sample.itch";
  fh::Backpressure bp = fh::Backpressure::Block;
  double speed = 0.0;
  for (int i = 1; i < argc; ++i) {
    std::string k = argv[i];
    auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : std::string{}; };
    if (k == "--file") file = next();
    else if (k == "--speed") speed = std::stod(next());
    else if (k == "--drop") bp = fh::Backpressure::Drop;
    else if (k == "--help") {
      std::cout << "usage: feed_handler [--file FILE] [--speed Nx] [--drop]\n";
      return 0;
    }
  }

  fh::ItchFileSource src(file);
  if (!src.ok()) {
    std::cerr << "feed_handler: cannot open ITCH file '" << file
              << "' (generate one with: itch_gen --out " << file << ")\n";
    return 1;
  }

  fh::PipelineConfig cfg;
  cfg.backpressure = bp;
  cfg.speed_multiplier = speed;

  std::cout << "Replaying " << file << " through " << src.name() << " ...\n";
  const fh::PipelineResult r = fh::run_pipeline(src, cfg);

  const auto& bs = r.book_stats;
  std::cout << "\n=== Pipeline ===\n";
  std::printf("  messages processed : %llu\n", static_cast<unsigned long long>(r.messages));
  std::printf("  parse errors       : %llu\n", static_cast<unsigned long long>(r.parse_errors));
  std::printf("  elapsed            : %.3f s\n", r.elapsed_s);
  std::printf("  throughput         : %.2f M msg/s\n", r.throughput_msgs_per_s / 1e6);
  std::printf("  peak queue depth   : %llu / %llu\n",
              static_cast<unsigned long long>(r.peak_queue),
              static_cast<unsigned long long>(fh::kRingCapacity));
  std::printf("  ring drops         : %llu\n", static_cast<unsigned long long>(r.drops));
  std::printf("  L1 deltas published: %llu\n", static_cast<unsigned long long>(r.l1_deltas));

  std::cout << "\n=== Book events ===\n";
  std::printf("  add %llu  cancel %llu  delete %llu  execute %llu  replace %llu  trade %llu\n",
              static_cast<unsigned long long>(bs.adds), static_cast<unsigned long long>(bs.cancels),
              static_cast<unsigned long long>(bs.deletes), static_cast<unsigned long long>(bs.executes),
              static_cast<unsigned long long>(bs.replaces), static_cast<unsigned long long>(bs.trades));
  std::printf("  unknown-ref %llu  out-of-band %llu  crossed-observed %llu\n",
              static_cast<unsigned long long>(bs.unknown_ref),
              static_cast<unsigned long long>(bs.rejected_out_of_band),
              static_cast<unsigned long long>(bs.crossed_observed));

  std::cout << "\n=== Top of book (final) ===\n";
  print_level("ask", r.final_ask);
  print_level("bid", r.final_bid);
  std::printf("  live orders: %llu\n", static_cast<unsigned long long>(r.final_live_orders));

  const auto& L = r.latency;
  std::cout << "\n=== End-to-end latency (received -> book-updated) ===\n";
  if (L.count > 0) {
    std::printf("  count %llu  min %s  mean %s  max %s\n",
                static_cast<unsigned long long>(L.count), ns_str(L.min_ns).c_str(),
                ns_str(static_cast<std::uint64_t>(L.mean_ns)).c_str(), ns_str(L.max_ns).c_str());
    std::printf("  p50 %s   p99 %s   p99.9 %s\n", ns_str(L.p50_ns).c_str(),
                ns_str(L.p99_ns).c_str(), ns_str(L.p999_ns).c_str());
  }
  return 0;
}
