// Single-thread microbenchmarks: isolate the parser and the parser+book hot paths (no
// threading / queueing) to report the per-stage ceiling in messages/sec.
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string>

#include "feedhandler/itch_file_source.hpp"
#include "feedhandler/itch_parser.hpp"
#include "feedhandler/order_book.hpp"

using namespace fh;

namespace {
double secs_since(std::chrono::steady_clock::time_point t0) {
  return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
}
}  // namespace

int main(int argc, char** argv) {
  std::string file = "data/sample.itch";
  for (int i = 1; i < argc; ++i) {
    std::string k = argv[i];
    if (k == "--file" && i + 1 < argc) file = argv[++i];
  }

  // Warm the OS page cache so the first timed section isn't charged for cold mmap faults.
  {
    ItchFileSource warm(file);
    if (!warm.ok()) { std::cerr << "microbench: cannot open " << file << "\n"; return 1; }
    RawFrame f;
    volatile std::uint64_t touch = 0;
    while (warm.next_frame(f)) touch += f.len;
    (void)touch;
  }

  // Parse-only.
  {
    ItchFileSource src(file);
    if (!src.ok()) { std::cerr << "microbench: cannot open " << file << "\n"; return 1; }
    std::uint64_t n = 0;
    std::uint64_t checksum = 0;  // consume a field so the parse can't be optimised away
    const auto t0 = std::chrono::steady_clock::now();
    RawFrame f;
    MarketEvent ev;
    while (src.next_frame(f)) {
      if (itch::parse(f.data, f.len, ev) == itch::ParseStatus::Ok) {
        checksum += static_cast<std::uint64_t>(ev.price_ticks) + ev.size +
                    static_cast<std::uint64_t>(ev.type);
        ++n;
      }
    }
    const double s = secs_since(t0);
    std::printf("parse-only      : %llu msgs in %.3f s = %.2f M msg/s  (chk %llu)\n",
                static_cast<unsigned long long>(n), s, static_cast<double>(n) / s / 1e6,
                static_cast<unsigned long long>(checksum));
  }

  // Parse + apply to the book.
  {
    ItchFileSource src(file);
    OrderBook book;
    std::uint64_t n = 0;
    const auto t0 = std::chrono::steady_clock::now();
    RawFrame f;
    MarketEvent ev;
    while (src.next_frame(f)) {
      if (itch::parse(f.data, f.len, ev) == itch::ParseStatus::Ok) {
        book.apply(ev);
        ++n;
      }
    }
    const double s = secs_since(t0);
    const auto bid = book.best_bid();
    std::printf("parse + book    : %llu msgs in %.3f s = %.2f M msg/s\n",
                static_cast<unsigned long long>(n), s, static_cast<double>(n) / s / 1e6);
    std::printf("final best bid  : %.4f x %llu  (live orders %llu)\n",
                price_ticks_to_double(bid.price_ticks),
                static_cast<unsigned long long>(bid.aggregate_size),
                static_cast<unsigned long long>(book.live_orders()));
  }
  return 0;
}
