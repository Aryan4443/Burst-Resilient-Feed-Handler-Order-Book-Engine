#pragma once

#include <cstddef>
#include <cstdint>

#include "feedhandler/order_book.hpp"

namespace fh {

// Emits top-of-book (L1) deltas downstream. In a production system on_update() would
// serialise and fan the delta out to consumers; here it detects an L1 change (best bid/ask
// price or aggregate size moved) and counts/retains it. It deliberately does NOT walk the
// full depth on every update (that would be wasteful on the hot path) — an L2 snapshot is
// available on demand via the book's depth() API for periodic/full publishes.
class Publisher {
 public:
  // Returns true if this update changed the top of book (an L1 delta worth emitting).
  bool on_update(const OrderBook& book) noexcept {
    ++updates_;
    const BookLevel bid = book.best_bid();
    const BookLevel ask = book.best_ask();
    const bool changed = !same(bid, last_bid_) || !same(ask, last_ask_);
    if (changed) {
      ++l1_deltas_;
      last_bid_ = bid;
      last_ask_ = ask;
    }
    return changed;
  }

  std::uint64_t updates() const noexcept { return updates_; }
  std::uint64_t l1_deltas() const noexcept { return l1_deltas_; }
  BookLevel last_bid() const noexcept { return last_bid_; }
  BookLevel last_ask() const noexcept { return last_ask_; }

 private:
  static bool same(const BookLevel& a, const BookLevel& b) noexcept {
    return a.valid == b.valid && a.price_ticks == b.price_ticks &&
           a.aggregate_size == b.aggregate_size;
  }

  std::uint64_t updates_ = 0;
  std::uint64_t l1_deltas_ = 0;
  BookLevel last_bid_{};
  BookLevel last_ask_{};
};

}  // namespace fh
