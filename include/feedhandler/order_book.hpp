#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "feedhandler/market_event.hpp"

namespace fh {

// A read-only view of one price level, returned by best_bid()/best_ask()/depth().
struct BookLevel {
  std::int64_t  price_ticks = 0;
  std::uint64_t aggregate_size = 0;
  std::uint32_t order_count = 0;
  bool          valid = false;  // false => no such level (empty side)
};

struct OrderBookConfig {
  // Inclusive tick band the array covers. Adds outside the band are rejected + counted.
  // Default: $0.0001 .. $200.0000 at 1/10000 ticks => 2,000,000 levels (~64 MB of levels).
  std::int64_t min_price_ticks = 1;
  std::int64_t max_price_ticks = 2'000'000;
  std::int64_t tick = 1;                 // ticks per array slot
  std::size_t  pool_chunk = 1u << 16;    // Order nodes allocated per pool growth
};

// Correctness / measurability counters.
struct OrderBookStats {
  std::uint64_t applied = 0;
  std::uint64_t adds = 0, cancels = 0, deletes = 0, executes = 0, replaces = 0, trades = 0;
  std::uint64_t rejected_out_of_band = 0;  // add/replace price outside the configured band
  std::uint64_t unknown_ref = 0;           // cancel/execute/delete/replace for unknown order
  std::uint64_t crossed_observed = 0;       // times best_bid >= best_ask after an apply
};

// Single-threaded limit order book.
//
// Key data structures (the centrepiece):
//   * `levels_` : tick-indexed array of PriceLevel. index = (price - min)/tick => O(1) access.
//   * each PriceLevel: an intrusive doubly-linked FIFO list of Orders (time priority) plus a
//     cached aggregate size and order count.
//   * `index_` : unordered_map<order_ref, Order*> => O(1) cancel / modify / delete.
//   * cached best_bid_idx_ / best_ask_idx_ => O(1) top-of-book.
//   * `pool_` : free-list object pool => no per-message heap allocation in steady state.
//
// Complexity: add O(1), cancel/execute O(1), delete O(1) amortised (best-pointer advance is
// amortised O(1); worst case scans empty levels toward the next populated price), replace O(1).
class OrderBook {
 public:
  explicit OrderBook(const OrderBookConfig& cfg = {});
  ~OrderBook();

  OrderBook(const OrderBook&) = delete;
  OrderBook& operator=(const OrderBook&) = delete;
  OrderBook(OrderBook&&) noexcept = default;
  OrderBook& operator=(OrderBook&&) noexcept = default;

  // Apply one decoded event. Dispatches on EventType; Trade/SystemEvent/Unknown are no-ops
  // on the book (Trade still counted). Never throws.
  void apply(const MarketEvent& ev) noexcept;

  // L2 / price-level apply path: set the ABSOLUTE resting size at a price level (total_size 0
  // removes the level). Used by aggregated depth feeds (e.g. Binance diff-depth) that publish
  // per-level totals instead of individual orders; complements the order-by-order apply()
  // above and shares the same tick-indexed array, best-of-book pointers, and crossed counter.
  // `live_orders()` then counts populated price levels. Never throws.
  void set_level(Side side, std::int64_t price_ticks, std::uint64_t total_size) noexcept;

  // Top of book.
  BookLevel best_bid() const noexcept;
  BookLevel best_ask() const noexcept;

  // Fill up to `n` levels from the top of each side into `out`. Returns levels written.
  // No allocation: caller owns `out`.
  std::size_t depth_bids(BookLevel* out, std::size_t n) const noexcept;
  std::size_t depth_asks(BookLevel* out, std::size_t n) const noexcept;

  std::uint64_t live_orders() const noexcept { return live_orders_; }
  bool is_crossed() const noexcept;
  std::int64_t spread_ticks() const noexcept;  // best_ask - best_bid, or -1 if not both sides

  const OrderBookStats& stats() const noexcept { return stats_; }

  // Clear the entire book (used by recovery / snapshot reload). Frees all orders.
  void reset() noexcept;

  // Exposed for tests / display.
  std::int64_t min_price_ticks() const noexcept { return cfg_.min_price_ticks; }
  std::int64_t max_price_ticks() const noexcept { return cfg_.max_price_ticks; }

 private:
  // Intrusive FIFO list node + map value. `next` doubles as the pool free-list link.
  struct Order {
    std::uint64_t order_ref;
    std::int64_t  price_ticks;
    std::uint32_t size;
    std::uint32_t level_idx;
    Side          side;
    Order*        prev;
    Order*        next;
  };

  struct PriceLevel {
    std::uint64_t aggregate_size = 0;
    std::uint32_t order_count = 0;
    Side          side = Side::None;  // owning side while non-empty (robust to crossed books)
    Order*        head = nullptr;     // FIFO front (oldest)
    Order*        tail = nullptr;     // FIFO back (newest)
  };

  // Free-list object pool with stable pointers (chunks never move once allocated).
  class OrderPool {
   public:
    explicit OrderPool(std::size_t chunk) : chunk_(chunk ? chunk : 1) {}
    Order* alloc();
    void   release(Order* o) noexcept;
    void   reset_all() noexcept;  // relink every node into the free list
   private:
    void grow();
    std::size_t chunk_;
    std::vector<std::unique_ptr<Order[]>> chunks_;
    Order* free_head_ = nullptr;
  };

  // --- helpers ---
  std::int64_t to_index(std::int64_t price_ticks) const noexcept;  // -1 if out of band
  std::int64_t to_price(std::int64_t index) const noexcept;
  void link_back(PriceLevel& lvl, Order* o) noexcept;
  void unlink(PriceLevel& lvl, Order* o) noexcept;
  void remove_order(Order* o) noexcept;        // unlink + free + erase from index + best fix
  void reduce_order(Order* o, std::uint32_t shares) noexcept;  // cancel/execute shared path
  void on_add(const MarketEvent& ev) noexcept;
  void rescan_best_bid_from(std::int64_t idx) noexcept;
  void rescan_best_ask_from(std::int64_t idx) noexcept;
  void note_crossed() noexcept;

  OrderBookConfig cfg_;
  std::vector<PriceLevel> levels_;
  std::unordered_map<std::uint64_t, Order*> index_;
  OrderPool pool_;
  std::int64_t best_bid_idx_ = -1;
  std::int64_t best_ask_idx_ = -1;
  std::uint64_t live_orders_ = 0;
  OrderBookStats stats_{};
};

}  // namespace fh
