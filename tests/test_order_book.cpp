#include <gtest/gtest.h>

#include <array>

#include "feedhandler/market_event.hpp"
#include "feedhandler/order_book.hpp"

using namespace fh;

namespace {

OrderBookConfig small_cfg() {
  OrderBookConfig c;
  c.min_price_ticks = 1;
  c.max_price_ticks = 1000;  // tiny band keeps the test book at 1000 levels
  c.tick = 1;
  return c;
}

MarketEvent add(std::uint64_t ref, Side side, std::int64_t px, std::uint32_t sz) {
  MarketEvent e{};
  e.type = EventType::Add;
  e.order_ref = ref;
  e.side = side;
  e.price_ticks = px;
  e.size = sz;
  return e;
}
MarketEvent cancel(std::uint64_t ref, std::uint32_t sz) {
  MarketEvent e{};
  e.type = EventType::Cancel;
  e.order_ref = ref;
  e.size = sz;
  return e;
}
MarketEvent del(std::uint64_t ref) {
  MarketEvent e{};
  e.type = EventType::Delete;
  e.order_ref = ref;
  return e;
}
MarketEvent exec(std::uint64_t ref, std::uint32_t sz) {
  MarketEvent e{};
  e.type = EventType::Execute;
  e.order_ref = ref;
  e.size = sz;
  return e;
}
MarketEvent replace(std::uint64_t orig, std::uint64_t neu, std::int64_t px, std::uint32_t sz) {
  MarketEvent e{};
  e.type = EventType::Replace;
  e.order_ref = orig;
  e.new_order_ref = neu;
  e.price_ticks = px;
  e.size = sz;
  return e;
}

}  // namespace

TEST(OrderBook, EmptyBookHasNoTopOfBook) {
  OrderBook b(small_cfg());
  EXPECT_FALSE(b.best_bid().valid);
  EXPECT_FALSE(b.best_ask().valid);
  EXPECT_EQ(b.spread_ticks(), -1);
  EXPECT_EQ(b.live_orders(), 0u);
}

TEST(OrderBook, BuildBookTopOfBookAfterEachEvent) {
  OrderBook b(small_cfg());

  b.apply(add(1, Side::Buy, 100, 10));
  EXPECT_EQ(b.best_bid().price_ticks, 100);
  EXPECT_EQ(b.best_bid().aggregate_size, 10u);
  EXPECT_FALSE(b.best_ask().valid);

  b.apply(add(2, Side::Buy, 99, 5));
  EXPECT_EQ(b.best_bid().price_ticks, 100);  // 100 still best (bids descending)

  b.apply(add(3, Side::Sell, 102, 7));
  EXPECT_EQ(b.best_ask().price_ticks, 102);
  EXPECT_EQ(b.best_ask().aggregate_size, 7u);

  b.apply(add(4, Side::Sell, 103, 3));
  EXPECT_EQ(b.best_ask().price_ticks, 102);  // 102 still best (asks ascending)

  b.apply(add(5, Side::Buy, 100, 20));  // joins level 100
  EXPECT_EQ(b.best_bid().price_ticks, 100);
  EXPECT_EQ(b.best_bid().aggregate_size, 30u);  // 10 + 20
  EXPECT_EQ(b.best_bid().order_count, 2u);

  EXPECT_EQ(b.spread_ticks(), 2);
  EXPECT_FALSE(b.is_crossed());
  EXPECT_EQ(b.live_orders(), 5u);
}

TEST(OrderBook, DepthIsOrderedAndBounded) {
  OrderBook b(small_cfg());
  b.apply(add(1, Side::Buy, 100, 1));
  b.apply(add(2, Side::Buy, 98, 2));
  b.apply(add(3, Side::Buy, 99, 3));
  b.apply(add(4, Side::Sell, 101, 4));
  b.apply(add(5, Side::Sell, 103, 5));
  b.apply(add(6, Side::Sell, 102, 6));

  std::array<BookLevel, 8> buf{};
  const std::size_t nb = b.depth_bids(buf.data(), buf.size());
  ASSERT_EQ(nb, 3u);
  EXPECT_EQ(buf[0].price_ticks, 100);  // descending
  EXPECT_EQ(buf[1].price_ticks, 99);
  EXPECT_EQ(buf[2].price_ticks, 98);

  const std::size_t na = b.depth_asks(buf.data(), buf.size());
  ASSERT_EQ(na, 3u);
  EXPECT_EQ(buf[0].price_ticks, 101);  // ascending
  EXPECT_EQ(buf[1].price_ticks, 102);
  EXPECT_EQ(buf[2].price_ticks, 103);

  // Bounded request returns only the top-most levels.
  const std::size_t n2 = b.depth_bids(buf.data(), 2);
  ASSERT_EQ(n2, 2u);
  EXPECT_EQ(buf[0].price_ticks, 100);
  EXPECT_EQ(buf[1].price_ticks, 99);
}

TEST(OrderBook, CancelReducesAggregate) {
  OrderBook b(small_cfg());
  b.apply(add(1, Side::Buy, 100, 10));
  b.apply(add(2, Side::Buy, 100, 20));
  ASSERT_EQ(b.best_bid().aggregate_size, 30u);

  b.apply(cancel(2, 5));  // reduce order 2 from 20 -> 15
  EXPECT_EQ(b.best_bid().aggregate_size, 25u);
  EXPECT_EQ(b.best_bid().order_count, 2u);
  EXPECT_EQ(b.live_orders(), 2u);
}

TEST(OrderBook, ExecuteRemovesAndAdvancesBest) {
  OrderBook b(small_cfg());
  b.apply(add(1, Side::Buy, 100, 10));
  b.apply(add(2, Side::Buy, 99, 5));

  b.apply(exec(1, 10));  // fully fill order 1 -> level 100 empties
  EXPECT_EQ(b.best_bid().price_ticks, 99);
  EXPECT_EQ(b.best_bid().aggregate_size, 5u);
  EXPECT_EQ(b.live_orders(), 1u);

  b.apply(exec(2, 5));  // empties the book
  EXPECT_FALSE(b.best_bid().valid);
  EXPECT_EQ(b.live_orders(), 0u);
}

TEST(OrderBook, PartialExecuteKeepsOrder) {
  OrderBook b(small_cfg());
  b.apply(add(1, Side::Sell, 50, 100));
  b.apply(exec(1, 30));
  EXPECT_EQ(b.best_ask().price_ticks, 50);
  EXPECT_EQ(b.best_ask().aggregate_size, 70u);
  EXPECT_EQ(b.live_orders(), 1u);
}

TEST(OrderBook, DeleteAdvancesBestBidToNextLevel) {
  OrderBook b(small_cfg());
  b.apply(add(1, Side::Buy, 100, 10));
  b.apply(add(2, Side::Buy, 99, 5));
  b.apply(del(1));
  EXPECT_EQ(b.best_bid().price_ticks, 99);
  b.apply(del(2));
  EXPECT_FALSE(b.best_bid().valid);
}

TEST(OrderBook, FifoLevelSurvivesPartialDeletion) {
  OrderBook b(small_cfg());
  b.apply(add(20, Side::Buy, 100, 5));
  b.apply(add(21, Side::Buy, 100, 7));  // same level, behind order 20
  ASSERT_EQ(b.best_bid().aggregate_size, 12u);
  ASSERT_EQ(b.best_bid().order_count, 2u);

  b.apply(del(20));  // remove the front order
  EXPECT_EQ(b.best_bid().price_ticks, 100);
  EXPECT_EQ(b.best_bid().aggregate_size, 7u);
  EXPECT_EQ(b.best_bid().order_count, 1u);
}

TEST(OrderBook, ReplaceMovesPriceAndRef) {
  OrderBook b(small_cfg());
  b.apply(add(10, Side::Sell, 105, 4));
  ASSERT_EQ(b.best_ask().price_ticks, 105);

  b.apply(replace(10, 11, 104, 6));  // move to 104, size 6, new ref 11
  EXPECT_EQ(b.best_ask().price_ticks, 104);
  EXPECT_EQ(b.best_ask().aggregate_size, 6u);
  EXPECT_EQ(b.live_orders(), 1u);

  // The old ref is gone; a cancel on it is now an unknown ref.
  const auto before = b.stats().unknown_ref;
  b.apply(cancel(10, 1));
  EXPECT_EQ(b.stats().unknown_ref, before + 1);

  // The new ref is live and can be deleted.
  b.apply(del(11));
  EXPECT_FALSE(b.best_ask().valid);
}

TEST(OrderBook, UnknownRefIsCountedNotCrashing) {
  OrderBook b(small_cfg());
  b.apply(cancel(999, 1));
  b.apply(del(888));
  b.apply(exec(777, 1));
  EXPECT_EQ(b.stats().unknown_ref, 3u);
  EXPECT_EQ(b.live_orders(), 0u);
}

TEST(OrderBook, OutOfBandAddRejected) {
  OrderBook b(small_cfg());          // band is [1, 1000]
  b.apply(add(1, Side::Buy, 5000, 10));  // out of band
  EXPECT_EQ(b.stats().rejected_out_of_band, 1u);
  EXPECT_FALSE(b.best_bid().valid);
  EXPECT_EQ(b.live_orders(), 0u);
}

TEST(OrderBook, CrossedBookDetected) {
  OrderBook b(small_cfg());
  b.apply(add(1, Side::Sell, 100, 1));
  b.apply(add(2, Side::Buy, 101, 1));  // bid above ask => crossed
  EXPECT_TRUE(b.is_crossed());
  EXPECT_GT(b.stats().crossed_observed, 0u);
}

TEST(OrderBook, ResetClearsBookButKeepsStats) {
  OrderBook b(small_cfg());
  b.apply(add(1, Side::Buy, 100, 10));
  b.apply(add(2, Side::Sell, 102, 5));
  const auto applied_before = b.stats().applied;
  ASSERT_EQ(b.live_orders(), 2u);

  b.reset();
  EXPECT_EQ(b.live_orders(), 0u);
  EXPECT_FALSE(b.best_bid().valid);
  EXPECT_FALSE(b.best_ask().valid);
  EXPECT_EQ(b.stats().applied, applied_before);  // stats survive recovery

  // Book is usable again after reset (pool nodes recycled).
  b.apply(add(3, Side::Buy, 100, 1));
  EXPECT_EQ(b.best_bid().price_ticks, 100);
}

TEST(OrderBook, StatsCountByType) {
  OrderBook b(small_cfg());
  b.apply(add(1, Side::Buy, 100, 10));
  b.apply(add(2, Side::Sell, 102, 10));
  b.apply(cancel(1, 1));
  b.apply(exec(2, 1));
  b.apply(del(1));
  MarketEvent trade{};
  trade.type = EventType::Trade;
  b.apply(trade);

  const auto& s = b.stats();
  EXPECT_EQ(s.adds, 2u);
  EXPECT_EQ(s.cancels, 1u);
  EXPECT_EQ(s.executes, 1u);
  EXPECT_EQ(s.deletes, 1u);
  EXPECT_EQ(s.trades, 1u);
  EXPECT_EQ(s.applied, 6u);
}
