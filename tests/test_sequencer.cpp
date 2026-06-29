#include <gtest/gtest.h>

#include <cstdint>
#include <utility>
#include <vector>

#include "feedhandler/market_event.hpp"
#include "feedhandler/order_book.hpp"
#include "feedhandler/sequencer.hpp"

using namespace fh;

namespace {

OrderBookConfig small_cfg() {
  OrderBookConfig c;
  c.min_price_ticks = 1;
  c.max_price_ticks = 1000;
  return c;
}

MarketEvent add(std::uint64_t ref, Side side, std::int64_t px, std::uint32_t sz) {
  MarketEvent e{}; e.type = EventType::Add; e.order_ref = ref; e.side = side;
  e.price_ticks = px; e.size = sz; return e;
}
MarketEvent cancel(std::uint64_t ref, std::uint32_t sz) {
  MarketEvent e{}; e.type = EventType::Cancel; e.order_ref = ref; e.size = sz; return e;
}
MarketEvent del(std::uint64_t ref) {
  MarketEvent e{}; e.type = EventType::Delete; e.order_ref = ref; return e;
}
MarketEvent exec(std::uint64_t ref, std::uint32_t sz) {
  MarketEvent e{}; e.type = EventType::Execute; e.order_ref = ref; e.size = sz; return e;
}

// Feed one (seq,event) through the sequencer into the book, draining any buffered run.
Disposition feed(OrderBook& bk, Sequencer& sq, std::uint64_t seq, const MarketEvent& ev) {
  const Disposition d = sq.offer(seq, ev);
  if (d == Disposition::Apply) {
    bk.apply(ev);
    MarketEvent r;
    while (sq.pop_ready(r)) bk.apply(r);
  }
  return d;
}

void expect_same_top_of_book(const OrderBook& a, const OrderBook& b) {
  EXPECT_EQ(a.best_bid().valid, b.best_bid().valid);
  EXPECT_EQ(a.best_bid().price_ticks, b.best_bid().price_ticks);
  EXPECT_EQ(a.best_bid().aggregate_size, b.best_bid().aggregate_size);
  EXPECT_EQ(a.best_ask().valid, b.best_ask().valid);
  EXPECT_EQ(a.best_ask().price_ticks, b.best_ask().price_ticks);
  EXPECT_EQ(a.best_ask().aggregate_size, b.best_ask().aggregate_size);
  EXPECT_EQ(a.live_orders(), b.live_orders());
}

// The canonical 10-event stream used by the recovery tests.
std::vector<MarketEvent> canonical_stream() {
  return {
      add(1, Side::Buy, 100, 10),   // seq 1
      add(2, Side::Sell, 102, 5),   // seq 2
      add(3, Side::Buy, 99, 7),     // seq 3
      add(4, Side::Sell, 103, 8),   // seq 4
      add(5, Side::Buy, 101, 3),    // seq 5  (best bid -> 101)
      cancel(5, 1),                 // seq 6  (order 5: 3 -> 2)
      add(7, Side::Buy, 100, 4),    // seq 7  (joins level 100)
      del(2),                       // seq 8  (best ask -> 103)
      exec(4, 8),                   // seq 9  (no asks left)
      add(10, Side::Sell, 105, 9),  // seq 10 (best ask -> 105)
  };
}

OrderBook reference_book() {
  OrderBook ref(small_cfg());
  const auto s = canonical_stream();
  for (const auto& e : s) ref.apply(e);
  return ref;
}

}  // namespace

TEST(Sequencer, InOrderAllApplied) {
  OrderBook bk(small_cfg());
  Sequencer sq;
  const auto s = canonical_stream();
  for (std::size_t i = 0; i < s.size(); ++i) {
    EXPECT_EQ(feed(bk, sq, i + 1, s[i]), Disposition::Apply);
  }
  EXPECT_FALSE(sq.stale());
  EXPECT_EQ(sq.stats().gap_episodes, 0u);
  OrderBook ref = reference_book();
  expect_same_top_of_book(bk, ref);
}

TEST(Sequencer, DuplicatesDropped) {
  OrderBook bk(small_cfg());
  Sequencer sq;
  EXPECT_EQ(feed(bk, sq, 1, add(1, Side::Buy, 100, 10)), Disposition::Apply);
  EXPECT_EQ(feed(bk, sq, 2, add(2, Side::Sell, 102, 5)), Disposition::Apply);
  EXPECT_EQ(feed(bk, sq, 2, add(2, Side::Sell, 102, 5)), Disposition::Drop);  // replayed
  EXPECT_EQ(feed(bk, sq, 1, add(1, Side::Buy, 100, 10)), Disposition::Drop);  // old
  EXPECT_EQ(sq.stats().duplicates, 2u);
  EXPECT_EQ(bk.live_orders(), 2u);
}

// Inject a gap that is later filled by the missing message (delta replay) and assert the
// book recovers to exactly the in-order state.
TEST(Sequencer, GapFilledByReplayRecoversBook) {
  OrderBook bk(small_cfg());
  Sequencer sq;
  const auto s = canonical_stream();

  // Deliver 1,2 then 4,5 (3 is delayed). 4 opens the gap; 4 and 5 are buffered.
  EXPECT_EQ(feed(bk, sq, 1, s[0]), Disposition::Apply);
  EXPECT_EQ(feed(bk, sq, 2, s[1]), Disposition::Apply);
  EXPECT_EQ(feed(bk, sq, 4, s[3]), Disposition::Buffered);
  EXPECT_TRUE(sq.stale());
  EXPECT_EQ(feed(bk, sq, 5, s[4]), Disposition::Buffered);
  EXPECT_EQ(sq.buffered(), 2u);

  // The missing seq 3 arrives: it is applied and then 4,5 drain in order.
  EXPECT_EQ(feed(bk, sq, 3, s[2]), Disposition::Apply);
  EXPECT_FALSE(sq.stale());        // gap filled
  EXPECT_EQ(sq.buffered(), 0u);
  EXPECT_EQ(sq.stats().gap_episodes, 1u);
  EXPECT_EQ(sq.stats().buffered_released, 2u);

  // Deliver the rest in order.
  for (std::size_t i = 5; i < s.size(); ++i) {
    EXPECT_EQ(feed(bk, sq, i + 1, s[i]), Disposition::Apply);
  }
  OrderBook ref = reference_book();
  expect_same_top_of_book(bk, ref);
}

// An unrecoverably large gap forces a snapshot reload; after catch-up the book is correct.
TEST(Sequencer, SnapshotReloadRecoversBook) {
  const auto s = canonical_stream();
  OrderBook bk(small_cfg());
  Sequencer sq;

  // Process seqs 1..3 normally.
  for (std::size_t i = 0; i < 3; ++i) ASSERT_EQ(feed(bk, sq, i + 1, s[i]), Disposition::Apply);

  // Snapshot reload at seq 5: the snapshot encodes the resting book through seq 5, so we
  // reset and replay 1..5 (this is what a real Glimpse/recovery snapshot would deliver).
  constexpr std::uint64_t kSnapshotSeq = 5;
  bk.reset();
  for (std::size_t i = 0; i < kSnapshotSeq; ++i) bk.apply(s[i]);
  sq.resync_to_snapshot(kSnapshotSeq);
  EXPECT_FALSE(sq.stale());
  EXPECT_EQ(sq.expected_seq(), kSnapshotSeq + 1);
  EXPECT_EQ(sq.stats().snapshot_recoveries, 1u);

  // Catch up on live deltas after the snapshot.
  for (std::size_t i = kSnapshotSeq; i < s.size(); ++i) {
    EXPECT_EQ(feed(bk, sq, i + 1, s[i]), Disposition::Apply);
  }
  OrderBook ref = reference_book();
  expect_same_top_of_book(bk, ref);
}

TEST(Sequencer, ReorderOverflowRequestsSnapshot) {
  SequencerConfig cfg;
  cfg.start_seq = 1;
  cfg.reorder_capacity = 4;
  Sequencer sq(cfg);

  MarketEvent e = add(99, Side::Buy, 100, 1);
  // expected = 1; offer future seqs 2,3,4,5 -> buffered (fills the 4-slot buffer).
  EXPECT_EQ(sq.offer(2, e), Disposition::Buffered);
  EXPECT_EQ(sq.offer(3, e), Disposition::Buffered);
  EXPECT_EQ(sq.offer(4, e), Disposition::Buffered);
  EXPECT_EQ(sq.offer(5, e), Disposition::Buffered);
  EXPECT_FALSE(sq.needs_snapshot());
  // Buffer full -> next future event overflows and demands a snapshot.
  EXPECT_EQ(sq.offer(6, e), Disposition::Overflow);
  EXPECT_TRUE(sq.needs_snapshot());
  EXPECT_TRUE(sq.stale());
  EXPECT_EQ(sq.stats().reorder_overflow, 1u);

  // A snapshot resync clears the alarm.
  sq.resync_to_snapshot(6);
  EXPECT_FALSE(sq.needs_snapshot());
  EXPECT_FALSE(sq.stale());
  EXPECT_EQ(sq.expected_seq(), 7u);
}
