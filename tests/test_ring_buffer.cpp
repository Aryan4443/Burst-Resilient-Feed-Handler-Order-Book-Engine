#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

#include "feedhandler/spsc_ring_buffer.hpp"

using namespace fh;

TEST(SpscRingBuffer, SingleThreadFifoFullEmpty) {
  SpscRingBuffer<int, 4> q;
  EXPECT_EQ(q.capacity(), 4u);
  EXPECT_TRUE(q.empty_approx());

  // Fill to capacity.
  EXPECT_TRUE(q.push(10));
  EXPECT_TRUE(q.push(20));
  EXPECT_TRUE(q.push(30));
  EXPECT_TRUE(q.push(40));
  EXPECT_FALSE(q.push(50));  // full
  EXPECT_EQ(q.size_approx(), 4u);

  int v = 0;
  EXPECT_TRUE(q.pop(v));
  EXPECT_EQ(v, 10);          // FIFO order
  EXPECT_TRUE(q.push(50));   // room again
  EXPECT_TRUE(q.pop(v)); EXPECT_EQ(v, 20);
  EXPECT_TRUE(q.pop(v)); EXPECT_EQ(v, 30);
  EXPECT_TRUE(q.pop(v)); EXPECT_EQ(v, 40);
  EXPECT_TRUE(q.pop(v)); EXPECT_EQ(v, 50);
  EXPECT_FALSE(q.pop(v));     // empty
}

// Deterministic proof that the drop counter is exact: overflow a full channel by a known
// amount and check accepted/dropped precisely.
TEST(SpscChannel, DropCounterIsExactDeterministic) {
  SpscChannel<int, 4> ch(Backpressure::Drop);
  int accepted = 0;
  for (int i = 0; i < 10; ++i) accepted += ch.send(i) ? 1 : 0;

  EXPECT_EQ(accepted, 4);          // only capacity fit
  EXPECT_EQ(ch.dropped(), 6u);     // the other six were dropped, counted exactly

  int v = 0, seen = 0;
  while (ch.receive(v)) {
    EXPECT_EQ(v, seen);            // the four that fit are 0,1,2,3 in order
    ++seen;
  }
  EXPECT_EQ(seen, 4);
}

// Block policy must never lose data: every value arrives exactly once, in order.
TEST(SpscChannel, BlockingDeliversEverythingInOrder) {
  constexpr std::uint64_t N = 500'000;
  SpscChannel<std::uint64_t, 1024> ch(Backpressure::Block);
  std::atomic<bool> done{false};

  std::thread producer([&] {
    for (std::uint64_t i = 0; i < N; ++i) ch.send(i);
    done.store(true, std::memory_order_release);
  });

  std::uint64_t expected = 0;
  std::uint64_t v = 0;
  auto drain = [&] {
    while (ch.receive(v)) {
      ASSERT_EQ(v, expected);  // contiguous, in order, no loss / dup
      ++expected;
    }
  };
  while (!done.load(std::memory_order_acquire)) drain();
  drain();  // final drain after producer finished

  producer.join();
  EXPECT_EQ(expected, N);
  EXPECT_EQ(ch.dropped(), 0u);
}

// Producer faster than consumer + Drop policy: exact accounting (nothing lost uncounted,
// nothing duplicated), and drops actually occur.
TEST(SpscChannel, DropStressExactAccounting) {
  constexpr std::uint64_t N = 300'000;
  SpscChannel<std::uint64_t, 256> ch(Backpressure::Drop);  // small ring => pressure
  std::atomic<bool> done{false};
  std::atomic<std::uint64_t> accepted{0};

  std::thread producer([&] {
    std::uint64_t acc = 0;
    for (std::uint64_t i = 0; i < N; ++i) acc += ch.send(i) ? 1 : 0;  // i strictly increasing
    accepted.store(acc, std::memory_order_relaxed);
    done.store(true, std::memory_order_release);
  });

  std::uint64_t consumed = 0;
  std::uint64_t last = 0;
  bool have_last = false;
  std::uint64_t v = 0;
  std::uint64_t since_pause = 0;
  auto drain = [&] {
    while (ch.receive(v)) {
      if (have_last) ASSERT_GT(v, last);  // accepted subsequence is strictly increasing
      last = v;
      have_last = true;
      ++consumed;
      if (++since_pause == 4096) {  // deliberately lag the consumer to force drops
        since_pause = 0;
        std::this_thread::sleep_for(std::chrono::microseconds(50));
      }
    }
  };
  while (!done.load(std::memory_order_acquire)) drain();
  drain();

  producer.join();
  const std::uint64_t acc = accepted.load(std::memory_order_relaxed);
  EXPECT_EQ(consumed, acc);                 // everything accepted was delivered exactly once
  EXPECT_EQ(acc + ch.dropped(), N);         // exact: accepted + dropped == offered
  EXPECT_GT(ch.dropped(), 0u);              // the consumer really did fall behind
}
