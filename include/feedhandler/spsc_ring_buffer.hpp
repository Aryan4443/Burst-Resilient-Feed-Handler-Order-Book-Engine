#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "feedhandler/detail/cpu.hpp"

namespace fh {

// Lock-free, bounded, single-producer / single-consumer ring buffer.
//
// Exactly one thread may call push() and exactly one (other) thread may call pop(). That
// SPSC restriction is what lets the whole thing run with only acquire/release atomics and no
// CAS/locks.
//
// Design points (be ready to defend these):
//   * Capacity is a power of two => index -> slot is a single `& mask`, no modulo.
//   * head_/tail_ are *monotonic* 64-bit counters (never wrapped). size == tail - head, so
//     full vs empty is unambiguous (no "waste one slot" trick, no ABA on the counters).
//   * head_ and tail_ live on separate cache lines (alignas) => the producer writing tail_
//     never invalidates the consumer's head_ line (no false sharing).
//   * Each side keeps a *cached* copy of the other side's index and only reloads the real
//     atomic when its cache says the buffer looks full/empty. This removes a cross-core
//     atomic load from the common-case fast path (the Disruptor / folly ProducerConsumer
//     trick): far fewer cache-line bounces under load.
template <class T, std::size_t Capacity>
class SpscRingBuffer {
  static_assert(Capacity >= 2, "capacity must be >= 2");
  static_assert((Capacity & (Capacity - 1)) == 0, "capacity must be a power of two");
  static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");

 public:
  SpscRingBuffer() = default;
  SpscRingBuffer(const SpscRingBuffer&) = delete;
  SpscRingBuffer& operator=(const SpscRingBuffer&) = delete;

  static constexpr std::size_t capacity() noexcept { return Capacity; }

  // Producer side. Returns false if the buffer is full (caller decides backpressure policy).
  bool push(const T& item) noexcept {
    const std::size_t tail = tail_.load(std::memory_order_relaxed);  // we own tail_
    if (tail - cached_head_ >= Capacity) {
      // Cache says full; reload the real consumer position before giving up.
      cached_head_ = head_.load(std::memory_order_acquire);
      if (tail - cached_head_ >= Capacity) return false;  // genuinely full
    }
    buffer_[tail & kMask] = item;                       // publish payload ...
    tail_.store(tail + 1, std::memory_order_release);   // ... then the index (release)
    return true;
  }

  // Consumer side. Returns false if the buffer is empty.
  bool pop(T& out) noexcept {
    const std::size_t head = head_.load(std::memory_order_relaxed);  // we own head_
    if (head == cached_tail_) {
      // Cache says empty; reload the real producer position before giving up.
      cached_tail_ = tail_.load(std::memory_order_acquire);
      if (head == cached_tail_) return false;  // genuinely empty
    }
    out = buffer_[head & kMask];                        // read payload ...
    head_.store(head + 1, std::memory_order_release);   // ... then release the slot
    return true;
  }

  // Approximate occupancy for instrumentation. Safe to call from either thread; may be
  // momentarily stale (it is just two atomic loads, not a consistent snapshot).
  std::size_t size_approx() const noexcept {
    const std::size_t tail = tail_.load(std::memory_order_acquire);
    const std::size_t head = head_.load(std::memory_order_acquire);
    return tail - head;
  }
  bool empty_approx() const noexcept { return size_approx() == 0; }

 private:
  static constexpr std::size_t kMask = Capacity - 1;

  // Cross-thread indices, each isolated on its own cache line.
  alignas(detail::kCacheLine) std::atomic<std::size_t> head_{0};  // consumer writes
  alignas(detail::kCacheLine) std::atomic<std::size_t> tail_{0};  // producer writes
  // Thread-private caches of the opposite index (kept off the hot atomics' lines).
  alignas(detail::kCacheLine) std::size_t cached_tail_{0};  // consumer-private
  alignas(detail::kCacheLine) std::size_t cached_head_{0};  // producer-private
  alignas(detail::kCacheLine) std::array<T, Capacity> buffer_{};
};

// Backpressure policy applied by a producer when the ring is full.
enum class Backpressure {
  Block,  // spin-wait (with a relax hint) until space frees up: never lose data
  Drop,   // drop the item and increment an exact counter: bounded latency, counted loss
};

// Thin wrapper that binds a ring buffer to a backpressure policy + an exact drop counter.
// "Never silently lose data without counting it."
template <class T, std::size_t Capacity>
class SpscChannel {
 public:
  explicit SpscChannel(Backpressure policy) noexcept : policy_(policy) {}

  // Producer: deliver `item`. Returns true if it entered the ring, false if it was dropped
  // (only possible under Backpressure::Drop). Under Block this always returns true.
  bool send(const T& item) noexcept {
    if (ring_.push(item)) return true;
    if (policy_ == Backpressure::Block) {
      do {
        detail::cpu_relax();
      } while (!ring_.push(item));
      return true;
    }
    dropped_.fetch_add(1, std::memory_order_relaxed);  // Drop: count it, exactly once
    return false;
  }

  // Consumer.
  bool receive(T& out) noexcept { return ring_.pop(out); }

  std::uint64_t dropped() const noexcept { return dropped_.load(std::memory_order_relaxed); }
  std::size_t size_approx() const noexcept { return ring_.size_approx(); }
  static constexpr std::size_t capacity() noexcept { return Capacity; }
  Backpressure policy() const noexcept { return policy_; }

 private:
  Backpressure policy_;
  SpscRingBuffer<T, Capacity> ring_;
  std::atomic<std::uint64_t> dropped_{0};
};

}  // namespace fh
