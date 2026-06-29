#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>

namespace fh {

// Monotonic timestamp in nanoseconds. steady_clock is backed by a fast user-space counter
// (mach_absolute_time on Apple arm64, CLOCK_MONOTONIC/rdtsc-derived elsewhere): ~20 ns per
// read, no syscall. We read it exactly twice per message (at receive and after book-update),
// so timing adds a constant handful of ns and never touches the parse/apply compute.
inline std::uint64_t now_ns() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

struct LatencySnapshot {
  std::uint64_t count = 0;
  std::uint64_t min_ns = 0;
  std::uint64_t max_ns = 0;
  double        mean_ns = 0.0;
  std::uint64_t p50_ns = 0;
  std::uint64_t p90_ns = 0;
  std::uint64_t p99_ns = 0;
  std::uint64_t p999_ns = 0;
  std::uint64_t p9999_ns = 0;
};

// A High Dynamic Range histogram (HdrHistogram algorithm): values are bucketed by a power-of-
// two exponent with a fixed number of linear sub-buckets per octave, giving constant relative
// error (here ~0.1% with 3 significant figures) across a huge range with bounded memory.
//
// record() is allocation-free and O(1) (a count++ at an index derived from a `clz`). Counts
// are relaxed atomics so a reporter thread can snapshot at any time without locking and
// without perturbing the single writer (the book/consumer thread).
class LatencyHistogram {
 public:
  explicit LatencyHistogram(std::uint64_t highest_ns = 60'000'000'000ull, int sig_figs = 3) {
    init(highest_ns, sig_figs);
  }
  LatencyHistogram(const LatencyHistogram&) = delete;
  LatencyHistogram& operator=(const LatencyHistogram&) = delete;

  void record(std::uint64_t value_ns) noexcept {
    if (value_ns < 1) value_ns = 1;
    if (value_ns > highest_) value_ns = highest_;
    counts_[counts_index_for(value_ns)].fetch_add(1, std::memory_order_relaxed);
    total_count_.fetch_add(1, std::memory_order_relaxed);
    sum_.fetch_add(value_ns, std::memory_order_relaxed);
    if (value_ns < min_.load(std::memory_order_relaxed)) min_.store(value_ns, std::memory_order_relaxed);
    if (value_ns > max_.load(std::memory_order_relaxed)) max_.store(value_ns, std::memory_order_relaxed);
  }

  std::uint64_t count() const noexcept { return total_count_.load(std::memory_order_relaxed); }

  std::uint64_t value_at_percentile(double p) const noexcept {
    const std::uint64_t total = total_count_.load(std::memory_order_relaxed);
    if (total == 0) return 0;
    double target_d = std::ceil((p / 100.0) * static_cast<double>(total));
    std::uint64_t target = static_cast<std::uint64_t>(target_d);
    if (target < 1) target = 1;
    if (target > total) target = total;
    std::uint64_t running = 0;
    for (std::size_t i = 0; i < counts_len_; ++i) {
      running += counts_[i].load(std::memory_order_relaxed);
      if (running >= target) return highest_equivalent(value_from_index(i));
    }
    return max_.load(std::memory_order_relaxed);
  }

  LatencySnapshot snapshot() const noexcept {
    LatencySnapshot s;
    s.count = total_count_.load(std::memory_order_relaxed);
    if (s.count == 0) return s;
    s.min_ns = min_.load(std::memory_order_relaxed);
    s.max_ns = max_.load(std::memory_order_relaxed);
    s.mean_ns = static_cast<double>(sum_.load(std::memory_order_relaxed)) /
                static_cast<double>(s.count);
    s.p50_ns = value_at_percentile(50.0);
    s.p90_ns = value_at_percentile(90.0);
    s.p99_ns = value_at_percentile(99.0);
    s.p999_ns = value_at_percentile(99.9);
    s.p9999_ns = value_at_percentile(99.99);
    return s;
  }

  void reset() noexcept {
    for (std::size_t i = 0; i < counts_len_; ++i) counts_[i].store(0, std::memory_order_relaxed);
    total_count_.store(0, std::memory_order_relaxed);
    sum_.store(0, std::memory_order_relaxed);
    min_.store(std::numeric_limits<std::uint64_t>::max(), std::memory_order_relaxed);
    max_.store(0, std::memory_order_relaxed);
  }

 private:
  void init(std::uint64_t highest, int sig) {
    highest_ = highest < 2 ? 2 : highest;
    unit_magnitude_ = 0;  // lowest discernible value = 1
    std::uint64_t largest = 2;
    for (int i = 0; i < sig; ++i) largest *= 10;  // 2 * 10^sig
    const int sub_bucket_count_magnitude = ceil_log2(largest);
    sub_bucket_half_count_magnitude_ =
        sub_bucket_count_magnitude > 1 ? sub_bucket_count_magnitude - 1 : 0;
    sub_bucket_count_ = 1u << (sub_bucket_half_count_magnitude_ + 1);
    sub_bucket_half_count_ = sub_bucket_count_ >> 1;
    sub_bucket_mask_ = (static_cast<std::uint64_t>(sub_bucket_count_) - 1) << unit_magnitude_;
    leading_zero_count_base_ =
        64 - unit_magnitude_ - (sub_bucket_half_count_magnitude_ + 1);
    bucket_count_ = buckets_needed(highest_);
    counts_len_ = static_cast<std::size_t>(bucket_count_ + 1) * (sub_bucket_count_ / 2);
    counts_ = std::make_unique<std::atomic<std::uint64_t>[]>(counts_len_);  // value-init => 0
    min_.store(std::numeric_limits<std::uint64_t>::max(), std::memory_order_relaxed);
    max_.store(0, std::memory_order_relaxed);
  }

  static int ceil_log2(std::uint64_t v) noexcept {
    int m = 0;
    std::uint64_t x = 1;
    while (x < v) { x <<= 1; ++m; }
    return m;
  }
  int buckets_needed(std::uint64_t value) const noexcept {
    std::uint64_t smallest_untrackable =
        static_cast<std::uint64_t>(sub_bucket_count_) << unit_magnitude_;
    int needed = 1;
    while (smallest_untrackable <= value) {
      if (smallest_untrackable > (std::numeric_limits<std::uint64_t>::max() >> 1)) return needed + 1;
      smallest_untrackable <<= 1;
      ++needed;
    }
    return needed;
  }
  static int clz64(std::uint64_t v) noexcept { return __builtin_clzll(v); }

  int get_bucket_index(std::uint64_t v) const noexcept {
    return leading_zero_count_base_ - clz64(v | sub_bucket_mask_);
  }
  int get_sub_bucket_index(std::uint64_t v, int bucket_index) const noexcept {
    return static_cast<int>(v >> (bucket_index + unit_magnitude_));
  }
  std::size_t counts_index(int bucket_index, int sub_bucket_index) const noexcept {
    const int bucket_base = (bucket_index + 1) << sub_bucket_half_count_magnitude_;
    const int offset = sub_bucket_index - static_cast<int>(sub_bucket_half_count_);
    return static_cast<std::size_t>(bucket_base + offset);
  }
  std::size_t counts_index_for(std::uint64_t v) const noexcept {
    const int bi = get_bucket_index(v);
    return counts_index(bi, get_sub_bucket_index(v, bi));
  }
  std::uint64_t value_from_sub(int bucket_index, int sub_bucket_index) const noexcept {
    return static_cast<std::uint64_t>(sub_bucket_index) << (bucket_index + unit_magnitude_);
  }
  std::uint64_t value_from_index(std::size_t index) const noexcept {
    int bucket_index = static_cast<int>(index >> sub_bucket_half_count_magnitude_) - 1;
    int sub_bucket_index =
        static_cast<int>(index & (sub_bucket_half_count_ - 1)) + static_cast<int>(sub_bucket_half_count_);
    if (bucket_index < 0) {
      sub_bucket_index -= static_cast<int>(sub_bucket_half_count_);
      bucket_index = 0;
    }
    return value_from_sub(bucket_index, sub_bucket_index);
  }
  std::uint64_t size_of_equivalent_range(std::uint64_t v) const noexcept {
    return std::uint64_t{1} << (unit_magnitude_ + get_bucket_index(v));
  }
  std::uint64_t lowest_equivalent(std::uint64_t v) const noexcept {
    const int bi = get_bucket_index(v);
    return value_from_sub(bi, get_sub_bucket_index(v, bi));
  }
  std::uint64_t highest_equivalent(std::uint64_t v) const noexcept {
    return lowest_equivalent(v) + size_of_equivalent_range(v) - 1;
  }

  std::uint64_t highest_ = 0;
  int unit_magnitude_ = 0;
  int sub_bucket_half_count_magnitude_ = 0;
  int leading_zero_count_base_ = 0;
  std::uint32_t sub_bucket_count_ = 0;
  std::uint32_t sub_bucket_half_count_ = 0;
  std::uint64_t sub_bucket_mask_ = 0;
  int bucket_count_ = 0;
  std::size_t counts_len_ = 0;
  std::unique_ptr<std::atomic<std::uint64_t>[]> counts_;
  std::atomic<std::uint64_t> total_count_{0};
  std::atomic<std::uint64_t> sum_{0};
  std::atomic<std::uint64_t> min_{std::numeric_limits<std::uint64_t>::max()};
  std::atomic<std::uint64_t> max_{0};
};

// Monotone message counter (relaxed atomic; single writer, many readers).
class ThroughputCounter {
 public:
  void add(std::uint64_t n = 1) noexcept { c_.fetch_add(n, std::memory_order_relaxed); }
  std::uint64_t count() const noexcept { return c_.load(std::memory_order_relaxed); }
  void reset() noexcept { c_.store(0, std::memory_order_relaxed); }

 private:
  std::atomic<std::uint64_t> c_{0};
};

// Computes an instantaneous rate (messages/sec) between successive samples.
class RateSampler {
 public:
  double sample(std::uint64_t current_count) noexcept {
    const auto now = std::chrono::steady_clock::now();
    const double dt = std::chrono::duration<double>(now - last_).count();
    const double rate =
        dt > 0.0 ? static_cast<double>(current_count - last_count_) / dt : 0.0;
    last_ = now;
    last_count_ = current_count;
    return rate;
  }

 private:
  std::chrono::steady_clock::time_point last_ = std::chrono::steady_clock::now();
  std::uint64_t last_count_ = 0;
};

// A consistent point-in-time view of the whole pipeline, assembled off the hot path.
struct PipelineMetricsSnapshot {
  LatencySnapshot latency;
  std::uint64_t messages = 0;
  double        throughput_msgs_per_s = 0.0;
  std::uint64_t queue_occupancy = 0;
  std::uint64_t queue_capacity = 0;
  std::uint64_t drops = 0;
};

}  // namespace fh
