#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>

#include "feedhandler/instrumentation.hpp"

using namespace fh;

TEST(LatencyHistogram, EmptyReportsZero) {
  LatencyHistogram h;
  EXPECT_EQ(h.count(), 0u);
  EXPECT_EQ(h.value_at_percentile(50.0), 0u);
  EXPECT_EQ(h.snapshot().count, 0u);
}

TEST(LatencyHistogram, UniformDistributionPercentiles) {
  LatencyHistogram h;
  for (std::uint64_t v = 1; v <= 1000; ++v) h.record(v);

  const auto s = h.snapshot();
  EXPECT_EQ(s.count, 1000u);
  EXPECT_EQ(s.min_ns, 1u);
  EXPECT_EQ(s.max_ns, 1000u);
  EXPECT_DOUBLE_EQ(s.mean_ns, 500.5);
  // Values < 2048 land in the linear bucket => exact percentiles here.
  EXPECT_EQ(s.p50_ns, 500u);
  EXPECT_EQ(s.p90_ns, 900u);
  EXPECT_EQ(s.p99_ns, 990u);
  // p99.9 of exactly 1000 samples sits on the 999/1000 rank boundary; floating-point ceil()
  // of 0.999*1000 may round to either rank (the reference HdrHistogram has the same effect).
  EXPECT_GE(s.p999_ns, 999u);
  EXPECT_LE(s.p999_ns, 1000u);
}

TEST(LatencyHistogram, SingleValueAllPercentilesEqual) {
  LatencyHistogram h;
  for (int i = 0; i < 1000; ++i) h.record(42);
  const auto s = h.snapshot();
  EXPECT_EQ(s.count, 1000u);
  EXPECT_EQ(s.min_ns, 42u);
  EXPECT_EQ(s.max_ns, 42u);
  EXPECT_EQ(s.p50_ns, 42u);
  EXPECT_EQ(s.p99_ns, 42u);
  EXPECT_EQ(s.p999_ns, 42u);
}

TEST(LatencyHistogram, LargeValuesWithinRelativeError) {
  // Across octaves HDR guarantees bounded *relative* error (~0.1% at 3 sig figs).
  LatencyHistogram h;
  const std::uint64_t v = 1'234'567;  // ~1.23 ms in ns
  for (int i = 0; i < 100; ++i) h.record(v);
  const std::uint64_t p50 = h.value_at_percentile(50.0);
  const double rel_err = std::abs(static_cast<double>(p50) - static_cast<double>(v)) /
                         static_cast<double>(v);
  EXPECT_LT(rel_err, 0.001);
}

TEST(LatencyHistogram, ClampsAboveHighest) {
  LatencyHistogram h(/*highest_ns=*/1000, /*sig_figs=*/3);
  h.record(5000);  // clamped to 1000
  const auto s = h.snapshot();
  EXPECT_EQ(s.count, 1u);
  EXPECT_EQ(s.max_ns, 1000u);
}

TEST(LatencyHistogram, ResetClears) {
  LatencyHistogram h;
  for (int i = 0; i < 10; ++i) h.record(100);
  ASSERT_EQ(h.count(), 10u);
  h.reset();
  EXPECT_EQ(h.count(), 0u);
  EXPECT_EQ(h.value_at_percentile(50.0), 0u);
}

TEST(Throughput, CountAndReset) {
  ThroughputCounter c;
  c.add();
  c.add(9);
  EXPECT_EQ(c.count(), 10u);
  c.reset();
  EXPECT_EQ(c.count(), 0u);
}

TEST(RateSampler, NonNegativeRate) {
  ThroughputCounter c;
  RateSampler r;
  r.sample(c.count());
  c.add(1000);
  const double rate = r.sample(c.count());
  EXPECT_GE(rate, 0.0);  // exact value is timing-dependent; just sanity-check sign
}
