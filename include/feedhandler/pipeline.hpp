#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "feedhandler/feed_source.hpp"
#include "feedhandler/instrumentation.hpp"
#include "feedhandler/market_event.hpp"
#include "feedhandler/order_book.hpp"
#include "feedhandler/sequencer.hpp"
#include "feedhandler/spsc_ring_buffer.hpp"

namespace fh {

// Ring capacity (power of two) for the producer->consumer hand-off. ~64k slots.
inline constexpr std::size_t kRingCapacity = 1u << 16;

// What crosses the ring buffer from the ingest thread to the book thread: the decoded event
// plus the receive timestamp (for end-to-end latency) and the feed sequence number (so gap
// detection runs co-located with the book on the consumer thread).
struct PipelineMsg {
  MarketEvent   event;
  std::uint64_t recv_ns = 0;
  std::uint64_t sequence = 0;
};
static_assert(std::is_trivially_copyable_v<PipelineMsg>);

struct PipelineConfig {
  OrderBookConfig book{};
  Backpressure    backpressure = Backpressure::Block;

  // Replay speed: 0 => as fast as possible (max burst); >0 => pace to Nx the original ITCH
  // timestamp spacing.
  double speed_multiplier = 0.0;

  // Fixed offered load in messages/sec with uniform spacing (0 => ignore). Takes precedence
  // over speed_multiplier. This is what the harness sweeps to draw the latency-vs-load curve
  // and find the saturation point.
  double offered_rate = 0.0;

  // Gap injection (stress harness): hold back the frame whose sequence == inject_gap_at and
  // re-inject it `gap_reorder_distance` frames later, forcing the sequencer's reorder/replay
  // recovery path. 0 disables injection.
  std::uint64_t inject_gap_at = 0;
  std::uint64_t gap_reorder_distance = 0;

  // Simulated downstream cost per consumed message (busy iterations). Lets the harness make
  // the consumer a deterministic bottleneck to demonstrate saturation / backpressure. 0 = off.
  std::uint32_t consumer_spin = 0;
};

struct PipelineResult {
  OrderBookStats book_stats{};
  SequencerStats seq_stats{};
  LatencySnapshot latency{};

  std::uint64_t messages = 0;        // PipelineMsgs consumed
  std::uint64_t parse_errors = 0;    // frames that failed to decode
  std::uint64_t drops = 0;           // ring drops (Drop policy)
  std::uint64_t peak_queue = 0;      // max ring occupancy observed
  std::uint64_t l1_deltas = 0;       // top-of-book changes published

  double elapsed_s = 0.0;
  double throughput_msgs_per_s = 0.0;

  std::uint64_t recovery_max_ns = 0;  // worst gap-open -> resync duration
  std::uint64_t recovery_last_ns = 0;

  BookLevel final_bid{};
  BookLevel final_ask{};
  std::uint64_t final_live_orders = 0;

  static constexpr std::size_t kDepthSample = 5;
  BookLevel top_bids[kDepthSample]{};
  BookLevel top_asks[kDepthSample]{};
  std::size_t n_bids = 0;
  std::size_t n_asks = 0;
};

// Run the full two-thread pipeline over `src` to exhaustion and return measurements.
PipelineResult run_pipeline(FeedSource& src, const PipelineConfig& cfg);

}  // namespace fh
