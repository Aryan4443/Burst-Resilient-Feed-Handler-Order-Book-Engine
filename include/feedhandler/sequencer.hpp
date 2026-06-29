#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_map>

#include "feedhandler/market_event.hpp"

namespace fh {

struct SequencerConfig {
  std::uint64_t start_seq = 1;          // first sequence number we expect
  std::size_t   reorder_capacity = 4096;  // max future msgs held open while a gap is unfilled
};

struct SequencerStats {
  std::uint64_t accepted = 0;            // events delivered in order (incl. drained buffered)
  std::uint64_t duplicates = 0;          // seq < expected (already processed)
  std::uint64_t buffered_released = 0;   // future events later drained in order (delta replay)
  std::uint64_t gap_episodes = 0;        // distinct times a gap opened
  std::uint64_t missing_total = 0;       // sum of hole sizes at gap-open time
  std::uint64_t reorder_overflow = 0;    // future buffer full => snapshot recovery required
  std::uint64_t snapshot_recoveries = 0; // times we resynced from a snapshot
};

// What the caller should do with the event it just offered.
enum class Disposition : std::uint8_t {
  Apply,     // it was the expected seq: apply it now, then drain pop_ready()
  Drop,      // duplicate / old: ignore
  Buffered,  // future seq stored to fill a gap later: nothing to apply yet
  Overflow,  // gap too large to buffer: caller must resync from a snapshot
};

// Per-stream sequence tracking + gap detection + recovery state machine.
//
// Two recovery modes, exactly as real feeds use:
//   * delta replay  -- a small out-of-order/retransmit gap is filled by buffering the future
//     messages and draining them once the missing seq arrives (UDP A/B arbitration / a
//     retransmit request on ITCH/MoldUDP).
//   * snapshot reload -- an unrecoverably large gap: discard state, reload a snapshot at some
//     sequence, then catch up on deltas after it (ITCH "Glimpse", CME MDP recovery feed).
//
// While any gap is open the stream is `stale()` -- the consumer should suppress publishing a
// possibly-wrong book until we resync and clear the flag.
class Sequencer {
 public:
  explicit Sequencer(const SequencerConfig& cfg = {});

  // Offer the next received (seq, event). See Disposition. O(1) average.
  Disposition offer(std::uint64_t seq, const MarketEvent& ev) noexcept;

  // After an Apply, drain any now-contiguous buffered events in order. Returns false when the
  // contiguous run ends. When the reorder buffer empties, the gap is filled and stale clears.
  bool pop_ready(MarketEvent& out) noexcept;

  // Snapshot recovery: discard pending state and resync so the next expected sequence is
  // snapshot_seq + 1. Clears stale. Caller is responsible for reloading the book snapshot.
  void resync_to_snapshot(std::uint64_t snapshot_seq) noexcept;

  bool stale() const noexcept { return stale_; }
  bool needs_snapshot() const noexcept { return needs_snapshot_; }
  std::uint64_t expected_seq() const noexcept { return expected_; }
  std::size_t buffered() const noexcept { return reorder_.size(); }
  const SequencerStats& stats() const noexcept { return stats_; }

 private:
  SequencerConfig cfg_;
  std::uint64_t expected_;
  bool stale_ = false;
  bool needs_snapshot_ = false;
  std::unordered_map<std::uint64_t, MarketEvent> reorder_;  // future seq -> event
  SequencerStats stats_{};
};

}  // namespace fh
