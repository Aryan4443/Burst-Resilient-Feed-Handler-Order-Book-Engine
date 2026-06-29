#include "feedhandler/sequencer.hpp"

namespace fh {

Sequencer::Sequencer(const SequencerConfig& cfg) : cfg_(cfg), expected_(cfg.start_seq) {
  reorder_.reserve(cfg_.reorder_capacity * 2);
}

Disposition Sequencer::offer(std::uint64_t seq, const MarketEvent& ev) noexcept {
  if (seq < expected_) {
    ++stats_.duplicates;          // already processed this (or older)
    return Disposition::Drop;
  }
  if (seq == expected_) {
    ++expected_;
    ++stats_.accepted;
    return Disposition::Apply;     // caller applies ev, then drains pop_ready()
  }

  // seq > expected_  => one or more messages in [expected_, seq-1] are missing.
  if (!stale_) {
    ++stats_.gap_episodes;
    stats_.missing_total += (seq - expected_);
    stale_ = true;                 // mark the book stale until the hole is filled
  }

  // Buffer the future event so we can replay it once the gap fills (delta replay). If the
  // buffer is full the gap is too large to recover by replay => require a snapshot reload.
  if (reorder_.size() >= cfg_.reorder_capacity) {
    ++stats_.reorder_overflow;
    needs_snapshot_ = true;
    return Disposition::Overflow;
  }
  reorder_.emplace(seq, ev);       // duplicate future seq simply overwrites (idempotent)
  return Disposition::Buffered;
}

bool Sequencer::pop_ready(MarketEvent& out) noexcept {
  auto it = reorder_.find(expected_);
  if (it == reorder_.end()) {
    // No contiguous event. If nothing is buffered, every hole is filled => no longer stale.
    if (reorder_.empty()) stale_ = false;
    return false;
  }
  out = it->second;
  reorder_.erase(it);
  ++expected_;
  ++stats_.accepted;
  ++stats_.buffered_released;
  if (reorder_.empty()) stale_ = false;  // drained the last buffered event: gap filled
  return true;
}

void Sequencer::resync_to_snapshot(std::uint64_t snapshot_seq) noexcept {
  reorder_.clear();
  expected_ = snapshot_seq + 1;
  stale_ = false;
  needs_snapshot_ = false;
  ++stats_.snapshot_recoveries;
}

}  // namespace fh
