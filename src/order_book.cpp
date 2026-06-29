#include "feedhandler/order_book.hpp"

#include <algorithm>
#include <cstddef>

namespace fh {

// ---------------------------------------------------------------------------
// OrderPool: free-list object pool. Chunks are heap arrays that never move, so
// every Order* handed out (and stored in the index map / level lists) stays valid.
// ---------------------------------------------------------------------------
void OrderBook::OrderPool::grow() {
  auto block = std::make_unique<Order[]>(chunk_);
  Order* base = block.get();
  for (std::size_t i = 0; i < chunk_; ++i) {
    base[i].next = free_head_;
    free_head_ = &base[i];
  }
  chunks_.push_back(std::move(block));
}

OrderBook::Order* OrderBook::OrderPool::alloc() {
  if (free_head_ == nullptr) grow();
  Order* o = free_head_;
  free_head_ = o->next;
  return o;
}

void OrderBook::OrderPool::release(Order* o) noexcept {
  o->next = free_head_;
  free_head_ = o;
}

void OrderBook::OrderPool::reset_all() noexcept {
  free_head_ = nullptr;
  for (auto& block : chunks_) {
    Order* base = block.get();
    for (std::size_t i = 0; i < chunk_; ++i) {
      base[i].next = free_head_;
      free_head_ = &base[i];
    }
  }
}

// ---------------------------------------------------------------------------
// OrderBook
// ---------------------------------------------------------------------------
OrderBook::OrderBook(const OrderBookConfig& cfg) : cfg_(cfg), pool_(cfg.pool_chunk) {
  if (cfg_.tick < 1) cfg_.tick = 1;
  if (cfg_.max_price_ticks < cfg_.min_price_ticks) {
    cfg_.max_price_ticks = cfg_.min_price_ticks;
  }
  const std::int64_t span = cfg_.max_price_ticks - cfg_.min_price_ticks;
  const std::size_t num_levels = static_cast<std::size_t>(span / cfg_.tick) + 1;
  levels_.assign(num_levels, PriceLevel{});
  index_.reserve(1u << 16);
}

OrderBook::~OrderBook() = default;

std::int64_t OrderBook::to_index(std::int64_t price_ticks) const noexcept {
  if (price_ticks < cfg_.min_price_ticks || price_ticks > cfg_.max_price_ticks) return -1;
  return (price_ticks - cfg_.min_price_ticks) / cfg_.tick;
}

std::int64_t OrderBook::to_price(std::int64_t index) const noexcept {
  return cfg_.min_price_ticks + index * cfg_.tick;
}

void OrderBook::link_back(PriceLevel& lvl, Order* o) noexcept {
  o->prev = lvl.tail;
  o->next = nullptr;
  if (lvl.tail) {
    lvl.tail->next = o;
  } else {
    lvl.head = o;
  }
  lvl.tail = o;
}

void OrderBook::unlink(PriceLevel& lvl, Order* o) noexcept {
  if (o->prev) {
    o->prev->next = o->next;
  } else {
    lvl.head = o->next;
  }
  if (o->next) {
    o->next->prev = o->prev;
  } else {
    lvl.tail = o->prev;
  }
}

void OrderBook::rescan_best_bid_from(std::int64_t idx) noexcept {
  for (std::int64_t i = idx; i >= 0; --i) {
    if (levels_[static_cast<std::size_t>(i)].order_count > 0 &&
        levels_[static_cast<std::size_t>(i)].side == Side::Buy) {
      best_bid_idx_ = i;
      return;
    }
  }
  best_bid_idx_ = -1;
}

void OrderBook::rescan_best_ask_from(std::int64_t idx) noexcept {
  const std::int64_t n = static_cast<std::int64_t>(levels_.size());
  for (std::int64_t i = idx; i < n; ++i) {
    if (levels_[static_cast<std::size_t>(i)].order_count > 0 &&
        levels_[static_cast<std::size_t>(i)].side == Side::Sell) {
      best_ask_idx_ = i;
      return;
    }
  }
  best_ask_idx_ = -1;
}

void OrderBook::on_add(const MarketEvent& ev) noexcept {
  const std::int64_t idx = to_index(ev.price_ticks);
  if (idx < 0) {
    ++stats_.rejected_out_of_band;
    return;
  }
  // Duplicate live order_ref would be a feed error; ignore the second add rather than orphan
  // the first node. (ITCH guarantees unique live refs.)
  if (index_.find(ev.order_ref) != index_.end()) return;

  Order* o = pool_.alloc();
  o->order_ref = ev.order_ref;
  o->price_ticks = ev.price_ticks;
  o->size = ev.size;
  o->level_idx = static_cast<std::uint32_t>(idx);
  o->side = ev.side;
  o->prev = o->next = nullptr;

  PriceLevel& lvl = levels_[static_cast<std::size_t>(idx)];
  if (lvl.order_count == 0) lvl.side = ev.side;  // claim side on empty -> non-empty
  link_back(lvl, o);
  lvl.aggregate_size += ev.size;
  ++lvl.order_count;

  index_.emplace(ev.order_ref, o);
  ++live_orders_;

  if (ev.side == Side::Buy) {
    if (best_bid_idx_ < 0 || idx > best_bid_idx_) best_bid_idx_ = idx;
  } else if (ev.side == Side::Sell) {
    if (best_ask_idx_ < 0 || idx < best_ask_idx_) best_ask_idx_ = idx;
  }
}

void OrderBook::remove_order(Order* o) noexcept {
  const std::int64_t idx = static_cast<std::int64_t>(o->level_idx);
  PriceLevel& lvl = levels_[static_cast<std::size_t>(idx)];
  const Side s = o->side;

  unlink(lvl, o);
  lvl.aggregate_size -= o->size;
  --lvl.order_count;

  index_.erase(o->order_ref);
  pool_.release(o);
  --live_orders_;

  if (lvl.order_count == 0) {
    lvl.side = Side::None;
    lvl.aggregate_size = 0;  // defensive: should already be 0
    if (s == Side::Buy && idx == best_bid_idx_) {
      rescan_best_bid_from(idx - 1);
    } else if (s == Side::Sell && idx == best_ask_idx_) {
      rescan_best_ask_from(idx + 1);
    }
  }
}

void OrderBook::reduce_order(Order* o, std::uint32_t shares) noexcept {
  const std::uint32_t dec = std::min(shares, o->size);
  levels_[o->level_idx].aggregate_size -= dec;
  o->size -= dec;
  if (o->size == 0) remove_order(o);
}

void OrderBook::note_crossed() noexcept {
  if (is_crossed()) ++stats_.crossed_observed;
}

void OrderBook::observe_event_boundary() noexcept {
  const bool crossed = is_crossed();
  if (crossed && !crossed_at_last_boundary_) ++stats_.crossed_observed;
  crossed_at_last_boundary_ = crossed;
}

void OrderBook::apply(const MarketEvent& ev) noexcept {
  switch (ev.type) {
    case EventType::Add:
      ++stats_.adds;
      on_add(ev);
      break;

    case EventType::Cancel: {
      ++stats_.cancels;
      auto it = index_.find(ev.order_ref);
      if (it == index_.end()) { ++stats_.unknown_ref; break; }
      reduce_order(it->second, ev.size);
      break;
    }

    case EventType::Execute:
    case EventType::ExecuteWithPrice: {
      ++stats_.executes;
      auto it = index_.find(ev.order_ref);
      if (it == index_.end()) { ++stats_.unknown_ref; break; }
      reduce_order(it->second, ev.size);
      break;
    }

    case EventType::Delete: {
      ++stats_.deletes;
      auto it = index_.find(ev.order_ref);
      if (it == index_.end()) { ++stats_.unknown_ref; break; }
      remove_order(it->second);
      break;
    }

    case EventType::Replace: {
      ++stats_.replaces;
      auto it = index_.find(ev.order_ref);
      if (it == index_.end()) { ++stats_.unknown_ref; break; }
      // Cancel original then add new at (possibly) new price/size -> loses time priority.
      const Side s = it->second->side;
      remove_order(it->second);
      MarketEvent add = ev;
      add.type = EventType::Add;
      add.order_ref = ev.new_order_ref;
      add.side = s;
      on_add(add);
      break;
    }

    case EventType::Trade:
      ++stats_.trades;  // tape only; no book mutation
      break;

    case EventType::SystemEvent:
    case EventType::Unknown:
    default:
      break;
  }

  ++stats_.applied;
  note_crossed();
}

void OrderBook::set_level(Side side, std::int64_t price_ticks, std::uint64_t total_size) noexcept {
  const std::int64_t idx = to_index(price_ticks);
  if (idx < 0) {
    ++stats_.rejected_out_of_band;
    ++stats_.applied;
    return;
  }
  PriceLevel& lvl = levels_[static_cast<std::size_t>(idx)];

  if (total_size == 0) {
    if (lvl.order_count > 0) {  // remove a populated level
      const Side old = lvl.side;
      lvl.aggregate_size = 0;
      lvl.order_count = 0;
      lvl.side = Side::None;
      --live_orders_;
      if (old == Side::Buy && idx == best_bid_idx_) {
        rescan_best_bid_from(idx - 1);
      } else if (old == Side::Sell && idx == best_ask_idx_) {
        rescan_best_ask_from(idx + 1);
      }
    }
    ++stats_.deletes;
  } else {
    if (lvl.order_count == 0) {  // first size at an empty level
      lvl.side = side;
      lvl.order_count = 1;
      lvl.aggregate_size = total_size;
      ++live_orders_;
    } else if (lvl.side == side) {  // resize an existing level (the common case)
      lvl.aggregate_size = total_size;
    } else {  // side flip on a populated level (inconsistent feed): treat as a fresh level
      const Side old = lvl.side;
      lvl.side = side;
      lvl.aggregate_size = total_size;  // order_count stays 1
      if (old == Side::Buy && idx == best_bid_idx_) {
        rescan_best_bid_from(idx - 1);
      } else if (old == Side::Sell && idx == best_ask_idx_) {
        rescan_best_ask_from(idx + 1);
      }
    }
    if (side == Side::Buy) {
      if (best_bid_idx_ < 0 || idx > best_bid_idx_) best_bid_idx_ = idx;
    } else if (side == Side::Sell) {
      if (best_ask_idx_ < 0 || idx < best_ask_idx_) best_ask_idx_ = idx;
    }
    ++stats_.adds;
  }
  ++stats_.applied;
}

BookLevel OrderBook::best_bid() const noexcept {
  if (best_bid_idx_ < 0) return {};
  const PriceLevel& lvl = levels_[static_cast<std::size_t>(best_bid_idx_)];
  return {to_price(best_bid_idx_), lvl.aggregate_size, lvl.order_count, true};
}

BookLevel OrderBook::best_ask() const noexcept {
  if (best_ask_idx_ < 0) return {};
  const PriceLevel& lvl = levels_[static_cast<std::size_t>(best_ask_idx_)];
  return {to_price(best_ask_idx_), lvl.aggregate_size, lvl.order_count, true};
}

std::size_t OrderBook::depth_bids(BookLevel* out, std::size_t n) const noexcept {
  std::size_t c = 0;
  for (std::int64_t i = best_bid_idx_; i >= 0 && c < n; --i) {
    const PriceLevel& lvl = levels_[static_cast<std::size_t>(i)];
    if (lvl.order_count > 0 && lvl.side == Side::Buy) {
      out[c++] = {to_price(i), lvl.aggregate_size, lvl.order_count, true};
    }
  }
  return c;
}

std::size_t OrderBook::depth_asks(BookLevel* out, std::size_t n) const noexcept {
  const std::int64_t sz = static_cast<std::int64_t>(levels_.size());
  std::size_t c = 0;
  for (std::int64_t i = best_ask_idx_; i >= 0 && i < sz && c < n; ++i) {
    const PriceLevel& lvl = levels_[static_cast<std::size_t>(i)];
    if (lvl.order_count > 0 && lvl.side == Side::Sell) {
      out[c++] = {to_price(i), lvl.aggregate_size, lvl.order_count, true};
    }
  }
  return c;
}

bool OrderBook::is_crossed() const noexcept {
  return best_bid_idx_ >= 0 && best_ask_idx_ >= 0 && best_bid_idx_ >= best_ask_idx_;
}

std::int64_t OrderBook::spread_ticks() const noexcept {
  if (best_bid_idx_ < 0 || best_ask_idx_ < 0) return -1;
  return to_price(best_ask_idx_) - to_price(best_bid_idx_);
}

void OrderBook::reset() noexcept {
  index_.clear();
  levels_.assign(levels_.size(), PriceLevel{});
  pool_.reset_all();
  best_bid_idx_ = -1;
  best_ask_idx_ = -1;
  live_orders_ = 0;
  crossed_at_last_boundary_ = false;
  // stats_ are cumulative across recovery and intentionally preserved.
}

}  // namespace fh
