#pragma once

#include <cstdint>
#include <type_traits>

namespace fh {

// Logical classification of a decoded feed message.
//
// We deliberately collapse the raw ITCH "message type" alphabet (A/F/E/C/X/D/U/P/S/...)
// into a small set of *book actions*. The parser is responsible for the mapping; every
// downstream component (book, publisher, instrumentation) reasons about EventType only.
enum class EventType : std::uint8_t {
  Unknown = 0,        // parsed but not book-relevant (or unsupported type)
  Add,                // new resting order enters the book
  Cancel,             // partial cancel: reduce a resting order's size by `size`
  Delete,             // full delete: remove a resting order entirely
  Execute,            // resting order executed for `size` shares (price = resting price)
  ExecuteWithPrice,   // resting order executed for `size` shares at `price_ticks`
  Replace,            // atomic cancel(order_ref) + add(new_order_ref) at new price/size
  Trade,              // non-displayable trade: tape only, no book mutation
  SystemEvent,        // session / system message (start/end of messages, halts, ...)
};

// Order side. ITCH encodes this as 'B'/'S'.
enum class Side : std::uint8_t {
  None = 0,
  Buy,
  Sell,
};

// A decoded, self-contained market-data event.
//
// Design constraints:
//   * Plain-old-data / trivially copyable so it can be memcpy'd through the lock-free
//     SPSC ring buffer with no construction/destruction and no heap ownership.
//   * Fixed inline `symbol` (no std::string) so the event owns its data and outlives the
//     network/file buffer it was decoded from. We key the book on `stock_locate` (the
//     feed's compact instrument id) and keep `symbol` for human-facing output.
//   * Field order chosen to minimise padding (8-byte members first). sizeof == 56.
struct MarketEvent {
  std::uint64_t timestamp_ns = 0;   // 48-bit ITCH timestamp (ns since midnight), widened
  std::uint64_t order_ref = 0;      // order reference number (Replace: the *original*)
  std::uint64_t new_order_ref = 0;  // Replace only: the new order reference number
  std::uint64_t match_number = 0;   // Execute / Trade: exchange match id (tape join key)
  std::int64_t  price_ticks = 0;    // fixed-point price as raw integer ticks from the feed
  std::uint32_t size = 0;           // shares (add size / cancelled / executed / new size)
  std::uint16_t stock_locate = 0;   // instrument id assigned by the feed (book key)
  EventType     type = EventType::Unknown;
  Side          side = Side::None;
  char          symbol[8] = {};     // right space-padded ASCII ticker (ITCH "Stock" field)
};

static_assert(std::is_trivially_copyable_v<MarketEvent>,
              "MarketEvent must be trivially copyable to pass through the SPSC ring buffer");
static_assert(std::is_trivially_destructible_v<MarketEvent>,
              "MarketEvent must be trivially destructible (no heap ownership on the hot path)");

// ITCH 5.0 prices are 4-decimal fixed point (price = raw / 10000). Helper for display only.
constexpr double price_ticks_to_double(std::int64_t price_ticks) noexcept {
  return static_cast<double>(price_ticks) / 10000.0;
}

}  // namespace fh
