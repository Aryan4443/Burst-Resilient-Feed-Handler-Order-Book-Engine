#include "feedhandler/itch_parser.hpp"

#include <cstring>

#include "feedhandler/detail/byte_read.hpp"

namespace fh::itch {
namespace {

using detail::load_be16;
using detail::load_be32;
using detail::load_be48;
using detail::load_be64;
using detail::load_u8;

constexpr Side decode_side(std::uint8_t c) noexcept {
  switch (c) {
    case 'B': return Side::Buy;
    case 'S': return Side::Sell;
    default:  return Side::None;
  }
}

// Common ITCH header is identical across messages: type(1) locate(2) tracking(2) ts(6).
inline void decode_header(const std::byte* m, MarketEvent& out) noexcept {
  out.stock_locate = load_be16(m + 1);
  out.timestamp_ns = load_be48(m + 5);
}

inline void copy_symbol(const std::byte* src8, MarketEvent& out) noexcept {
  std::memcpy(out.symbol, src8, 8);
}

}  // namespace

std::size_t message_length(char type) noexcept {
  switch (type) {
    case 'S': return 12;  // System Event
    case 'R': return 39;  // Stock Directory
    case 'H': return 25;  // Stock Trading Action
    case 'Y': return 20;  // Reg SHO Restriction
    case 'L': return 26;  // Market Participant Position
    case 'V': return 35;  // MWCB Decline Level
    case 'W': return 12;  // MWCB Status
    case 'K': return 28;  // IPO Quoting Period Update
    case 'J': return 35;  // LULD Auction Collar
    case 'h': return 21;  // Operational Halt
    case 'A': return 36;  // Add Order (no MPID)
    case 'F': return 40;  // Add Order (MPID)
    case 'E': return 31;  // Order Executed
    case 'C': return 36;  // Order Executed With Price
    case 'X': return 23;  // Order Cancel
    case 'D': return 19;  // Order Delete
    case 'U': return 35;  // Order Replace
    case 'P': return 44;  // Trade (non-cross)
    case 'Q': return 40;  // Cross Trade
    case 'B': return 19;  // Broken Trade
    case 'I': return 50;  // NOII
    case 'N': return 20;  // Retail Price Improvement Indicator
    case 'O': return 48;  // Direct Listing with Capital Raise
    default:  return 0;   // unknown
  }
}

ParseStatus parse(const std::byte* msg, std::size_t len, MarketEvent& out) noexcept {
  if (len == 0) return ParseStatus::ShortBuffer;

  const char type = static_cast<char>(load_u8(msg));
  const std::size_t need = message_length(type);
  if (need == 0) return ParseStatus::UnknownType;
  if (len < need) return ParseStatus::ShortBuffer;

  // Reset to a clean event so reused instances never carry stale fields (correctness > the
  // ~tens of cycles a 56-byte clear costs; it is dwarfed by the field decoding below).
  out = MarketEvent{};
  decode_header(msg, out);

  switch (type) {
    case 'A':    // Add Order (no MPID)
    case 'F': {  // Add Order (with MPID) -- identical prefix, trailing attribution ignored
      out.type        = EventType::Add;
      out.order_ref   = load_be64(msg + 11);
      out.side        = decode_side(load_u8(msg + 19));
      out.size        = load_be32(msg + 20);
      copy_symbol(msg + 24, out);
      out.price_ticks = static_cast<std::int64_t>(load_be32(msg + 32));
      return ParseStatus::Ok;
    }
    case 'E': {  // Order Executed (at the resting order's price)
      out.type         = EventType::Execute;
      out.order_ref    = load_be64(msg + 11);
      out.size         = load_be32(msg + 19);   // executed shares
      out.match_number = load_be64(msg + 23);
      return ParseStatus::Ok;
    }
    case 'C': {  // Order Executed With Price
      out.type         = EventType::ExecuteWithPrice;
      out.order_ref    = load_be64(msg + 11);
      out.size         = load_be32(msg + 19);   // executed shares
      out.match_number = load_be64(msg + 23);
      // msg + 31 = printable flag (Y/N); does not affect the book.
      out.price_ticks  = static_cast<std::int64_t>(load_be32(msg + 32));
      return ParseStatus::Ok;
    }
    case 'X': {  // Order Cancel (partial)
      out.type      = EventType::Cancel;
      out.order_ref = load_be64(msg + 11);
      out.size      = load_be32(msg + 19);       // cancelled shares
      return ParseStatus::Ok;
    }
    case 'D': {  // Order Delete (full)
      out.type      = EventType::Delete;
      out.order_ref = load_be64(msg + 11);
      return ParseStatus::Ok;
    }
    case 'U': {  // Order Replace (cancel orig + add new, loses time priority)
      out.type          = EventType::Replace;
      out.order_ref     = load_be64(msg + 11);   // original order ref
      out.new_order_ref = load_be64(msg + 19);   // new order ref
      out.size          = load_be32(msg + 27);   // new shares
      out.price_ticks   = static_cast<std::int64_t>(load_be32(msg + 31));  // new price
      return ParseStatus::Ok;
    }
    case 'P': {  // Trade (non-cross) -- tape only, no book mutation
      out.type         = EventType::Trade;
      out.order_ref    = load_be64(msg + 11);
      out.side         = decode_side(load_u8(msg + 19));
      out.size         = load_be32(msg + 20);
      copy_symbol(msg + 24, out);
      out.price_ticks  = static_cast<std::int64_t>(load_be32(msg + 32));
      out.match_number = load_be64(msg + 36);
      return ParseStatus::Ok;
    }
    case 'S': {  // System Event
      out.type = EventType::SystemEvent;
      return ParseStatus::Ok;
    }
    default:
      // Known-but-book-irrelevant type (R/H/Y/L/V/W/K/J/h/Q/B/I/N/O).
      // Header already decoded; leave type = Unknown so callers can skip by length.
      out.type = EventType::Unknown;
      return ParseStatus::Ok;
  }
}

}  // namespace fh::itch
