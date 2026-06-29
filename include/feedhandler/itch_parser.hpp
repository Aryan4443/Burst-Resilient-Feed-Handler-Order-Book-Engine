#pragma once

#include <cstddef>
#include <cstdint>

#include "feedhandler/market_event.hpp"

// NASDAQ TotalView-ITCH 5.0 decoder.
//
// Reference: Nasdaq TotalView-ITCH 5.0 spec. All multi-byte integers are big-endian.
// Message layouts below are the *payload* only (no MoldUDP / length framing). Offsets are
// from the type byte at offset 0. We decode the message types that mutate a price book
// (plus the system event); other valid types are reported as Ok / EventType::Unknown so a
// caller can skip them by their known length.
namespace fh::itch {

// Raw ITCH 5.0 message type bytes we recognise.
enum class MsgType : char {
  SystemEvent          = 'S',
  StockDirectory       = 'R',
  AddOrder             = 'A',  // add, no MPID attribution
  AddOrderMpid         = 'F',  // add, with MPID attribution
  OrderExecuted        = 'E',
  OrderExecutedPrice   = 'C',
  OrderCancel          = 'X',  // partial cancel (reduce shares)
  OrderDelete          = 'D',  // full delete
  OrderReplace         = 'U',
  TradeNonCross        = 'P',
};

// Fixed total payload lengths (bytes) for the message types we decode.
inline constexpr std::size_t kLenSystemEvent        = 12;
inline constexpr std::size_t kLenAddOrder           = 36;
inline constexpr std::size_t kLenAddOrderMpid       = 40;
inline constexpr std::size_t kLenOrderExecuted      = 31;
inline constexpr std::size_t kLenOrderExecutedPrice = 36;
inline constexpr std::size_t kLenOrderCancel        = 23;
inline constexpr std::size_t kLenOrderDelete        = 19;
inline constexpr std::size_t kLenOrderReplace       = 35;
inline constexpr std::size_t kLenTradeNonCross      = 44;

enum class ParseStatus : std::uint8_t {
  Ok,            // decoded successfully (out.type may be Unknown for irrelevant messages)
  ShortBuffer,   // len < required length for this message type
  UnknownType,   // first byte is not a known ITCH 5.0 message type
};

// Expected total length for an ITCH 5.0 message type byte, or 0 if the type is unknown.
// Lets a framing-less reader advance by the correct stride and lets callers validate framed
// lengths. Covers all 5.0 types, not just the book-relevant subset.
std::size_t message_length(char type) noexcept;

// Decode one ITCH message into `out`. Zero-copy: reads straight from `msg`, writes decoded
// fields into the caller-owned `out`. No heap allocation. `len` is the available bytes.
ParseStatus parse(const std::byte* msg, std::size_t len, MarketEvent& out) noexcept;

}  // namespace fh::itch
