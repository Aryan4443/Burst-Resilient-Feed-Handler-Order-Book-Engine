#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <vector>

#include "feedhandler/itch_parser.hpp"
#include "feedhandler/market_event.hpp"

using namespace fh;

namespace {

// Independent big-endian message builder (does NOT reuse the library's store helpers, so
// the parser is genuinely black-box tested against bytes we lay out by hand).
struct Builder {
  std::vector<std::byte> bytes;
  void u8(std::uint8_t v) { bytes.push_back(std::byte{v}); }
  void be16(std::uint16_t v) { u8(static_cast<std::uint8_t>(v >> 8)); u8(static_cast<std::uint8_t>(v)); }
  void be32(std::uint32_t v) {
    u8(static_cast<std::uint8_t>(v >> 24)); u8(static_cast<std::uint8_t>(v >> 16));
    u8(static_cast<std::uint8_t>(v >> 8));  u8(static_cast<std::uint8_t>(v));
  }
  void be48(std::uint64_t v) { for (int i = 5; i >= 0; --i) u8(static_cast<std::uint8_t>(v >> (8 * i))); }
  void be64(std::uint64_t v) { for (int i = 7; i >= 0; --i) u8(static_cast<std::uint8_t>(v >> (8 * i))); }
  void str(const char* s, std::size_t n) { for (std::size_t i = 0; i < n; ++i) u8(static_cast<std::uint8_t>(s[i])); }
  const std::byte* data() const { return bytes.data(); }
  std::size_t size() const { return bytes.size(); }
};

}  // namespace

// The canonical rigorous test: a fully hand-written Add Order ('A') byte sequence with
// every byte spelled out, asserting every decoded field.
TEST(ItchParser, AddOrderKnownByteSequence) {
  const std::uint8_t raw[] = {
      0x41,                                            // 'A'
      0x12, 0x34,                                      // stock_locate = 0x1234
      0x00, 0x01,                                      // tracking number
      0x01, 0x02, 0x03, 0x04, 0x05, 0x06,              // timestamp (48-bit)
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xD2,  // order_ref = 1234
      0x42,                                            // 'B' (buy)
      0x00, 0x00, 0x00, 0x64,                          // shares = 100
      0x41, 0x41, 0x50, 0x4C, 0x20, 0x20, 0x20, 0x20,  // "AAPL    "
      0x00, 0x01, 0x86, 0xA0,                          // price = 100000 ($10.0000)
  };
  static_assert(sizeof(raw) == 36, "Add Order is 36 bytes");

  MarketEvent ev;
  const auto st = itch::parse(reinterpret_cast<const std::byte*>(raw), sizeof(raw), ev);

  ASSERT_EQ(st, itch::ParseStatus::Ok);
  EXPECT_EQ(ev.type, EventType::Add);
  EXPECT_EQ(ev.stock_locate, 0x1234u);
  EXPECT_EQ(ev.timestamp_ns, 0x010203040506ull);  // = 1108152157446
  EXPECT_EQ(ev.order_ref, 1234u);
  EXPECT_EQ(ev.side, Side::Buy);
  EXPECT_EQ(ev.size, 100u);
  EXPECT_EQ(0, std::memcmp(ev.symbol, "AAPL    ", 8));
  EXPECT_EQ(ev.price_ticks, 100000);
  EXPECT_DOUBLE_EQ(price_ticks_to_double(ev.price_ticks), 10.0);
}

TEST(ItchParser, OrderExecuted) {
  Builder b;
  b.u8('E'); b.be16(7); b.be16(0); b.be48(42);
  b.be64(1234); b.be32(50); b.be64(999);
  ASSERT_EQ(b.size(), itch::kLenOrderExecuted);

  MarketEvent ev;
  ASSERT_EQ(itch::parse(b.data(), b.size(), ev), itch::ParseStatus::Ok);
  EXPECT_EQ(ev.type, EventType::Execute);
  EXPECT_EQ(ev.stock_locate, 7u);
  EXPECT_EQ(ev.order_ref, 1234u);
  EXPECT_EQ(ev.size, 50u);
  EXPECT_EQ(ev.match_number, 999u);
  EXPECT_EQ(ev.price_ticks, 0);  // E has no price
}

TEST(ItchParser, OrderExecutedWithPrice) {
  Builder b;
  b.u8('C'); b.be16(1); b.be16(0); b.be48(42);
  b.be64(1234); b.be32(50); b.be64(1000); b.u8('Y'); b.be32(123456);
  ASSERT_EQ(b.size(), itch::kLenOrderExecutedPrice);

  MarketEvent ev;
  ASSERT_EQ(itch::parse(b.data(), b.size(), ev), itch::ParseStatus::Ok);
  EXPECT_EQ(ev.type, EventType::ExecuteWithPrice);
  EXPECT_EQ(ev.order_ref, 1234u);
  EXPECT_EQ(ev.size, 50u);
  EXPECT_EQ(ev.match_number, 1000u);
  EXPECT_EQ(ev.price_ticks, 123456);
}

TEST(ItchParser, OrderCancel) {
  Builder b;
  b.u8('X'); b.be16(1); b.be16(0); b.be48(42); b.be64(1234); b.be32(30);
  ASSERT_EQ(b.size(), itch::kLenOrderCancel);

  MarketEvent ev;
  ASSERT_EQ(itch::parse(b.data(), b.size(), ev), itch::ParseStatus::Ok);
  EXPECT_EQ(ev.type, EventType::Cancel);
  EXPECT_EQ(ev.order_ref, 1234u);
  EXPECT_EQ(ev.size, 30u);  // cancelled shares
}

TEST(ItchParser, OrderDelete) {
  Builder b;
  b.u8('D'); b.be16(1); b.be16(0); b.be48(42); b.be64(1234);
  ASSERT_EQ(b.size(), itch::kLenOrderDelete);

  MarketEvent ev;
  ASSERT_EQ(itch::parse(b.data(), b.size(), ev), itch::ParseStatus::Ok);
  EXPECT_EQ(ev.type, EventType::Delete);
  EXPECT_EQ(ev.order_ref, 1234u);
}

TEST(ItchParser, OrderReplace) {
  Builder b;
  b.u8('U'); b.be16(1); b.be16(0); b.be48(42);
  b.be64(1234); b.be64(5678); b.be32(200); b.be32(200000);
  ASSERT_EQ(b.size(), itch::kLenOrderReplace);

  MarketEvent ev;
  ASSERT_EQ(itch::parse(b.data(), b.size(), ev), itch::ParseStatus::Ok);
  EXPECT_EQ(ev.type, EventType::Replace);
  EXPECT_EQ(ev.order_ref, 1234u);      // original
  EXPECT_EQ(ev.new_order_ref, 5678u);  // new
  EXPECT_EQ(ev.size, 200u);
  EXPECT_EQ(ev.price_ticks, 200000);
}

TEST(ItchParser, TradeNonCross) {
  Builder b;
  b.u8('P'); b.be16(1); b.be16(0); b.be48(42);
  b.be64(0); b.u8('S'); b.be32(10); b.str("MSFT    ", 8); b.be32(300000); b.be64(77);
  ASSERT_EQ(b.size(), itch::kLenTradeNonCross);

  MarketEvent ev;
  ASSERT_EQ(itch::parse(b.data(), b.size(), ev), itch::ParseStatus::Ok);
  EXPECT_EQ(ev.type, EventType::Trade);
  EXPECT_EQ(ev.side, Side::Sell);
  EXPECT_EQ(ev.size, 10u);
  EXPECT_EQ(0, std::memcmp(ev.symbol, "MSFT    ", 8));
  EXPECT_EQ(ev.price_ticks, 300000);
  EXPECT_EQ(ev.match_number, 77u);
}

TEST(ItchParser, SystemEvent) {
  Builder b;
  b.u8('S'); b.be16(0); b.be16(0); b.be48(0); b.u8('O');  // 'O' = start of messages
  ASSERT_EQ(b.size(), itch::kLenSystemEvent);

  MarketEvent ev;
  ASSERT_EQ(itch::parse(b.data(), b.size(), ev), itch::ParseStatus::Ok);
  EXPECT_EQ(ev.type, EventType::SystemEvent);
}

TEST(ItchParser, KnownButIrrelevantTypeIsUnknownEvent) {
  // 'R' Stock Directory (39 bytes) is valid ITCH but not book-relevant.
  std::vector<std::byte> b(itch::message_length('R'), std::byte{0});
  b[0] = std::byte{'R'};
  MarketEvent ev;
  ASSERT_EQ(itch::parse(b.data(), b.size(), ev), itch::ParseStatus::Ok);
  EXPECT_EQ(ev.type, EventType::Unknown);
}

TEST(ItchParser, ShortBufferRejected) {
  Builder b;
  b.u8('A'); b.be16(1);  // only 3 bytes, far short of 36
  MarketEvent ev;
  EXPECT_EQ(itch::parse(b.data(), b.size(), ev), itch::ParseStatus::ShortBuffer);
}

TEST(ItchParser, UnknownTypeRejected) {
  const std::uint8_t raw[] = {0x5A, 0x00, 0x00};  // 'Z' is not an ITCH 5.0 type
  MarketEvent ev;
  EXPECT_EQ(itch::parse(reinterpret_cast<const std::byte*>(raw), sizeof(raw), ev),
            itch::ParseStatus::UnknownType);
}

TEST(ItchParser, EmptyBufferRejected) {
  MarketEvent ev;
  EXPECT_EQ(itch::parse(nullptr, 0, ev), itch::ParseStatus::ShortBuffer);
}

TEST(ItchParser, ReusedEventHasNoStaleFields) {
  // Parse an Add (sets symbol + price), then a Delete into the same event. The Delete must
  // not inherit the Add's symbol/price/side.
  Builder add;
  add.u8('A'); add.be16(1); add.be16(0); add.be48(1);
  add.be64(1); add.u8('B'); add.be32(100); add.str("AAPL    ", 8); add.be32(100000);

  MarketEvent ev;
  ASSERT_EQ(itch::parse(add.data(), add.size(), ev), itch::ParseStatus::Ok);
  ASSERT_EQ(ev.side, Side::Buy);

  Builder del;
  del.u8('D'); del.be16(1); del.be16(0); del.be48(2); del.be64(1);
  ASSERT_EQ(itch::parse(del.data(), del.size(), ev), itch::ParseStatus::Ok);

  EXPECT_EQ(ev.type, EventType::Delete);
  EXPECT_EQ(ev.side, Side::None);
  EXPECT_EQ(ev.price_ticks, 0);
  EXPECT_EQ(ev.size, 0u);
  char zero[8] = {};
  EXPECT_EQ(0, std::memcmp(ev.symbol, zero, 8));
}

TEST(ItchParser, MessageLengthTable) {
  EXPECT_EQ(itch::message_length('A'), 36u);
  EXPECT_EQ(itch::message_length('F'), 40u);
  EXPECT_EQ(itch::message_length('E'), 31u);
  EXPECT_EQ(itch::message_length('C'), 36u);
  EXPECT_EQ(itch::message_length('X'), 23u);
  EXPECT_EQ(itch::message_length('D'), 19u);
  EXPECT_EQ(itch::message_length('U'), 35u);
  EXPECT_EQ(itch::message_length('P'), 44u);
  EXPECT_EQ(itch::message_length('S'), 12u);
  EXPECT_EQ(itch::message_length('Z'), 0u);  // unknown
}
