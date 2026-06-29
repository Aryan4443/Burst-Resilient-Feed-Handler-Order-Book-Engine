// Synthetic NASDAQ ITCH 5.0 generator.
//
// Emits a length-framed (2-byte big-endian length + payload) stream of *wire-valid* ITCH 5.0
// messages (A/E/X/D/U/P plus framing S events). It keeps a set of live orders so that
// cancel/execute/delete/replace reference real resting orders -> the resulting book stays
// consistent (≈0 unknown-ref events) and never crosses (bids and asks sit in disjoint bands
// around a fixed mid). This lets the whole engine run end-to-end with no external download.
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "feedhandler/detail/byte_read.hpp"

using fh::detail::store_be16;
using fh::detail::store_be32;
using fh::detail::store_be48;
using fh::detail::store_be64;

namespace {

struct Args {
  std::string out = "data/sample.itch";
  std::uint64_t messages = 500'000;
  std::uint64_t seed = 42;
  std::string symbol = "SYNTH";
};

Args parse_args(int argc, char** argv) {
  Args a;
  for (int i = 1; i < argc; ++i) {
    std::string k = argv[i];
    auto next = [&]() -> std::string { return (i + 1 < argc) ? argv[++i] : std::string{}; };
    if (k == "--out") a.out = next();
    else if (k == "--orders" || k == "--messages" || k == "-n") a.messages = std::stoull(next());
    else if (k == "--seed") a.seed = std::stoull(next());
    else if (k == "--symbol") a.symbol = next();
    else if (k == "--help") {
      std::cout << "usage: itch_gen [--out FILE] [--orders N] [--seed S] [--symbol SYM]\n";
      std::exit(0);
    }
  }
  return a;
}

struct Live {
  std::uint64_t ref;
  std::uint8_t  side;  // 'B' / 'S'
  std::int64_t  px;
  std::uint32_t sz;
};

class ItchWriter {
 public:
  explicit ItchWriter(std::ofstream& os) : os_(os) {}
  void emit(std::size_t len) {
    std::byte hdr[2];
    store_be16(hdr, static_cast<std::uint16_t>(len));
    os_.write(reinterpret_cast<const char*>(hdr), 2);
    os_.write(reinterpret_cast<const char*>(buf_), static_cast<std::streamsize>(len));
    ++count_;
  }
  std::byte* buf() { return buf_; }
  std::uint64_t count() const { return count_; }

 private:
  std::ofstream& os_;
  std::byte buf_[64]{};
  std::uint64_t count_ = 0;
};

}  // namespace

int main(int argc, char** argv) {
  const Args args = parse_args(argc, argv);

  std::ofstream os(args.out, std::ios::binary | std::ios::trunc);
  if (!os) {
    std::cerr << "itch_gen: cannot open '" << args.out << "' for writing\n";
    return 1;
  }

  std::mt19937_64 rng(args.seed);
  auto uni = [&](std::uint64_t lo, std::uint64_t hi) {
    return std::uniform_int_distribution<std::uint64_t>(lo, hi)(rng);
  };

  char sym[8];
  std::memset(sym, ' ', sizeof(sym));
  std::memcpy(sym, args.symbol.data(), std::min<std::size_t>(args.symbol.size(), 8));

  constexpr std::uint16_t kLocate = 1;
  constexpr std::int64_t kMid = 1'000'000;  // $100.0000
  constexpr std::int64_t kBand = 50;        // ticks above/below mid
  std::uint64_t ts = 34'200'000'000'000ull;  // 09:30:00.000000000
  std::uint64_t next_ref = 1;
  std::uint64_t match = 1;
  const std::uint64_t kMaxLive = 20'000;

  std::vector<Live> live;
  live.reserve(kMaxLive + 16);

  ItchWriter w(os);

  auto hdr = [&](std::byte* b, char type) {
    b[0] = static_cast<std::byte>(type);
    store_be16(b + 1, kLocate);
    store_be16(b + 3, 0);  // tracking number
    ts += uni(1, 1000);    // monotonically advance the clock
    store_be48(b + 5, ts);
  };

  auto emit_system = [&](char code) {
    std::byte* b = w.buf();
    hdr(b, 'S');
    b[11] = static_cast<std::byte>(code);
    w.emit(12);
  };

  auto emit_add = [&](const Live& o) {
    std::byte* b = w.buf();
    hdr(b, 'A');
    store_be64(b + 11, o.ref);
    b[19] = static_cast<std::byte>(o.side);
    store_be32(b + 20, o.sz);
    std::memcpy(b + 24, sym, 8);
    store_be32(b + 32, static_cast<std::uint32_t>(o.px));
    w.emit(36);
  };
  auto emit_exec = [&](std::uint64_t ref, std::uint32_t shares) {
    std::byte* b = w.buf();
    hdr(b, 'E');
    store_be64(b + 11, ref);
    store_be32(b + 19, shares);
    store_be64(b + 23, match++);
    w.emit(31);
  };
  auto emit_cancel = [&](std::uint64_t ref, std::uint32_t shares) {
    std::byte* b = w.buf();
    hdr(b, 'X');
    store_be64(b + 11, ref);
    store_be32(b + 19, shares);
    w.emit(23);
  };
  auto emit_delete = [&](std::uint64_t ref) {
    std::byte* b = w.buf();
    hdr(b, 'D');
    store_be64(b + 11, ref);
    w.emit(19);
  };
  auto emit_replace = [&](std::uint64_t orig, std::uint64_t neu, std::uint32_t sz, std::int64_t px) {
    std::byte* b = w.buf();
    hdr(b, 'U');
    store_be64(b + 11, orig);
    store_be64(b + 19, neu);
    store_be32(b + 27, sz);
    store_be32(b + 31, static_cast<std::uint32_t>(px));
    w.emit(35);
  };
  auto emit_trade = [&]() {
    std::byte* b = w.buf();
    hdr(b, 'P');
    store_be64(b + 11, 0);
    b[19] = static_cast<std::byte>(uni(0, 1) ? 'B' : 'S');
    store_be32(b + 20, static_cast<std::uint32_t>(uni(1, 5) * 100));
    std::memcpy(b + 24, sym, 8);
    store_be32(b + 32, static_cast<std::uint32_t>(kMid));
    store_be64(b + 36, match++);
    w.emit(44);
  };

  auto random_price = [&](std::uint8_t side) -> std::int64_t {
    return side == 'B' ? kMid - 1 - static_cast<std::int64_t>(uni(0, kBand))
                       : kMid + 1 + static_cast<std::int64_t>(uni(0, kBand));
  };
  auto do_add = [&]() {
    Live o;
    o.ref = next_ref++;
    o.side = static_cast<std::uint8_t>(uni(0, 1) ? 'B' : 'S');
    o.px = random_price(o.side);
    o.sz = static_cast<std::uint32_t>(uni(1, 10) * 100);
    emit_add(o);
    live.push_back(o);
  };
  auto remove_at = [&](std::size_t idx) {
    live[idx] = live.back();
    live.pop_back();
  };

  emit_system('O');  // start of messages

  while (w.count() < args.messages) {
    const bool too_many = live.size() >= kMaxLive;
    const bool too_few = live.size() < 8;
    std::uint64_t roll = uni(0, 99);
    if (too_few) roll = 0;             // force adds to build the book
    else if (too_many) roll = 60;      // bias to removals when saturated

    if (roll < 50) {                   // ADD
      do_add();
    } else if (roll < 68) {            // EXECUTE
      const std::size_t i = static_cast<std::size_t>(uni(0, live.size() - 1));
      const std::uint32_t shares = static_cast<std::uint32_t>(uni(1, live[i].sz));
      emit_exec(live[i].ref, shares);
      if (shares >= live[i].sz) remove_at(i);
      else live[i].sz -= shares;
    } else if (roll < 80) {            // CANCEL (partial reduce)
      const std::size_t i = static_cast<std::size_t>(uni(0, live.size() - 1));
      const std::uint32_t shares = static_cast<std::uint32_t>(uni(1, live[i].sz));
      emit_cancel(live[i].ref, shares);
      if (shares >= live[i].sz) remove_at(i);
      else live[i].sz -= shares;
    } else if (roll < 92) {            // DELETE
      const std::size_t i = static_cast<std::size_t>(uni(0, live.size() - 1));
      emit_delete(live[i].ref);
      remove_at(i);
    } else if (roll < 99) {            // REPLACE
      const std::size_t i = static_cast<std::size_t>(uni(0, live.size() - 1));
      const std::uint64_t neu = next_ref++;
      const std::int64_t px = random_price(live[i].side);
      const std::uint32_t sz = static_cast<std::uint32_t>(uni(1, 10) * 100);
      emit_replace(live[i].ref, neu, sz, px);
      live[i].ref = neu;
      live[i].px = px;
      live[i].sz = sz;
    } else {                           // TRADE (tape only)
      emit_trade();
    }
  }

  emit_system('C');  // end of messages
  os.flush();

  std::cout << "itch_gen: wrote " << w.count() << " messages to " << args.out
            << " (" << static_cast<double>(os.tellp()) / 1e6 << " MB), "
            << live.size() << " orders live at end\n";
  return 0;
}
