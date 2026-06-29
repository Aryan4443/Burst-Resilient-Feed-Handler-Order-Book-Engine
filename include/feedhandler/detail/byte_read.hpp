#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

// Unaligned, big-endian (network byte order) integer loads.
//
// ITCH multi-byte integers are big-endian and are NOT guaranteed to be aligned within a
// frame, so we must not reinterpret_cast a (possibly unaligned) pointer to e.g. uint32_t*
// -- that is undefined behaviour. Instead we assemble the value byte-by-byte. Modern
// compilers (clang/gcc) recognise this idiom and emit a single load + byte-swap (or a
// `movbe`/`rev`), so it is both correct and fast.
namespace fh::detail {

inline std::uint8_t load_u8(const std::byte* p) noexcept {
  return static_cast<std::uint8_t>(p[0]);
}

inline std::uint16_t load_be16(const std::byte* p) noexcept {
  const auto* b = reinterpret_cast<const std::uint8_t*>(p);
  return static_cast<std::uint16_t>((std::uint32_t{b[0]} << 8) | std::uint32_t{b[1]});
}

inline std::uint32_t load_be32(const std::byte* p) noexcept {
  const auto* b = reinterpret_cast<const std::uint8_t*>(p);
  return (std::uint32_t{b[0]} << 24) | (std::uint32_t{b[1]} << 16) |
         (std::uint32_t{b[2]} << 8) | std::uint32_t{b[3]};
}

// 48-bit big-endian load (ITCH timestamps): nanoseconds since midnight, widened to 64 bits.
inline std::uint64_t load_be48(const std::byte* p) noexcept {
  const auto* b = reinterpret_cast<const std::uint8_t*>(p);
  return (std::uint64_t{b[0]} << 40) | (std::uint64_t{b[1]} << 32) |
         (std::uint64_t{b[2]} << 24) | (std::uint64_t{b[3]} << 16) |
         (std::uint64_t{b[4]} << 8) | std::uint64_t{b[5]};
}

inline std::uint64_t load_be64(const std::byte* p) noexcept {
  const auto* b = reinterpret_cast<const std::uint8_t*>(p);
  return (std::uint64_t{b[0]} << 56) | (std::uint64_t{b[1]} << 48) |
         (std::uint64_t{b[2]} << 40) | (std::uint64_t{b[3]} << 32) |
         (std::uint64_t{b[4]} << 24) | (std::uint64_t{b[5]} << 16) |
         (std::uint64_t{b[6]} << 8) | std::uint64_t{b[7]};
}

// Big-endian store helpers (used by the ITCH generator / tests to build wire bytes).
inline void store_be16(std::byte* p, std::uint16_t v) noexcept {
  auto* b = reinterpret_cast<std::uint8_t*>(p);
  b[0] = static_cast<std::uint8_t>(v >> 8);
  b[1] = static_cast<std::uint8_t>(v);
}

inline void store_be32(std::byte* p, std::uint32_t v) noexcept {
  auto* b = reinterpret_cast<std::uint8_t*>(p);
  b[0] = static_cast<std::uint8_t>(v >> 24);
  b[1] = static_cast<std::uint8_t>(v >> 16);
  b[2] = static_cast<std::uint8_t>(v >> 8);
  b[3] = static_cast<std::uint8_t>(v);
}

inline void store_be48(std::byte* p, std::uint64_t v) noexcept {
  auto* b = reinterpret_cast<std::uint8_t*>(p);
  b[0] = static_cast<std::uint8_t>(v >> 40);
  b[1] = static_cast<std::uint8_t>(v >> 32);
  b[2] = static_cast<std::uint8_t>(v >> 24);
  b[3] = static_cast<std::uint8_t>(v >> 16);
  b[4] = static_cast<std::uint8_t>(v >> 8);
  b[5] = static_cast<std::uint8_t>(v);
}

inline void store_be64(std::byte* p, std::uint64_t v) noexcept {
  auto* b = reinterpret_cast<std::uint8_t*>(p);
  b[0] = static_cast<std::uint8_t>(v >> 56);
  b[1] = static_cast<std::uint8_t>(v >> 48);
  b[2] = static_cast<std::uint8_t>(v >> 40);
  b[3] = static_cast<std::uint8_t>(v >> 32);
  b[4] = static_cast<std::uint8_t>(v >> 24);
  b[5] = static_cast<std::uint8_t>(v >> 16);
  b[6] = static_cast<std::uint8_t>(v >> 8);
  b[7] = static_cast<std::uint8_t>(v);
}

}  // namespace fh::detail
