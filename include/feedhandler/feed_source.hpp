#pragma once

#include <cstddef>
#include <cstdint>

namespace fh {

// A single framed message handed up by a FeedSource.
//
// `data` points into memory owned by the FeedSource (e.g. an mmap'd file or a socket
// receive buffer). It is valid only until the next call to next_frame() -- this is what
// keeps the read path zero-copy: the parser decodes straight out of this buffer.
struct RawFrame {
  const std::byte* data = nullptr;  // pointer to message payload (after framing header)
  std::size_t      len = 0;         // payload length in bytes
  std::uint64_t    sequence = 0;    // per-stream sequence number for gap detection
};

// Common interface for every market-data adapter.
//
// Concrete adapters:
//   * ItchFileSource  -- replays a length-framed NASDAQ ITCH 5.0 file (implemented first).
//   * CryptoWsSource  -- live exchange depth stream (future; same interface).
//
// Contract: next_frame() returns true and fills `out` while messages remain; returns
// false at end-of-stream. The pointer in `out.data` is only guaranteed valid until the
// next next_frame() call.
class FeedSource {
 public:
  virtual ~FeedSource() = default;

  // Fetch the next framed message. Returns false when the stream is exhausted.
  virtual bool next_frame(RawFrame& out) = 0;

  // Human-readable adapter name (for logs / reports).
  virtual const char* name() const noexcept = 0;
};

}  // namespace fh
