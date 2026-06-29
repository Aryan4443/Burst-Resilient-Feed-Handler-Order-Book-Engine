#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "feedhandler/feed_source.hpp"

namespace fh {

// Replays a length-framed NASDAQ ITCH 5.0 file: each message is a 2-byte big-endian length
// prefix followed by that many payload bytes (the public BinaryFILE framing). The file is
// mmap'd, so frames are handed up as borrowed pointers into the mapping — truly zero-copy on
// the read path. Messages are numbered 1,2,3,... and that index is used as the feed sequence
// number for gap detection (the simple file format has no embedded MoldUDP sequence).
class ItchFileSource : public FeedSource {
 public:
  explicit ItchFileSource(const std::string& path);
  ~ItchFileSource() override;

  ItchFileSource(const ItchFileSource&) = delete;
  ItchFileSource& operator=(const ItchFileSource&) = delete;

  bool next_frame(RawFrame& out) override;
  const char* name() const noexcept override { return "ITCH 5.0 file (length-framed, mmap)"; }

  bool ok() const noexcept { return data_ != nullptr; }
  std::size_t file_size() const noexcept { return size_; }
  std::uint64_t frames_read() const noexcept { return seq_; }

 private:
  int            fd_ = -1;
  const std::byte* data_ = nullptr;
  std::size_t    size_ = 0;
  std::size_t    pos_ = 0;
  std::uint64_t  seq_ = 0;
};

}  // namespace fh
