// glibc hides BSD/POSIX symbols (madvise, MADV_SEQUENTIAL, mmap, ...) under strict -std=c++20
// unless a feature-test macro is set. This must precede every standard/system header so it is
// seen before <features.h> is first pulled in. (No effect on Apple libc, which exposes them
// regardless — which is why the omission only bites on Linux/GCC.)
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE 1
#endif

#include "feedhandler/itch_file_source.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "feedhandler/detail/byte_read.hpp"

namespace fh {

ItchFileSource::ItchFileSource(const std::string& path) {
  fd_ = ::open(path.c_str(), O_RDONLY);
  if (fd_ < 0) return;

  struct stat st {};
  if (::fstat(fd_, &st) != 0 || st.st_size <= 0) {
    ::close(fd_);
    fd_ = -1;
    return;
  }
  size_ = static_cast<std::size_t>(st.st_size);

  void* p = ::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
  if (p == MAP_FAILED) {
    ::close(fd_);
    fd_ = -1;
    size_ = 0;
    return;
  }
  // Hint sequential access so the kernel reads ahead aggressively for the replay.
  ::madvise(p, size_, MADV_SEQUENTIAL);
  data_ = static_cast<const std::byte*>(p);
}

ItchFileSource::~ItchFileSource() {
  if (data_ != nullptr) ::munmap(const_cast<std::byte*>(data_), size_);
  if (fd_ >= 0) ::close(fd_);
}

bool ItchFileSource::next_frame(RawFrame& out) {
  if (data_ == nullptr) return false;
  if (pos_ + 2 > size_) return false;  // no room for a length prefix

  const std::uint16_t len = detail::load_be16(data_ + pos_);
  const std::size_t payload = pos_ + 2;
  if (payload + len > size_) return false;  // truncated final frame

  out.data = data_ + payload;
  out.len = len;
  out.sequence = ++seq_;
  pos_ = payload + len;
  return true;
}

}  // namespace fh
