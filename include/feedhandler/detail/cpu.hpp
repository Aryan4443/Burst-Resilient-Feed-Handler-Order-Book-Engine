#pragma once

#include <new>
#include <thread>

namespace fh::detail {

// Hardware destructive interference size (cache line) for padding atomics apart and avoiding
// false sharing. std::hardware_destructive_interference_size is the standard answer but is
// not provided by all libc++ versions; fall back to 64 (true on x86-64 and Apple arm64).
#if defined(__cpp_lib_hardware_interference_size) && __cpp_lib_hardware_interference_size >= 201603
inline constexpr std::size_t kCacheLine = std::hardware_destructive_interference_size;
#else
inline constexpr std::size_t kCacheLine = 64;
#endif

// A spin-wait hint: tells the core we're in a busy-wait so it can save power / yield the
// pipeline to a sibling hyperthread, without giving up the OS time slice (low latency).
inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(__arm__)
  asm volatile("yield" ::: "memory");
#else
  std::this_thread::yield();
#endif
}

}  // namespace fh::detail
