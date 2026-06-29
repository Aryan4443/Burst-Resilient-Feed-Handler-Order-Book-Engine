cmake_minimum_required(VERSION 3.20)
project(feedhandler
        VERSION 0.1.0
        DESCRIPTION "Burst-resilient market-data feed handler and order-book engine"
        LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Standard / global settings
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
  message(STATUS "No build type specified; defaulting to Release")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(FH_BUILD_TESTS "Build unit tests" ON)
option(FH_BUILD_BENCH "Build benchmarks / harness" ON)
option(FH_ASAN       "Build with AddressSanitizer + UBSan" OFF)
option(FH_TSAN       "Build with ThreadSanitizer" OFF)

# Hot-path friendly optimisation defaults for Release.
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

add_library(fh_warnings INTERFACE)
target_compile_options(fh_warnings INTERFACE
  -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion)

add_library(fh_sanitizers INTERFACE)
if(FH_ASAN AND FH_TSAN)
  message(FATAL_ERROR "FH_ASAN and FH_TSAN are mutually exclusive")
endif()
if(FH_ASAN)
  target_compile_options(fh_sanitizers INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(fh_sanitizers INTERFACE -fsanitize=address,undefined)
endif()
if(FH_TSAN)
  target_compile_options(fh_sanitizers INTERFACE -fsanitize=thread -fno-omit-frame-pointer)
  target_link_options(fh_sanitizers INTERFACE -fsanitize=thread)
endif()

# ---------------------------------------------------------------------------
# Core library
# ---------------------------------------------------------------------------
add_library(feedhandler
  src/itch_parser.cpp
  src/order_book.cpp
  src/sequencer.cpp
  src/publisher.cpp
  src/itch_file_source.cpp
  src/pipeline.cpp
)
target_include_directories(feedhandler PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(feedhandler PUBLIC cxx_std_20)
target_link_libraries(feedhandler PUBLIC fh_warnings fh_sanitizers)

find_package(Threads REQUIRED)
target_link_libraries(feedhandler PUBLIC Threads::Threads)

# ---------------------------------------------------------------------------
# Tools / apps
# ---------------------------------------------------------------------------
add_executable(itch_gen tools/itch_gen.cpp)
target_link_libraries(itch_gen PRIVATE feedhandler)

add_executable(feed_handler apps/feed_handler.cpp)
target_link_libraries(feed_handler PRIVATE feedhandler)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(FH_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

# ---------------------------------------------------------------------------
# Benchmarks / stress harness
# ---------------------------------------------------------------------------
if(FH_BUILD_BENCH)
  add_subdirectory(benchmarks)
endif()
