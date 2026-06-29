# GoogleTest via FetchContent (downloaded + built as part of the project).
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG        v1.15.2
  GIT_SHALLOW    TRUE
)
# Don't install gtest with our project; keep it self-contained.
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

# Helper: fh_add_test(<name> <source>...) -> builds + registers a ctest target.
function(fh_add_test name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} PRIVATE feedhandler GTest::gtest_main fh_warnings fh_sanitizers)
  gtest_discover_tests(${name})
endfunction()

fh_add_test(test_parser test_parser.cpp)
fh_add_test(test_order_book test_order_book.cpp)
fh_add_test(test_ring_buffer test_ring_buffer.cpp)
fh_add_test(test_sequencer test_sequencer.cpp)
fh_add_test(test_instrumentation test_instrumentation.cpp)
