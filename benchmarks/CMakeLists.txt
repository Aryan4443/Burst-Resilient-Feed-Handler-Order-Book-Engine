# Stress harness + benchmark reporter, and single-thread microbenchmarks.
add_executable(bench_harness bench_harness.cpp)
target_link_libraries(bench_harness PRIVATE feedhandler fh_warnings)

add_executable(microbench microbench.cpp)
target_link_libraries(microbench PRIVATE feedhandler fh_warnings)
