# Link binary ${basename} against the threading library,
# and all libraries that are specified as additional arguments.
function (linkBenchmark basename)
    target_link_libraries(${basename} ${ARGN} ${CMAKE_THREAD_LIBS_INIT})
endfunction()

# Compile the benchmark with BenchmarkMainPrint and link the benchmark, but
# do not run it.
# Usage: addAndLinkBenchmark(basename, [additionalLibraries...])
function(addAndLinkBenchmarkWithPrint basename)
    add_executable(${basename}  Benchmark.cpp "${basename}.cpp" BenchmarkMainPrint.cpp)
    linkBenchmark(${basename} ${ARGN})
endfunction()

# Add benchmarks after here.
addAndLinkBenchmarkWithPrint(BasicJoinBenchmark engine)
