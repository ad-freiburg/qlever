# Link binary ${basename} against the threading library,
# and all libraries that are specified as additional arguments.
function (linkBenchmark basename)
    target_link_libraries(${basename} ${ARGN} ${CMAKE_THREAD_LIBS_INIT})
endfunction()

# Compile the benchmark with MainPrintTimes and link the benchmark, but
# do not run it.
# Usage: addAndLinkBenchmarkWithPrintTimes(basename, [additionalLibraries...])
function(addAndLinkBenchmarkWithPrintTimes basename)
    add_executable(${basename}  Benchmark.cpp "${basename}.cpp" MainPrintTimes.cpp)
    linkBenchmark(${basename} ${ARGN})
endfunction()

# Add benchmarks after here.
addAndLinkBenchmarkWithPrintTimes(SortedJoinBenchmark engine)

addAndLinkBenchmarkWithPrintTimes(UnsortedJoinBenchmark engine)
