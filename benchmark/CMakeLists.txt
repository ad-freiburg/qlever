# Link binary ${basename} against the threading library,
# and all libraries that are specified as additional arguments.
function (linkBenchmark basename)
    target_link_libraries(${basename} ${ARGN} ${CMAKE_THREAD_LIBS_INIT})
endfunction()

# Compile the benchmark with MainPrintTimes and link the benchmark, but
# do not run it.
# Usage: addAndLinkBenchmarkWithPrintTimes(compileName, cppFileName, [additionalLibraries...])
#   - compileName : The name, that the compiled end product should have.
#   - cppFileName : The .cpp file, that contains your benchmark without the file extension.
function(addAndLinkBenchmarkWithPrintTimes compileName cppFileName)
    add_executable(${compileName}  Benchmark.cpp "${cppFileName}.cpp" MainPrintTimes.cpp)
    linkBenchmark(${compileName} ${ARGN})
endfunction()

# Add benchmarks after here.
addAndLinkBenchmarkWithPrintTimes(SortedJoinBenchmark SortedJoinBenchmark engine)

addAndLinkBenchmarkWithPrintTimes(UnsortedJoinBenchmark UnsortedJoinBenchmark engine)
