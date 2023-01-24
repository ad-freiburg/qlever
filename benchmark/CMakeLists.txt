# Link binary ${basename} against the threading library,
# and all libraries that are specified as additional arguments.
function (linkBenchmark basename)
    target_link_libraries(${basename} absl::flat_hash_map ${ARGN})
endfunction()

# Compile the benchmark and link it, but do not run it.
# Usage: addAndLinkBenchmark(baseName, [additionalLibraries...])
#   - baseName: The name of the benchmark file without the .cpp ending.
function(addAndLinkBenchmark baseName)
    add_executable(${baseName}  Benchmark.cpp "${baseName}.cpp" BenchmarkMain.cpp)
    linkBenchmark(${baseName} ${ARGN})
endfunction()

# Add benchmarks after here.
addAndLinkBenchmark(BenchmarkExamples)

addAndLinkBenchmark(UnsortedAndSortedJoinBenchmark engine)
