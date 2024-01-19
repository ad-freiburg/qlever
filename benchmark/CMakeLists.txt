# Re-compiling all the infrastructure all the time is a waste of time, so we
# pre-compile that part as a library.

# This part compiles 'just' the benchmark infrastructure and doesn't provide a main function.
add_library(benchmark infrastructure/Benchmark.cpp infrastructure/BenchmarkMeasurementContainer.cpp
    infrastructure/BenchmarkToJson.cpp infrastructure/BenchmarkToString.cpp)
qlever_target_link_libraries(benchmark Boost::program_options absl::str_format util configManager)

# This provides the library of the benchmark infrastructure together with its default main function.
add_library(benchmarkWithMain infrastructure/BenchmarkMain.cpp)
qlever_target_link_libraries(benchmarkWithMain benchmark)

# Link binary ${basename} against the benchmark library,
# and all libraries that are specified as additional arguments.
function (linkBenchmark basename)
    qlever_target_link_libraries(${basename} benchmarkWithMain ${ARGN})
endfunction()

# Compile the benchmark and link it.
# Usage: addAndLinkBenchmark(baseName, [additionalLibraries...])
#   - baseName: The name of the benchmark file without the .cpp ending.
function(addAndLinkBenchmark baseName)
    add_executable(${baseName}  "${baseName}.cpp")
    linkBenchmark(${baseName} ${ARGN})
endfunction()

# Add benchmarks after here.
addAndLinkBenchmark(BenchmarkExamples)

addAndLinkBenchmark(IdTableCompressedWriterBenchmark engine testUtil)

addAndLinkBenchmark(ParallelMergeBenchmark)
