# Re-compiling all the infrastructure all the time is a waste of time, so we
# pre-compile that part as a library.
add_library(benchmark infrastructure/Benchmark.cpp infrastructure/BenchmarkMeasurementContainer.cpp infrastructure/BenchmarkToJson.cpp infrastructure/BenchmarkResultToString.cpp infrastructure/BenchmarkConfiguration.cpp infrastructure/BenchmarkMain.cpp)
qlever_target_link_libraries(benchmark boost_program_options absl::str_format)

# Link binary ${basename} against the benchmark library,
# and all libraries that are specified as additional arguments.
function (linkBenchmark basename)
    qlever_target_link_libraries(${basename} benchmark ${ARGN})
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
