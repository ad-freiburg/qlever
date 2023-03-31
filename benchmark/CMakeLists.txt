# Re-compiling all the infrastructure all the time is a waste of time, so we
# pre-compile that part as a library.
add_library(benchmark Benchmark.cpp BenchmarkMeasurementContainer.cpp BenchmarkToJson.cpp BenchmarkRecordToString.cpp BenchmarkConfiguration.cpp BenchmarkMain.cpp)
target_link_libraries(benchmark absl::flat_hash_map boost_program_options absl::strings)

# Link binary ${basename} against the benchmark library,
# and all libraries that are specified as additional arguments.
function (linkBenchmark basename)
    target_link_libraries(${basename} benchmark ${ARGN})
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
