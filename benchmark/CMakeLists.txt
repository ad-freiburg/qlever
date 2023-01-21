# Link binary ${basename} against the threading library,
# and all libraries that are specified as additional arguments.
function (linkBenchmark basename)
    target_link_libraries(${basename} absl::flat_hash_map ${ARGN})
endfunction()

# Compile the benchmark with MainPrintTimes and link the benchmark, but
# do not run it.
# Usage: addAndLinkBenchmarkWithPrintTimes(compileName, cppFileName, [additionalLibraries...])
#   - compileName : The name, that the compiled end product should have.
#   - cppFileName : The .cpp file, that contains your benchmark without the file extension.
function(addAndLinkBenchmarkWithPrintTimes compileName cppFileName)
    add_executable(${compileName}  Benchmark.cpp ${cppFileName} MainPrintTimes.cpp)
    linkBenchmark(${compileName} ${ARGN})
endfunction()

# Compile the benchmark with MainPrintRelativeSpeedup and link the benchmark, but
# do not run it.
# Usage: addAndLinkBenchmarkWithPrintRelativSpeedup(compileName, cppFileName, [additionalLibraries...])
#   - compileName : The name, that the compiled end product should have.
#   - cppFileName : The .cpp file, that contains your benchmark without the file extension.
function(addAndLinkBenchmarkWithPrintRelativeSpeedup compileName cppFileName)
    add_executable(${compileName}  Benchmark.cpp ${cppFileName} MainPrintRelativeSpeedup.cpp)
    linkBenchmark(${compileName} ${ARGN})
endfunction()

# Compile the benchmark with MainPrintAverageTime and link the benchmark, but
# do not run it.
# Usage: addAndLinkBenchmarkWithPrintAverageTime(compileName, cppFileName, [additionalLibraries...])
#   - compileName : The name, that the compiled end product should have.
#   - cppFileName : The .cpp file, that contains your benchmark without the file extension.
function(addAndLinkBenchmarkWithPrintAverageTime compileName cppFileName)
  add_executable(${compileName}  Benchmark.cpp ${cppFileName} MainPrintAverageTimeSingleMeasurment.cpp)
    linkBenchmark(${compileName} ${ARGN})
endfunction()

# Add benchmarks after here.
addAndLinkBenchmarkWithPrintTimes(SortedJoinBenchmarkPrintTimes SortedJoinBenchmark.cpp engine)

addAndLinkBenchmarkWithPrintTimes(UnsortedJoinBenchmarkPrintTimes UnsortedJoinBenchmark.cpp engine)

addAndLinkBenchmarkWithPrintRelativeSpeedup(SortedJoinBenchmarkPrintSpeedup SortedJoinBenchmark.cpp engine)

addAndLinkBenchmarkWithPrintRelativeSpeedup(UnsortedJoinBenchmarkPrintSpeedup UnsortedJoinBenchmark.cpp engine)

addAndLinkBenchmarkWithPrintAverageTime(SortedJoinBenchmarkPrintAverage SortedJoinBenchmark.cpp engine)

addAndLinkBenchmarkWithPrintAverageTime(UnsortedJoinBenchmarkPrintAverage UnsortedJoinBenchmark.cpp engine)

addAndLinkBenchmarkWithPrintTimes(BenchmarkExamples BenchmarkExamples.cpp)

addAndLinkBenchmarkWithPrintTimes(UnsortedAndSortedJoinBenchmark UnsortedAndSortedJoinBenchmark.cpp engine)
