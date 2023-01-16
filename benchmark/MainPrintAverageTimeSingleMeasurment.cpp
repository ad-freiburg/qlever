// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>

#include "../benchmark/Benchmark.h"
#include "../benchmark/util/MainFunctionHelperFunction.h"

/*
 * @brief Goes through the registerd single measurments benchmarks
 * N, here 100, times, and prints the average execution time of every
 * benchmark.
 */
int main() {
  const size_t N = 100;

  // The first execution is copied, so that we can easier calculate the
  // average.
  std::vector<BenchmarkRecords::RecordEntry>
    averageExecutionTime(measureTimeForAllSingleMeasurments());

  for (size_t i = 0; i < N - 1; i++) {
    // The descriptors and measured times of all the register benchmarks.
    const std::vector<BenchmarkRecords::RecordEntry> records = measureTimeForAllSingleMeasurments();

    // The items in measureTimeForAllBenchmarks() always have the same order.
    // So we can just go by position.
    for (size_t entryNumber = 0; entryNumber < records.size(); entryNumber++) {
      // Adding all the execution times of a single benchmark up.
      averageExecutionTime[entryNumber].measuredTime +=
        records[entryNumber].measuredTime;
    }
  }

  // Calculating the average.
  for (size_t i = 0; i < averageExecutionTime.size(); i++) {
    averageExecutionTime[i].measuredTime /= N;
  }
 
  std::stringstream visualization;
  visualization << "##############\n# Benchmarks #\n##############\n\n" <<
    "Number of executions per benchmark: " << N;

  for (const BenchmarkRecords::RecordEntry& entry: averageExecutionTime) {
    visualization << "\nBenchmark '" << entry.descriptor <<
      "' has an average execution time of " << entry.measuredTime << " seconds.";
  }

  std::cout << visualization.str() << "\n\n";
};
