// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <iostream>
#include <sstream>
#include <algorithm>

#include "../benchmark/Benchmark.h"

/*
 * @brief Goes through the registerd benchmarks, measures their time and
 * prints their measured time.
 */
int main() {
  // For measuring and saving the times.
  BenchmarkRecords benchmarksTime;
 
  // Goes through all registerd benchmarks and measures them.
  for (const auto& benchmarkFunction: BenchmarkRegister::getRegisterdBenchmarks()) {
    benchmarkFunction(&benchmarksTime);
  }
 
  // Visualizes the measured times.
  std::stringstream visualization;
  visualization << "##############\n# Benchmarks #\n##############\n";
  for (const BenchmarkRecords::RecordEntry& entry: benchmarksTime.getRecords()) {
    visualization << "\nBenchmark '" << entry.descriptor << "' took " << entry.measuredTime << " seconds.";
  }

  std::cout << visualization.str() << "\n";
};
