// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <iostream>
#include <sstream>
#include <algorithm>

#include "../benchmark/Benchmark.h"

int main() {
  BenchmarkRecords benchmarksTime;
  
  for (const auto& benchmarkFunction: BenchmarkRegister::getRegisterdBenchmarks()) {
    benchmarkFunction(&benchmarksTime);
  }
  
  std::stringstream visualization;
  visualization << "##############\n# Benchmarks #\n##############\n";
  for (const BenchmarkRecords::RecordEntry& entry: benchmarksTime.getRecords()) {
    visualization << "\nBenchmark '" << entry.descriptor << "' took " << entry.measuredTime << " seconds.";
  }

  std::cout << visualization.str() << "\n";
};
