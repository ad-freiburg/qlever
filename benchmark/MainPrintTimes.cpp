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
 * @brief Goes through the registerd single measurment benchmarks, measures their time and
 * prints their measured time.
 */
int main() {
  // The descriptors and measured times of all the single measurment benchmarks.
  const std::vector<BenchmarkRecords::RecordEntry> records = measureTimeForAllSingleMeasurments(); 
 
  // Visualizes the measured times.
  std::stringstream visualization;
  visualization << "##############\n# Benchmarks #\n##############\n";
  for (const BenchmarkRecords::RecordEntry& entry: records) {
    visualization << "\nBenchmark '" << entry.descriptor << "' took " << entry.measuredTime << " seconds.";
  }

  std::cout << visualization.str() << "\n";
};
