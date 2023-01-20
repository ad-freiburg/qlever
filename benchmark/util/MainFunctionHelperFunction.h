// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include "../benchmark/Benchmark.h"

/*
 * @brief Measure the time needed for the execution of every registered
 *  benchmark and return a copy of the used BenchmarkRecords.
 */
const BenchmarkRecords measureTimeForAllBenchmarks() {
  // For measuring and saving the times.
  BenchmarkRecords benchmarksTime;
 
  // Goes through all registered benchmarks and measures them.
  for (const auto& benchmarkFunction: BenchmarkRegister::getRegisteredBenchmarks()) {
    benchmarkFunction(&benchmarksTime);
  }

  return benchmarksTime;
};
