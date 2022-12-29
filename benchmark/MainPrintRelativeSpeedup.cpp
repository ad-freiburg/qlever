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
 * @brief Goes through the registerd benchmarks, measures their time and
 * compares their times. Then it prints the relativ speedup of every possible
 * benchmark combination, with the exception of a benchmark with itself.
 */
int main() {
  // The descriptors and measured times of all the register benchmarks.
  const std::vector<BenchmarkRecords::RecordEntry> records = measureTimeForAllBenchmarks(); 
 
  std::stringstream visualization;
  visualization << "##############\n# Benchmarks #\n##############\n";

  // Going through all pairs.
  for (const BenchmarkRecords::RecordEntry& entrya: records) {
    for (const BenchmarkRecords::RecordEntry& entryb: records) {
      // If the entries are the same, we skip. I mean, who wants to know the
      // relativ speedup in comparison to youself???
      if (entrya.descriptor == entryb.descriptor) {
        continue;
      }
      
      // The relative speedup in terms of: entrya is ??? times slower/faster
      // than entryb.
      float speedup = entryb.measuredTime / entrya.measuredTime;

      // Constructing the sentence.
      visualization << "\nBenchmark '" << entrya.descriptor << "' is " << speedup << " times ";
      visualization << ( (speedup > 1) ? "faster " : "slower " );
      visualization << "than benchmark '" << entryb.descriptor << "'.";
    }
  }

  std::cout << visualization.str() << "\n\n";
};
