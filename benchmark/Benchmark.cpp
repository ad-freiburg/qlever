// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <vector>
#include <functional>
#include <string>

#include "benchmark/Benchmark.h"
#include "src/util/Timer.h"

// ____________________________________________________________________________
void BenchmarkRecords::measureTime(std::string descriptor, std::function<void(void)> functionToMeasure) {
    ad_utility::Timer benchmarkTimer;
     
    benchmarkTimer.start();
    functionToMeasure();
    benchmarkTimer.stop();

    _records.push_back(RecordEntry{descriptor, benchmarkTimer.value()});
  }

