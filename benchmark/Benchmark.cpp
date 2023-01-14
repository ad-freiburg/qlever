// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <vector>
#include <functional>
#include <string>
#include <algorithm>

#include "../benchmark/Benchmark.h"
#include "util/Timer.h"

// ____________________________________________________________________________
std::vector<std::function<void(BenchmarkRecords*)>> BenchmarkRegister::_registerdBenchmarks(0);

// ____________________________________________________________________________
const std::vector<std::function<void(BenchmarkRecords*)>>& BenchmarkRegister::getRegisterdBenchmarks() {
  return _registerdBenchmarks;
}

// ____________________________________________________________________________
BenchmarkRegister::BenchmarkRegister(const std::vector<std::function<void(BenchmarkRecords*)>>& benchmarks) {
  for (const auto& benchmark: benchmarks) {
    _registerdBenchmarks.push_back(benchmark);
  }
}

// ____________________________________________________________________________
void BenchmarkRecords::addSingleMeasurment(std::string descriptor, std::function<void()> functionToMeasure) {
    ad_utility::Timer benchmarkTimer;
     
    benchmarkTimer.start();
    functionToMeasure();
    benchmarkTimer.stop();

    _singleMeasurments.push_back(RecordEntry{descriptor, benchmarkTimer.secs()});
}

// ____________________________________________________________________________
const std::vector<BenchmarkRecords::RecordEntry>&
   BenchmarkRecords::getSingleMeasurments() const{
  return _singleMeasurments;
}
