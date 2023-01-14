// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <vector>
#include <functional>
#include <string>
#include <algorithm>

#include "../benchmark/Benchmark.h"
#include "util/Timer.h"
#include <util/HashMap.h>
#include <util/Exception.h>

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
float BenchmarkRecords::measureTimeOfFunction(
    const std::function<void()>& functionToMeasure) const {
  ad_utility::Timer benchmarkTimer;
     
  benchmarkTimer.start();
  functionToMeasure();
  benchmarkTimer.stop();

  return benchmarkTimer.secs();
}

// ____________________________________________________________________________
void BenchmarkRecords::addSingleMeasurment(const std::string descriptor,
      const std::function<void()>& functionToMeasure) {
    _singleMeasurments.push_back(RecordEntry{descriptor,
        measureTimeOfFunction(functionToMeasure)});
}

// ____________________________________________________________________________
const std::vector<BenchmarkRecords::RecordEntry>&
   BenchmarkRecords::getSingleMeasurments() const{
  return _singleMeasurments;
}

// ____________________________________________________________________________
void BenchmarkRecords::addGroup(const std::string descriptor) {
  // Is there already a group with this descriptor? If so, that is not allowed.
  AD_CHECK(_recordGroups.find(descriptor) == _recordGroups.end());

  // There is no group, so create one without any entries and add them to
  // the hash map.
  _recordGroups[descriptor] = BenchmarkRecords::RecordGroup{descriptor, {}};
}

// ____________________________________________________________________________
void BenchmarkRecords::addToExistingGroup(const std::string& groupDescriptor,
    const std::string descriptor,
    const std::function<void()>& functionToMeasure) {
  // Does the group exist?
  auto groupEntry = _recordGroups.find(groupDescriptor);
  AD_CHECK(groupEntry != _recordGroups.end())

  // Add the descriptor and measured time to the group.
  groupEntry->second.entries.push_back(
      RecordEntry{descriptor, measureTimeOfFunction(functionToMeasure)});
}

// ____________________________________________________________________________
const ad_utility::HashMap<std::string, BenchmarkRecords::RecordGroup>&
   BenchmarkRecords::getGroups() const {
  return _recordGroups;
}
