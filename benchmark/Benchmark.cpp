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
std::vector<std::function<void(BenchmarkRecords*)>>&
    BenchmarkRegister::getRegister(){
  static std::vector<std::function<void(BenchmarkRecords*)>>
    registeredBenchmarks(0);
  return registeredBenchmarks;
}

// ____________________________________________________________________________
const std::vector<std::function<void(BenchmarkRecords*)>>& BenchmarkRegister::getRegisteredBenchmarks() {
  return getRegister();
}

// ____________________________________________________________________________
BenchmarkRegister::BenchmarkRegister(const std::vector<std::function<void(BenchmarkRecords*)>>& benchmarks) {
  auto& registeredBenchmarks = getRegister();
  // Append all the benchmarks to the internal register.
  registeredBenchmarks.insert(registeredBenchmarks.end(), benchmarks.begin(), benchmarks.end());
}

// ____________________________________________________________________________
const std::vector<BenchmarkRecords::RecordEntry>&
   BenchmarkRecords::getSingleMeasurements() const{
  return singleMeasurements_;
}

// ____________________________________________________________________________
void BenchmarkRecords::addGroup(const std::string& descriptor) {
  // Is there already a group with this descriptor? If so, that is not allowed.
  AD_CHECK(!recordGroups_.contains(descriptor));

  // There is no group, so create one without any entries and add them to
  // the hash map.
  recordGroups_[descriptor] = BenchmarkRecords::RecordGroup{descriptor, {}};
}

// ____________________________________________________________________________
const ad_utility::HashMap<std::string, BenchmarkRecords::RecordGroup>&
   BenchmarkRecords::getGroups() const {
  return recordGroups_;
}

// ____________________________________________________________________________
void BenchmarkRecords::addTable(const std::string& descriptor,
    const std::vector<std::string>& rowNames,
    const std::vector<std::string>& columnNames) {
  // Is there already a table with this descriptor? If so, that is not allowed.
  AD_CHECK(!recordTables_.contains(descriptor));

  // Add a new entry.
  recordTables_[descriptor] = BenchmarkRecords::RecordTable(descriptor,
    rowNames, columnNames);
}

// ____________________________________________________________________________
const ad_utility::HashMap<std::string, BenchmarkRecords::RecordTable>&
   BenchmarkRecords::getTables() const {
  return recordTables_;
}

