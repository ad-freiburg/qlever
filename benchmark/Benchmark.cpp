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
#include "../benchmark/util/HashMapWithInsertionOrder.h"

// ____________________________________________________________________________
auto BenchmarkRegister::getRegister() -> std::vector<BenchmarkFunction>& {
  static std::vector<BenchmarkRegister::BenchmarkFunction>
    registeredBenchmarks;
  return registeredBenchmarks;
}

// ____________________________________________________________________________
auto BenchmarkRegister::getRegisteredBenchmarks() ->
    const std::vector<BenchmarkFunction>& {
  return getRegister();
}

// ____________________________________________________________________________
BenchmarkRegister::BenchmarkRegister(
    const std::vector<BenchmarkRegister::BenchmarkFunction>& benchmarks) {
  auto& registeredBenchmarks = getRegister();
  // Append all the benchmarks to the internal register.
  registeredBenchmarks.insert(registeredBenchmarks.end(), benchmarks.begin(),
      benchmarks.end());
}

// ____________________________________________________________________________
auto BenchmarkRecords::getSingleMeasurements() const
    -> const std::vector<RecordEntry>& {
  return singleMeasurements_;
}

// ____________________________________________________________________________
void BenchmarkRecords::addGroup(const std::string& descriptor) {
  recordGroups_.addEntry(descriptor, BenchmarkRecords::RecordGroup{descriptor,
      {}});
}

// ____________________________________________________________________________
auto BenchmarkRecords::getGroups() const
    -> const std::vector<BenchmarkRecords::RecordGroup> {
  return recordGroups_.getAllValues();
}

// ____________________________________________________________________________
void BenchmarkRecords::addTable(const std::string& descriptor,
    const std::vector<std::string>& rowNames,
    const std::vector<std::string>& columnNames) {
  recordTables_.addEntry(descriptor,
      BenchmarkRecords::RecordTable(descriptor, rowNames, columnNames));
}

// ____________________________________________________________________________
auto BenchmarkRecords::getTables() const -> const std::vector<RecordTable> {
  return recordTables_.getAllValues();
}

