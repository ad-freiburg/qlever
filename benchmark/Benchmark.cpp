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
    registerdBenchmarks(0);
  return registerdBenchmarks;
}

// ____________________________________________________________________________
const std::vector<std::function<void(BenchmarkRecords*)>>& BenchmarkRegister::getRegisterdBenchmarks() {
  return getRegister();
}

// ____________________________________________________________________________
BenchmarkRegister::BenchmarkRegister(const std::vector<std::function<void(BenchmarkRecords*)>>& benchmarks) {
  auto& registerdBenchmarks = getRegister();
  // Append all the benchmarks to the internal register.
  registerdBenchmarks.insert(registerdBenchmarks.end(), benchmarks.begin(), benchmarks.end());
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
void BenchmarkRecords::addSingleMeasurment(const std::string& descriptor,
      const std::function<void()>& functionToMeasure) {
    singleMeasurements_.push_back(RecordEntry{descriptor,
        measureTimeOfFunction(functionToMeasure)});
}

// ____________________________________________________________________________
const std::vector<BenchmarkRecords::RecordEntry>&
   BenchmarkRecords::getSingleMeasurments() const{
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
void BenchmarkRecords::addToExistingGroup(const std::string& groupDescriptor,
    const std::string& descriptor,
    const std::function<void()>& functionToMeasure) {
  // Does the group exist?
  auto groupEntry = recordGroups_.find(groupDescriptor);
  AD_CHECK(groupEntry != recordGroups_.end());

  // Add the descriptor and measured time to the group.
  groupEntry->second.entries.push_back(
      RecordEntry{descriptor, measureTimeOfFunction(functionToMeasure)});
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
  recordTables_[descriptor] = BenchmarkRecords::RecordTable{descriptor,
    rowNames, columnNames, {}};

  // We already know the size of the table, so lets resize the vectors.
  // The std::optional are all empty.
  recordTables_[descriptor].entries.resize(rowNames.size(),
      std::vector<std::optional<float>>(rowNames.size()));
}

// ____________________________________________________________________________
void BenchmarkRecords::addToExistingTable(const std::string& tableDescriptor,
        const size_t row, const size_t column,
        const std::function<void()>& functionToMeasure){
  // Does the table exist?
  auto tableEntry = recordTables_.find(tableDescriptor);
  AD_CHECK(tableEntry != recordTables_.end());

  // For easier usage.
  BenchmarkRecords::RecordTable& table = tableEntry->second;

  // Are the given row and column number inside the table range?
  // size_t is unsigned, so we only need to check, that they are not to big.
  AD_CHECK(row < table.rowNames.size() && column < table.columnNames.size());
  
  // Add the measured time to the table.
  table.entries[row][column] = measureTimeOfFunction(functionToMeasure);
}

// ____________________________________________________________________________
const ad_utility::HashMap<std::string, BenchmarkRecords::RecordTable>&
   BenchmarkRecords::getTables() const {
  return recordTables_;
}

