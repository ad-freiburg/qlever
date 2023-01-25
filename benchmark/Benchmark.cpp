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
  recordGroupsOrder_.push_back(descriptor);
}

// ____________________________________________________________________________
template<typename MAP_KEY_TYPE, typename MAP_VALUE_TYPE>
const std::vector<MAP_VALUE_TYPE>
BenchmarkRecords::createVectorOfHashMapValues(
        const ad_utility::HashMap<MAP_KEY_TYPE, MAP_VALUE_TYPE>& hashMap,
        const std::vector<MAP_KEY_TYPE>& hashMapKeys){
  // The new vector containing the values for the keys in hashMapKeys in the
  // same order.
  std::vector<MAP_VALUE_TYPE> hashMapValues(hashMapKeys.size());

  // Copying the values into hashMapValues.
  std::ranges::for_each(hashMapKeys,
      // We already know the size of hashMapValues and set it, so every call of
      // this lambda has to walk through the vector. Which is what entryNumber
      // is for.
      [&hashMapValues, entryNumber = 0](const MAP_VALUE_TYPE& value)
      mutable{
        hashMapValues[entryNumber++] = value;
      },
      [&hashMap](const MAP_KEY_TYPE& key){return hashMap.at(key);});

  return hashMapValues;
}

// ____________________________________________________________________________
const std::vector<BenchmarkRecords::RecordGroup> BenchmarkRecords::getGroups()
  const {
  return createVectorOfHashMapValues(recordGroups_, recordGroupsOrder_);
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
  recordTablesOrder_.push_back(descriptor);
}

// ____________________________________________________________________________
const std::vector<BenchmarkRecords::RecordTable> BenchmarkRecords::getTables()
  const {
  return createVectorOfHashMapValues(recordTables_, recordTablesOrder_);
}

