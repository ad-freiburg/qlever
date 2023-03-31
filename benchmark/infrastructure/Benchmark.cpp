// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <memory>
#include <vector>
#include <functional>
#include <string>
#include <algorithm>
#include <iterator>
#include <utility>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "util/Timer.h"
#include "util/HashMap.h"
#include "util/Exception.h"
#include "../benchmark/util/HashMapWithInsertionOrder.h"
#include "util/Algorithm.h"

// ____________________________________________________________________________
auto BenchmarkRegister::getRegister() -> std::vector<BenchmarkPointer>& {
  static std::vector<BenchmarkPointer> registeredBenchmarks;
  return registeredBenchmarks;
}

// ____________________________________________________________________________
void BenchmarkRegister::passConfigurationToAllRegisteredBenchmarks(
  const BenchmarkConfiguration& config){
  for(BenchmarkPointer& instance: getRegister()){
      instance->parseConfiguration(config);
  }
}

// ____________________________________________________________________________
const std::vector<BenchmarkMetadata> BenchmarkRegister::getAllGeneralMetadata(){
  // Go through every registered instance of a benchmark class and collect
  // their general metadata.
  return ad_utility::transform(getRegister(),
  [](const auto& instance){return instance->getMetadata();});
}

// ____________________________________________________________________________
const std::vector<BenchmarkRecords>
BenchmarkRegister::runAllRegisteredBenchmarks(){
  // Go through every registered instance of a benchmark class, measure their
  // benchmarks and return the resulting `BenchmarkRecords` in a new vector.
  return ad_utility::transform(getRegister(), [](BenchmarkPointer& instance){
      return instance->runAllBenchmarks();
  });
}

// ____________________________________________________________________________
BenchmarkRegister::BenchmarkRegister(
  BenchmarkPointer&& benchmarkClassInstance){
  // Append the benchmark to the internal register.
  getRegister().push_back(std::move(benchmarkClassInstance));
}

// ____________________________________________________________________________
auto BenchmarkRecords::getSingleMeasurements() const
-> const std::vector<BenchmarkMeasurementContainer::RecordEntry> {
  return ad_utility::transform(singleMeasurements_,
  [](const auto& pointer)->BenchmarkMeasurementContainer::RecordEntry{
      return (*pointer);
  });
}

// ____________________________________________________________________________
BenchmarkMeasurementContainer::RecordGroup& BenchmarkRecords::addGroup(
    const std::string& descriptor) {
    return addEntryToContainerVector(recordGroups_, descriptor);
}

// ____________________________________________________________________________
auto BenchmarkRecords::getGroups() const
    -> const std::vector<BenchmarkMeasurementContainer::RecordGroup> {
  /*
  std::vector<BenchmarkMeasurementContainer::RecordGroup> toReturn;
  toReturn.reserve(recordGroups_.size());
  std::transform(recordGroups_.begin(), recordGroups_.end(), std::back_inserter(toReturn), [](const auto& pointer){return *pointer;});
  return toReturn;
  */
  return ad_utility::transform(recordGroups_,
  [](const auto& pointer)->BenchmarkMeasurementContainer::RecordGroup{
      return (*pointer);
  });
}

// ____________________________________________________________________________
BenchmarkMeasurementContainer::RecordTable& BenchmarkRecords::addTable(
    const std::string& descriptor,
    const std::vector<std::string>& rowNames,
    const std::vector<std::string>& columnNames) {
    return addEntryToContainerVector(recordTables_, descriptor, rowNames,
    columnNames);
}

// ____________________________________________________________________________
auto BenchmarkRecords::getTables() const ->
const std::vector<BenchmarkMeasurementContainer::RecordTable> {
  return ad_utility::transform(recordTables_,
  [](const auto& pointer)->BenchmarkMeasurementContainer::RecordTable{
      return (*pointer);
  });
}

// ____________________________________________________________________________
BenchmarkRecords::BenchmarkRecords(const BenchmarkRecords& records){
  // Copy the vectors of unique pointer.
  auto copyPointerVector =
    []<typename T>(const PointerVector<T>& sourceVector, auto& targetVector){
    targetVector = ad_utility::transform(sourceVector, [](const auto& pointer){
      return std::make_unique<T>(*pointer);
    });
  };

  copyPointerVector(records.singleMeasurements_, singleMeasurements_);
  copyPointerVector(records.recordGroups_, recordGroups_);
  copyPointerVector(records.recordTables_, recordTables_);
}
