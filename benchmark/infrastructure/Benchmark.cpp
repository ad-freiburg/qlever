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
void BenchmarkRegister::passConfigurationToAllRegisteredBenchmarks(
  const BenchmarkConfiguration& config){
  for(BenchmarkPointer& instance: registeredBenchmarks){
      instance->parseConfiguration(config);
  }
}

// ____________________________________________________________________________
std::vector<BenchmarkMetadata> BenchmarkRegister::getAllGeneralMetadata(){
  // Go through every registered instance of a benchmark class and collect
  // their general metadata.
  return ad_utility::transform(registeredBenchmarks,
  [](const auto& instance){return instance->getMetadata();});
}

// ____________________________________________________________________________
std::vector<BenchmarkResults>
BenchmarkRegister::runAllRegisteredBenchmarks(){
  // Go through every registered instance of a benchmark class, measure their
  // benchmarks and return the resulting `BenchmarkResults` in a new vector.
  return ad_utility::transform(registeredBenchmarks, [](BenchmarkPointer& instance){
      return instance->runAllBenchmarks();
  });
}

// ____________________________________________________________________________
BenchmarkRegister::BenchmarkRegister(
  BenchmarkPointer&& benchmarkClassInstance){
  // Append the benchmark to the internal register.
  registeredBenchmarks.push_back(std::move(benchmarkClassInstance));
}

// ____________________________________________________________________________
auto BenchmarkResults::getSingleMeasurements() const
-> std::vector<BenchmarkMeasurementContainer::RecordEntry> {
  return ad_utility::transform(singleMeasurements_,
  [](const auto& pointer)->BenchmarkMeasurementContainer::RecordEntry{
      return (*pointer);
  });
}

// ____________________________________________________________________________
BenchmarkMeasurementContainer::RecordGroup& BenchmarkResults::addGroup(
    const std::string& descriptor) {
    return addEntryToContainerVector(recordGroups_, descriptor);
}

// ____________________________________________________________________________
auto BenchmarkResults::getGroups() const
    -> std::vector<BenchmarkMeasurementContainer::RecordGroup> {
  return ad_utility::transform(recordGroups_,
  [](const auto& pointer)->BenchmarkMeasurementContainer::RecordGroup{
      return (*pointer);
  });
}

// ____________________________________________________________________________
BenchmarkMeasurementContainer::RecordTable& BenchmarkResults::addTable(
    const std::string& descriptor,
    const std::vector<std::string>& rowNames,
    const std::vector<std::string>& columnNames) {
    return addEntryToContainerVector(recordTables_, descriptor, rowNames,
    columnNames);
}

// ____________________________________________________________________________
auto BenchmarkResults::getTables() const ->
std::vector<BenchmarkMeasurementContainer::RecordTable> {
  return ad_utility::transform(recordTables_,
  [](const auto& pointer)->BenchmarkMeasurementContainer::RecordTable{
      return (*pointer);
  });
}
