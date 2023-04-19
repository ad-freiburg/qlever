// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022,
// schlegea@informatik.uni-freiburg.de)

#include "../benchmark/infrastructure/Benchmark.h"

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Timer.h"

namespace ad_benchmark {
// ____________________________________________________________________________
void BenchmarkRegister::passConfigurationToAllRegisteredBenchmarks(
    const BenchmarkConfiguration& config) {
  for (BenchmarkPointer& instance : registeredBenchmarks) {
    instance->parseConfiguration(config);
  }
}

// ____________________________________________________________________________
std::vector<BenchmarkMetadata> BenchmarkRegister::getAllGeneralMetadata() {
  // Go through every registered instance of a benchmark class and collect
  // their general metadata.
  return ad_utility::transform(registeredBenchmarks, [](const auto& instance) {
    return instance->getMetadata();
  });
}

// ____________________________________________________________________________
std::vector<BenchmarkResults> BenchmarkRegister::runAllRegisteredBenchmarks() {
  // Go through every registered instance of a benchmark class, measure their
  // benchmarks and return the resulting `BenchmarkResults` in a new vector.
  return ad_utility::transform(
      registeredBenchmarks,
      [](BenchmarkPointer& instance) { return instance->runAllBenchmarks(); });
}

// ____________________________________________________________________________
BenchmarkRegister::BenchmarkRegister(
    BenchmarkPointer&& benchmarkClassInstance) {
  // Append the benchmark to the internal register.
  registeredBenchmarks.push_back(std::move(benchmarkClassInstance));
}

// ____________________________________________________________________________
auto BenchmarkResults::getSingleMeasurements() const
    -> std::vector<ResultEntry> {
  return ad_utility::transform(
      singleMeasurements_,
      [](const auto& pointer) -> ResultEntry { return (*pointer); });
}

// ____________________________________________________________________________
ResultGroup& BenchmarkResults::addGroup(const std::string& descriptor) {
  return addEntryToContainerVector(resultGroups_, descriptor);
}

// ____________________________________________________________________________
auto BenchmarkResults::getGroups() const -> std::vector<ResultGroup> {
  return ad_utility::transform(
      resultGroups_,
      [](const auto& pointer) -> ResultGroup { return (*pointer); });
}

// ____________________________________________________________________________
ResultTable& BenchmarkResults::addTable(
    const std::string& descriptor, const std::vector<std::string>& rowNames,
    const std::vector<std::string>& columnNames) {
  return addEntryToContainerVector(resultTables_, descriptor, rowNames,
                                   columnNames);
}

// ____________________________________________________________________________
auto BenchmarkResults::getTables() const -> std::vector<ResultTable> {
  return ad_utility::transform(
      resultTables_,
      [](const auto& pointer) -> ResultTable { return (*pointer); });
}
}  // namespace ad_benchmark
