// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <vector>
#include <functional>
#include <string>
#include <algorithm>
#include <iterator>

#include "../benchmark/Benchmark.h"
#include "util/Timer.h"
#include <bits/ranges_algo.h>
#include <util/HashMap.h>
#include <util/Exception.h>
#include "../benchmark/util/HashMapWithInsertionOrder.h"

// ____________________________________________________________________________
auto BenchmarkRegister::getRegister() -> std::vector<BenchmarkPointer>& {
    static std::vector<BenchmarkRegister::BenchmarkPointer>
    registeredBenchmarkClassInstances;
    return registeredBenchmarkClassInstances;
}

// ____________________________________________________________________________
void BenchmarkRegister::passConfigurationToAllRegisteredBenchmarks(
    const BenchmarkConfiguration& config){
    for(BenchmarkRegister::BenchmarkPointer& instance:
    BenchmarkRegister::getRegister()){
        instance->parseConfiguration(config);
    }
}

// ____________________________________________________________________________
const std::vector<BenchmarkRecords>
BenchmarkRegister::runAllRegisteredBenchmarks(){
    // Access to the static memory of all registerd benchmark class instances.
    auto& registeredBenchmarkClassInstances = BenchmarkRegister::getRegister();
        
    // For measuring and saving the times. We have one entry for every
    // registered benchmark class instance.
    std::vector<BenchmarkRecords> records(
        registeredBenchmarkClassInstances.size());

    // Go through all registered benchmarks and measure them.
    for (size_t i = 0; i < registeredBenchmarkClassInstances.size(); i++) {
      registeredBenchmarkClassInstances.at(i)->runAllBenchmarks(&(records.at(i)));
    }

    return records;
}

// ____________________________________________________________________________
BenchmarkRegister::BenchmarkRegister(
    const std::vector<BenchmarkClassInterface*>& benchmarkClassInstances){
    auto& registeredBenchmarkClassInstances = BenchmarkRegister::getRegister();
    // Append all the benchmarks to the internal register.
    registeredBenchmarkClassInstances.reserve(
        registeredBenchmarkClassInstances.size() +
        benchmarkClassInstances.size());
    std::ranges::transform(benchmarkClassInstances,
        std::back_inserter(registeredBenchmarkClassInstances),
        [](auto& instance){
            return BenchmarkRegister::BenchmarkPointer{instance};
        }, {});
}

// ____________________________________________________________________________
auto BenchmarkRecords::getSingleMeasurements() const
    -> const std::vector<RecordEntry> {
  return singleMeasurements_.getAllValues();
}

// ____________________________________________________________________________
void BenchmarkRecords::addGroup(const std::string& descriptor) {
  recordGroups_.addEntry(std::string(descriptor),
      BenchmarkRecords::RecordGroup{descriptor, {}, {}});
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
  recordTables_.addEntry(std::string(descriptor),
      BenchmarkRecords::RecordTable(descriptor, rowNames, columnNames));
}

// ____________________________________________________________________________
auto BenchmarkRecords::getTables() const -> const std::vector<RecordTable> {
  return recordTables_.getAllValues();
}

/*
 * @brief Returns the metadata object of an object held by one
 *  of the HashMapWithInsertionOrder in the BenchmarkRecords class.
 *  A local helper function to minimize code duplication.
 *
 * @tparam Value What kind of object does the HashMapWithInsertionOrder hold?
 *
 * @param hMap Where to look for the object with the metadata object.
 * @param key The identifier for the hMap.
 */
template<typename Value>
static BenchmarkMetadata& getReferenceToMetadataOfObjectInHashMapWithInsertionOrder(
    HashMapWithInsertionOrder<std::string, Value>& hMap,
    const std::string& key){
  return hMap.getReferenceToValue(key).metadata_;
}

// ____________________________________________________________________________
BenchmarkMetadata& BenchmarkRecords::getReferenceToMetadataOfSingleMeasurment(
        const std::string& descriptor) {
  return getReferenceToMetadataOfObjectInHashMapWithInsertionOrder(
      singleMeasurements_, descriptor);
}

// ____________________________________________________________________________
BenchmarkMetadata& BenchmarkRecords::getReferenceToMetadataOfGroup(
        const std::string& descriptor) {
  return getReferenceToMetadataOfObjectInHashMapWithInsertionOrder(
      recordGroups_, descriptor);
}

// ____________________________________________________________________________
BenchmarkMetadata& BenchmarkRecords::getReferenceToMetadataOfGroupMember(
        const std::string& groupDescriptor,
        const std::string& groupMemberDescriptor){
  return getReferenceToMetadataOfObjectInHashMapWithInsertionOrder(
      recordGroups_.getReferenceToValue(groupDescriptor).entries_,
      groupMemberDescriptor);
}

// ____________________________________________________________________________
BenchmarkMetadata& BenchmarkRecords::getReferenceToMetadataOfTable(
        const std::string& descriptor) {
  return getReferenceToMetadataOfObjectInHashMapWithInsertionOrder(
      recordTables_, descriptor);
}
