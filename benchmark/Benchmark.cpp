// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <vector>
#include <functional>
#include <string>
#include <algorithm>
#include <iterator>
#include <utility>

#include "../benchmark/Benchmark.h"
#include "BenchmarkMetadata.h"
#include "util/Timer.h"
#include "util/HashMap.h"
#include "util/Exception.h"
#include "../benchmark/util/HashMapWithInsertionOrder.h"
#include "util/Algorithm.h"

auto BenchmarkRecords::getHashMapTableEntry(const std::string& tableDescriptor,
    const size_t row, const size_t column) -> RecordTable::EntryType&{
  // Adding more details to a possible exception.
  try {
    // Get the entry of the hash map.
    auto& table = recordTables_.getValue(tableDescriptor);

    // Are the given row and column number inside the table range?
    // size_t is unsigned, so we only need to check, that they are not to big.
    AD_CONTRACT_CHECK(row < table.rowNames_.size() &&
        column < table.columnNames_.size());
    
    // Return  the reference to the table entry.
    return table.entries_[row][column];
  } catch(KeyIsntRegisteredException const&) {
    // The exception INSIDE the object, do not know, what they object is
    // called, but that information is helpful for the exception. So we
    // do it here.
    throw KeyIsntRegisteredException(tableDescriptor, "recordTables_");
  }
}

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
static BenchmarkMetadata& getMetadataOfObjectInHashMapWithInsertionOrder(
    HashMapWithInsertionOrder<std::string, Value>& hMap,
    const std::string& key){
  return hMap.getValue(key).metadata_;
}

// ____________________________________________________________________________
BenchmarkMetadata& BenchmarkRecords::getMetadataOfSingleMeasurment(
        const std::string& descriptor) {
  return getMetadataOfObjectInHashMapWithInsertionOrder(
      singleMeasurements_, descriptor);
}

// ____________________________________________________________________________
BenchmarkMetadata& BenchmarkRecords::getMetadataOfGroup(
        const std::string& descriptor) {
  return getMetadataOfObjectInHashMapWithInsertionOrder(
      recordGroups_, descriptor);
}

// ____________________________________________________________________________
BenchmarkMetadata& BenchmarkRecords::getMetadataOfGroupMember(
        const std::string& groupDescriptor,
        const std::string& groupMemberDescriptor){
  return getMetadataOfObjectInHashMapWithInsertionOrder(
      recordGroups_.getValue(groupDescriptor).entries_,
      groupMemberDescriptor);
}

// ____________________________________________________________________________
BenchmarkMetadata& BenchmarkRecords::getMetadataOfTable(
        const std::string& descriptor) {
  return getMetadataOfObjectInHashMapWithInsertionOrder(
      recordTables_, descriptor);
}
