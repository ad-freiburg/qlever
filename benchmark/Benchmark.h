// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <vector>
#include <functional>
#include <string>
#include <variant>
#include <memory>

#include "BenchmarkConfiguration.h"
#include "util/json.h"
#include "util/Timer.h"
#include "util/HashMap.h"
#include "util/Exception.h"
#include "../benchmark/util/HashMapWithInsertionOrder.h"
#include "../benchmark/BenchmarkMetadata.h"

/*
 * Used for measuring the time needed for the execution of a function and
 * organizing those measured times.
 */
class BenchmarkRecords {

  public:

    // Describes a measured function.
    struct RecordEntry {
      std::string descriptor_; // Needed, because without it, nobody could tell,
                              // which time belongs to which benchmark.
      float measuredTime_; // The measured time in seconds.
      BenchmarkMetadata metadata_;

      // JSON (d)serialization for all the created structures in BenchmarkRecords.
      NLOHMANN_DEFINE_TYPE_INTRUSIVE(BenchmarkRecords::RecordEntry, descriptor_, measuredTime_, metadata_)
    };

    // Describes a group of measured functions.
    struct RecordGroup {
      std::string descriptor_; // Needed for identifying groups.
      // When accessing members of the group, we need an identifier.
      HashMapWithInsertionOrder<std::string, RecordEntry> entries_;
      BenchmarkMetadata metadata_;

      // JSON (d)serialization for all the created structures in BenchmarkRecords.
      NLOHMANN_DEFINE_TYPE_INTRUSIVE(BenchmarkRecords::RecordGroup, descriptor_, entries_, metadata_)
    };
    
    // Describes a table of measured functions.
    struct RecordTable {
      // For identification.
      std::string descriptor_;
      // The names of the columns and rows.
      std::vector<std::string> rowNames_;
      std::vector<std::string> columnNames_;
      // The entries in the table. Access is [row, column]. Can be the time in
      // seconds, a string, or nothing.
      using EntryType = std::variant<std::monostate, float, std::string>;
      std::vector<std::vector<EntryType>> entries_;
      BenchmarkMetadata metadata_;

      RecordTable(const std::string& pDescriptor, 
          const std::vector<std::string>& pRowNames,
          const std::vector<std::string>& pColumnNames):
          descriptor_{pDescriptor}, rowNames_{pRowNames},
          columnNames_{pColumnNames},
          entries_(pRowNames.size(),
              std::vector<EntryType>(pColumnNames.size())) {}

      // JSON (d)serialization for all the created structures in BenchmarkRecords.
      NLOHMANN_DEFINE_TYPE_INTRUSIVE(BenchmarkRecords::RecordTable, descriptor_, rowNames_, columnNames_, entries_, metadata_)
    };

  private:

    // A hash map of all the created single measurements. For faster access.
    // The key for a record entry is it's descriptor.
    HashMapWithInsertionOrder<std::string, RecordEntry> singleMeasurements_;

    // A hash map of all the created RecordGroups. For faster access.
    // The key for a RecordGroup is it's descriptor.
    HashMapWithInsertionOrder<std::string, RecordGroup> recordGroups_;
  
    // A hash map of all the created RecordTables. For faster access.
    // The key for a RecordTable is it's descriptor.
    HashMapWithInsertionOrder<std::string, RecordTable> recordTables_;

    /*
     * @brief Return execution time of function in seconds.
     *
     * @tparam Function Best left to type inference.
     *
     * @param functionToMeasure Must be a function, or callable.
     */
    template<typename Function>
      requires std::invocable<Function>
    float measureTimeOfFunction(const Function& functionToMeasure) const{
      ad_utility::timer::Timer
      benchmarkTimer(ad_utility::timer::Timer::Started);
      functionToMeasure();
      benchmarkTimer.stop();

      return ad_utility::timer::Timer::toSeconds(benchmarkTimer.value());
    }

    /*
     * @brief Returns a reference to an entry in a recordTable of the hash map
     *  recordTables_. Strictly a helper function.
     */
    auto getHashMapTableEntry(const std::string& tableDescriptor,
    const size_t row, const size_t column) -> RecordTable::EntryType&;

  public:

    /*
     * @brief Measures the time needed for the execution of the given function and
     * saves it, together with a description, as a single measurement.
     *
     * @tparam Function Best left to type inference.
     *
     * @param descriptor A description, of what kind of benchmark case is
     *  getting measured. Needed, because otherwise nobody would be able to
     *  tell, which time corresponds to which benchmark.
     * @param functionToMeasure The function, that represents the benchmark.
     *  Most of the time a lambda, that calls the actual function to benchmark
     *  with the needed parameters.
     */
    template<typename Function>
      requires std::invocable<Function>
    void addSingleMeasurement(const std::string& descriptor,
        const Function& functionToMeasure) {
      singleMeasurements_.addEntry(std::string(descriptor),
          RecordEntry{descriptor, measureTimeOfFunction(functionToMeasure), {}});
    }


    // Returns a const view of all single recorded times.
    const std::vector<RecordEntry> getSingleMeasurements() const;

    /*
     * @brief Creates an empty group, which can
     *  be accesed/identified using the descriptor.
     */
    void addGroup(const std::string& descriptor);

    /*
     * @brief Measures the time needed for the execution of the given function and
     * saves it, together with a description, as an item in a group.
     *
     * @tparam Function Best left to type inference.
     *
     * @param groupDescriptor The identification of the group.
     * @param descriptor A description, of what kind of benchmark case is
     *  getting measured. Needed, because otherwise nobody would be able to
     *  tell, which time corresponds to which benchmark.
     * @param functionToMeasure The function, that represents the benchmark.
     *  Most of the time a lambda, that calls the actual function to benchmark
     *  with the needed parameters.
     */
    template<typename Function>
      requires std::invocable<Function>
    void addToExistingGroup(const std::string& groupDescriptor,
        const std::string& descriptor,
        const Function& functionToMeasure) {
      // Adding more details to a possible exception.
      try {
        // Get the entry of the hash map and add to it.
        auto& groupEntry = recordGroups_.getValue(groupDescriptor);
        groupEntry.entries_.addEntry(std::string(descriptor),
            RecordEntry{descriptor, measureTimeOfFunction(functionToMeasure), {}});
      } catch(KeyIsntRegisteredException const&) {
        // The exception INSIDE the object, do not know, what they object is
        // called, but that information is helpful for the exception. So we
        // do it here.
        throw KeyIsntRegisteredException(groupDescriptor, "recordGroups_");
      }
    }

    /*
     * @brief Returns a vector of all the groups.
     */
    const std::vector<BenchmarkRecords::RecordGroup> getGroups() const;

    /*
     * @brief Creates an empty table, which can be accesed/identified using the
     *  descriptor.
     *
     * @param descriptor The name/identifier of the table.
     * @param rowNames,columnNames The names for the rows/columns.
     */
    void addTable(const std::string& descriptor,
        const std::vector<std::string>& rowNames,
        const std::vector<std::string>& columnNames);

    /*
     * @brief Measures the time needed for the execution of the given function and
     * saves it as an entry in the table.
     *
     * @tparam Function Best left to type inference.
     *
     * @param tableDescriptor The identification of the table.
     * @param row, column Where in the tables to write the measured time.
     *  Starts with (0,0).
     * @param functionToMeasure The function, that represents the benchmark.
     *  Most of the time a lambda, that calls the actual function to benchmark
     *  with the needed parameters.
     */
    template<typename Function>
      requires std::invocable<Function>
    void addToExistingTable(const std::string& tableDescriptor,
        const size_t row, const size_t column,
        const Function& functionToMeasure) {
      // Add the measured time to the table.
      RecordTable::EntryType& entry =
        getHashMapTableEntry(tableDescriptor, row, column);
      entry = measureTimeOfFunction(functionToMeasure);
    }

    /*
     * @brief Manually set the entry of an existing table.
     *
     * @tparam T Must be either float, or string.
     *
     * @param tableDescriptor The identification of the table.
     * @param row, column Where in the tables to write the measured time.
     *  Starts with (0,0).
     * @param newEntryContent What to set the entry to.
     */
    template<typename T>
      requires std::is_same<T, float>::value ||
      std::is_same<T, std::string>::value
    void setEntryOfExistingTable(const std::string& tableDescriptor,
        const size_t row, const size_t column,
        const T& newEntryContent) {
      RecordTable::EntryType& entry =
        getHashMapTableEntry(tableDescriptor, row, column);
      entry = newEntryContent;
    }

    /*
     * @brief Returns the content of a table entry, if the entry holds a value.
     *
     * @tparam T What type the entry has. Must be either float, or string. If
     *  you give the wrong one, or the entry was never set/added, then this
     *  function will cause an exception.
     *
     * @param tableDescriptor The identification of the table.
     * @param row, column Where in the tables to write the measured time.
     *  Starts with (0,0).
     */
    template<typename T>
      requires std::is_same<T, float>::value ||
      std::is_same<T, std::string>::value
    const T getEntryOfExistingTable(const std::string& tableDescriptor,
        const size_t row, const size_t column){
      RecordTable::EntryType& entry =
        getHashMapTableEntry(tableDescriptor, row, column);

      // There is a chance, that the entry of the table does NOT have type T,
      // in which case this will cause an error. As this is a mistake on the
      // side of the user, we don't really care.
      return std::get<T>(entry);
    }

    /*
     * @brief Returns a vector of all the tables.
     */
    const std::vector<RecordTable> getTables() const;

    // For access to the metadata of measurements. So that people can set them,
    // add to them, whatever.
    BenchmarkMetadata& getMetadataOfSingleMeasurment(
        const std::string& descriptor);
    BenchmarkMetadata& getMetadataOfGroup(
        const std::string& descriptor);
    BenchmarkMetadata& getMetadataOfGroupMember(
        const std::string& groupDescriptor,
        const std::string& groupMemberDescriptor);
    BenchmarkMetadata& getMetadataOfTable(
        const std::string& descriptor);
};

/*
The interface for benchmark classes. More specifically, it's the interface
between a collection of benchmarks of any type (single, group, table) and
the processing/management of those benchmarks.
*/
class BenchmarkClassInterface{
  public:

  // Used to transport values, that you want to set at runtime.
  virtual void parseConfiguration(
    [[maybe_unused]] const BenchmarkConfiguration& config){
    // Default behaviour.
    return;
  };

  /*
  For the general metadata of a class. Mostly information, that is the same
  for every benchmark, so that every entry of the `BenchmarkRecords` doesn't
  repeat the same thing over and over again.
  For example: Let's say, you are measuring the same benchmarks for different
  versions of an algorithm. You could add the metadata information, which
  version it is, to every `RecordGroup`, `RecordTable`, etc., but that is a bit
  clunky. Instead, you make one `BenchmarkClassInterface` instance for every
  version and simply return which version you are using as metadata through
  `getMetadata`.
  */
  virtual const BenchmarkMetadata getMetadata() const{
    // Default behaviour.
    return BenchmarkMetadata{};
  }

  /*
  Run all your benchmarks. The `BenchmarkRecords` class is a management
  class for measuering the execution time of functions and organizing the
  results.
  */
  virtual BenchmarkRecords runAllBenchmarks() = 0; 
  
  // Without this, we get memory problems.
  virtual ~BenchmarkClassInterface() = default;
};

/*
Used to register your benchmark classes, so that the benchmarking system can
access and use them.
*/
class BenchmarkRegister {

  // Alias for a type, so that we don't repeat things so often.
  using BenchmarkPointer = std::unique_ptr<BenchmarkClassInterface>;

  /*
   * @brief Returns a reference to the static vector of all registered
   *  benchmark classe instances.
   */
  static std::vector<BenchmarkPointer>& getRegister();

  public:

  /*
   * @brief Register one benchmark class, by creating a global
   *  instance of this class and passing the instances of your class, that
   *  implemented the `BenchmarkClassInterface`. Shouldn't take up much space
   *  and I couldn't find a better way of doing it.
   *
   * @param benchmarkClasseInstance The memory managment of the passed
   *  instances will be taken over by `BenchmarkRegister`.
   */
  BenchmarkRegister(BenchmarkPointer&& benchmarkClasseInstance);

  /*
  @brief Passes the `BenchmarkConfiguration` to the `parseConfiguration`
   function of all the registered instances of benchmark classes.
  */
  static void passConfigurationToAllRegisteredBenchmarks(
    const BenchmarkConfiguration& config = BenchmarkConfiguration{});

  /*
   * @brief Measures all the registered benchmarks and returns the resulting
   *  BenchmarkRecords objects.
   *
   * @return Every benchmark class get's measured with their own
   *  `BenchmarkRecords`. They should be in the same order as the
   *  registrations.
   */
  static const std::vector<BenchmarkRecords> runAllRegisteredBenchmarks();

  /*
   * @brief Returns the general metadata of all the registered benchmarks. As
   *  in, it collects and return the outputs of all those `getMetadata`
   *  functions from the interface.
   *
   * @return They should be in the same order as the registrations.
   */
  static const std::vector<BenchmarkMetadata> getAllGeneralMetadata();
};
