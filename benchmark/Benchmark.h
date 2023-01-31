// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <vector>
#include <functional>
#include <string>
#include <optional>
#include "../src/util/json.h"

#include "util/Timer.h"
#include <util/HashMap.h>
#include <util/Exception.h>

/*
 * Used for measuring the time needed for the execution of a function. It also
 * saves the measured time togehter with a description of the function
 * measured.
 */
class BenchmarkRecords {

  public:

    // Describes a measured function.
    struct RecordEntry {
      std::string descriptor; // Needed, because without it, nobody could tell,
                              // which time belongs to which benchmark.
      float measuredTime; // The measured time in seconds.
    };

    // Describes a group of measured functions.
    struct RecordGroup {
      std::string descriptor; // Needed for identifying groups.
      std::vector<RecordEntry> entries;
    };
    
    // Describes a table of measured functions.
    struct RecordTable {
      // For identification.
      std::string descriptor;
      // The names of the columns and rows.
      std::vector<std::string> rowNames;
      std::vector<std::string> columnNames;
      // The time measurements in seconds. Access is [row, column].
      std::vector<std::vector<std::optional<float>>> entries;

      // Default constructor. Without it, some of the included headers generate
      // errors.
      RecordTable(): descriptor{""}, rowNames{{}}, columnNames{{}}, entries{{}} {}

      RecordTable(const std::string& pDescriptor, 
          const std::vector<std::string>& pRowNames,
          const std::vector<std::string>& pColumnNames):
          descriptor{pDescriptor}, rowNames{pRowNames},
          columnNames{pColumnNames},
          entries(rowNames.size(), std::vector<std::optional<float>>(rowNames.size())) {}
    };

  private:

    // A vector of all single functions measured.
    std::vector<RecordEntry> singleMeasurements_;

    // A hash map of all the created RecordGroups. For faster access.
    // The key for a RecordGroup is it's descriptor.
    ad_utility::HashMap<std::string, RecordGroup> recordGroups_;
    // The order of creation for the entries of recordGroups_.
    std::vector<std::string> recordGroupsOrder_;
  
    // A hash map of all the created RecordTables. For faster access.
    // The key for a RecordTable is it's descriptor.
    ad_utility::HashMap<std::string, RecordTable> recordTables_;
     // The order of creation for the entries of recordTables_.
    std::vector<std::string> recordTablesOrder_;

    /*
     * @brief Return execution time of function in seconds.
     *
     * @tparam FUNCTION_TYPE Best left to type inference.
     *
     * @param functionToMeasure Must be a function, or callable.
     */
    template<typename FUNCTION_TYPE>
      requires std::invocable<FUNCTION_TYPE>
    float measureTimeOfFunction(const FUNCTION_TYPE& functionToMeasure) const{
      ad_utility::Timer benchmarkTimer;
         
      benchmarkTimer.start();
      functionToMeasure();
      benchmarkTimer.stop();

      return benchmarkTimer.secs();
    }

    /*
     * @brief Translate the given hash map keys to their values and return
     * those values in the same order as the keys.
     *
     * @tparam MAP_KEY_TYPE, MAP_VALUE_TYPE The types for the (key, value)
     *  pairs of the hash map.
     *
     * @param hashMap The hash map, where the values should be looked up in.
     * @param hashMapKeys The keys, for which the values should be looked up
     *  and inserted in a vector.
     *
     * @returns A vector of the hashMap[hashMapKeys] values, in the same order
     *  as the keys in hashMapKeys.
     */
    template<typename MAP_KEY_TYPE, typename MAP_VALUE_TYPE>
    static const std::vector<MAP_VALUE_TYPE> createVectorOfHashMapValues(
        const ad_utility::HashMap<MAP_KEY_TYPE, MAP_VALUE_TYPE>& hashMap,
        const std::vector<MAP_KEY_TYPE>& hashMapKeys);

  public:

    /*
     * @brief Measures the time needed for the execution of the given function and
     * saves it, together with a description, in a normal list.
     *
     * @tparam FUNCTION_TYPE Best left to type inference.
     *
     * @param descriptor A description, of what kind of benchmark case is
     *  getting measured. Needed, because otherwise nobody would be able to
     *  tell, which time corresponds to which benchmark.
     * @param functionToMeasure The function, that represents the benchmark.
     *  Most of the time a lambda, that calls the actual function to benchmark
     *  with the needed parameters.
     */
    template<typename FUNCTION_TYPE>
      requires std::invocable<FUNCTION_TYPE>
    void addSingleMeasurement(const std::string& descriptor,
        const FUNCTION_TYPE& functionToMeasure) {
      singleMeasurements_.push_back(RecordEntry{descriptor,
          measureTimeOfFunction(functionToMeasure)});
    }


    // Returns a const view of all single recorded times.
    const std::vector<RecordEntry>& getSingleMeasurements() const;

    /*
     * @brief Creates an empty group, which can
     *  be accesed/identified using the descriptor.
     */
    void addGroup(const std::string& descriptor);

    /*
     * @brief Measures the time needed for the execution of the given function and
     * saves it, together with a description, as an item in a group.
     *
     * @tparam FUNCTION_TYPE Best left to type inference.
     *
     * @param groupDescriptor The identification of the group.
     * @param descriptor A description, of what kind of benchmark case is
     *  getting measured. Needed, because otherwise nobody would be able to
     *  tell, which time corresponds to which benchmark.
     * @param functionToMeasure The function, that represents the benchmark.
     *  Most of the time a lambda, that calls the actual function to benchmark
     *  with the needed parameters.
     */
    template<typename FUNCTION_TYPE>
      requires std::invocable<FUNCTION_TYPE>
    void addToExistingGroup(const std::string& groupDescriptor,
        const std::string& descriptor,
        const FUNCTION_TYPE& functionToMeasure) {
      // Does the group exist?
      auto groupEntry = recordGroups_.find(groupDescriptor);
      AD_CHECK(groupEntry != recordGroups_.end());

      // Add the descriptor and measured time to the group.
      groupEntry->second.entries.push_back(
          RecordEntry{descriptor, measureTimeOfFunction(functionToMeasure)});
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
     * @tparam FUNCTION_TYPE Best left to type inference.
     *
     * @param tableDescriptor The identification of the table.
     * @param row, column Where in the tables to write the measured time.
     *  Starts with (0,0).
     * @param functionToMeasure The function, that represents the benchmark.
     *  Most of the time a lambda, that calls the actual function to benchmark
     *  with the needed parameters.
     */
    template<typename FUNCTION_TYPE>
      requires std::invocable<FUNCTION_TYPE>
    void addToExistingTable(const std::string& tableDescriptor,
        const size_t row, const size_t column,
        const FUNCTION_TYPE& functionToMeasure) {
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


    /*
     * @brief Returns a vector of all the tables.
     */
    const std::vector<RecordTable> getTables() const;
};

// JSON (d)serilization for all the created structures in BenchmarkRecords.
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(BenchmarkRecords::RecordEntry, descriptor, measuredTime)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(BenchmarkRecords::RecordGroup, descriptor, entries)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(BenchmarkRecords::RecordTable, descriptor, rowNames, columnNames, entries)

/*
 * This class exists as a round about way, of registering functions as
 * benchmarks before entering the main function.
 */
class BenchmarkRegister {
 
    /*
     * @brief Returns a reference to the static vector of all registered
     *  benchmark functions.
     *  That is, the functions, that do the setup needed for a benchmark and
     *  then record the execution time with the passed BenchmarkRecords.
     *  The calling of the registered benchmark functions and the processing of
     *  the recorded times, will be done in a main function later.
     */
    static std::vector<std::function<void(BenchmarkRecords*)>>& getRegister();

  public:

    /*
     * @brief Register one, or more, functions as benchmarks by creating a
     * global instance of this class. Shouldn't take up much space and I
     * couldn't find a better way of doing it.
     *
     * @param benchmarks The functions can be passed as
     *  `{&functionName1, &functionname2, ...}`. For more information about
     *  their usage see getRegister . 
     */
    BenchmarkRegister(const std::vector<std::function<void(BenchmarkRecords*)>>& benchmarks);

    // Return a const view of all registered benchmarks.
    static const std::vector<std::function<void(BenchmarkRecords*)>>& getRegisteredBenchmarks();
};
