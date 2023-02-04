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
#include "../benchmark/util/HashMapWithInsertionOrder.h"

/*
 * Used for measuring the time needed for the execution of a function. It also
 * saves the measured time togehter with a description of the function
 * measured.
 */
class BenchmarkRecords {

  public:

    // Describes a measured function.
    struct RecordEntry {
      std::string descriptor_; // Needed, because without it, nobody could tell,
                              // which time belongs to which benchmark.
      float measuredTime_; // The measured time in seconds.
    };

    // Describes a group of measured functions.
    struct RecordGroup {
      std::string descriptor_; // Needed for identifying groups.
      std::vector<RecordEntry> entries_;
    };
    
    // Describes a table of measured functions.
    struct RecordTable {
      // For identification.
      std::string descriptor_;
      // The names of the columns and rows.
      std::vector<std::string> rowNames_;
      std::vector<std::string> columnNames_;
      // The time measurements in seconds. Access is [row, column].
      std::vector<std::vector<std::optional<float>>> entries_;

      RecordTable(const std::string& pDescriptor, 
          const std::vector<std::string>& pRowNames,
          const std::vector<std::string>& pColumnNames):
          descriptor_{pDescriptor}, rowNames_{pRowNames},
          columnNames_{pColumnNames},
          entries_(pRowNames.size(), std::vector<std::optional<float>>(pColumnNames.size())) {}
    };

  private:

    // A vector of all single functions measured.
    std::vector<RecordEntry> singleMeasurements_;

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
      ad_utility::Timer benchmarkTimer;
         
      benchmarkTimer.start();
      functionToMeasure();
      benchmarkTimer.stop();

      return benchmarkTimer.secs();
    }

  public:

    /*
     * @brief Measures the time needed for the execution of the given function and
     * saves it, together with a description, in a normal list.
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
      // Get the entry of the hash map and add to it.
      auto& groupEntry = recordGroups_.getReferenceToValue(groupDescriptor);
      groupEntry.entries_.push_back(
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
      // Get the entry of the hash map.
      auto& table = recordTables_.getReferenceToValue(tableDescriptor);

      // Are the given row and column number inside the table range?
      // size_t is unsigned, so we only need to check, that they are not to big.
      AD_CHECK(row < table.rowNames_.size() && column < table.columnNames_.size());
      
      // Add the measured time to the table.
      table.entries_[row][column] = measureTimeOfFunction(functionToMeasure);
    }


    /*
     * @brief Returns a vector of all the tables.
     */
    const std::vector<RecordTable> getTables() const;
};

// JSON (d)serilization for all the created structures in BenchmarkRecords.
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(BenchmarkRecords::RecordEntry, descriptor_, measuredTime_)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(BenchmarkRecords::RecordGroup, descriptor_, entries_)
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(BenchmarkRecords::RecordTable, descriptor_, rowNames_, columnNames_, entries_)

/*
 * This class exists as a round about way, of registering functions as
 * benchmarks before entering the main function.
 */
class BenchmarkRegister {

    // Alias for a type, so that we don't repeat things so often.
    using BenchmarkFunction = std::function<void(BenchmarkRecords*)>;
 
    /*
     * @brief Returns a reference to the static vector of all registered
     *  benchmark functions.
     *  That is, the functions, that do the setup needed for a benchmark and
     *  then record the execution time with the passed BenchmarkRecords.
     *  The calling of the registered benchmark functions and the processing of
     *  the recorded times, will be done in a main function later.
     */
    static std::vector<BenchmarkFunction>& getRegister();

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
    BenchmarkRegister(const std::vector<BenchmarkFunction>& benchmarks);

    // Return a const view of all registered benchmarks.
    static const std::vector<BenchmarkFunction>& getRegisteredBenchmarks();
};
