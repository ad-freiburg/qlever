// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <vector>
#include <functional>
#include <string>

#include "util/Timer.h"
#include <util/HashMap.h>

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
      std::vector<std::vector<float>> entries;
    };

  private:

    // A vector of all single functions measured.
    std::vector<RecordEntry> _singleMeasurments;

    // A hash map of all the created RecordGroups. For faster access.
    // The key for a RecordGroup is it's descriptor.
    ad_utility::HashMap<std::string, RecordGroup> _recordGroups;
  
    // A hash map of all the created RecordTables. For faster access.
    // The key for a RecordTable is it's descriptor.
    ad_utility::HashMap<std::string, RecordTable> _recordTables;
    
    /*
     * @brief Return execution time of function in seconds.
     */
    float measureTimeOfFunction(const std::function<void()>& functionToMeasure) const;

  public:

    /*
     * @brief Measures the time needed for the execution of the given function and
     * saves it, together with a description, in a normal list.
     *
     * @param descriptor A description, of what kind of benchmark case is
     *  getting measured. Needed, because otherwise nobody would be able to
     *  tell, which time corresponds to which benchmark.
     * @param functionToMeasure The function, that represents the benchmark.
     *  Most of the time a lambda, that calls the actual function to benchmark
     *  with the needed parameters.
     */
    void addSingleMeasurment(const std::string& descriptor,
        const std::function<void()>& functionToMeasure);

    // Returns a const view of all single recorded times.
    const std::vector<RecordEntry>& getSingleMeasurments() const;

    /*
     * @brief Creates an empty group, which can
     *  be accesed/identified using the descriptor.
     */
    void addGroup(const std::string& descriptor);

    /*
     * @brief Measures the time needed for the execution of the given function and
     * saves it, together with a description, as an item in a group.
     *
     * @param groupDescriptor The identification of the group.
     * @param descriptor A description, of what kind of benchmark case is
     *  getting measured. Needed, because otherwise nobody would be able to
     *  tell, which time corresponds to which benchmark.
     * @param functionToMeasure The function, that represents the benchmark.
     *  Most of the time a lambda, that calls the actual function to benchmark
     *  with the needed parameters.
     */
    void addToExistingGroup(const std::string& groupDescriptor,
        const std::string& descriptor,
        const std::function<void()>& functionToMeasure);

    /*
     * @brief Returns a const view of all the groups.
     */
    const ad_utility::HashMap<std::string, RecordGroup>& getGroups() const;

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
     * @param tableDescriptor The identification of the table.
     * @param row, column Where in the tables to write the measured time.
     *  Starts with (0,0).
     * @param functionToMeasure The function, that represents the benchmark.
     *  Most of the time a lambda, that calls the actual function to benchmark
     *  with the needed parameters.
     */
    void addToExistingTable(const std::string& tableDescriptor,
        const size_t row, const size_t column,
        const std::function<void()>& functionToMeasure);

    /*
     * @brief Returns a const view of all the tables.
     */
    const ad_utility::HashMap<std::string, RecordTable>& getTables() const;
};

/*
 * This class exists as a round about way, of registering functions as
 * benchmarks before entering the main function.
 */
class BenchmarkRegister {
 
    /*
     * @brief Returns a reference to the static vector of all registerd
     *  benchmark functions.
     *  That is, the functions, that do the setup needed for a benchmark and
     *  then record the execution time with the passed BenchmarkRecords.
     *  The calling of the registerd benchmark functions and the processing of
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
     *  their usage see _registerdBenchmarks . 
     */
    BenchmarkRegister(const std::vector<std::function<void(BenchmarkRecords*)>>& benchmarks);

    // Return a const view of all registerd benchmarks.
    static const std::vector<std::function<void(BenchmarkRecords*)>>& getRegisterdBenchmarks();
};
