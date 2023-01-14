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

  private:

    // A vector of all single functions measured.
    std::vector<RecordEntry> _singleMeasurments;

    // A hash map of all the created RecordGroups. For faster access.
    // The key for a RecordGroup is it's descriptor.
    ad_utility::HashMap<std::string, RecordGroup> _recordGroups;

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
    void addSingleMeasurment(const std::string descriptor,
        const std::function<void()>& functionToMeasure);

    // Returns a const view of all single recorded times.
    const std::vector<RecordEntry>& getSingleMeasurments() const;

    void addGroup(const std::string descriptor);

    void addToExistingGroup(const std::string descriptor,
        const std::function<void()>& functionToMeasure);

    const ad_utility::HashMap<std::string, RecordGroup>& getGroups() const;
};

/*
 * This class exists as a round about way, of registering functions as
 * benchmarks before entering the main function.
 */
class BenchmarkRegister {
 
    // All registerd benchmark functions. That is, the functions, that do the
    // setup needed for a benchmark and then record the execution time with
    // the passed BenchmarkRecords.
    // The calling of the registerd benchmark functions and the processing of
    // the recorded times, will be done in a main function later.
    static std::vector<std::function<void(BenchmarkRecords*)>> _registerdBenchmarks;

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
