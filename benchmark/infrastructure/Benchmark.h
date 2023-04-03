// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <vector>
#include <functional>
#include <string>
#include <variant>
#include <memory>
#include <utility>

#include "../benchmark/infrastructure/BenchmarkConfiguration.h"
#include "util/json.h"
#include "util/Timer.h"
#include "util/HashMap.h"
#include "util/Exception.h"
#include "util/SourceLocation.h"
#include "../benchmark/util/HashMapWithInsertionOrder.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"

/*
 * Used for measuring the time needed for the execution of a function and
 * organizing those measured times.
 */
class BenchmarkRecords {
    template<typename T>
    using PointerVector = std::vector<std::unique_ptr<T>>;

    // A vector of all the created single measurements.
    PointerVector<BenchmarkMeasurementContainer::RecordEntry>
    singleMeasurements_;

    // A vector of all the created RecordGroups.
    PointerVector<BenchmarkMeasurementContainer::RecordGroup>
    recordGroups_;
  
    // A hash map of all the created RecordTables. For faster access.
    // The key for a RecordTable is it's descriptor.
    PointerVector<BenchmarkMeasurementContainer::RecordTable>
    recordTables_;

    /*
    @brief Adds an entry to the given vector, by creating an instance of
    `std::unique_ptr` for the given type and appending it. Strictly an
    internal helper function.

    @tparam EntryType The type, that the `std::unique_ptr` in the vector points
    to.
    @tparam ConstructorArgs Types for the constructor arguments for creating
    a new instance of `EntryType`.

    @param targetVector A vector of `std::unique_ptr`, that own values.
    @param constructorArgs Arguments to pass to the constructor of the object,
    that the new `std::unique_ptr` will own.
    */
    template<typename EntryType, typename... ConstructorArgs>
    static EntryType& addEntryToContainerVector(
      PointerVector<EntryType>& targetVector,
      ConstructorArgs&&... constructorArgs){
      targetVector.push_back(std::make_unique<EntryType>(
        std::forward<ConstructorArgs>(constructorArgs)...));
      return (*targetVector.back());
    }

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
    BenchmarkMeasurementContainer::RecordEntry& addMeasurement(
      const std::string& descriptor, const Function& functionToMeasure){
      return addEntryToContainerVector(singleMeasurements_, descriptor,
      functionToMeasure);
    }

    /*
     * @brief Returns a vector of all the singe measurements.
     */
    const std::vector<BenchmarkMeasurementContainer::RecordEntry>
    getSingleMeasurements() const;

    /*
     * @brief Creates an empty group with the given descriptor.
     */
    BenchmarkMeasurementContainer::RecordGroup& addGroup(
      const std::string& descriptor);

    /*
     * @brief Returns a vector of all the groups.
     */
    const std::vector<BenchmarkMeasurementContainer::RecordGroup> getGroups()
    const;

    /*
     * @brief Creates an empty table, which can be accesed/identified using the
     *  descriptor.
     *
     * @param descriptor The name/identifier of the table.
     * @param rowNames,columnNames The names for the rows/columns.
     */
    BenchmarkMeasurementContainer::RecordTable& addTable(
        const std::string& descriptor,
        const std::vector<std::string>& rowNames,
        const std::vector<std::string>& columnNames);
    
    /*
     * @brief Returns a vector of all the tables.
     */
    const std::vector<BenchmarkMeasurementContainer::RecordTable>
    getTables() const;

    // Default constructor.
    BenchmarkRecords() = default;

    // Custom copy constructor because of the vectors of unique pointers.
    BenchmarkRecords(const BenchmarkRecords& records);
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

/*
Macros for easier registering of benchmark classes.
`declareRegisterVariable` and `declareRegisterVariableHidden` are needed for
the implementation. Only `registerBenchmark` needs to be 'called', when one
want's to register a benchmark class.
*/
#define AD_DECLARE_REGISTER_VARIABLE_HIDDEN(line, benchmarkClass, ...) static BenchmarkRegister gRegisterVariable##benchmarkClass##line{std::make_unique<benchmarkClass>(__VA_ARGS__)};
#define AD_DECLARE_REGISTER_VARIABLE(line, benchmarkClass, ...) AD_DECLARE_REGISTER_VARIABLE_HIDDEN(line, benchmarkClass __VA_OPT__(,) __VA_ARGS__)

/*
@brief Registers a benchmark class with `BenchmarkRegister`. Very important:
Every call has to be in it's own line in a file, otherwise you will get an
error, and the call also has to be in static/global scope.

@param benchmarkClass The class, that you wish to register. Not an instance
of the class, just the type.
@param `...` Should your class not be default constructible, or you want to
pass some arguments to a special constructor, you can pass any extra
constructor arguments here. Just treat it like a variadic template function.
*/
#define AD_REGISTER_BENCHMARK(benchmarkClass, ...) AD_DECLARE_REGISTER_VARIABLE(__LINE__, benchmarkClass __VA_OPT__(,) __VA_ARGS__)
