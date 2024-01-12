// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022,
// schlegea@informatik.uni-freiburg.de)

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "util/ConfigManager/ConfigManager.h"
#include "util/CopyableUniquePtr.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/HashMap.h"
#include "util/SourceLocation.h"
#include "util/Timer.h"
#include "util/TypeTraits.h"
#include "util/json.h"

namespace ad_benchmark {
/*
 * Used for measuring the time needed for the execution of a function and
 * organizing those measured times.
 */
class BenchmarkResults {
  /*
  A quick explanation, **why** this class uses pointers:
  All the container for benchmark measurements are created in place and then a
  reference to the new container returned. This returning of a reference is the
  sole reason for the usage of pointers.
  Otherwise adding more entries to the vectors, could lead to all previous
  references being made invalid, because a vector had to re-allocate memory.
  If the entries are pointers to the objects, the references to the object stay
  valid and we don't have this problem.
  */
  template <typename T>
  using PointerVector = std::vector<ad_utility::CopyableUniquePtr<T>>;

  // A vector of all the created single measurements.
  PointerVector<ResultEntry> singleMeasurements_;

  // A vector of all the created resultGroups.
  PointerVector<ResultGroup> resultGroups_;

  // A vector of all the created resultTables.
  PointerVector<ResultTable> resultTables_;

  /*
  @brief Adds an entry to the given vector, by creating an instance of
  `CopyableUniquePtr` for the given type and appending it. Strictly an
  internal helper function.

  @tparam EntryType The type, that the `CopyableUniquePtr` in the vector points
  to.
  @tparam ConstructorArgs Types for the constructor arguments for creating
  a new instance of `EntryType`.

  @param targetVector A vector of `CopyableUniquePtr`, that own values.
  @param constructorArgs Arguments to pass to the constructor of the object,
  that the new `CopyableUniquePtr` will own.
  */
  template <
      ad_utility::SameAsAny<ResultTable, ResultEntry, ResultGroup> EntryType>
  static EntryType& addEntryToContainerVector(
      PointerVector<EntryType>& targetVector, auto&&... constructorArgs) {
    targetVector.push_back(ad_utility::make_copyable_unique<EntryType>(
        AD_FWD(constructorArgs)...));
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
  template <std::invocable Function>
  ResultEntry& addMeasurement(const std::string& descriptor,
                              const Function& functionToMeasure) {
    return addEntryToContainerVector(singleMeasurements_, descriptor,
                                     functionToMeasure);
  }

  /*
   * @brief Returns a vector of all the singe measurements.
   */
  std::vector<ResultEntry> getSingleMeasurements() const;

  /*
   * @brief Creates and returns an empty group with the given descriptor.
   */
  ResultGroup& addGroup(const std::string& descriptor);

  /*
   * @brief Returns a vector of all the groups.
   */
  std::vector<ResultGroup> getGroups() const;

  /*
  @brief Creates and returns an empty table.

  @param descriptor A string to identify this instance in json format later.
  @param rowNames The names for the rows. The amount of rows in this table is
  equal to the amount of row names. Important: This first column will be filled
  with those names.
  @param columnNames The names for the columns. The amount of columns in this
  table is equal to the amount of column names.
  */
  ResultTable& addTable(const std::string& descriptor,
                        const std::vector<std::string>& rowNames,
                        const std::vector<std::string>& columnNames);

  /*
   * @brief Returns a vector of all the tables.
   */
  std::vector<ResultTable> getTables() const;

  // Json serialization. The implementation can be found in
  // `BenchmarkToJson`.
  friend void to_json(nlohmann::json& j, const BenchmarkResults& results);
};

/*
The interface for benchmark classes. More specifically, it's the interface
between a collection of benchmarks of any type (single, group, table) and
the processing/management of those benchmarks.
*/
class BenchmarkInterface {
  /*
  For adding configuration options and getting the values passed at runtime.
  If you want to add configuration options, do it in the constructor of your
  class.
  */
  ad_utility::ConfigManager manager_;

 public:
  // A human-readable name that will be printed as part of the output.
  virtual std::string name() const = 0;

  /*
  For the general metadata of a class. Mostly information, that is the same
  for every benchmark, so that every entry of the `BenchmarkResults` doesn't
  repeat the same thing over and over again.
  For example: Let's say, you are measuring the same benchmarks for different
  versions of an algorithm. You could add the metadata information, which
  version it is, to every `resultGroup`, `resultTable`, etc., but that is a bit
  clunky. Instead, you make one `BenchmarkInterface` instance for every
  version and simply return which version you are using as metadata through
  `getMetadata`.
  */
  virtual BenchmarkMetadata getMetadata() const {
    // Default behaviour.
    return BenchmarkMetadata{};
  }

  /*
  Run all your benchmarks. The `BenchmarkResults` class is a management
  class for measuering the execution time of functions and organizing the
  results.
  */
  virtual BenchmarkResults runAllBenchmarks() = 0;

  // Without this, we get memory problems.
  virtual ~BenchmarkInterface() = default;

  // Needed for manipulation by the infrastructure.
  ad_utility::ConfigManager& getConfigManager() { return manager_; }
  const ad_utility::ConfigManager& getConfigManager() const { return manager_; }
};

/*
Used to register your benchmark classes, so that the benchmarking system can
access and use them.
*/
class BenchmarkRegister {
  // Alias for a type, so that we don't repeat things so often.
  using BenchmarkPointer = std::unique_ptr<BenchmarkInterface>;

  /*
  Static vector of all registered benchmark classe instances.
   */
  inline static std::vector<BenchmarkPointer> registeredBenchmarks{};

 public:
  /*
   * @brief Register one benchmark class, by creating a global
   *  instance of this class and passing the instances of your class, that
   *  implemented the `BenchmarkInterface`. Shouldn't take up much space
   *  and I couldn't find a better way of doing it.
   *
   * @param benchmarkClasseInstance The memory managment of the passed
   *  instances will be taken over by `BenchmarkRegister`.
   */
  explicit BenchmarkRegister(BenchmarkPointer&& benchmarkClasseInstance);

  /*
  @brief Passes the `nlohmann::json` object to the
  `ConfigManager::parseConfig()` of every registered benchmark class.
  */
  static void parseConfigWithAllRegisteredBenchmarks(const nlohmann::json& j);

  /*
   * @brief Measures all the registered benchmarks and returns the resulting
   *  `BenchmarkResuls` objects.
   *
   * @return Every benchmark class get's measured with their own
   *  `BenchmarkResults`. They should be in the same order as the
   *  registrations.
   */
  static std::vector<BenchmarkResults> runAllRegisteredBenchmarks();

  /*
  @brief Returns pointers to all the registered benchmark class objects.
  */
  static std::vector<const BenchmarkInterface*> getAllRegisteredBenchmarks();
};

/*
Macros for easier registering of benchmark classes.
`declareRegisterVariable` and `declareRegisterVariableHidden` are needed for
the implementation. Only `registerBenchmark` needs to be 'called', when one
want's to register a benchmark class.
*/
#define AD_DECLARE_REGISTER_VARIABLE_HIDDEN(line, benchmarkClass, ...) \
  static BenchmarkRegister gRegisterVariable##benchmarkClass##line{    \
      std::make_unique<benchmarkClass>(__VA_ARGS__)};
#define AD_DECLARE_REGISTER_VARIABLE(line, benchmarkClass, ...) \
  AD_DECLARE_REGISTER_VARIABLE_HIDDEN(                          \
      line, benchmarkClass __VA_OPT__(, ) __VA_ARGS__)

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
#define AD_REGISTER_BENCHMARK(benchmarkClass, ...) \
  AD_DECLARE_REGISTER_VARIABLE(__LINE__,           \
                               benchmarkClass __VA_OPT__(, ) __VA_ARGS__)
}  // namespace ad_benchmark
