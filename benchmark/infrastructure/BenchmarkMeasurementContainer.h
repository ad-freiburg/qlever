// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <concepts>
#include <vector>
#include <memory>
#include <utility>

#include "util/Exception.h"
#include "util/Timer.h"
#include "util/json.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "../benchmark/infrastructure/CopybaleUniquePtr.h"

// All the classes, that hold the measurements of measured benchmarks.
namespace BenchmarkMeasurementContainer{

// Helper function for adding time entries to the classes.
/*
@brief Return execution time of function in seconds.

@tparam Function Best left to type inference.

@param functionToMeasure Must be a function, or callable.
*/
template<typename Function>
  requires std::invocable<Function>
static float measureTimeOfFunction(const Function& functionToMeasure){
  ad_utility::timer::Timer
  benchmarkTimer(ad_utility::timer::Timer::Started);
  functionToMeasure();
  benchmarkTimer.stop();

  // This is used for a macro benchmark, so we don't need that high of a
  // precision.
  return static_cast<float>(
  ad_utility::timer::Timer::toSeconds(benchmarkTimer.value()));
}

// Describes the measured execution time of a function.
class RecordEntry{
  /*
  Needed, because without it, nobody could tell, which time belongs to which
  benchmark.
  */
  std::string descriptor_;
  // The measured time in seconds.
  float measuredTime_;

  public:
  BenchmarkMetadata metadata_;

  /*
  @brief Creates an instance of `RecordEntry`.

  @tparam Function Lambda function with no function arguments and returns void.

  @param descriptor A string to identify this instance in json format later.
  @param functionToMeasure The function, who's execution time will be
  measured and saved.
  */
  template<typename Function>
    requires std::invocable<Function>
  RecordEntry(const std::string& descriptor, const Function& functionToMeasure):
  descriptor_{descriptor},
  measuredTime_{measureTimeOfFunction(functionToMeasure)} {}

  // User defined conversion to `std::string`.
  explicit operator std::string() const;
  
  // JSON serialization.
  friend void to_json(nlohmann::json& j, const RecordEntry& recordEntry);
};

// Describes a group of `RecordEntry`.
class RecordGroup {
  // Needed for identifying groups.
  std::string descriptor_;
  // Members of the group.
  std::vector<CopybaleUniquePtr<RecordEntry>> entries_;

  public:
  BenchmarkMetadata metadata_;

  /*
  @brief Creates an empty group of `RecordEntry`s.

  @param descriptor A string to identify this instance in json format later.
  */
  explicit RecordGroup(const std::string& descriptor): descriptor_{descriptor}
  {}

  /*
  @brief Adds a new instance of `RecordEntry` to the group.

  @tparam Function Lambda function with no function arguments and returns void.

  @param descriptor A string to identify this instance in json format later.
  @param functionToMeasure The function, who's execution time will be
  measured and saved.
  */
  template<typename Function>
    requires std::invocable<Function>
  RecordEntry& addMeasurement(const std::string& descriptor,
      const Function& functionToMeasure){
      entries_.push_back(CopybaleUniquePtr<RecordEntry>{
        std::make_unique<RecordEntry>(descriptor, functionToMeasure)});
      return (*entries_.back());
    }

  // User defined conversion to `std::string`.
  explicit operator std::string() const;
  
  // JSON serialization.
  friend void to_json(nlohmann::json& j, const RecordGroup& recordGroup);
};

// Describes a table of measured execution times of functions.
class RecordTable {
  // For identification.
  std::string descriptor_;
  // The names of the columns and rows.
  std::vector<std::string> rowNames_;
  std::vector<std::string> columnNames_;
  // The entries in the table. Access is [row, column]. Can be the time in
  // seconds, a string, or empty.
  using EntryType = std::variant<std::monostate, float, std::string>;
  std::vector<std::vector<EntryType>> entries_;

  public:
  BenchmarkMetadata metadata_;

  /*
  @brief Create an empty `RecordTable`.

  @param descriptor A string to identify this instance in json format later.
  @param rowNames The names for the rows. The amount of rows in this table is
  equal to the amount of row names.
  @param columnNames The names for the columns. The amount of columns in this
  table is equal to the amount of column names.
  */
  RecordTable(const std::string& descriptor, 
      const std::vector<std::string>& rowNames,
      const std::vector<std::string>& columnNames):
      descriptor_{descriptor}, rowNames_{rowNames},
      columnNames_{columnNames},
      entries_(rowNames.size(),
          std::vector<EntryType>(columnNames.size())) {}

  /*
  @brief Measures the time needed for the execution of the given function and
  saves it as an entry in the table.

  @tparam Function Lambda function with no function arguments and returns void.

  @param row, column Where in the tables to write the measured time.
   Starts with `(0,0)`.
  @param functionToMeasure The function, which execution time will be measured.
  */
  template<typename Function>
    requires std::invocable<Function>
  void addMeasurement(const size_t& row, const size_t& column,
    const Function& functionToMeasure){
    AD_CONTRACT_CHECK(row < rowNames_.size() && column < columnNames_.size());
    entries_.at(row).at(column) = measureTimeOfFunction(functionToMeasure);
  }

  /*
  @brief Manually set an entry.

  @param row, column Which entry in the table to set. Starts with `(0,0)`.
  @param newEntryContent What to set the entry to. Works well with implicit
  type conversion.
  */
  void setEntry(const size_t& row, const size_t& column,
    const EntryType& newEntryContent);

  /*
  @brief Returns the content of a table entry, if the the correct type was
  given. Otherwise, causes an error.
 
  @tparam T What type the entry has. Must be either `float`, or `string`. If
   you give the wrong one, or the entry was never set/added, then this
   function will cause an exception.
 
  @param row, column Which table entry to read. Starts with `(0,0)`.
  */
  template<typename T>
    requires std::is_same_v<T, float> ||
    std::is_same_v<T, std::string>
  T getEntry(const size_t row, const size_t column){
    // There is a chance, that the entry of the table does NOT have type T,
    // in which case this will cause an error. As this is a mistake on the
    // side of the user, we don't really care.
    return std::get<T>(entries_.at(row).at(column));
  }

  // User defined conversion to `std::string`.
  explicit operator std::string() const;

  // JSON serialization.
  friend void to_json(nlohmann::json& j, const RecordTable& recordTable);
};
  
}
