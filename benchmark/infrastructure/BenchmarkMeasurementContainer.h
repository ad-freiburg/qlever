// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <concepts>
#include <memory>
#include <utility>
#include <variant>
#include <vector>

#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "util/CopyableUniquePtr.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/Timer.h"
#include "util/TypeTraits.h"
#include "util/json.h"

namespace ad_benchmark {

// Helper function for adding time entries to the classes.
/*
@brief Return execution time of function in seconds and inserts the progress in
`LOG(INFO)`.

@tparam Function Best left to type inference.

@param functionToMeasure Must be a function, or callable.
@param measurementSubjectName A description/name of what is being measured.
*/
template <std::invocable Function>
static float measureTimeOfFunction(
    const Function& functionToMeasure,
    std::string_view measurementSubjectIdentifier) {
  LOG(INFO) << "Running measurement \"" << measurementSubjectIdentifier
            << "\" ..." << std::endl;

  // Measuring the time.
  ad_utility::timer::Timer benchmarkTimer(ad_utility::timer::Timer::Started);
  functionToMeasure();
  benchmarkTimer.stop();

  // This is used for a macro benchmark, so we don't need that high of a
  // precision.
  const auto measuredTime = static_cast<float>(
      ad_utility::timer::Timer::toSeconds(benchmarkTimer.value()));
  LOG(INFO) << "Done in " << measuredTime << " seconds." << std::endl;

  return measuredTime;
}

// A very simple wrapper for a `BenchmarkMetadata` getter.
class BenchmarkMetadataGetter {
  BenchmarkMetadata metadata_;

 public:
  /*
  @brief Get a reference to the held metadata object.
  */
  BenchmarkMetadata& metadata();

  /*
  @brief Get a reference to the held metadata object.
  */
  const BenchmarkMetadata& metadata() const;
};

// Describes the measured execution time of a function.
class ResultEntry : public BenchmarkMetadataGetter {
  /*
  Needed, because without it, nobody could tell, which time belongs to which
  benchmark.
  */
  std::string descriptor_;
  // The measured time in seconds.
  float measuredTime_;

  // Needed for testing purposes.
  FRIEND_TEST(BenchmarkMeasurementContainerTest, ResultEntry);
  FRIEND_TEST(BenchmarkMeasurementContainerTest, ResultGroup);

 public:
  /*
  @brief Creates an instance of `ResultEntry`.

  @tparam Function Lambda function with no function arguments and returns void.

  @param descriptor A string to identify this instance in json format later.
  @param functionToMeasure The function, who's execution time will be
  measured and saved.
  */
  ResultEntry(const std::string& descriptor,
              const std::invocable auto& functionToMeasure)
      : descriptor_{descriptor},
        measuredTime_{measureTimeOfFunction(functionToMeasure, descriptor)} {}

  /*
  @brief Creates an instance of `ResultEntry` with a special descriptor for
  usage within the log, instead of the normal descriptor.

  @tparam Function Lambda function with no function arguments and returns void.

  @param descriptor A string to identify this instance in json format later.
  @param descriptorForLog A string to identify this instance in the log.
  @param functionToMeasure The function, who's execution time will be
  measured and saved.
  */
  ResultEntry(const std::string& descriptor, std::string_view descriptorForLog,
              const std::invocable auto& functionToMeasure)
      : descriptor_{descriptor},
        measuredTime_{
            measureTimeOfFunction(functionToMeasure, descriptorForLog)} {}

  // User defined conversion to `std::string`.
  explicit operator std::string() const;
  friend std::ostream& operator<<(std::ostream& os,
                                  const ResultEntry& resultEntry);

  // JSON serialization.
  friend void to_json(nlohmann::json& j, const ResultEntry& resultEntry);
};

// Describes a table of measured execution times of functions.
class ResultTable : public BenchmarkMetadataGetter {
 public:
  /*
  What type of entry a `ResultTable` can hold. The float is for the measured
  time in seconds, the `monostate` for empty entries, and the rest for custom
  entries by the user for better readability.
  */
  using EntryType =
      std::variant<std::monostate, float, std::string, bool, size_t, int>;

 private:
  // For identification.
  std::string descriptor_;
  /*
  For identification within `Log(Info)`. This class knows nothing about the
  groups, where it is a member, but we want to include this information in
  the log. This is our workaround.
  */
  std::string descriptorForLog_;
  // The names of the columns.
  std::vector<std::string> columnNames_;
  // The entries in the table. Access is [row, column].
  std::vector<std::vector<EntryType>> entries_;

  // Needed for testing purposes.
  FRIEND_TEST(BenchmarkMeasurementContainerTest, ResultTable);
  FRIEND_TEST(BenchmarkMeasurementContainerTest, ResultTableEraseRow);
  FRIEND_TEST(BenchmarkMeasurementContainerTest, ResultGroup);

 public:
  /*
  @brief Create an empty `ResultTable`.

  @param descriptor A string to identify this instance in json format later.
  @param rowNames The names for the rows. The amount of rows in this table is
  equal to the amount of row names. Important: This first column will be filled
  with those names.
  @param columnNames The names for the columns. The amount of columns in this
  table is equal to the amount of column names.
  */
  ResultTable(const std::string& descriptor,
              const std::vector<std::string>& rowNames,
              const std::vector<std::string>& columnNames);

  /*
  @brief Create an empty `ResultTable` with a special descriptor for usage
  within the log, instead of the normal descriptor.

  @param descriptor A string to identify this instance in json format later.
  @param descriptorForLog A string to identify this instance in the log.
  @param rowNames The names for the rows. The amount of rows in this table is
  equal to the amount of row names. Important: This first column will be filled
  with those names.
  @param columnNames The names for the columns. The amount of columns in this
  table is equal to the amount of column names.
  */
  ResultTable(const std::string& descriptor, std::string descriptorForLog,
              const std::vector<std::string>& rowNames,
              const std::vector<std::string>& columnNames);

  /*
  @brief Measures the time needed for the execution of the given function and
  saves it as an entry in the table.

  @tparam Function Lambda function with no function arguments and returns void.

  @param row, column Where in the tables to write the measured time.
   Starts with `(0,0)`.
  @param functionToMeasure The function, which execution time will be measured.
  */
  template <std::invocable Function>
  void addMeasurement(const size_t& row, const size_t& column,
                      const Function& functionToMeasure) {
    AD_CONTRACT_CHECK(row < numRows() && column < numColumns());
    entries_.at(row).at(column) = measureTimeOfFunction(
        functionToMeasure,
        absl::StrCat("Entry at row ", row, ", column ", column,
                     " of ResultTable ", descriptorForLog_));
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

  @tparam T What type the entry has. Must be contained in `EntryType`. If
   you give the wrong one, or the entry was never set/added, then this
   function will cause an exception.

  @param row, column Which table entry to read. Starts with `(0,0)`.
  */
  template <ad_utility::SameAsAnyTypeIn<EntryType> T>
  T getEntry(const size_t row, const size_t column) const {
    AD_CONTRACT_CHECK(row < numRows() && column < numColumns());
    static_assert(!ad_utility::isSimilar<T, std::monostate>);

    // There is a chance, that the entry of the table does NOT have type T,
    // in which case this will cause an error. As this is a mistake on the
    // side of the user, we don't really care.
    return std::get<T>(entries_.at(row).at(column));
  }

  // User defined conversion to `std::string`.
  explicit operator std::string() const;
  friend std::ostream& operator<<(std::ostream& os,
                                  const ResultTable& resultTable);

  /*
  @brief Adds a new empty row at the bottom of the table.
  */
  void addRow();

  /*
  @brief Delete the given row.

  @param rowIdx Uses matrix coordinates.
  */
  void deleteRow(const size_t rowIdx);

  /*
  The number of rows.
  */
  size_t numRows() const;

  /*
  The number of columns.
  */
  size_t numColumns() const;

  // JSON serialization.
  friend void to_json(nlohmann::json& j, const ResultTable& resultTable);

 private:
  /*
  @brief Sets the member variables for the constructure functions.

  @param descriptor A string to identify this instance in json format later.
  @param descriptorForLog A string to identify this instance in the log.
  @param rowNames The names for the rows. The amount of rows in this table is
  equal to the amount of row names. Important: This first column will be filled
  with those names.
  @param columnNames The names for the columns. The amount of columns in this
  table is equal to the amount of column names.
  */
  void init(const std::string& descriptor, std::string descriptorForLog,
            const std::vector<std::string>& rowNames,
            const std::vector<std::string>& columnNames);
};

// Describes a group of `ResultEntry` and `ResultTable`.
class ResultGroup : public BenchmarkMetadataGetter {
  // Needed for identifying groups.
  std::string descriptor_;

  /*
  A quick explanation, **why** this class uses pointers:
  New members are created in place and then a reference to the new member
  returned. This returning of a reference is the sole reason for the usage of
  pointers.
  Otherwise adding more entries to the vectors, could lead to all
  previous references being made invalid, because a vector had to re-allocate
  memory. If the entries are pointers to the objects, the references to the
  object stay valid and we don't have this problem.
  */
  // Members of the group.
  std::vector<ad_utility::CopyableUniquePtr<ResultEntry>> resultEntries_;
  std::vector<ad_utility::CopyableUniquePtr<ResultTable>> resultTables_;

  // Needed for testing purposes.
  FRIEND_TEST(BenchmarkMeasurementContainerTest, ResultGroup);
  FRIEND_TEST(BenchmarkMeasurementContainerTest, ResultGroupDeleteMember);

 public:
  /*
  @brief Creates an empty group of `ResultEntry`s.

  @param descriptor A string to identify this instance in json format later.
  */
  explicit ResultGroup(const std::string& descriptor)
      : descriptor_{descriptor} {}

  /*
  @brief Adds a new instance of `ResultEntry` to the group.

  @tparam Function Lambda function with no function arguments and returns void.

  @param descriptor A string to identify this instance in json format later.
  @param functionToMeasure The function, who's execution time will be
  measured and saved.
  */
  template <std::invocable Function>
  ResultEntry& addMeasurement(const std::string& descriptor,
                              const Function& functionToMeasure) {
    resultEntries_.push_back(ad_utility::make_copyable_unique<ResultEntry>(
        descriptor, absl::StrCat(descriptor, " of group ", descriptor_),
        functionToMeasure));
    return (*resultEntries_.back());
  }

  /*
  Delete the given `ResultEntry` from the group. Note: Because the group has
  ownership of all contained entries,  this will invalidate the argument
  `ResultEntry`.
  */
  void deleteMeasurement(ResultEntry& entry);

  /*
  @brief Adds a new instance of `ResultTable` to the group.

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
  Delete the given `ResultTable` from the group. Note: Because the group has
  ownership of all contained entries, this will invalidate the argument
  `ResultTable`.
  */
  void deleteTable(ResultTable& table);

  // User defined conversion to `std::string`.
  explicit operator std::string() const;
  friend std::ostream& operator<<(std::ostream& os,
                                  const ResultGroup& resultGroup);

  // JSON serialization.
  friend void to_json(nlohmann::json& j, const ResultGroup& resultGroup);

 private:
  // The implementation for the general deletion of entries.
  template <ad_utility::SameAsAny<ResultEntry, ResultTable> T>
  void deleteEntryImpl(T& entry);
};

}  // namespace ad_benchmark
