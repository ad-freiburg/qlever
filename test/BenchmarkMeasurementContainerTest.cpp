// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <chrono>
#include <type_traits>
#include <variant>

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "util/Timer.h"
#include "util/json.h"

namespace ad_benchmark {
/*
@brief Creates a lambda, that waits the given amount of milliseconds,
before finishing. Note: 1000 milliseconds are 1 second.
*/
static auto createWaitLambda(const size_t& waitDuration) {
  return [&waitDuration]() {
    // This will measure, if at least the given amount of time has passed.
    ad_utility::TimeoutTimer timer(std::chrono::milliseconds{waitDuration},
                                   ad_utility::Timer::InitialStatus::Started);
    // Just do nothing, while waiting.
    while (!timer.hasTimedOut())
      ;
  };
}

TEST(BenchmarkMeasurementContainerTest, ResultEntry) {
  // There's really no special cases.
  const std::string entryDescriptor{"entry"};
  // The function should just wait 0.1 seconds.
  constexpr size_t waitTimeinMillieseconds = 100;

  ResultEntry entry(entryDescriptor, createWaitLambda(waitTimeinMillieseconds));

  ASSERT_EQ(entry.descriptor_, entryDescriptor);

  // The time measurements can't be 100% accurat, so we give it a 'window'.
  ASSERT_NEAR(waitTimeinMillieseconds / 1000.0, entry.measuredTime_, 0.01);
}

TEST(BenchmarkMeasurementContainerTest, ResultGroup) {
  // There's really no special cases.
  ResultGroup group("group");

  ASSERT_EQ(group.descriptor_, "group");
  ASSERT_EQ(group.entries_.size(), 0);

  // Adding a measurement and checking, if it was added correctly.
  ResultEntry& entry = group.addMeasurement("new entry", createWaitLambda(100));

  ASSERT_EQ(group.entries_.size(), 1);
  ASSERT_EQ(entry.descriptor_, "new entry");

  // The time measurements can't be 100% accurat, so we give it a 'window'.
  ASSERT_NEAR(0.1, entry.measuredTime_, 0.01);
}

TEST(BenchmarkMeasurementContainerTest, ResultTable) {
  // Looks, if the general form is correct.
  auto checkForm = [](const ResultTable& table, const std::string& name,
                      const std::vector<std::string>& rowNames,
                      const std::vector<std::string>& columnNames) {
    ASSERT_EQ(name, table.descriptor_);
    ASSERT_EQ(rowNames, table.rowNames_);
    ASSERT_EQ(columnNames, table.columnNames_);
    ASSERT_EQ(rowNames.size(), table.numRows());
    ASSERT_EQ(columnNames.size(), table.numColumns());
  };

  // Calls the correct assert function based on type.
  auto assertEqual = []<typename T>(const T& a, const T& b) {
    if constexpr (std::is_floating_point_v<T>) {
      ASSERT_NEAR(a, b, 0.01);
    } else {
      ASSERT_EQ(a, b);
    }
  };

  // Checks, if a table entry was never set.
  auto checkNeverSet = [](const ResultTable& table, const size_t& row,
                          const size_t& column) {
    // Is it empty?
    ASSERT_TRUE(std::holds_alternative<std::monostate>(
        table.entries_.at(row).at(column)));

    // Does trying to access it anyway cause an error?
    ASSERT_ANY_THROW(table.getEntry<std::string>(row, column));
    ASSERT_ANY_THROW(table.getEntry<float>(row, column));
  };

  /*
  Check the content of a tables row.
  */
  auto checkRow = [&assertEqual](const ResultTable& table,
                                 const size_t& rowNumber,
                                 const auto&... wantedContent) {
    size_t column = 0;
    auto check = [&table, &rowNumber, &column,
                  &assertEqual](const auto& wantedContent) mutable {
      assertEqual(wantedContent,
                  table.getEntry<std::decay_t<decltype(wantedContent)>>(
                      rowNumber, column++));
    };
    ((check(wantedContent)), ...);
  };
  /*
  Special case: A table with no rows, or columns. Should throw an exception
  on creation, because it's quite the stupid idea.
  Additionally, operations on such an empty table can create segmentation
  faults. The string conversion of `ResultTable` uses `std::ranges::max`, which
  really doesn't play well with empty vectors.
  */
  ASSERT_ANY_THROW(ResultTable("0 by 0 table", {}, {}));

  // Normal case.
  const std::vector<std::string> rowNames{"row1", "row2"};
  const std::vector<std::string> columnNames{"column1", "column2"};
  ResultTable table("My table", rowNames, columnNames);

  // Was it created correctly?
  checkForm(table, "My table", rowNames, columnNames);

  // Add measured function to it.
  table.addMeasurement(0, 0, createWaitLambda(100));

  // Set custom entries.
  table.setEntry(0, 1, (float)4.9);
  table.setEntry(1, 0, "Custom entry");

  // Check the entries.
  checkRow(table, 0, static_cast<float>(0.1), static_cast<float>(4.9));
  checkRow(table, 1, std::string{"Custom entry"});
  checkNeverSet(table, 1, 1);

  // Trying to get entries with the wrong type, should cause an error.
  ASSERT_ANY_THROW(table.getEntry<std::string>(0, 1));
  ASSERT_ANY_THROW(table.getEntry<float>(1, 0));

  // Can we add a new row, without changing things?
  table.addRow("row3");
  checkForm(table, "My table", {"row1", "row2", "row3"}, columnNames);
  checkRow(table, 0, static_cast<float>(0.1), static_cast<float>(4.9));
  checkRow(table, 1, std::string{"Custom entry"});

  // Are the entries of the new row empty?
  checkNeverSet(table, 2, 0);
  checkNeverSet(table, 2, 1);

  // To those new fields work like the old ones?
  table.addMeasurement(2, 0, createWaitLambda(290));
  table.setEntry(2, 1, "Custom entry #2");
  checkRow(table, 2, static_cast<float>(0.29), std::string{"Custom entry #2"});
}
}  // namespace ad_benchmark
