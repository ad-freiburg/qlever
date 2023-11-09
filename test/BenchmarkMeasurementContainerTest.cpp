// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <chrono>

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"

namespace ad_benchmark {
/// Create a lambda that waits the given amount of time.
static auto createWaitLambda(std::chrono::milliseconds waitDuration) {
  return [waitDuration]() {
    auto end = std::chrono::steady_clock::now() + waitDuration;
    while (std::chrono::steady_clock::now() < end)
      ;
  };
}

using namespace std::chrono_literals;

TEST(BenchmarkMeasurementContainerTest, ResultEntry) {
  // There's really no special cases.
  const std::string entryDescriptor{"entry"};
  // The function should just wait 0.01 seconds.
  constexpr auto waitTime = 10ms;

  // The normal constructor.
  ResultEntry entryNormalConstructor(entryDescriptor,
                                     createWaitLambda(waitTime));

  ASSERT_EQ(entryNormalConstructor.descriptor_, entryDescriptor);

  // The time measurements can't be 100% accurate, so we give it a 'window'.
  ASSERT_NEAR(waitTime.count() / 1000.0, entryNormalConstructor.measuredTime_,
              0.01);

  // The constructor with custom log descriptor.
  ResultEntry entryLogConstructor(entryDescriptor, "t",
                                  createWaitLambda(waitTime));

  // The time measurements can't be 100% accurate, so we give it a 'window'.
  ASSERT_NEAR(waitTime.count() / 1000.0, entryLogConstructor.measuredTime_,
              0.01);
}

TEST(BenchmarkMeasurementContainerTest, ResultGroup) {
  // The function should just wait 0.01 seconds.
  constexpr auto waitTime = 10ms;
  // There's really no special cases.
  ResultGroup group("group");

  ASSERT_EQ(group.descriptor_, "group");
  ASSERT_EQ(group.resultEntries_.size(), 0);
  ASSERT_EQ(group.resultTables_.size(), 0);

  // Adding a measurement and checking, if it was added correctly.
  ResultEntry& entry =
      group.addMeasurement("new entry", createWaitLambda(waitTime));

  ASSERT_EQ(group.resultEntries_.size(), 1);
  ASSERT_EQ(entry.descriptor_, "new entry");

  // The time measurements can't be 100% accurate, so we give it a 'window'.
  ASSERT_NEAR(waitTime.count() / 1000.0, entry.measuredTime_, 0.01);

  // Adding a table and checking, if it was added correctly.
  const std::vector<std::string> rowNames = {"row1", "row2"};
  const std::vector<std::string> columnNames = {"column1"};
  ResultTable& table = group.addTable("table", rowNames, columnNames);

  ASSERT_EQ(group.resultTables_.size(), 1);
  ASSERT_EQ(table.descriptor_, "table");
  ASSERT_EQ(table.numRows(), 2);
  ASSERT_EQ(table.numColumns(), 1);
  ASSERT_EQ(table.getEntry<std::string>(0, 0), rowNames.at(0));
  ASSERT_EQ(table.getEntry<std::string>(1, 0), rowNames.at(1));
}

TEST(BenchmarkMeasurementContainerTest, ResultTable) {
  // Looks, if the general form is correct.
  auto checkForm = [](const ResultTable& table, const std::string& name,
                      const std::string& descriptorForLog,
                      const std::vector<std::string>& rowNames,
                      const std::vector<std::string>& columnNames) {
    ASSERT_EQ(name, table.descriptor_);
    ASSERT_EQ(descriptorForLog, table.descriptorForLog_);
    ASSERT_EQ(columnNames, table.columnNames_);
    ASSERT_EQ(rowNames.size(), table.numRows());
    ASSERT_EQ(columnNames.size(), table.numColumns());

    // Row names in the first column.
    for (size_t row = 0; row < rowNames.size(); row++) {
      ASSERT_EQ(rowNames.at(row), table.getEntry<std::string>(row, 0));
    }
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
  Special case: A table with no columns. Should throw an exception
  on creation, because you can't add columns after creation and a table without
  columns is quite the stupid idea. Additionally, operations on such an empty
  table can create segmentation faults. The string conversion of `ResultTable`
  uses `std::ranges::max`, which really doesn't play well with empty vectors.
  */
  ASSERT_ANY_THROW(ResultTable("1 by 0 table", {"Test"}, {}));

  // In the same sense, because you can add rows after creation, a table without
  // rows should be creatable.
  ASSERT_NO_THROW(ResultTable("0 by 1 table", {}, {"Test"}));

  // Normal case.
  const std::vector<std::string> rowNames{"row1", "row2"};
  const std::vector<std::string> columnNames{"rowNames", "column1", "column2"};
  ResultTable table("My table", rowNames, columnNames);

  // Was it created correctly?
  checkForm(table, "My table", "My table", rowNames, columnNames);

  // Add measured function to it.
  table.addMeasurement(0, 1, createWaitLambda(10ms));

  // Set custom entries.
  table.setEntry(0, 2, 4.9f);
  table.setEntry(1, 1, "Custom entry");

  // Check the entries.
  checkRow(table, 0, std::string{"row1"}, 0.01f, 4.9f);
  checkRow(table, 1, std::string{"row2"}, std::string{"Custom entry"});
  checkNeverSet(table, 1, 2);

  // Trying to get entries with the wrong type, should cause an error.
  ASSERT_ANY_THROW(table.getEntry<std::string>(0, 2));
  ASSERT_ANY_THROW(table.getEntry<float>(1, 1));

  // Can we add a new row, without changing things?
  table.addRow();
  table.setEntry(2, 0, std::string{"row3"});
  checkForm(table, "My table", "My table", {"row1", "row2", "row3"},
            columnNames);
  checkRow(table, 0, std::string{"row1"}, 0.01f, 4.9f);
  checkRow(table, 1, std::string{"row2"}, std::string{"Custom entry"});

  // Are the entries of the new row empty?
  checkNeverSet(table, 2, 1);
  checkNeverSet(table, 2, 2);

  // To those new fields work like the old ones?
  table.addMeasurement(2, 1, createWaitLambda(29ms));
  table.setEntry(2, 2, "Custom entry #2");
  checkRow(table, 2, std::string{"row3"}, 0.029f,
           std::string{"Custom entry #2"});

  // Does the constructor for the custom log name work?
  checkForm(ResultTable("My table", "T", rowNames, columnNames), "My table",
            "T", rowNames, columnNames);
}
}  // namespace ad_benchmark
