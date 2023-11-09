// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "util/Exception.h"

using namespace std::chrono_literals;
using namespace std::string_literals;

namespace ad_benchmark {
/// Create a lambda that waits the given amount of time.
static auto createWaitLambda(std::chrono::milliseconds waitDuration) {
  return [waitDuration]() {
    auto end = std::chrono::steady_clock::now() + waitDuration;
    while (std::chrono::steady_clock::now() < end)
      ;
  };
}

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

/*
@brief Call the function with each of the alternatives in
`ad_benchmark::ResultTable::EntryType`, except `std::monostate`, as template
parameter.

@tparam Function The loop body should be a templated function, with one
`typename` template argument and no more. It also shouldn't take any function
arguments. Should be passed per deduction.
*/
template <typename Function>
static void doForTypeInResultTableEntryType(Function function) {
  ad_utility::ConstexprForLoop(
      std::make_index_sequence<std::variant_size_v<ResultTable::EntryType>>{},
      [&function]<size_t index, typename IndexType = std::variant_alternative_t<
                                    index, ResultTable::EntryType>>() {
        // `std::monostate` is not important for these kinds of tests.
        if constexpr (!ad_utility::isSimilar<IndexType, std::monostate>) {
          function.template operator()<IndexType>();
        }
      });
}

// Helper function for creating `ad_benchmark::ResultTable::EntryType` dummy
// values.
template <typename Type>
requires ad_utility::isTypeContainedIn<Type, ResultTable::EntryType>
static Type createDummyValueEntryType() {
  if constexpr (ad_utility::isSimilar<Type, float>) {
    return 4.2f;
  } else if constexpr (ad_utility::isSimilar<Type, std::string>) {
    return "test"s;
  } else if constexpr (ad_utility::isSimilar<Type, bool>) {
    return true;
  } else if constexpr (ad_utility::isSimilar<Type, size_t>) {
    return 17361644613946UL;
  } else if constexpr (ad_utility::isSimilar<Type, int>) {
    return -42;
  } else {
    // Not a supported type.
    AD_FAIL();
  }
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
    doForTypeInResultTableEntryType([&table, &row, &column]<typename T>() {
      ASSERT_ANY_THROW(table.getEntry<T>(row, column));
    });
  };

  /*
  Check the content of a tables row.
  */
  auto checkRow = [&assertEqual](const ResultTable& table,
                                 const size_t& rowNumber,
                                 const auto&... wantedContent) {
    size_t column = 0;
    auto check = [&table, &rowNumber, &column,
                  &assertEqual]<typename T>(const T& wantedContent) mutable {
      // `getEntry` should ONLY work with `T`
      doForTypeInResultTableEntryType(
          [&table, &rowNumber, &column, &assertEqual,
           &wantedContent]<typename PossiblyWrongType>() {
            if constexpr (ad_utility::isSimilar<PossiblyWrongType, T>) {
              assertEqual(wantedContent,
                          table.getEntry<std::decay_t<T>>(rowNumber, column));
            } else {
              ASSERT_ANY_THROW(
                  table.getEntry<PossiblyWrongType>(rowNumber, column));
            }
          });
      column++;
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

  // Does the constructor for the custom log name work?
  checkForm(ResultTable("My table", "T", rowNames, columnNames), "My table",
            "T", rowNames, columnNames);

  // Add measured function to it.
  table.addMeasurement(0, 1, createWaitLambda(10ms));

  // Check, if it works with custom entries.
  doForTypeInResultTableEntryType(
      [&table, &checkNeverSet, &checkRow]<typename T1>() {
        doForTypeInResultTableEntryType([&table, &checkNeverSet,
                                         &checkRow]<typename T2>() {
          // Set custom entries.
          table.setEntry(0, 2, createDummyValueEntryType<T1>());
          table.setEntry(1, 1, createDummyValueEntryType<T2>());

          // Check the entries.
          checkRow(table, 0, "row1"s, 0.01f, createDummyValueEntryType<T1>());
          checkRow(table, 1, "row2"s, createDummyValueEntryType<T2>());
          checkNeverSet(table, 1, 2);
        });
      });

  // For keeping track of the new row names.
  std::vector<std::string> addRowRowNames(rowNames);
  // Testing `addRow`.
  doForTypeInResultTableEntryType([&table, &checkNeverSet, &checkRow,
                                   &checkForm, &columnNames,
                                   &addRowRowNames]<typename T>() {
    // What is the index of the new row?
    const size_t indexNewRow = table.numRows();

    // Can we add a new row, without changing things?
    table.addRow();
    addRowRowNames.emplace_back(absl::StrCat("row", indexNewRow + 1));
    table.setEntry(indexNewRow, 0, addRowRowNames.back());
    checkForm(table, "My table", "My table", addRowRowNames, columnNames);
    checkRow(table, 0, "row1"s, 0.01f);
    checkRow(table, 1, "row2"s);
    checkNeverSet(table, 1, 2);

    // Are the entries of the new row empty?
    checkNeverSet(table, indexNewRow, 1);
    checkNeverSet(table, indexNewRow, 2);

    // To those new fields work like the old ones?
    table.addMeasurement(indexNewRow, 1, createWaitLambda(29ms));
    table.setEntry(indexNewRow, 2, createDummyValueEntryType<T>());
    checkRow(table, indexNewRow, addRowRowNames.back(), 0.029f,
             createDummyValueEntryType<T>());
  });

  // Just a simple existence test for printing.
  const auto tableAsString = static_cast<std::string>(table);
}
}  // namespace ad_benchmark
