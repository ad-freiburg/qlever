// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (April of 2023,
// schlegea@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <string>

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../test/util/BenchmarkMeasurementContainerHelpers.h"
#include "../test/util/GTestHelpers.h"
#include "util/Exception.h"
#include "util/TypeTraits.h"

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
Check the content of a `Result` row.
*/
static void checkResultTableRow(const ResultTable& table,
                                const size_t& rowNumber,
                                const auto&... wantedContent) {
  // Calls the correct assert function based on type.
  auto assertEqual = [](const auto& a, const auto& b) {
    static_assert(std::is_same_v<decltype(a), decltype(b)>,
                  "Arguments shall be of the same type");
    using T = std::decay_t<decltype(a)>;
    if constexpr (std::is_floating_point_v<T>) {
      ASSERT_NEAR(a, b, 0.01);
    } else {
      ASSERT_EQ(a, b);
    }
  };

  size_t column = 0;
  auto check = [&table, &rowNumber, &column,
                &assertEqual](const auto& wantedContent) mutable {
    using T = decltype(wantedContent);
    // `getEntry` should ONLY work with `T`
    doForTypeInResultTableEntryType([&table, &rowNumber, &column, &assertEqual,
                                     &wantedContent](auto t) {
      using PossiblyWrongType = typename decltype(t)::type;
      if constexpr (ad_utility::isSimilar<PossiblyWrongType, T>) {
        assertEqual(wantedContent,
                    table.getEntry<std::decay_t<T>>(rowNumber, column));
      } else {
        ASSERT_ANY_THROW(table.getEntry<PossiblyWrongType>(rowNumber, column));
      }
    });
    column++;
  };
  ((check(wantedContent)), ...);
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

  // Checks, if a table entry was never set.
  auto checkNeverSet = [](const ResultTable& table, const size_t& row,
                          const size_t& column) {
    // Is it empty?
    ASSERT_TRUE(std::holds_alternative<std::monostate>(
        table.entries_.at(row).at(column)));

    // Does trying to access it anyway cause an error?
    doForTypeInResultTableEntryType([&table, &row, &column](auto t) {
      using T = typename decltype(t)::type;
      ASSERT_ANY_THROW(table.getEntry<T>(row, column));
    });
  };

  /*
  Special case: A table with no columns. Should throw an exception
  on creation, because you can't add columns after creation and a table without
  columns is quite the stupid idea. Additionally, operations on such an empty
  table can create segmentation faults. The string conversion of `Result`
  uses `ql::ranges::max`, which really doesn't play well with empty vectors.
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
  doForTypeInResultTableEntryType([&table, &checkNeverSet](auto t1) {
    using T1 = typename decltype(t1)::type;
    doForTypeInResultTableEntryType([&table, &checkNeverSet](auto t2) {
      using T2 = typename decltype(t2)::type;

      // Set custom entries.
      table.setEntry(0, 2, createDummyValueEntryType<T1>());
      table.setEntry(1, 1, createDummyValueEntryType<T2>());

      // Check the entries.
      checkResultTableRow(table, 0, "row1"s, 0.01f,
                          createDummyValueEntryType<T1>());
      checkResultTableRow(table, 1, "row2"s, createDummyValueEntryType<T2>());
      checkNeverSet(table, 1, 2);
    });
  });

  // For keeping track of the new row names.
  std::vector<std::string> addRowRowNames(rowNames);
  // Testing `addRow`.
  doForTypeInResultTableEntryType([&table, &checkNeverSet, &checkForm,
                                   &columnNames, &addRowRowNames](auto t) {
    using T = typename decltype(t)::type;

    // What is the index of the new row?
    const size_t indexNewRow = table.numRows();

    // Can we add a new row, without changing things?
    table.addRow();
    addRowRowNames.emplace_back(absl::StrCat("row", indexNewRow + 1));
    table.setEntry(indexNewRow, 0, addRowRowNames.back());
    checkForm(table, "My table", "My table", addRowRowNames, columnNames);
    checkResultTableRow(table, 0, "row1"s, 0.01f);
    checkResultTableRow(table, 1, "row2"s);
    checkNeverSet(table, 1, 2);

    // Are the entries of the new row empty?
    checkNeverSet(table, indexNewRow, 1);
    checkNeverSet(table, indexNewRow, 2);

    // To those new fields work like the old ones?
    table.addMeasurement(indexNewRow, 1, createWaitLambda(29ms));
    table.setEntry(indexNewRow, 2, createDummyValueEntryType<T>());
    checkResultTableRow(table, indexNewRow, addRowRowNames.back(), 0.029f,
                        createDummyValueEntryType<T>());
  });

  // Just a simple existence test for printing.
  const auto tableAsString = static_cast<std::string>(table);
}

TEST(BenchmarkMeasurementContainerTest, ResultTableEraseRow) {
  // Values of the test tables, that are always the same.
  const std::string testTableDescriptor{""};
  const std::string testTableDescriptorForLog{""};
  const std::vector<std::string> testTableColumnNames{{"column0"s, "column1"s}};

  // Looks, if the general form is correct.
  auto checkForm = [&testTableDescriptor, &testTableDescriptorForLog,
                    &testTableColumnNames](const ResultTable& table,
                                           const size_t numRows) {
    ASSERT_EQ(testTableDescriptor, table.descriptor_);
    ASSERT_EQ(testTableDescriptorForLog, table.descriptorForLog_);
    ASSERT_EQ(testTableColumnNames, table.columnNames_);
    ASSERT_EQ(numRows, table.numRows());
    ASSERT_EQ(testTableColumnNames.size(), table.numColumns());
  };

  // Create a test table with the given amount of rows and every row filled with
  // `rowNumber, rowNumber+1`.
  auto createTestTable = [&testTableColumnNames,
                          &testTableDescriptor](const size_t numRows) {
    ResultTable table(testTableDescriptor, std::vector(numRows, ""s),
                      testTableColumnNames);
    for (size_t i = 0; i < numRows; i++) {
      table.setEntry(i, 0, i);
      table.setEntry(i, 1, i + 1);
    }
    return table;
  };

  /*
  Test, if everything works as intended, when you delete a single row once.

  @param numRows How many rows should the created test table have?
  @param rowToDelete The number of the row, which will be deleted.
  */
  auto singleEraseOperationTest =
      [&createTestTable, &checkForm, &testTableColumnNames](
          const size_t numRows, const size_t rowToDelete,
          ad_utility::source_location l =
              ad_utility::source_location::current()) {
        // For generating better messages, when failing a test.
        auto trace{generateLocationTrace(l, "singleEraseOperationTest")};
        ResultTable table{createTestTable(numRows)};

        // Delete the row and check, if everything changed as expected.
        table.deleteRow(rowToDelete);
        checkForm(table, numRows - 1);
        for (size_t row = 0; row < rowToDelete; row++) {
          checkResultTableRow(table, row, row, row + 1);
        }
        for (size_t row = rowToDelete; row < table.numRows(); row++) {
          checkResultTableRow(table, row, row + 1, row + 2);
        }

        // Test, if `addRow` works afterwards.
        table.addRow();
        checkForm(table, numRows);
        table.setEntry(numRows - 1, 0, testTableColumnNames.at(0));
        table.setEntry(numRows - 1, 1, testTableColumnNames.at(1));
        for (size_t row = 0; row < rowToDelete; row++) {
          checkResultTableRow(table, row, row, row + 1);
        }
        for (size_t row = rowToDelete; row < table.numRows() - 1; row++) {
          checkResultTableRow(table, row, row + 1, row + 2);
        }
        checkResultTableRow(table, numRows - 1, testTableColumnNames.at(0),
                            testTableColumnNames.at(1));

        // Test, if trying to delete a non-existent row results in an error.
        ASSERT_ANY_THROW(table.deleteRow(table.numRows()));
      };
  for (size_t rowToDelete = 0; rowToDelete < 50; rowToDelete++) {
    singleEraseOperationTest(50, rowToDelete);
  }
}

TEST(BenchmarkMeasurementContainerTest, ResultGroupDeleteMember) {
  /*
  Add the given number of dummy `ResultEntry`s and dummy `Result`s to the
  given group.
  */
  auto addDummyMembers = [](ResultGroup* group, const size_t numOfEntries) {
    for (size_t i = 0; i < numOfEntries; i++) {
      group->addMeasurement("d", []() { return 4; });
      group->addTable("c", {"row1"}, {"column1"});
    }
  };

  /*
  Test, if everything works as intended, when you delete a single member once.

  @param numMembers How many dummy members of each possible type should be added
  to the test group?
  @param memberDeletionPoint The number of dummy members, that are added, before
  we add the member, that will be later deleted.
  */
  auto singleDeleteTest = [&addDummyMembers](
                              const size_t numMembers,
                              const size_t memberDeletionPoint,
                              ad_utility::source_location l =
                                  ad_utility::source_location::current()) {
    AD_CONTRACT_CHECK(memberDeletionPoint < numMembers);
    // For generating better messages, when failing a test.
    auto trace{generateLocationTrace(l, "singleDeleteTest")};
    ResultGroup group("");

    // Add the dummy members.
    addDummyMembers(&group, memberDeletionPoint);
    ResultEntry* const entryToDelete{
        &group.addMeasurement("d", []() { return 4; })};
    ResultTable* const tableToDelete{
        &group.addTable("c", {"row1"}, {"column1"})};
    addDummyMembers(&group, numMembers - (memberDeletionPoint + 1));

    /*
    Delete the entries and check, if there are no longer inside the group. Note,
    that this will make the pointer invalid, but things should be alright, as
    long as we never dereference them again.
    */
    group.deleteMeasurement(*entryToDelete);
    group.deleteTable(*tableToDelete);
    auto getAddressOfObject = [](const auto& obj) { return obj.get(); };
    ASSERT_TRUE(ql::ranges::find(group.resultEntries_, entryToDelete,
                                 getAddressOfObject) ==
                std::end(group.resultEntries_));
    ASSERT_TRUE(ql::ranges::find(group.resultTables_, tableToDelete,
                                 getAddressOfObject) ==
                std::end(group.resultTables_));

    // Test, if trying to delete a non-existent member results in an error.
    ResultEntry nonMemberEntry("d", []() { return 4; });
    ASSERT_ANY_THROW(group.deleteMeasurement(nonMemberEntry));
    ResultTable nonMemberTable("c", {"row1"}, {"column1"});
    ASSERT_ANY_THROW(group.deleteTable(nonMemberTable));
  };
  for (size_t memberDeletionPoint = 0; memberDeletionPoint < 50;
       memberDeletionPoint++) {
    singleDeleteTest(50, memberDeletionPoint);
  }
}
}  // namespace ad_benchmark
