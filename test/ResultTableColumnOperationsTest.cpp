// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <chrono>
#include <concepts>
#include <string>
#include <variant>

#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/util/ResultTableColumnOperations.h"
#include "../test/util/BenchmarkMeasurementContainerHelpers.h"
#include "../test/util/GTestHelpers.h"
#include "gmock/gmock.h"
#include "util/Exception.h"
#include "util/Random.h"
#include "util/TypeTraits.h"

namespace ad_benchmark {
// How many rows should the test tables have?
constexpr size_t NUM_ROWS = 10;

// Does `T` support addition?
template <typename T>
concept SupportsAddition = requires(T a, T b) {
  { a + b } -> std::same_as<T>;
};

/*
@brief Create a table for testing purpose.

@param numRows, numColumns How many rows and columns the created table should
have.
@param columnsWithDummyValues The designated columns are filled with dummy
values via `createDummyValueEntryType`.
*/
template <typename... ColumnTypes>
static ResultTable createTestTable(
    const size_t numRows, const size_t numColumns,
    const ColumnNumWithType<ColumnTypes>&... columnsWithDummyValues) {
  ResultTable table("", std::vector<std::string>(numRows, ""),
                    std::vector<std::string>(numColumns, ""));
  for (size_t i = 0; i < table.numRows(); i++) {
    (table.setEntry(i, columnsWithDummyValues.columnNum_,
                    createDummyValueEntryType<ColumnTypes>()),
     ...);
  }
  return table;
}

// Compare the column of a `ResultTable` with a `std::vector`.
template <typename T>
static void compareToColumn(
    const std::vector<T>& expectedContent,
    const ResultTable& tableToCompareAgainst,
    const ColumnNumWithType<T>& columnsToCompareAgainst,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "compareToColumn")};

  // Compare every entry with the fitting comparison function.
  AD_CONTRACT_CHECK(expectedContent.size() == tableToCompareAgainst.numRows());
  for (size_t i = 0; i < expectedContent.size(); i++) {
    if constexpr (std::floating_point<T>) {
      ASSERT_FLOAT_EQ(expectedContent.at(i),
                      tableToCompareAgainst.getEntry<T>(
                          i, columnsToCompareAgainst.columnNum_));
    } else if constexpr (ad_utility::isSimilar<T, std::string>) {
      ASSERT_STREQ(expectedContent.at(i).c_str(),
                   tableToCompareAgainst
                       .getEntry<T>(i, columnsToCompareAgainst.columnNum_)
                       .c_str());
    } else {
      ASSERT_EQ(expectedContent.at(i),
                tableToCompareAgainst.getEntry<T>(
                    i, columnsToCompareAgainst.columnNum_));
    }
  }
}

/*
@brief Test the general exception cases for a function of
`ResultTableColumnOperations`, that takes two input columns.

@param callTransform Lambda, that transforms the call arguments into arguments
for the function, that you want to test. Must have the signature `ResultTable*
,const ColumnNumWithType<ColumnReturnType>& columnToPutResultIn, const
ColumnNumWithType<ColumnInputTypeOne> inputColumnOne, const
ColumnNumWithType<ColumnInputTypeTwo> inputColumnTwo`.
*/
static void generalExceptionTestTwoInputColumns(
    const auto& callTransform,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "generalExceptionTestTwoInputColumns")};

  doForTypeInResultTableEntryType([&callTransform]<typename T>() {
    // A call with a `ResultTable`, who has no rows, is valid.
    auto table{createTestTable(0, 3, ColumnNumWithType<T>{0},
                               ColumnNumWithType<T>{1})};
    ASSERT_NO_THROW(callTransform(&table, ColumnNumWithType<T>{2},
                                  ColumnNumWithType<T>{1},
                                  ColumnNumWithType<T>{0}));

    // A call, in which the result column is also an input column, is valid.
    table = createTestTable(NUM_ROWS, 3, ColumnNumWithType<T>{0},
                            ColumnNumWithType<T>{1});
    ASSERT_NO_THROW(callTransform(&table, ColumnNumWithType<T>{1},
                                  ColumnNumWithType<T>{1},
                                  ColumnNumWithType<T>{0}));

    // Exception tests.
    // A column contains more than 1 type.
    table = createTestTable(std::variant_size_v<ResultTable::EntryType> - 1, 3);
    doForTypeInResultTableEntryType([row = 0, &table]<typename T2>() mutable {
      table.setEntry(row++, 0, createDummyValueEntryType<T2>());
    });
    ASSERT_ANY_THROW(callTransform(&table, ColumnNumWithType<T>{1},
                                   ColumnNumWithType<T>{0},
                                   ColumnNumWithType<T>{2}));

    // Wrong input column type.
    table = createTestTable(NUM_ROWS, 3, ColumnNumWithType<T>{0},
                            ColumnNumWithType<T>{1});
    doForTypeInResultTableEntryType([&table,
                                     &callTransform]<typename WrongType>() {
      if constexpr (!ad_utility::isSimilar<WrongType, T>) {
        ASSERT_ANY_THROW(callTransform(&table, ColumnNumWithType<WrongType>{2},
                                       ColumnNumWithType<WrongType>{1},
                                       ColumnNumWithType<WrongType>{0}));
      }
    });

    // Column is outside boundaries.
    table = createTestTable(NUM_ROWS, 3, ColumnNumWithType<T>{0},
                            ColumnNumWithType<T>{1}, ColumnNumWithType<T>{2});
    ASSERT_ANY_THROW(callTransform(&table, ColumnNumWithType<T>{10},
                                   ColumnNumWithType<T>{1},
                                   ColumnNumWithType<T>{2}));
    ASSERT_ANY_THROW(callTransform(&table, ColumnNumWithType<T>{0},
                                   ColumnNumWithType<T>{10},
                                   ColumnNumWithType<T>{2}));
    ASSERT_ANY_THROW(callTransform(&table, ColumnNumWithType<T>{0},
                                   ColumnNumWithType<T>{1},
                                   ColumnNumWithType<T>{20}));

    // One column is used more than once as an input.
    table = createTestTable(NUM_ROWS, 2, ColumnNumWithType<T>{0},
                            ColumnNumWithType<T>{1});
    ASSERT_ANY_THROW(callTransform(&table, ColumnNumWithType<T>{1},
                                   ColumnNumWithType<T>{0},
                                   ColumnNumWithType<T>{0}));
  });
}

/*
@brief Test the general exception cases for a function of
`ResultTableColumnOperations`, that takes an unlimited number of input columns
and has a minimum of two input columns.

@param callTransform Lambda, that transforms the call arguments into arguments
for the function, that you want to test. Must have the signature `ResultTable*
,const ColumnNumWithType<ColumnReturnType>& columnToPutResultIn, const
ColumnNumWithType<ColumnInputTypes>&... inputColumns`.
*/
static void generalExceptionTestUnlimitedInputColumns(
    const auto& callTransform,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{
      generateLocationTrace(l, "generalExceptionTestUnlimitedInputColumns")};

  // We can pass a lot to `generalExceptionTestTwoInputColumns`.
  generalExceptionTestTwoInputColumns(callTransform);

  doForTypeInResultTableEntryType([&callTransform]<typename T>() {
    // Column is outside boundaries.
    ResultTable table{createTestTable(
        NUM_ROWS, 4, ColumnNumWithType<T>{0}, ColumnNumWithType<T>{1},
        ColumnNumWithType<T>{2}, ColumnNumWithType<T>{3})};
    ASSERT_ANY_THROW(
        callTransform(&table, ColumnNumWithType<T>{10}, ColumnNumWithType<T>{1},
                      ColumnNumWithType<T>{2}, ColumnNumWithType<T>{3}));
    ASSERT_ANY_THROW(
        callTransform(&table, ColumnNumWithType<T>{0}, ColumnNumWithType<T>{10},
                      ColumnNumWithType<T>{2}, ColumnNumWithType<T>{3}));
    ASSERT_ANY_THROW(
        callTransform(&table, ColumnNumWithType<T>{0}, ColumnNumWithType<T>{1},
                      ColumnNumWithType<T>{20}, ColumnNumWithType<T>{3}));
    ASSERT_ANY_THROW(
        callTransform(&table, ColumnNumWithType<T>{0}, ColumnNumWithType<T>{1},
                      ColumnNumWithType<T>{2}, ColumnNumWithType<T>{30}));
  });
}

TEST(ResultTableColumnOperations, generateColumnWithColumnInput) {
  // How many rows should the test table have?
  constexpr size_t NUM_ROWS = 10;

  // A lambda, that copies on column into another.
  auto columnCopyLambda = [](const auto& d) { return d; };

  doForTypeInResultTableEntryType([&NUM_ROWS, &columnCopyLambda]<typename T>() {
    // Single parameter operators.
    // Two columns. Transcribe column 0 into column 1.
    ResultTable table{createTestTable(NUM_ROWS, 2, ColumnNumWithType<T>{0})};
    generateColumnWithColumnInput(&table, columnCopyLambda,
                                  ColumnNumWithType<T>{1},
                                  ColumnNumWithType<T>{0});
    compareToColumn(std::vector<T>(NUM_ROWS, createDummyValueEntryType<T>()),
                    table, ColumnNumWithType<T>{1});

    // Double parameter operators.
    // Different cases for different `T`.
    table = createTestTable(NUM_ROWS, 3, ColumnNumWithType<T>{0},
                            ColumnNumWithType<T>{1});
    if constexpr (ad_utility::isSimilar<T, bool>) {
      // Do XOR on the column content.
      generateColumnWithColumnInput(
          &table, [](const T& a, const T& b) { return a != b; },
          ColumnNumWithType<T>{2}, ColumnNumWithType<T>{0},
          ColumnNumWithType<T>{1});
      compareToColumn(std::vector<T>(NUM_ROWS, false), table,
                      ColumnNumWithType<T>{2});
    } else if constexpr (SupportsAddition<T>) {
      // Just do addition.
      generateColumnWithColumnInput(
          &table, [](const T& a, const T& b) { return a + b; },
          ColumnNumWithType<T>{2}, ColumnNumWithType<T>{0},
          ColumnNumWithType<T>{1});
      compareToColumn(
          std::vector<T>(NUM_ROWS, createDummyValueEntryType<T>() +
                                       createDummyValueEntryType<T>()),
          table, ColumnNumWithType<T>{2});
    } else {
      // We need more cases!
      AD_FAIL();
    };
  });

  // Exception tests.
  generalExceptionTestUnlimitedInputColumns([](ResultTable* table,
                                               const auto& columnToPutResultIn,
                                               const auto&... inputColumns) {
    generateColumnWithColumnInput(
        table,
        [](const auto& firstInput, const auto&...) { return firstInput; },
        columnToPutResultIn, inputColumns...);
  });
}

TEST(ResultTableColumnOperations, SumUpColumns) {
  // Normal tests.
  doForTypeInResultTableEntryType([]<typename T>() {
    // We only do tests on types, that support addition.
    if constexpr (SupportsAddition<T>) {
      // Minimal amount of columns.
      ResultTable table{createTestTable(NUM_ROWS, 3, ColumnNumWithType<T>{0},
                                        ColumnNumWithType<T>{1})};
      sumUpColumns(&table, ColumnNumWithType<T>{2}, ColumnNumWithType<T>{1},
                   ColumnNumWithType<T>{0});
      compareToColumn(
          std::vector<T>(NUM_ROWS, createDummyValueEntryType<T>() +
                                       createDummyValueEntryType<T>()),
          table, ColumnNumWithType<T>{2});

      // Test with more columns.
      table = createTestTable(NUM_ROWS, 10, ColumnNumWithType<T>{0},
                              ColumnNumWithType<T>{1}, ColumnNumWithType<T>{2},
                              ColumnNumWithType<T>{3}, ColumnNumWithType<T>{4},
                              ColumnNumWithType<T>{5}, ColumnNumWithType<T>{6},
                              ColumnNumWithType<T>{7}, ColumnNumWithType<T>{8});
      sumUpColumns(&table, ColumnNumWithType<T>{9}, ColumnNumWithType<T>{0},
                   ColumnNumWithType<T>{1}, ColumnNumWithType<T>{2},
                   ColumnNumWithType<T>{3}, ColumnNumWithType<T>{4},
                   ColumnNumWithType<T>{5}, ColumnNumWithType<T>{6},
                   ColumnNumWithType<T>{7}, ColumnNumWithType<T>{8});
      compareToColumn(
          std::vector<T>(NUM_ROWS, createDummyValueEntryType<T>() +
                                       createDummyValueEntryType<T>() +
                                       createDummyValueEntryType<T>() +
                                       createDummyValueEntryType<T>() +
                                       createDummyValueEntryType<T>() +
                                       createDummyValueEntryType<T>() +
                                       createDummyValueEntryType<T>() +
                                       createDummyValueEntryType<T>() +
                                       createDummyValueEntryType<T>()),
          table, ColumnNumWithType<T>{9});
    }
  });

  // Exception tests.
  generalExceptionTestUnlimitedInputColumns([](ResultTable* table,
                                               const auto& columnToPutResultIn,
                                               const auto&... inputColumns) {
    sumUpColumns(table, columnToPutResultIn, inputColumns...);
  });
}

TEST(ResultTableColumnOperations, calculateSpeedupOfColumn) {
  // Fill two columns so, that they have the desired speedup.
  auto fillColumnsForSpeedup = [](ResultTable* table, const float wantedSpeedup,
                                  const ColumnNumWithType<float>& columnOne,
                                  const ColumnNumWithType<float>& columnTwo) {
    /*
    For the math:
    wantedSpeedup = columnTwo / columnOne
    <=> wantedSpeeup * columnOne = columnTwo
    */
    for (size_t row = 0; row < table->numRows(); row++) {
      table->setEntry(row, columnOne.columnNum_, static_cast<float>(row + 1));
      table->setEntry(row, columnTwo.columnNum_, (row + 1) * wantedSpeedup);
    }
  };

  // Test things for a range of speedups.
  ql::ranges::for_each(
      std::array{2.f, 16.f, 73.696f, 4.2f},
      [&fillColumnsForSpeedup](const float wantedSpeedup,
                               ad_utility::source_location l =
                                   ad_utility::source_location::current()) {
        // For generating better messages, when failing a test.
        auto trace{generateLocationTrace(l, "testRangeOfSpeedups")};
        ResultTable table{createTestTable(NUM_ROWS, 10)};

        /*
        Needed for exception test, to choose a random entry inside a column.
        Note: The described range is inclusive.
        */
        ad_utility::SlowRandomIntGenerator<size_t> rowGenerator(
            0, table.numRows() - 1);

        // Test things through for all possible input and output columns.
        for (size_t outputColumn = 0; outputColumn < table.numColumns();
             outputColumn++) {
          for (size_t firstInputColumn = 0;
               firstInputColumn < table.numColumns(); firstInputColumn++) {
            for (size_t secondInputColumn = 0;
                 secondInputColumn < table.numColumns(); secondInputColumn++) {
              // The same column repeated as input is not allowed.
              if (firstInputColumn == secondInputColumn) {
                continue;
              }

              // Test, if things are calculated as wanted.
              fillColumnsForSpeedup(&table, wantedSpeedup, {firstInputColumn},
                                    {secondInputColumn});
              calculateSpeedupOfColumn(&table, {outputColumn},
                                       {firstInputColumn}, {secondInputColumn});
              compareToColumn(std::vector(NUM_ROWS, wantedSpeedup), table,
                              ColumnNumWithType<float>{outputColumn});

              // Exception test, if input values, that are smaller than 0, are
              // not allowed.
              const size_t firstInputColumnRow{rowGenerator()};
              const size_t secondInputColumnRow{rowGenerator()};
              const float firstInputColumnOldEntry{
                  table.getEntry<float>(firstInputColumnRow, firstInputColumn)};
              const float secondInputColumnOldEntry{table.getEntry<float>(
                  secondInputColumnRow, secondInputColumn)};
              auto doExceptionTest = [&table, &outputColumn, &firstInputColumn,
                                      &secondInputColumn]() {
                ASSERT_ANY_THROW(calculateSpeedupOfColumn(
                    &table, {outputColumn}, {firstInputColumn},
                    {secondInputColumn}));
              };

              // The actual test.
              // Only an error in the first column.
              table.setEntry(firstInputColumnRow, firstInputColumn,
                             -firstInputColumnOldEntry);
              doExceptionTest();
              table.setEntry(firstInputColumnRow, firstInputColumn, 0);
              doExceptionTest();

              // Only an error in the second column.
              table.setEntry(firstInputColumnRow, firstInputColumn,
                             firstInputColumnOldEntry);
              table.setEntry(secondInputColumnRow, secondInputColumn,
                             -secondInputColumnOldEntry);
              doExceptionTest();
              table.setEntry(secondInputColumnRow, secondInputColumn, 0);
              doExceptionTest();

              // Error in both the input columns.
              table.setEntry(firstInputColumnRow, firstInputColumn, 0);
              table.setEntry(secondInputColumnRow, secondInputColumn, 0);
              doExceptionTest();
              table.setEntry(firstInputColumnRow, firstInputColumn,
                             -firstInputColumnOldEntry);
              table.setEntry(secondInputColumnRow, secondInputColumn, 0);
              doExceptionTest();
              table.setEntry(firstInputColumnRow, firstInputColumn, 0);
              table.setEntry(secondInputColumnRow, secondInputColumn,
                             -secondInputColumnOldEntry);
              doExceptionTest();
              table.setEntry(firstInputColumnRow, firstInputColumn,
                             -firstInputColumnOldEntry);
              table.setEntry(secondInputColumnRow, secondInputColumn,
                             -secondInputColumnOldEntry);
              doExceptionTest();
            }
          }
        }
      });

  // General exception tests.
  generalExceptionTestTwoInputColumns(
      []<typename FirstType, typename SecondType>(
          ResultTable* table, const auto& columnToPutResultIn,
          const ColumnNumWithType<FirstType>& firstInputColumns,
          const ColumnNumWithType<SecondType>& secondInputColumns) {
        /*
        Unlike the other functions, `` only works with measured execution times.
        So, whenever the inputs are not for type `float`, we pass the
        responsibility to a trivial function.
        */
        if constexpr (std::same_as<FirstType, float> &&
                      std::same_as<SecondType, float>) {
          calculateSpeedupOfColumn(table, columnToPutResultIn,
                                   firstInputColumns, secondInputColumns);
        } else {
          sumUpColumns(table, columnToPutResultIn, firstInputColumns,
                       secondInputColumns);
        };
      });
}
}  // namespace ad_benchmark
