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
#include "util/Exception.h"
#include "util/TypeTraits.h"

namespace ad_benchmark {

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

  // Compare every entry with the fitting comparsion function.
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

TEST(ResultTableColumnOperations, generateColumnWithColumnInput) {
  // How many rows should the test table have?
  constexpr size_t NUM_ROWS = 10;

  // A lambda, that copies on column into another.
  auto columnCopyLambda = [](const auto& d) { return d; };

  // A lambda, that always return true.
  auto trivialLambda = [](const auto&...) { return true; };

  doForTypeInResultTableEntryType([&NUM_ROWS, &columnCopyLambda,
                                   &trivialLambda]<typename T>() {
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

    // A call with a `ResultTable`, who has no rows, is valid.
    table = createTestTable(0, 2, ColumnNumWithType<T>{0});
    generateColumnWithColumnInput(
        &table, [](const auto& d) { return d; }, ColumnNumWithType<T>{1},
        ColumnNumWithType<T>{0});

    // Exception tests.
    // A column contains more than 1 type.
    table = createTestTable(std::variant_size_v<ResultTable::EntryType> - 1, 3);
    doForTypeInResultTableEntryType([row = 0, &table]<typename T2>() mutable {
      table.setEntry(row++, 0, createDummyValueEntryType<T2>());
    });
    ASSERT_ANY_THROW(generateColumnWithColumnInput(&table, columnCopyLambda,
                                                   ColumnNumWithType<T>{1},
                                                   ColumnNumWithType<T>{0}));

    // Wrong input column type.
    table = createTestTable(NUM_ROWS, 2, ColumnNumWithType<T>{0});
    doForTypeInResultTableEntryType(
        [&table, &trivialLambda]<typename WrongType>() {
          if constexpr (!ad_utility::isSimilar<WrongType, T>) {
            ASSERT_ANY_THROW(generateColumnWithColumnInput(
                &table, trivialLambda, ColumnNumWithType<bool>{1},
                ColumnNumWithType<WrongType>{0}));
          }
        });

    // Column is outside boundaries.
    table = createTestTable(NUM_ROWS, 2, ColumnNumWithType<T>{0},
                            ColumnNumWithType<T>{1});
    ASSERT_ANY_THROW(generateColumnWithColumnInput(&table, columnCopyLambda,
                                                   ColumnNumWithType<T>{2},
                                                   ColumnNumWithType<T>{0}));
    ASSERT_ANY_THROW(generateColumnWithColumnInput(&table, columnCopyLambda,
                                                   ColumnNumWithType<T>{2},
                                                   ColumnNumWithType<T>{1}));
    ASSERT_ANY_THROW(generateColumnWithColumnInput(&table, columnCopyLambda,
                                                   ColumnNumWithType<T>{3},
                                                   ColumnNumWithType<T>{4}));
    table = createTestTable(NUM_ROWS, 4, ColumnNumWithType<T>{0},
                            ColumnNumWithType<T>{1}, ColumnNumWithType<T>{2},
                            ColumnNumWithType<T>{3});
    ASSERT_ANY_THROW(generateColumnWithColumnInput(
        &table, trivialLambda, ColumnNumWithType<bool>{10},
        ColumnNumWithType<T>{1}, ColumnNumWithType<T>{2},
        ColumnNumWithType<T>{3}));
    ASSERT_ANY_THROW(generateColumnWithColumnInput(
        &table, trivialLambda, ColumnNumWithType<bool>{0},
        ColumnNumWithType<T>{10}, ColumnNumWithType<T>{2},
        ColumnNumWithType<T>{3}));
    ASSERT_ANY_THROW(generateColumnWithColumnInput(
        &table, trivialLambda, ColumnNumWithType<bool>{0},
        ColumnNumWithType<T>{1}, ColumnNumWithType<T>{20},
        ColumnNumWithType<T>{3}));
    ASSERT_ANY_THROW(generateColumnWithColumnInput(
        &table, trivialLambda, ColumnNumWithType<bool>{0},
        ColumnNumWithType<T>{1}, ColumnNumWithType<T>{2},
        ColumnNumWithType<T>{30}));

    // One column is used more than once as an input.
    table = createTestTable(NUM_ROWS, 2, ColumnNumWithType<T>{0},
                            ColumnNumWithType<T>{1});
    ASSERT_ANY_THROW(generateColumnWithColumnInput(
        &table, trivialLambda, ColumnNumWithType<bool>{1},
        ColumnNumWithType<T>{0}, ColumnNumWithType<T>{0}));
  });
}
}  // namespace ad_benchmark
