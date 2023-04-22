// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (January of 2023, schlegea@informatik.uni-freiburg.de)
// Author of the file this file is based on: Björn Buchhold
// (buchhold@informatik.uni-freiburg.de)
#include <absl/strings/str_cat.h>
#include <algorithm>
#include <concepts>
#include <cstdio>
#include <type_traits>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/util/IdTableHelperFunction.h"
#include "../test/util/IdTableHelpers.h"
#include "../test/util/JoinHelpers.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/ResultTable.h"
#include "engine/idTable/IdTable.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/Random.h"
#include "util/TypeTraits.h"
#include "../benchmark/util/BenchmarkTableCommonCalculations.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"

namespace ad_benchmark {
/*
 * @brief Creates an overlap between the join columns of the IdTables, by
 *  randomly overiding entries of the smaller table with entries of the bigger
 *  table.
 *
 * @param smallerTable The table, where join column entries will be
 *  overwritten.
 * @param biggerTable The table, where join column entries will be copied from.
 * @param probabilityToCreateOverlap The height of the probability for any
 *  join column entry of smallerTable to be overwritten by a random join column
 *  entry of biggerTable.
 */
static void createOverlapRandomly(IdTableAndJoinColumn* const smallerTable,
                                  const IdTableAndJoinColumn& biggerTable,
                                  const double probabilityToCreateOverlap) {
  // For easier reading.
  const size_t smallerTableJoinColumn = (*smallerTable).joinColumn;
  const size_t smallerTableNumberRows = (*smallerTable).idTable.numRows();

  // The probability for creating an overlap must be in (0,100], any other
  // values make no sense.
  AD_CONTRACT_CHECK(0 < probabilityToCreateOverlap &&
                    probabilityToCreateOverlap <= 100);

  // Is the bigger table actually bigger?
  AD_CONTRACT_CHECK(smallerTableNumberRows <= biggerTable.idTable.numRows());

  // Creating the generator for choosing a random row in the bigger table.
  SlowRandomIntGenerator<size_t> randomBiggerTableRow(
      0, biggerTable.idTable.numRows() - 1);

  // Generator for checking, if an overlap should be created.
  RandomDoubleGenerator randomDouble(0, 100);

  for (size_t i = 0; i < smallerTableNumberRows; i++) {
    // Only do anything, if the probability is right.
    if (randomDouble() <= probabilityToCreateOverlap) {
      // Create an overlap.
      (*smallerTable).idTable(i, smallerTableJoinColumn) =
          biggerTable.idTable(randomBiggerTableRow(), biggerTable.joinColumn);
    }
  }
}

// Benchmarks for unsorted and sorted tables, with and without overlapping
// values in IdTables. Done with normal join and hash join.
class BM_UnsortedAndSortedIdTable : public BenchmarkInterface {
  // The amount of rows and columns of the tables.
  size_t numberRows;
  size_t numberColumns;

 public:
  /*
  Sets the amount of rows and columns based on the configuration options
  `numberRows` and `numberColumns`.
  */
  void parseConfiguration(const BenchmarkConfiguration& config) {
    numberRows =
        config.getValueByNestedKeys<size_t>("numberRows").value_or(100);

    // The maximum number of columns, we should ever have, is 20.
    numberColumns = std::min(
        static_cast<size_t>(20),
        config.getValueByNestedKeys<size_t>("numberColumns").value_or(10));
  }

  BenchmarkResults runAllBenchmarks() {
    BenchmarkResults results{};

    auto hashJoinLambda = makeHashJoinLambda();
    auto joinLambda = makeJoinLambda();

    // Tables, that have no overlapping values in their join columns.
    IdTableAndJoinColumn a{
        createRandomlyFilledIdTable(numberRows, numberColumns, 0, 0, 10), 0};
    IdTableAndJoinColumn b{
        createRandomlyFilledIdTable(numberRows, numberColumns, 0, 20, 30), 0};

    // Lambda wrapper for the functions, that I measure.

    // Sorts the table IN PLACE and the uses the normal join on them.
    auto sortThenJoinLambdaWrapper = [&]() {
      // Sorting the tables by the join column.
      sortIdTableByJoinColumnInPlace(a);
      sortIdTableByJoinColumnInPlace(b);

      useJoinFunctionOnIdTables(a, b, joinLambda);
    };

    auto joinLambdaWrapper = [&]() {
      useJoinFunctionOnIdTables(a, b, joinLambda);
    };

    auto hashJoinLambdaWrapper = [&]() {
      useJoinFunctionOnIdTables(a, b, hashJoinLambda);
    };

    // Because it's easier to read/interpret, the benchmarks are entries in
    // tables.
    auto& sortedIdTablesTable = results.addTable(
        "Sorted IdTables", {"Merge/Galloping join", "Hashed join"},
        {"Overlapping join column entries",
         "Non-overlapping join column entries"});
    auto& unsortedIdTablesTable = results.addTable(
        "Unsorted IdTables", {"Merge/Galloping join", "Hashed join"},
        {"Overlapping join column entries",
         "Non-overlapping join column entries"});

    // Benchmarking with non-overlapping IdTables.

    unsortedIdTablesTable.addMeasurement(1, 1, hashJoinLambdaWrapper);
    unsortedIdTablesTable.addMeasurement(0, 1, sortThenJoinLambdaWrapper);

    // Because the sortThenJoinLambdaWrapper sorts tables IN PLACE, a and b are
    // now sorted.
    sortedIdTablesTable.addMeasurement(1, 1, hashJoinLambdaWrapper);
    sortedIdTablesTable.addMeasurement(0, 1, joinLambdaWrapper);

    // Benchmarking with overlapping IdTables.

    // We make the tables overlapping and then randomly shuffle them.
    createOverlapRandomly(&a, b, 10.0);

    randomShuffle(a.idTable.begin(), a.idTable.end());
    randomShuffle(b.idTable.begin(), b.idTable.end());

    unsortedIdTablesTable.addMeasurement(1, 0, hashJoinLambdaWrapper);
    unsortedIdTablesTable.addMeasurement(0, 0, sortThenJoinLambdaWrapper);

    // Because the sortThenJoinLambdaWrapper sorts tables IN PLACE, a and b are
    // now sorted.
    sortedIdTablesTable.addMeasurement(1, 0, hashJoinLambdaWrapper);
    sortedIdTablesTable.addMeasurement(0, 0, joinLambdaWrapper);

    return results;
  }
};

// Is the given type `T` of type `Type`, or is it a vector, containing
// objects of type `Type`?
template <typename T, typename Type>
concept isTypeOrVectorOfType =
    std::is_same_v<Type, T> || std::is_same_v<std::vector<Type>, T>;

// Is exactly one of the given types a `std::vector`?
template <typename... Ts>
concept exactlyOneVector = (ad_utility::isVector<Ts> + ...) == 1;

/*
 * @brief Create a benchmark table for join algorithm, with the given
 *  parameters for the IdTables. The columns will be the algorithm and the
 *  rows will be the parameter, you gave a list for.
 *
 * @tparam TF, T5, T6 Must be a float, or a std::vector<float>. Can only be a
 *  vector, if all other template parameter are not vectors. Type inference
 *  should be able to handle it, if you give the right function arguments.
 * @tparam T1, T2, T3, T4 Must be a size_t, or a std::vector<size_t>. Can only
 *  be a vector, if all other template parameter are not vectors. Type inference
 *  should be able to handle it, if you give the right function arguments.
 *
 * @param records The BenchmarkRecords, in which you want to create a new
 *  benchmark table.
 * @param overlap The height of the probability for any join column entry of
 *  smallerTable to be overwritten by a random join column entry of biggerTable.
 * @param smallerTableSorted, biggerTableSorted Should the bigger/smaller table
 *  be sorted by his join column before being joined? More specificly, some
 *  join algorithm require one, or both, of the IdTables to be sorted. If this
 *  argument is false, the time needed for sorting the required table will
 *  added to the time of the join algorithm.
 * @param ratioRows How many more rows than the smaller table should the
 *  bigger table have? In more mathematical words: Number of rows of the
 *  bigger table divided by the number of rows of the smaller table is equal
 *  to ratioRows.
 * @param smallerTableAmountRows How many rows should the smaller table have?
 * @param smallerTableAmountColumns, biggerTableAmountColumns How many columns
 *  should the bigger/smaller tables have?
 * @param smallerTableJoinColumnSampleSizeRatio,
 *  biggerTableJoinColumnSampleSizeRatio The join column of the tables normally
 *  get random entries out of a sample size with the same amount of possible
 *  numbers as there are rows in the table. (With every number having the same
 *  chance to be picked.) This adjusts the number of elements in the sample
 *  size to `Amount of rows * ratio`, which affects the possibility of
 *  duplicates. Important: `Amount of rows * ratio` must be a natural number.
 */
template <isTypeOrVectorOfType<size_t> T1, isTypeOrVectorOfType<size_t> T2,
          isTypeOrVectorOfType<size_t> T3, isTypeOrVectorOfType<size_t> T4,
          isTypeOrVectorOfType<float> TF,
          isTypeOrVectorOfType<float> T5 = float,
          isTypeOrVectorOfType<float> T6 = float>
requires exactlyOneVector<T1, T2, T3, T4, TF, T5, T6>
static void makeBenchmarkTable(
    BenchmarkResults* records, const std::string_view tableDescriptor,
    const TF& overlap, const bool smallerTableSorted,
    const bool biggerTableSorted, const T1& ratioRows,
    const T2& smallerTableAmountRows, const T3& smallerTableAmountColumns,
    const T4& biggerTableAmountColumns,
    const T5& smallerTableJoinColumnSampleSizeRatio = 1.0,
    const T6& biggerTableJoinColumnSampleSizeRatio = 1.0) {
  // Checking, if smallerTableJoinColumnSampleSizeRatio and
  // biggerTableJoinColumnSampleSizeRatio are floats bigger than 0. Otherwise
  // , they don't make sense.
  auto biggerThanZero = []<typename T>(const T& object) {
    if constexpr (std::is_same<T, std::vector<float>>::value) {
      std::ranges::for_each(
          object, [](float number) { AD_CONTRACT_CHECK(number > 0); }, {});
    } else {
      // Object must be a float.
      AD_CONTRACT_CHECK(object > 0);
    }
  };
  biggerThanZero(smallerTableJoinColumnSampleSizeRatio);
  biggerThanZero(biggerTableJoinColumnSampleSizeRatio);

  // Add all the function arguments to the metadata, so that you know the
  // values used for the runtimes in the benchmark table.
  auto addMetadata = [&overlap, &smallerTableSorted, &biggerTableSorted,
    &ratioRows, &smallerTableAmountRows, &smallerTableAmountColumns,
    &biggerTableAmountColumns, &smallerTableJoinColumnSampleSizeRatio,
    &biggerTableJoinColumnSampleSizeRatio](ResultTable* table){
      BenchmarkMetadata& meta = table->metadata();

      meta.addKeyValuePair("overlap", overlap);
      meta.addKeyValuePair("smallerTableSorted", smallerTableSorted);
      meta.addKeyValuePair("biggerTableSorted", biggerTableSorted);
      meta.addKeyValuePair("ratioRows", ratioRows);
      meta.addKeyValuePair("smallerTableAmountRows", smallerTableAmountRows);
      meta.addKeyValuePair("smallerTableAmountColumns",
        smallerTableAmountColumns);
      meta.addKeyValuePair("biggerTableAmountColumns",
        biggerTableAmountColumns);
      meta.addKeyValuePair("smallerTableJoinColumnSampleSizeRatio",
        smallerTableJoinColumnSampleSizeRatio);
      meta.addKeyValuePair("biggerTableJoinColumnSampleSizeRatio",
        biggerTableJoinColumnSampleSizeRatio);
    };

  // The lambdas for the join algorithms.
  auto hashJoinLambda = makeHashJoinLambda();
  auto joinLambda = makeJoinLambda();

  // To reduce code duplication, the creation of the benchmark table is done
  // per lambda.
  auto createBenchmarkTable =
      [&tableDescriptor, &records]<typename VectorContentType>(
          const std::vector<VectorContentType>& unconvertedRowNames)
      -> ResultTable& {
    // Creating the names for the rows for the benchmark table creation.
    std::vector<std::string> rowNames(unconvertedRowNames.size());
    std::ranges::transform(
        unconvertedRowNames, rowNames.begin(),
        [](const VectorContentType& entry) { return std::to_string(entry); },
        {});

    return records->addTable(std::string{tableDescriptor}, rowNames,
                             {"Merge/Galloping join", "Hash join",
                              "Number of rows in joined IdTable",
                              "Speedup with hash join"});
  };

  // Setup for easier creation of the tables, that will be joined.
  IdTableAndJoinColumn smallerTable{makeIdTableFromVector({{}}), 0};
  IdTableAndJoinColumn biggerTable{makeIdTableFromVector({{}}), 0};
  auto replaceIdTables = [&smallerTableSorted, &biggerTableSorted,
                          &smallerTable, &biggerTable](
                             float overlap, size_t smallerTableAmountRows,
                             size_t smallerTableAmountColumns,
                             float smallerTableJoinColumnSampleSizeRatio,
                             size_t ratioRows, size_t biggerTableAmountColumns,
                             float biggerTableJoinColumnSampleSizeRatio) {
    // For easier use, we only calculate the boundaries for the random
    // join column entry generators once.
    // Reminder: The $-1$ in the upper bounds is, because a range [a, b]
    // of natural numbers has $b - a + 1$ elements.
    const size_t smallerTableJoinColumnLowerBound = 0;
    const size_t smallerTableJoinColumnUpperBound =
        (smallerTableAmountRows * smallerTableJoinColumnSampleSizeRatio) - 1;
    const size_t biggerTableJoinColumnLowerBound =
        smallerTableJoinColumnUpperBound + 1;
    const size_t biggerTableJoinColumnUpperBound =
        biggerTableJoinColumnLowerBound +
        (smallerTableAmountRows * ratioRows *
         biggerTableJoinColumnSampleSizeRatio) -
        1;

    // Replacing the old id tables with newly generated ones, based
    // on specification.
    smallerTable.idTable = createRandomlyFilledIdTable(
        smallerTableAmountRows, smallerTableAmountColumns, 0,
        smallerTableJoinColumnLowerBound, smallerTableJoinColumnUpperBound);
    // We want the tables to have no overlap at this stage, so we have to
    // move the bounds of the entries for the join column a bit, to make
    // sure.
    biggerTable.idTable = createRandomlyFilledIdTable(
        smallerTableAmountRows * ratioRows, biggerTableAmountColumns, 0,
        biggerTableJoinColumnLowerBound, biggerTableJoinColumnUpperBound);

    // Creating overlap, if wanted.
    if (overlap > 0) {
      createOverlapRandomly(&smallerTable, biggerTable, overlap);
    }

    // Sort the tables, if wanted.
    if (smallerTableSorted) {
      sortIdTableByJoinColumnInPlace(smallerTable);
    };
    if (biggerTableSorted) {
      sortIdTableByJoinColumnInPlace(biggerTable);
    };
  };

  // Add the next row of join algorithm measurements to the benchmark table.
  auto addNextRowToBenchmarkTable = [i = 0, &hashJoinLambda,
                                     &smallerTableSorted, &biggerTableSorted,
                                     &joinLambda, &smallerTable,
                                     &biggerTable](ResultTable* table) mutable {
    // The number of rows, that the joined `ItdTable`s end up having.
    size_t numberRowsOfResult;

    // Hash join first, because merge/galloping join sorts all tables, if
    // needed, before joining them.
    table->addMeasurement(i, 1, [&]() {
      numberRowsOfResult =
          useJoinFunctionOnIdTables(smallerTable, biggerTable, hashJoinLambda)
              .numRows();
    });

    // The merge/galloping join may have to sort non, one, or both tables,
    // before using them. That decision shouldn't happen in the wrapper for the
    // call, to minimize overhead.
    if ((!smallerTableSorted) && (!biggerTableSorted)) {
      table->addMeasurement(i, 0, [&]() {
        sortIdTableByJoinColumnInPlace(smallerTable);
        sortIdTableByJoinColumnInPlace(biggerTable);
        numberRowsOfResult =
            useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda)
                .numRows();
      });
    } else if (!smallerTableSorted) {
      table->addMeasurement(i, 0, [&]() {
        sortIdTableByJoinColumnInPlace(smallerTable);
        numberRowsOfResult =
            useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda)
                .numRows();
      });
    } else if (!biggerTableSorted) {
      table->addMeasurement(i, 0, [&]() {
        sortIdTableByJoinColumnInPlace(biggerTable);
        numberRowsOfResult =
            useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda)
                .numRows();
      });
    } else {
      table->addMeasurement(i, 0, [&]() {
        numberRowsOfResult =
            useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda)
                .numRows();
      });
    }

    // Adding the number of rows of the result.
    table->setEntry(i, 2, std::to_string(numberRowsOfResult));

    // The next call of the lambda, will be one row further.
    i++;
  };

  // For adding the speedup of the hash join algorithm in comparison
  // to the merge/galloping join algorithm.
  auto addSpeedup = [](ResultTable* table){
    calculateSpeedupOfColumn(table, 1, 0, 3);
  };

  // We have to adjust a few things, based on which argument is the
  // vector. Fortunaly, we can do it so, that the uneeded parts get deleted
  // at compile time.
  if constexpr (std::is_same<T1, std::vector<size_t>>::value) {
    ResultTable& table = createBenchmarkTable(ratioRows);
    for (const size_t& ratioRow : ratioRows) {
      replaceIdTables(
          overlap, smallerTableAmountRows, smallerTableAmountColumns,
          smallerTableJoinColumnSampleSizeRatio, ratioRow,
          biggerTableAmountColumns, biggerTableJoinColumnSampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
    addSpeedup(&table);
    addMetadata(&table);
  } else if constexpr (std::is_same<T2, std::vector<size_t>>::value) {
    ResultTable& table = createBenchmarkTable(smallerTableAmountRows);
    for (const size_t& smallerTableAmountRow : smallerTableAmountRows) {
      replaceIdTables(overlap, smallerTableAmountRow, smallerTableAmountColumns,
                      smallerTableJoinColumnSampleSizeRatio, ratioRows,
                      biggerTableAmountColumns,
                      biggerTableJoinColumnSampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
    addSpeedup(&table);
    addMetadata(&table);
  } else if constexpr (std::is_same<T3, std::vector<size_t>>::value) {
    ResultTable& table = createBenchmarkTable(smallerTableAmountColumns);
    for (const size_t& smallerTableAmountColumn : smallerTableAmountColumns) {
      replaceIdTables(overlap, smallerTableAmountRows, smallerTableAmountColumn,
                      smallerTableJoinColumnSampleSizeRatio, ratioRows,
                      biggerTableAmountColumns,
                      biggerTableJoinColumnSampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
    addSpeedup(&table);
    addMetadata(&table);
  } else if constexpr (std::is_same<T4, std::vector<size_t>>::value) {
    ResultTable& table = createBenchmarkTable(biggerTableAmountColumns);
    for (const size_t& biggerTableAmountColumn : biggerTableAmountColumns) {
      replaceIdTables(
          overlap, smallerTableAmountRows, smallerTableAmountColumns,
          smallerTableJoinColumnSampleSizeRatio, ratioRows,
          biggerTableAmountColumn, biggerTableJoinColumnSampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
    addSpeedup(&table);
    addMetadata(&table);
  } else if constexpr (std::is_same<T4, std::vector<float>>::value) {
    ResultTable& table = createBenchmarkTable(overlap);
    for (const size_t& overlapChance : overlap) {
      replaceIdTables(
          overlapChance, smallerTableAmountRows, smallerTableAmountColumns,
          smallerTableJoinColumnSampleSizeRatio, ratioRows,
          biggerTableAmountColumns, biggerTableJoinColumnSampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
    addSpeedup(&table);
    addMetadata(&table);
  } else if constexpr (std::is_same<T5, std::vector<float>>::value) {
    ResultTable& table =
        createBenchmarkTable(smallerTableJoinColumnSampleSizeRatio);
    for (const size_t& sampleSizeRatio :
         smallerTableJoinColumnSampleSizeRatio) {
      replaceIdTables(overlap, smallerTableAmountRows,
                      smallerTableAmountColumns, sampleSizeRatio, ratioRows,
                      biggerTableAmountColumns,
                      biggerTableJoinColumnSampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
    addSpeedup(&table);
    addMetadata(&table);
  } else if constexpr (std::is_same<T6, std::vector<float>>::value) {
    ResultTable& table =
        createBenchmarkTable(biggerTableJoinColumnSampleSizeRatio);
    for (const size_t& sampleSizeRatio : biggerTableJoinColumnSampleSizeRatio) {
      replaceIdTables(overlap, smallerTableAmountRows,
                      smallerTableAmountColumns,
                      smallerTableJoinColumnSampleSizeRatio, ratioRows,
                      biggerTableAmountColumns, sampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
    addSpeedup(&table);
    addMetadata(&table);
  }
}

/*
 * @brief Returns a vector of exponents $base^x$, with $x$ staring at 0, and
 *  ending with the last $base^x$ that is smaller, or equal, to the stopping
 *  point.
 *
 * @param base The base of the exponents.
 * @param stoppingPoint What the biggest exponent is allowed to be.
 *
 * @return `{base^0, base^1, ...., base^N}` with `base^N <= stoppingPoint` and
 *  `base^(N+1) > stoppingPoint`.
 */
static std::vector<size_t> createExponentVectorUntilSize(
    const size_t base, const size_t stoppingPoint) {
  std::vector<size_t> exponentVector{};

  // Calculating and adding the exponents.
  size_t currentExponent = 1;  // base^0 = 1
  while (currentExponent <= stoppingPoint) {
    exponentVector.push_back(currentExponent);
    currentExponent *= base;
  }

  return exponentVector;
}

/*
Partly implements the interface `BenchmarkInterface`, by providing the member
variables, that most of the benchmark classes here set using the
`BenchmarkConfiguration` and delivers a default configuration parser, that sets
them.
*/
class GeneralConfigurationOption : public BenchmarkInterface {
 protected:
  /*
  The benchmark classes after this point always make tables, where one
  attribute of the `IdTables` gets bigger with every row, while the other
  attributes stay the same.
  For the attributes, that don't stay the same, they create a list of
  exponents, with basis $2$, from $1$ up to an upper boundary for the
  exponents. The variables, that define such boundaries, always begin with
  `max`.

  The name of a variable in the configuration is just the variable name.
  Should that configuration option not be set, it will be set to a default
  value.
  */

  // Amount of rows for the smaller `IdTable`, if we always use the amount.
  size_t smallerTableAmountRows;

  // The maximum amount of rows, that the smaller `IdTable` should have.
  size_t maxSmallerTableRows;

  // Amount of columns for the `IdTable`s
  size_t smallerTableAmountColumns;
  size_t biggerTableAmountColumns;

  /*
  Chance for an entry in the join column of the smaller `IdTable` to be the
  same value as an entry in the join column of the bigger `IdTable`.
  Must be in the range $(0,100]$.
  */
  float overlapChance;

  /*
  The row ratio between the smaller and the bigger `IdTable`. That is
  the value of the amount of rows in the bigger `IdTable` divided through the
  amount of rows in the smaller `IdTable`.
  */
  size_t ratioRows;

  // The maximal row ratio between the smaller and the bigger `IdTable`.
  size_t maxRatioRows;

 public:
  void parseConfiguration(const BenchmarkConfiguration& config) final {
    /*
    @brief Assigns the value in the configuration option to the variable. If the
    option wasn't set, assigns a default value.

    @tparam VariableType The type of the variable. This is also what the
    configuration option will be interpreted as.

    @param key The key for the configuration option.
    @param variable The variable, that will be set.
    @param defaultValue The default value, that the variable will be set to, if
    the configuration option wasn't set.
    */
    auto setToValueInConfigurationOrDefault =
        [&config]<typename VariableType>(VariableType& variable,
                                         const std::string& key,
                                         const VariableType& defaultValue) {
          variable = config.getValueByNestedKeys<VariableType>(key).value_or(
              defaultValue);
        };

    setToValueInConfigurationOrDefault(smallerTableAmountRows,
                                       "smallerTableAmountRows",
                                       static_cast<size_t>(256));

    setToValueInConfigurationOrDefault(
        maxSmallerTableRows, "maxSmallerTableRows", static_cast<size_t>(2000));

    setToValueInConfigurationOrDefault(smallerTableAmountColumns,
                                       "smallerTableAmountColumns",
                                       static_cast<size_t>(20));
    setToValueInConfigurationOrDefault(biggerTableAmountColumns,
                                       "biggerTableAmountColumns",
                                       static_cast<size_t>(20));

    setToValueInConfigurationOrDefault(overlapChance, "overlapChance",
                                       static_cast<float>(42.0));

    setToValueInConfigurationOrDefault(ratioRows, "ratioRows",
                                       static_cast<size_t>(1));

    setToValueInConfigurationOrDefault(maxRatioRows, "maxRatioRows",
                                       static_cast<size_t>(256));
  }
};

// Create benchmark tables, where the smaller table stays at 2000 rows and
// the bigger tables keeps getting bigger. Amount of columns stays the same.
class BM_OnlyBiggerTableSizeChanges final : public GeneralConfigurationOption {
 public:
  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    // Easier reading.
    const std::vector<size_t> ratioRows{
        createExponentVectorUntilSize(2, maxRatioRows)};
    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        makeBenchmarkTable(&results, absl::StrCat("Smaller table stays at ",
                           smallerTableAmountRows, " rows, ratio to rows of",
                           " bigger table grows."),
                           overlapChance, smallerTableSorted,
                           biggerTableSorted, ratioRows, smallerTableAmountRows,
                           smallerTableAmountColumns, biggerTableAmountColumns);
      }
    }

    return results;
  }
};

// Create benchmark tables, where the smaller table grows and the ratio
// between tables stays the same. As does the amount of columns.
class BM_OnlySmallerTableSizeChanges final : public GeneralConfigurationOption {
 public:
  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    // Easier reading.
    const std::vector<size_t> smallerTableAmountRows{
        createExponentVectorUntilSize(2, maxSmallerTableRows)};
    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        // We also make multiple tables for different row ratios.
        for (const size_t ratioRows :
             createExponentVectorUntilSize(2, maxRatioRows)) {
          makeBenchmarkTable(&results, absl::StrCat("The amount of rows in ",
                             "the smaller table grows and the ratio, to the ",
                             "amount of rows in the bigger table, stays at ",
                             ratioRows, "."),
                             overlapChance, smallerTableSorted,
                             biggerTableSorted, ratioRows,
                             smallerTableAmountRows, smallerTableAmountColumns,
                             biggerTableAmountColumns);
        }
      }
    }

    return results;
  }
};

// Create benchmark tables, where the tables are the same size and
// both just get more rows.
class BM_SameSizeRowGrowth final : public GeneralConfigurationOption {
 public:
  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    // Easier reading.
    const std::vector<size_t> smallerTableAmountRows{
        createExponentVectorUntilSize(2, maxSmallerTableRows)};
    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        makeBenchmarkTable(&results, "Both tables always have the same amount"
                           "of rows and that amount grows." ,overlapChance,
                           smallerTableSorted,
                           biggerTableSorted, ratioRows, smallerTableAmountRows,
                           smallerTableAmountColumns, biggerTableAmountColumns);
      }
    }

    return results;
  }
};

// Registering the benchmarks
AD_REGISTER_BENCHMARK(BM_UnsortedAndSortedIdTable);
AD_REGISTER_BENCHMARK(BM_SameSizeRowGrowth);
AD_REGISTER_BENCHMARK(BM_OnlySmallerTableSizeChanges);
AD_REGISTER_BENCHMARK(BM_OnlyBiggerTableSizeChanges);
}  // namespace ad_benchmark
