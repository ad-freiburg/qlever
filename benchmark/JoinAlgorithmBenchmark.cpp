// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (January of 2023, schlegea@informatik.uni-freiburg.de)
// Author of the file this file is based on: Björn Buchhold
// (buchhold@informatik.uni-freiburg.de)
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <cstdio>
#include <ctime>
#include <string_view>
#include <tuple>
#include <type_traits>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "../benchmark/util/BenchmarkTableCommonCalculations.h"
#include "../benchmark/util/IdTableHelperFunction.h"
#include "../test/util/IdTableHelpers.h"
#include "../test/util/JoinHelpers.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/ResultTable.h"
#include "engine/idTable/IdTable.h"
#include "util/Algorithm.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/Random.h"
#include "util/TypeTraits.h"

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
class BmUnsortedAndSortedIdTable : public BenchmarkInterface {
  // The amount of rows and columns of the tables.
  size_t numberRows_;
  size_t numberColumns_;

 public:
  std::string name() const override {
    return "Basic benchmarks, for which I had no time to delete yet.";
  }

  /*
  Sets the amount of rows and columns based on the configuration options
  `numberRows_` and `numberColumns_`.
  */
  void parseConfiguration(const BenchmarkConfiguration& config) final {
    numberRows_ =
        config.getValueByNestedKeys<size_t>("numberRows").value_or(100);

    // The maximum number of columns, we should ever have, is 20.
    numberColumns_ = std::min(
        static_cast<size_t>(20),
        config.getValueByNestedKeys<size_t>("numberColumns").value_or(10));
  }

  BenchmarkResults runAllBenchmarks() final {
    BenchmarkResults results{};

    auto hashJoinLambda = makeHashJoinLambda();
    auto joinLambda = makeJoinLambda();

    // Tables, that have no overlapping values in their join columns.
    IdTableAndJoinColumn a{
        createRandomlyFilledIdTable(numberRows_, numberColumns_, 0, 0, 10), 0};
    IdTableAndJoinColumn b{
        createRandomlyFilledIdTable(numberRows_, numberColumns_, 0, 20, 30), 0};

    // Lambda wrapper for the functions, that I measure.

    // Sorts the table IN PLACE and the uses the normal join on them.
    auto sortThenJoinLambdaWrapper = [&a, &b, &joinLambda]() {
      // Sorting the tables by the join column.
      sortIdTableByJoinColumnInPlace(a);
      sortIdTableByJoinColumnInPlace(b);

      useJoinFunctionOnIdTables(a, b, joinLambda);
    };

    auto joinLambdaWrapper = [&a, &b, &joinLambda]() {
      useJoinFunctionOnIdTables(a, b, joinLambda);
    };

    auto hashJoinLambdaWrapper = [&a, &b, &hashJoinLambda]() {
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

/*
@brief Adds the function time measurements to a row of the benchmark table
in `makeBenchmarkTable`.
For an explanation of the parameters, see `makeBenchmarkTable`.
*/
static void addMeasurementsToRowOfBenchmarkTable(
    ResultTable* table, const size_t& row, const float overlap,
    const bool smallerTableSorted, const bool biggerTableSorted,
    const size_t& ratioRows, const size_t& smallerTableAmountRows,
    const size_t& smallerTableAmountColumns,
    const size_t& biggerTableAmountColumns,
    const float smallerTableJoinColumnSampleSizeRatio = 1.0,
    const float biggerTableJoinColumnSampleSizeRatio = 1.0) {
  // Checking, if smallerTableJoinColumnSampleSizeRatio and
  // biggerTableJoinColumnSampleSizeRatio are floats bigger than 0. Otherwise
  // , they don't make sense.
  AD_CONTRACT_CHECK(smallerTableJoinColumnSampleSizeRatio > 0);
  AD_CONTRACT_CHECK(biggerTableJoinColumnSampleSizeRatio > 0);

  // The lambdas for the join algorithms.
  auto hashJoinLambda = makeHashJoinLambda();
  auto joinLambda = makeJoinLambda();

  // Create new `IdTable`s.

  /*
  First we calculate the value boundaries for the join column entries. These
  are needed for the creation of randomly filled tables.
  Reminder: The $-1$ in the upper bounds is, because a range [a, b]
  of natural numbers has $b - a + 1$ elements.
  */
  const size_t smallerTableJoinColumnLowerBound = 0;
  const size_t smallerTableJoinColumnUpperBound =
      static_cast<size_t>(
          std::floor(static_cast<float>(smallerTableAmountRows) *
                     smallerTableJoinColumnSampleSizeRatio)) -
      1;
  const size_t biggerTableJoinColumnLowerBound =
      smallerTableJoinColumnUpperBound + 1;
  const size_t biggerTableJoinColumnUpperBound =
      biggerTableJoinColumnLowerBound +
      static_cast<size_t>(
          std::floor(static_cast<float>(smallerTableAmountRows) *
                     static_cast<float>(ratioRows) *
                     biggerTableJoinColumnSampleSizeRatio)) -
      1;

  // Now we create two randomly filled `IdTable`, which have no overlap, and
  // save them together with the information, where their join column is.
  IdTableAndJoinColumn smallerTable{
      createRandomlyFilledIdTable(
          smallerTableAmountRows, smallerTableAmountColumns, 0,
          smallerTableJoinColumnLowerBound, smallerTableJoinColumnUpperBound),
      0};
  IdTableAndJoinColumn biggerTable{
      createRandomlyFilledIdTable(
          smallerTableAmountRows * ratioRows, biggerTableAmountColumns, 0,
          biggerTableJoinColumnLowerBound, biggerTableJoinColumnUpperBound),
      0};

  // Creating overlap, if wanted.
  if (overlap > 0) {
    createOverlapRandomly(&smallerTable, biggerTable, overlap);
  }

  // Sort the `IdTables`, if they should be.
  if (smallerTableSorted) {
    sortIdTableByJoinColumnInPlace(smallerTable);
  };
  if (biggerTableSorted) {
    sortIdTableByJoinColumnInPlace(biggerTable);
  };

  // Adding the benchmark measurements to the current row.

  // The number of rows, that the joined `ItdTable`s end up having.
  size_t numberRowsOfResult;

  // Hash join first, because merge/galloping join sorts all tables, if
  // needed, before joining them.
  table->addMeasurement(
      row, 3,
      [&numberRowsOfResult, &smallerTable, &biggerTable, &hashJoinLambda]() {
        numberRowsOfResult =
            useJoinFunctionOnIdTables(smallerTable, biggerTable, hashJoinLambda)
                .numRows();
      });

  /*
  The sorting of the `IdTables`. That must be done before the
  merge/galloping, otherwise their algorithm won't result in a correct
  result.
  */
  table->addMeasurement(
      row, 0,
      [&smallerTable, &smallerTableSorted, &biggerTable, &biggerTableSorted]() {
        if (!smallerTableSorted) {
          sortIdTableByJoinColumnInPlace(smallerTable);
        }
        if (!biggerTableSorted) {
          sortIdTableByJoinColumnInPlace(biggerTable);
        }
      });

  // The merge/galloping join.
  table->addMeasurement(
      row, 1,
      [&numberRowsOfResult, &smallerTable, &biggerTable, &joinLambda]() {
        numberRowsOfResult =
            useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda)
                .numRows();
      });

  // Adding the number of rows of the result.
  table->setEntry(row, 4, std::to_string(numberRowsOfResult));
}

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
 *  parameters for the IdTables. The rows will be the parameter, you gave
 *  a list for, and the columns will be:
 *  - Time needed for sorting `IdTable`s.
 *  - Time needed for merge/galloping join.
 *  - Time needed for sorting and merge/galloping added togehter.
 *  - Time needed for the hash join.
 *  - How many rows the result of joining the tables has.
 *  - How much faster the hash join is. For example: Two times faster.
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
static ResultTable& makeBenchmarkTable(
    BenchmarkResults* records, const std::string_view tableDescriptor,
    const TF& overlap, const bool smallerTableSorted,
    const bool biggerTableSorted, const T1& ratioRows,
    const T2& smallerTableAmountRows, const T3& smallerTableAmountColumns,
    const T4& biggerTableAmountColumns,
    const T5& smallerTableJoinColumnSampleSizeRatio = 1.0,
    const T6& biggerTableJoinColumnSampleSizeRatio = 1.0) {
  // Returns the first argument, that is a vector.
  auto returnFirstVector = []<typename... Ts>(Ts&... args) -> auto& {
    // Put them into a tuple, so that we can easly look them up.
    auto tup = std::tuple<Ts&...>{AD_FWD(args)...};

    // Is something a vector?
    constexpr auto isVector = []<typename T>() {
      return ad_utility::isVector<std::decay_t<T>>;
    };

    // Get the index of the first vector.
    constexpr static size_t idx =
        ad_utility::getIndexOfFirstTypeToPassCheck<isVector, Ts...>();

    // Do we have a valid index?
    static_assert(idx < sizeof...(Ts),
                  "There was no vector in this parameter pack.");

    return std::get<idx>(tup);
  };

  /*
  @brief Returns the entry at position `pos` of the vector, if given a vector.
  Otherwise just returns the given `possibleVector`.
  */
  auto returnEntry = []<typename T>(const T& possibleVector, const size_t pos) {
    if constexpr (ad_utility::isVector<T>) {
      return possibleVector.at(pos);
    } else {
      return possibleVector;
    }
  };

  // Now on to creating the benchmark table. First, we find out, which of our
  // arguments was the vector and use it to create the row names for the table.
  auto& vec = returnFirstVector(
      overlap, ratioRows, smallerTableAmountRows, smallerTableAmountColumns,
      biggerTableAmountColumns, smallerTableJoinColumnSampleSizeRatio,
      biggerTableJoinColumnSampleSizeRatio);

  // Then, we convert the content of `vec` to strings and add the table.
  ResultTable* table = &(records->addTable(
      std::string{tableDescriptor},
      ad_utility::transform(
          vec, [](const auto& entry) { return std::to_string(entry); }),
      {"Time for sorting", "Merge/Galloping join",
       "Sorting + merge/galloping join", "Hash join",
       "Number of rows in resulting IdTable", "Speedup of hash join"}));

  // Adding measurements to the table.
  for (size_t i = 0; i < vec.size(); i++) {
    // Converting all our function parameters to non vectors.
    addMeasurementsToRowOfBenchmarkTable(
        table, i, returnEntry(overlap, i), smallerTableSorted,
        biggerTableSorted, returnEntry(ratioRows, i),
        returnEntry(smallerTableAmountRows, i),
        returnEntry(smallerTableAmountColumns, i),
        returnEntry(biggerTableAmountColumns, i),
        returnEntry(smallerTableJoinColumnSampleSizeRatio, i),
        returnEntry(biggerTableJoinColumnSampleSizeRatio, i));
  }

  // If should never to possible for table to be a null pointr, but better
  // safe than sorry.
  AD_CONTRACT_CHECK(table != nullptr);

  // Adding together the time for sorting the `IdTables` and then joining
  // them using merge/galloping join.
  sumUpColumns(table, 2, static_cast<size_t>(0), static_cast<size_t>(1));

  // Calculate, how much of a speedup the hash join algorithm has in comparison
  // to the merge/galloping join algrithm.
  calculateSpeedupOfColumn(table, 3, 2, 5);

  // For more specific adjustments.
  return (*table);
}

// `T` must be a function, that returns something of type `ReturnType`, when
// called with arguments of types `Args`.
template <typename T, typename ReturnType, typename... Args>
concept invocableWithReturnType = requires(T t, Args... args) {
  { t(args...) } -> std::same_as<ReturnType>;
};

// Is `T` of the given type, or a function, that takes `size_t` and return
// the given type?
template <typename T, typename Type>
concept isTypeOrGrowthFunction =
    std::same_as<T, Type> || invocableWithReturnType<T, Type, const size_t&>;

// There must be exactly one functioon, that takes a `size_t`.
template <typename... Ts>
concept exactlyOneGrowthFunction =
    (std::invocable<Ts, const size_t&> + ...) == 1;

/*

@brief Create a benchmark table for join algorithm, with the given
parameters for the IdTables, which will keep getting more rows, until the stop
function decides, that there are enough rows. The rows will be the return values
of the parameter, you gave a function for, and the columns will be:
- Time needed for sorting `IdTable`s.
- Time needed for merge/galloping join.
- Time needed for sorting and merge/galloping added togehter.
- Time needed for the hash join.
- How many rows the result of joining the tables has.
- How much faster the hash join is. For example: Two times faster.

@tparam StopFunction A function, that takes the current form of the
`ResultTable` as `const ResultTable&` and returns a bool.
@tparam T1, T6, T7 Must be a float, or a function, that takes the row number of
the next to be generated row as `const size_t&`, and returns a float. Can only
be a function, if all other template `T` parameter are not vectors.
@tparam T2, T3, T4, T5 Must be a size_t, or a function, that takes the row
number of the next to be generated row as `const size_t&`, and returns a size_t.
Can only be a function, if all other template `T` parameter are not vectors.

@param results The BenchmarkResults, in which you want to create a new
benchmark table.
@param tableDescriptor A identifier to the to be created benchmark table, so
that it can be easier identified later.
@param stopFunction Returns true, if the table benchmark should get a new row.
False, if there are enough rows and the created table should be returned.
Decides the final size of the benchmark table.
@param overlap The height of the probability for any join column entry of
smallerTable to be overwritten by a random join column entry of biggerTable.
@param smallerTableSorted, biggerTableSorted Should the bigger/smaller table
be sorted by his join column before being joined? More specificly, some
join algorithm require one, or both, of the IdTables to be sorted. If this
argument is false, the time needed for sorting the required table will
added to the time of the join algorithm.
@param ratioRows How many more rows than the smaller table should the
bigger table have? In more mathematical words: Number of rows of the
bigger table divided by the number of rows of the smaller table is equal
to ratioRows.
@param smallerTableAmountRows How many rows should the smaller table have?
@param smallerTableAmountColumns, biggerTableAmountColumns How many columns
should the bigger/smaller tables have?
@param smallerTableJoinColumnSampleSizeRatio,
biggerTableJoinColumnSampleSizeRatio The join column of the tables normally
get random entries out of a sample size with the same amount of possible
numbers as there are rows in the table. (With every number having the same
chance to be picked.) This adjusts the number of elements in the sample
size to `Amount of rows * ratio`, which affects the possibility of
duplicates. Important: `Amount of rows * ratio` must be a natural number.
 */
template <invocableWithReturnType<bool, const ResultTable&> StopFunction,
          isTypeOrGrowthFunction<float> T1, isTypeOrGrowthFunction<size_t> T2,
          isTypeOrGrowthFunction<size_t> T3, isTypeOrGrowthFunction<size_t> T4,
          isTypeOrGrowthFunction<size_t> T5,
          isTypeOrGrowthFunction<float> T6 = float,
          isTypeOrGrowthFunction<float> T7 = float>
requires exactlyOneGrowthFunction<T1, T2, T3, T4, T5, T6, T7>
static ResultTable& makeGrowingBenchmarkTable(
    BenchmarkResults* results, const std::string_view tableDescriptor,
    StopFunction stopFunction, const T1& overlap, const bool smallerTableSorted,
    const bool biggerTableSorted, const T2& ratioRows,
    const T3& smallerTableAmountRows, const T4& smallerTableAmountColumns,
    const T5& biggerTableAmountColumns,
    const T6& smallerTableJoinColumnSampleSizeRatio = 1.0,
    const T7& biggerTableJoinColumnSampleSizeRatio = 1.0) {
  // Is something a function?
  constexpr auto isFunction = []<typename T>() {
    /*
    We have to cheat a bit, because being a function is not something, that
    can easily be checked for to my knowledge. Instead, we simply check, if
    the given type ISN'T a function, of which there are only two possible
    types for, while inside this function: `float` and `size_t`.
    */
    if constexpr (std::is_arithmetic_v<T>) {
      return false;
    } else {
      return true;
    }
  };

  // Returns the first argument, that is a function.
  auto returnFirstFunction =
      [&isFunction]<typename... Ts>(Ts&... args) -> auto& {
    // Put them into a tuple, so that we can easly look them up.
    auto tup = std::tuple<Ts&...>{AD_FWD(args)...};

    // Get the index of the first vector.
    constexpr static size_t idx =
        ad_utility::getIndexOfFirstTypeToPassCheck<isFunction, Ts...>();

    // Do we have a valid index?
    static_assert(idx < sizeof...(Ts),
                  "There was no function in this parameter pack.");

    return std::get<idx>(tup);
  };

  /*
  @brief Calls the function with the number of the next row to be created, if
  it's a function, and returns the result. Otherwise just returns the given
  `possibleFunction`.
  */
  auto returnOrCall = [&isFunction]<typename T>(const T& possibleFunction,
                                                const size_t nextRowNumber) {
    if constexpr (isFunction.template operator()<T>()) {
      return possibleFunction(nextRowNumber);
    } else {
      return possibleFunction;
    }
  };

  /*
  Now on to creating the benchmark table. Because we don't know, how many row
  names we will have, we just create a table without row names.
  */
  ResultTable* table = &(results->addTable(
      std::string{tableDescriptor}, {},
      {"Time for sorting", "Merge/Galloping join",
       "Sorting + merge/galloping join", "Hash join",
       "Number of rows in resulting IdTable", "Speedup of hash join"}));
  /*
  Adding measurements to the table, as long as the stop function doesn't say
  anything else.
  */
  while (stopFunction(*table)) {
    // What's the row number of the next to be added row?
    const size_t rowNumber = table->numRows();

    // Add a new row without content.
    table->addRow(std::to_string(returnFirstFunction(
        overlap, ratioRows, smallerTableAmountRows, smallerTableAmountColumns,
        biggerTableAmountColumns, smallerTableJoinColumnSampleSizeRatio,
        biggerTableJoinColumnSampleSizeRatio)(rowNumber)));

    // Converting all our function parameters to non functions.
    addMeasurementsToRowOfBenchmarkTable(
        table, rowNumber, returnOrCall(overlap, rowNumber), smallerTableSorted,
        biggerTableSorted, returnOrCall(ratioRows, rowNumber),
        returnOrCall(smallerTableAmountRows, rowNumber),
        returnOrCall(smallerTableAmountColumns, rowNumber),
        returnOrCall(biggerTableAmountColumns, rowNumber),
        returnOrCall(smallerTableJoinColumnSampleSizeRatio, rowNumber),
        returnOrCall(biggerTableJoinColumnSampleSizeRatio, rowNumber));
  }

  // If should never to possible for table to be a null pointr, but better
  // safe than sorry.
  AD_CONTRACT_CHECK(table != nullptr);

  // Adding together the time for sorting the `IdTables` and then joining
  // them using merge/galloping join.
  sumUpColumns(table, 2, static_cast<size_t>(0), static_cast<size_t>(1));

  // Calculate, how much of a speedup the hash join algorithm has in comparison
  // to the merge/galloping join algrithm.
  calculateSpeedupOfColumn(table, 3, 2, 5);

  // For more specific adjustments.
  return (*table);
}

/*
@brief Verifies, that all the function measurements in a benchmark table row are
lower than a maximal time value.
*/
static bool checkIfFunctionMeasurementOfRowUnderMaxtime(
    const ResultTable& table, const size_t& row, const float& maxTime) {
  // Check if the time limit was followed, for a single column.
  auto checkTime = [&maxTime, &row, &table](const size_t& column) {
    return table.getEntry<float>(row, column) <= maxTime;
  };

  // We measure the functions in column 0, 1 and 3.
  return checkTime(0) && checkTime(1) && checkTime(3);
}

/*
@brief Calculates the smalles whole exponent, so that $base^n$ is equal, or
bigger, than the `startingPoint`.
*/
static size_t calculateNextWholeExponent(const size_t& base,
                                         const auto& startingPoint) {
  // This is a rather simple calculation: We calculate
  // $log_(base)(startingPoint)$ and round up.
  return static_cast<size_t>(
      std::ceil(std::log(startingPoint) / std::log(base)));
}

/*
 * @brief Returns a vector of exponents $base^x$, with $x$ being a natural
 * number and $base^x$ being all possible numbers of this type in a given
 * range.
 *
 * @param base The base of the exponents.
 * @param startingPoint What the smalles exponent must at minimum be.
 * @param stoppingPoint What the biggest exponent is allowed to be.
 *
 * @return `{base^i, base^(i+1), ...., base^(i+n)}` with
 * `base^(i-1) < startingPoint`, `startingPoint <= base^i`,
 * `base^(i+n) <= stoppingPoint`and
 * `stoppingPoint < base^(i+n+1)`.
 */
static std::vector<size_t> createExponentVectorUntilSize(
    const size_t& base, const size_t& startingPoint,
    const size_t& stoppingPoint) {
  // Quick check, if the given arguments make sense.
  AD_CONTRACT_CHECK(startingPoint <= stoppingPoint);

  std::vector<size_t> exponentVector{};

  /*
  We can calculate our first exponent by using the logarithm.
  A short explanation: We calculate $log_(base)(startingPoint)$ and
  round up. This should give us the $x$ of the first $base^x$ bigger
  than `startingPoint`.
  */
  size_t currentExponent = static_cast<size_t>(
      std::pow(base, calculateNextWholeExponent(base, startingPoint)));

  // The rest of the exponents.
  while (currentExponent <= stoppingPoint) {
    exponentVector.push_back(currentExponent);
    currentExponent *= base;
  }

  return exponentVector;
}

/*
Partly implements the interface `BenchmarkInterface`, by:

- Providing the member variables, that most of the benchmark classes here set
using the `BenchmarkConfiguration`.

- A default configuration parser, that sets tthe provded member variables.

- A default `getMetadata`, that adds the date and time, where the benchmark
measurements were taken.
*/
class GeneralInterfaceImplementation : public BenchmarkInterface {
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

  // Amount of rows for the smaller `IdTable`, if we always use the same amount.
  size_t smallerTableAmountRows_;

  // The minimum amount of rows, that the bigger `IdTable` should have.
  size_t minBiggerTableRows_;
  // The maximum amount of rows, that the bigger `IdTable` should have.
  size_t maxBiggerTableRows_;

  // Amount of columns for the `IdTable`s
  size_t smallerTableAmountColumns_;
  size_t biggerTableAmountColumns_;

  /*
  Chance for an entry in the join column of the smaller `IdTable` to be the
  same value as an entry in the join column of the bigger `IdTable`.
  Must be in the range $(0,100]$.
  */
  float overlapChance_;

  /*
  The row ratio between the smaller and the bigger `IdTable`. That is
  the value of the amount of rows in the bigger `IdTable` divided through the
  amount of rows in the smaller `IdTable`.
  */
  size_t ratioRows_;

  // The minimum row ratio between the smaller and the bigger `IdTable`.
  size_t minRatioRows_;
  // The maximal row ratio between the smaller and the bigger `IdTable`.
  size_t maxRatioRows_;

  /*
  The maximal amount of time, any function measurement is allowed to take. Will
  only have a value, if the configuration option was set.
  */
  std::optional<float> maxTimeSingleMeasurement_;

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

    setToValueInConfigurationOrDefault(smallerTableAmountRows_,
                                       "smallerTableAmountRows",
                                       static_cast<size_t>(1000));

    setToValueInConfigurationOrDefault(
        minBiggerTableRows_, "minBiggerTableRows", static_cast<size_t>(1000));
    setToValueInConfigurationOrDefault(
        maxBiggerTableRows_, "maxBiggerTableRows", static_cast<size_t>(100000));

    setToValueInConfigurationOrDefault(smallerTableAmountColumns_,
                                       "smallerTableAmountColumns",
                                       static_cast<size_t>(20));
    setToValueInConfigurationOrDefault(biggerTableAmountColumns_,
                                       "biggerTableAmountColumns",
                                       static_cast<size_t>(20));

    setToValueInConfigurationOrDefault(overlapChance_, "overlapChance",
                                       static_cast<float>(42.0));

    setToValueInConfigurationOrDefault(ratioRows_, "ratioRows",
                                       static_cast<size_t>(1));

    setToValueInConfigurationOrDefault(minRatioRows_, "minRatioRows",
                                       static_cast<size_t>(1));
    setToValueInConfigurationOrDefault(maxRatioRows_, "maxRatioRows",
                                       static_cast<size_t>(1000));

    /*
    `maxMemoryInMB` is the max amount, that the bigger `IdTable` is allowed to
    take up in memory. If the option was set, we adjust `maxBiggerTableRows`, so
    that it now conforms to it.
    */
    const std::optional<size_t>& maxMemoryInMB =
        config.getValueByNestedKeys<size_t>("maxMemoryInMB");

    if (maxMemoryInMB.has_value()) {
      /*
      The overhead can be, more or less, ignored. We are just concerned over
      the space needed for the entries.
      */
      constexpr size_t memoryPerIdTableEntryInByte =
          sizeof(IdTable::value_type);
      const size_t memoryPerRowInByte =
          memoryPerIdTableEntryInByte * biggerTableAmountColumns_;

      // Does a single row take more memory, than is allowed?
      if (maxMemoryInMB.value() * 1000000 < memoryPerRowInByte) {
        throw ad_utility::Exception(absl::StrCat(
            "A single row of an", " IdTable, with ", biggerTableAmountColumns_,
            " columns, takes more", " memory, than what was allowed."));
      }

      // Approximate, how many rows that would be at maximum memory usage.
      maxBiggerTableRows_ =
          (maxMemoryInMB.value() * 1000000) / memoryPerRowInByte;
    }

    maxTimeSingleMeasurement_ =
        config.getValueByNestedKeys<float>("maxTimeSingleMeasurement");
  }
};

/*
Create benchmark tables, where the smaller table stays at the same amount of
rows and the bigger tables keeps getting bigger. Amount of columns stays the
same.
*/
class BmOnlyBiggerTableSizeChanges final
    : public GeneralInterfaceImplementation {
 public:
  std::string name() const override {
    return "Benchmarktables, where the smaller table stays at the same amount "
           "of rows and the bigger tables keeps getting bigger.";
  }

  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    /*
    We got the fixed amount of rows for the smaller table and the variable
    amount of rows for the bigger table. Those can be used to calculate the
    ratios needed for a benchmark table, that has those values.
    */
    const std::vector<size_t> ratioRows{
        ad_utility::transform(createExponentVectorUntilSize(
                                  10, minBiggerTableRows_, maxBiggerTableRows_),
                              [this](const size_t& number) {
                                return number / smallerTableAmountRows_;
                              })};

    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        const std::string& tableName =
            absl::StrCat("Smaller table stays at ", smallerTableAmountRows_,
                         " rows, ratio to rows of bigger table grows.");

        // We have to call different functions depending on if there was a max
        // time limit to a single measurement.
        ResultTable* table;
        if (!maxTimeSingleMeasurement_.has_value()) {
          table = &makeBenchmarkTable(
              &results, tableName, overlapChance_, smallerTableSorted,
              biggerTableSorted, ratioRows, smallerTableAmountRows_,
              smallerTableAmountColumns_, biggerTableAmountColumns_);
        } else {
          table = &makeGrowingBenchmarkTable(
              &results, tableName,
              [maxTime{maxTimeSingleMeasurement_.value()}](
                  const ResultTable& table) {
                // If the tables has no rows, that's an automatic pass.
                if (table.numRows() == 0) {
                  return true;
                }

                // Did any measurement take to long in the newest row?
                return checkIfFunctionMeasurementOfRowUnderMaxtime(
                    table, table.numRows() - 1, static_cast<float>(maxTime));
              },
              overlapChance_, smallerTableSorted, biggerTableSorted,
              [this](const size_t& row) -> size_t {
                static size_t startingExponent = calculateNextWholeExponent(
                    2, minBiggerTableRows_ / smallerTableAmountRows_);
                return std::pow(2, startingExponent + row);
              },
              smallerTableAmountRows_, smallerTableAmountColumns_,
              biggerTableAmountColumns_);
        }

        // Add the metadata, that changes with every call and can't be
        // generalized.
        BenchmarkMetadata& meta = table->metadata();
        meta.addKeyValuePair("smallerTableSorted", smallerTableSorted);
        meta.addKeyValuePair("biggerTableSorted", biggerTableSorted);
      }
    }

    return results;
  }

  BenchmarkMetadata getMetadata() const override {
    BenchmarkMetadata meta{};

    meta.addKeyValuePair("Value changing with every row", "ratioRows");
    meta.addKeyValuePair("overlap", overlapChance_);
    meta.addKeyValuePair("smallerTableAmountRows", smallerTableAmountRows_);
    meta.addKeyValuePair("smallerTableAmountColumns",
                         smallerTableAmountColumns_);
    meta.addKeyValuePair("biggerTableAmountColumns", biggerTableAmountColumns_);

    return meta;
  }
};

// Create benchmark tables, where the smaller table grows and the ratio
// between tables stays the same. As does the amount of columns.
class BmOnlySmallerTableSizeChanges final
    : public GeneralInterfaceImplementation {
 public:
  std::string name() const override {
    return "Benchmarktables, where the smaller table grows and the ratio "
           "between tables stays the same.";
  }

  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        // We also make multiple tables for different row ratios.
        for (const size_t ratioRows :
             createExponentVectorUntilSize(10, minRatioRows_, maxRatioRows_)) {
          const std::string& tableName = absl::StrCat(
              "The amount of rows in the smaller table grows and the ratio, to "
              "the amount of rows in the bigger table, stays at ",
              ratioRows, ".");

          // We have to call different functions depending on if there was a max
          // time limit to a single measurement.
          ResultTable* table;
          if (!maxTimeSingleMeasurement_.has_value()) {
            /*
            We got the fixed ratio and the variable amount of rows for the
            bigger table. Those can be used to calculate the number of rows in
            the smaller table needed for a benchmark table, that has those
            values.
            */
            const std::vector<size_t> smallerTableAmountRows{
                ad_utility::transform(
                    createExponentVectorUntilSize(10, minBiggerTableRows_,
                                                  maxBiggerTableRows_),
                    [&ratioRows](const size_t& number) {
                      return number / ratioRows;
                    })};

            table = &makeBenchmarkTable(
                &results, tableName, overlapChance_, smallerTableSorted,
                biggerTableSorted, ratioRows, smallerTableAmountRows,
                smallerTableAmountColumns_, biggerTableAmountColumns_);
          } else {
            table = &makeGrowingBenchmarkTable(
                &results, tableName,
                [maxTime{maxTimeSingleMeasurement_.value()}](
                    const ResultTable& table) {
                  // If the tables has no rows, that's an automatic pass.
                  if (table.numRows() == 0) {
                    return true;
                  }

                  // Did any measurement take to long in the newest row?
                  return checkIfFunctionMeasurementOfRowUnderMaxtime(
                      table, table.numRows() - 1, static_cast<float>(maxTime));
                },
                overlapChance_, smallerTableSorted, biggerTableSorted,
                ratioRows,
                [this, &ratioRows](const size_t& row) -> size_t {
                  static size_t startingExponent = calculateNextWholeExponent(
                      2, minBiggerTableRows_ / ratioRows);
                  return std::pow(2, startingExponent + row);
                },
                smallerTableAmountColumns_, biggerTableAmountColumns_);
          }

          // Add the metadata, that changes with every call and can't be
          // generalized.
          BenchmarkMetadata& meta = table->metadata();
          meta.addKeyValuePair("ratioRows", ratioRows);
          meta.addKeyValuePair("smallerTableSorted", smallerTableSorted);
          meta.addKeyValuePair("biggerTableSorted", biggerTableSorted);
        }
      }
    }

    return results;
  }

  BenchmarkMetadata getMetadata() const override {
    BenchmarkMetadata meta{};

    meta.addKeyValuePair("Value changing with every row",
                         "smallerTableAmountRows");
    meta.addKeyValuePair("overlap", overlapChance_);
    meta.addKeyValuePair("smallerTableAmountColumns",
                         smallerTableAmountColumns_);
    meta.addKeyValuePair("biggerTableAmountColumns", biggerTableAmountColumns_);

    return meta;
  }
};

// Create benchmark tables, where the tables are the same size and
// both just get more rows.
class BmSameSizeRowGrowth final : public GeneralInterfaceImplementation {
 public:
  std::string name() const override {
    return "Benchmarktables, where the tables are the same size and both just "
           "get more rows.";
  }

  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    // Easier reading.
    const std::vector<size_t> smallerTableAmountRows{
        createExponentVectorUntilSize(10, minBiggerTableRows_,
                                      maxBiggerTableRows_)};

    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        constexpr std::string_view tableName =
            "Both tables always have the same amount of rows and that amount "
            "grows.";

        // We have to call different functions depending on if there was a max
        // time limit to a single measurement.
        ResultTable* table;
        if (!maxTimeSingleMeasurement_.has_value()) {
          table = &makeBenchmarkTable(
              &results, tableName, overlapChance_, smallerTableSorted,
              biggerTableSorted, static_cast<size_t>(1), smallerTableAmountRows,
              smallerTableAmountColumns_, biggerTableAmountColumns_);
        } else {
          table = &makeGrowingBenchmarkTable(
              &results, tableName,
              [maxTime{maxTimeSingleMeasurement_.value()}](
                  const ResultTable& table) {
                // If the tables has no rows, that's an automatic pass.
                if (table.numRows() == 0) {
                  return true;
                }

                // Did any measurement take to long in the newest row?
                return checkIfFunctionMeasurementOfRowUnderMaxtime(
                    table, table.numRows() - 1, static_cast<float>(maxTime));
              },
              overlapChance_, smallerTableSorted, biggerTableSorted,
              static_cast<size_t>(1),
              [this](const size_t& row) -> size_t {
                static size_t startingExponent =
                    calculateNextWholeExponent(2, minBiggerTableRows_);
                return std::pow(2, startingExponent + row);
              },
              smallerTableAmountColumns_, biggerTableAmountColumns_);
        }
        // Add the metadata, that changes with every call and can't be
        // generalized.
        BenchmarkMetadata& meta = table->metadata();
        meta.addKeyValuePair("smallerTableSorted", smallerTableSorted);
        meta.addKeyValuePair("biggerTableSorted", biggerTableSorted);
      }
    }

    return results;
  }

  BenchmarkMetadata getMetadata() const override {
    BenchmarkMetadata meta{};

    meta.addKeyValuePair("Value changing with every row",
                         "smallerTableAmountRows");
    meta.addKeyValuePair("overlap", overlapChance_);
    meta.addKeyValuePair("ratioRows", 1);
    meta.addKeyValuePair("smallerTableAmountColumns",
                         smallerTableAmountColumns_);
    meta.addKeyValuePair("biggerTableAmountColumns", biggerTableAmountColumns_);

    return meta;
  }
};

// Registering the benchmarks
AD_REGISTER_BENCHMARK(BmUnsortedAndSortedIdTable);
AD_REGISTER_BENCHMARK(BmSameSizeRowGrowth);
AD_REGISTER_BENCHMARK(BmOnlySmallerTableSizeChanges);
AD_REGISTER_BENCHMARK(BmOnlyBiggerTableSizeChanges);
}  // namespace ad_benchmark
