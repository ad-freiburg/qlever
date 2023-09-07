// Copyright 2023, University of Freiburg,
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
#include <functional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <valarray>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkMetadata.h"
#include "../benchmark/util/BenchmarkTableCommonCalculations.h"
#include "../test/util/IdTableHelpers.h"
#include "../test/util/JoinHelpers.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/ResultTable.h"
#include "engine/idTable/IdTable.h"
#include "util/Algorithm.h"
#include "util/ConfigManager/ConfigManager.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/MemorySize/MemorySize.h"
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
          smallerTableAmountRows, smallerTableAmountColumns,
          JoinColumnAndBounds{0, smallerTableJoinColumnLowerBound,
                              smallerTableJoinColumnUpperBound}),
      0};
  IdTableAndJoinColumn biggerTable{
      createRandomlyFilledIdTable(
          smallerTableAmountRows * ratioRows, biggerTableAmountColumns,
          JoinColumnAndBounds{0, biggerTableJoinColumnLowerBound,
                              biggerTableJoinColumnUpperBound}),
      0};

  // Creating overlap, if wanted.
  if (overlap > 0) {
    createOverlapRandomly(&smallerTable, biggerTable, overlap);
  }

  // Sort the `IdTables`, if they should be.
  if (smallerTableSorted) {
    sortIdTableByJoinColumnInPlace(smallerTable);
  }
  if (biggerTableSorted) {
    sortIdTableByJoinColumnInPlace(biggerTable);
  }

  // Adding the benchmark measurements to the current row.

  // The number of rows, that the joined `ItdTable`s end up having.
  size_t numberRowsOfResult;

  // Hash join first, because merge/galloping join sorts all tables, if
  // needed, before joining them.
  table->addMeasurement(
      row, 4,
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
      row, 1,
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
      row, 2,
      [&numberRowsOfResult, &smallerTable, &biggerTable, &joinLambda]() {
        numberRowsOfResult =
            useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda)
                .numRows();
      });

  // Adding the number of rows of the result.
  table->setEntry(row, 5, std::to_string(numberRowsOfResult));
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
 *  - The parameter, you gave a list for.
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
      {"Parameter value", "Time for sorting", "Merge/Galloping join",
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
  sumUpColumns(table, 3, static_cast<size_t>(1), static_cast<size_t>(2));

  // Calculate, how much of a speedup the hash join algorithm has in comparison
  // to the merge/galloping join algrithm.
  calculateSpeedupOfColumn(table, 4, 3, 6);

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
- Return values of the parameter, you gave a function for.
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
be a function, if all other template `T` parameter are vectors.
@tparam T2, T3, T4, T5 Must be a size_t, or a function, that takes the row
number of the next to be generated row as `const size_t&`, and returns a size_t.
Can only be a function, if all other template `T` parameter are vectors.

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

    // Get the index of the first function.
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
      {"Parameter value", "Time for sorting", "Merge/Galloping join",
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
    table->addRow();
    table->setEntry(rowNumber, 0,
                    std::to_string(returnFirstFunction(
                        overlap, ratioRows, smallerTableAmountRows,
                        smallerTableAmountColumns, biggerTableAmountColumns,
                        smallerTableJoinColumnSampleSizeRatio,
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
  sumUpColumns(table, 3, static_cast<size_t>(1), static_cast<size_t>(2));

  // Calculate, how much of a speedup the hash join algorithm has in comparison
  // to the merge/galloping join algrithm.
  calculateSpeedupOfColumn(table, 4, 3, 6);

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

  // We measure the functions in column 1, 2 and 4.
  return checkTime(1) && checkTime(2) && checkTime(4);
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
  auto currentExponent = static_cast<size_t>(
      std::pow(base, calculateNextWholeExponent(base, startingPoint)));

  // The rest of the exponents.
  while (currentExponent <= stoppingPoint) {
    exponentVector.push_back(currentExponent);
    currentExponent *= base;
  }

  return exponentVector;
}

/*
@brief Approximates the amount of memory (in byte), that a `IdTable` needs.

@param amountRows, amountColumns How many rows and columns the `IdTable` has.
*/
size_t approximateMemoryNeededByIdTable(const size_t& amountRows,
                                        const size_t& amountColumns) {
  /*
  The overhead can be, more or less, ignored. We are just concerned over
  the space needed for the entries.
  */
  constexpr size_t memoryPerIdTableEntryInByte = sizeof(IdTable::value_type);

  return amountRows * amountColumns * memoryPerIdTableEntryInByte;
}

/*
Partly implements the interface `BenchmarkInterface`, by:

- Providing the member variables, that most of the benchmark classes here set
using the `ConfigManager`.

- A default `getMetadata`, that adds the date and time, where the benchmark
measurements were taken.
*/
class GeneralInterfaceImplementation : public BenchmarkInterface {
  // TODO Can we get rid of the getter? I mean, it's just an implicit converter.
  /*
  The max time for a single measurement and the max memory, that a single
  `IdTable` is allowed to take up.
  Both are set via `ConfigManager` and the user at runtime, but because their
  pure form contains coded information, mainly that `0` stands for `infinite`,
  access is regulated via getter, which also transform them into easier to
  understand forms.
  */
  float maxTimeSingleMeasurement_;
  size_t maxMemoryInMB_;

 protected:
  /*
  The benchmark classes after this point always make tables, where one
  attribute of the `IdTables` gets bigger with every row, while the other
  attributes stay the same.
  For the attributes, that don't stay the same, they create a list of
  exponents, with basis $2$, from $1$ up to an upper boundary for the
  exponents. The variables, that define such boundaries, always begin with
  `max`.

  For an explanation, what a member variables does, see the created
  `ConfigOption`s.
  */

  size_t smallerTableAmountRows_;
  size_t minBiggerTableRows_;
  size_t maxBiggerTableRows_;
  size_t smallerTableAmountColumns_;
  size_t biggerTableAmountColumns_;
  float overlapChance_;
  size_t ratioRows_;
  size_t minRatioRows_;
  size_t maxRatioRows_;

 public:
  // The default constructor, which sets up the `ConfigManager`.
  GeneralInterfaceImplementation() {
    ad_utility::ConfigManager& config = getConfigManager();

    decltype(auto) smallerTableAmountRows = config.addOption(
        "smallerTableAmountRows",
        "Amount of rows for the smaller `IdTable`, if we always "
        "use the same amount.",
        &smallerTableAmountRows_, 1000UL);

    constexpr size_t minBiggerTableRowsDefault = 100000;
    decltype(auto) minBiggerTableRows = config.addOption(
        "minBiggerTableRows",
        "The minimum amount of rows, that the bigger `IdTable` should have.",
        &minBiggerTableRows_, minBiggerTableRowsDefault);
    decltype(auto) maxBiggerTableRows = config.addOption(
        "maxBiggerTableRows",
        "The maximum amount of rows, that the bigger `IdTable` should have.",
        &maxBiggerTableRows_, 10000000UL);

    decltype(auto) smallerTableAmountColumns =
        config.addOption("smallerTableAmountColumns",
                         "The amount of columns in the smaller IdTable.",
                         &smallerTableAmountColumns_, 20UL);
    decltype(auto) biggerTableAmountColumns =
        config.addOption("biggerTableAmountColumns",
                         "The amount of columns in the bigger IdTable.",
                         &biggerTableAmountColumns_, 20UL);

    decltype(auto) overlapChance = config.addOption(
        "overlapChance",
        "Chance for an entry in the join column of the smaller `IdTable` to be "
        "the same value as an entry in the join column of the bigger "
        "`IdTable`. Must be in the range $(0,100]$.",
        &overlapChance_, 42.f);

    decltype(auto) ratioRows = config.addOption(
        "ratioRows",
        "The row ratio between the smaller and the bigger `IdTable`. That is "
        "the value of the amount of rows in the bigger `IdTable` divided "
        "through the amount of rows in the smaller `IdTable`.",
        &ratioRows_, 10UL);

    decltype(auto) minRatioRows = config.addOption(
        "minRatioRows",
        "The minimum row ratio between the smaller and the bigger `IdTable`.",
        &minRatioRows_, 10UL);
    decltype(auto) maxRatioRows = config.addOption(
        "maxRatioRows",
        "The maximum row ratio between the smaller and the bigger `IdTable`.",
        &maxRatioRows_, 1000UL);

    decltype(auto) maxMemoryInMB = config.addOption(
        "maxMemoryInMB",
        "Max amount of memory, in MB, that a `IdTable` is allowed "
        "to take up. `0` for unlimited memory.",
        &maxMemoryInMB_, 0UL);

    decltype(auto) maxTimeSingleMeasurement = config.addOption(
        "maxTimeSingleMeasurement",
        "The maximal amount of time, in seconds, any function "
        "measurement is allowed to take. `0` for unlimited time.",
        &maxTimeSingleMeasurement_, 0.f);

    // Helper function for generating lambdas for validators.

    /*
    @brief The generated lambda returns true, iff if it is called with a value,
    that is bigger than the given minimum value

    @param canBeEqual If true, the generated lamba also returns true, if the
    values are equal.
    */
    auto generateBiggerEqualLambda = []<typename T>(const T& minimumValue,
                                                    bool canBeEqual) {
      return [minimumValue, canBeEqual](const T& valueToCheck) {
        return valueToCheck > minimumValue ||
               (canBeEqual && valueToCheck == minimumValue);
      };
    };

    // Object with a `operator()` for the `<=` operator.
    auto lessEqualLambda = std::less_equal<size_t>{};

    // Adding the validators.

    /*
    Is `maxMemoryInMB` big enough, to even allow for one row of the smaller
    table, bigger table, or the table resulting from joining the smaller and
    bigger table?`
    */
    auto checkIfMaxMemoryBigEnoughForOneRow =
        [](const ad_utility::MemorySize maxMemory,
           const size_t amountOfColumns) {
          // Remember: `0` is for unlimited memory.
          // TODO Use the normal comparison operator, once it got implemented
          // for `MemorySize`.
          if (maxMemory.getBytes() ==
              ad_utility::MemorySize::bytes(0).getBytes()) {
            return true;
          }

          return approximateMemoryNeededByIdTable(1, amountOfColumns) >=
                 maxMemory.getBytes();
        };
    config.addValidator(
        [checkIfMaxMemoryBigEnoughForOneRow](const size_t maxMemoryInMB,
                                             size_t smallerTableAmountColumns) {
          return checkIfMaxMemoryBigEnoughForOneRow(
              ad_utility::MemorySize::megabytes(maxMemoryInMB),
              smallerTableAmountColumns);
        },
        "'maxMemoryInMB' must be big enough, for at least one row in the the "
        "smaller table.",
        maxMemoryInMB, smallerTableAmountColumns);
    config.addValidator(
        [checkIfMaxMemoryBigEnoughForOneRow](const size_t maxMemoryInMB,
                                             size_t biggerTableAmountColumns) {
          return checkIfMaxMemoryBigEnoughForOneRow(
              ad_utility::MemorySize::megabytes(maxMemoryInMB),
              biggerTableAmountColumns);
        },
        "'maxMemoryInMB' must be big enough, for at least one row in the the "
        "bigger table.",
        maxMemoryInMB, biggerTableAmountColumns);
    config.addValidator(
        [checkIfMaxMemoryBigEnoughForOneRow](const size_t maxMemoryInMB,
                                             size_t smallerTableAmountColumns,
                                             size_t biggerTableAmountColumns) {
          return checkIfMaxMemoryBigEnoughForOneRow(
              ad_utility::MemorySize::megabytes(maxMemoryInMB),
              smallerTableAmountColumns + biggerTableAmountColumns - 1);
        },
        "'maxMemoryInMB' must be big enough, for at least one row in the the "
        "result of joining the smaller and bigger table.",
        maxMemoryInMB, smallerTableAmountColumns, biggerTableAmountColumns);

    // Is `smallerTableAmountRows` a valid value?
    config.addValidator(generateBiggerEqualLambda(1UL, true),
                        "'smallerTableAmountRows' must be at least 1.",
                        smallerTableAmountRows);

    // Is `smallerTableAmountRows` smaller than `minBiggerTableRows`?
    config.addValidator(
        lessEqualLambda,
        "'smallerTableAmountRows' must be smaller than, or equal to, "
        "'minBiggerTableRows'.",
        smallerTableAmountRows, minBiggerTableRows);

    // Is `minBiggerTableRows` big enough, to deliver interesting measurements?
    config.addValidator(
        generateBiggerEqualLambda(
            minBiggerTableRows.getConfigOption().getDefaultValue<size_t>(),
            true),
        absl::StrCat(
            "'minBiggerTableRows' is to small. Interessting measurement values "
            "only start to turn up at ",
            minBiggerTableRows.getConfigOption().getDefaultValueAsString(),
            " rows, or more."),
        minBiggerTableRows);

    // Is `minBiggerTableRows` smaller, or equal, to `maxBiggerTableRows`?
    config.addValidator(
        lessEqualLambda,
        "'minBiggerTableRows' must be smaller than, or equal to, "
        "'maxBiggerTableRows'.",
        minBiggerTableRows, maxBiggerTableRows);

    // Do we have at least 1 column?
    config.addValidator(generateBiggerEqualLambda(1UL, true),
                        "'smallerTableAmountColumns' must be at least 1.",
                        smallerTableAmountColumns);
    config.addValidator(generateBiggerEqualLambda(1UL, true),
                        "'biggerTableAmountColumns' must be at least 1.",
                        biggerTableAmountColumns);

    // Is `overlapChance_` bigger than 0?
    config.addValidator(generateBiggerEqualLambda(0UL, false),
                        "'overlapChance' must be bigger than 0.",
                        overlapChance);

    // Is `maxTimeSingleMeasurement` a positive number?
    config.addValidator(
        generateBiggerEqualLambda(0UL, true),
        "'maxTimeSingleMeasurement' must be bigger to, or equal to, 0.",
        maxTimeSingleMeasurement);

    // Is the ratio of rows at least 10?
    config.addValidator(generateBiggerEqualLambda(10UL, true),
                        "'ratioRows' must be at least 10 .", ratioRows);
    config.addValidator(generateBiggerEqualLambda(10UL, true),
                        "'minRatioRows' must be at least 10 .", minRatioRows);

    // Is `minRatioRows` smaller than `maxRatioRows`?
    config.addValidator(lessEqualLambda,
                        "'minRatioRows' must be smaller than, or equal to, "
                        "'maxRatioRows'.",
                        minRatioRows, maxRatioRows);
  }

  /*
  @brief Add metadata information, that is always interessting, if it has been
  set from outside:
  - "maxTimeSingleMeasurement"
  - "maxMemoryInByte"
  */
  void addExternallySetConfiguration(BenchmarkMetadata* meta) const {
    auto addInfiniteWhen0 = [&meta](const std::string& name,
                                    const auto& value) {
      if (value != 0) {
        meta->addKeyValuePair(name, value);
      } else {
        meta->addKeyValuePair(name, "infinite");
      }
    };

    addInfiniteWhen0("maxTimeSingleMeasurement", maxTimeSingleMeasurement_);
    addInfiniteWhen0(
        "maxMemoryInMB",
        ad_utility::MemorySize::megabytes(maxMemoryInMB_).getBytes());
  }

  /*
  Get the value of the corresponding configuration option. Empty optional stands
  for `infinite` time.
  */
  std::optional<float> maxTimeSingleMeasurement() const {
    if (maxTimeSingleMeasurement_ != 0) {
      return {maxTimeSingleMeasurement_};
    } else {
      return {std::nullopt};
    }
  }

  /*
  Get the value of the `maxMemoryInMB_` configuration option interpreted as a
  `MemorySize`. Empty optional stands for `infinite` time.
  */
  std::optional<ad_utility::MemorySize> maxMemory() const {
    if (maxMemoryInMB_ != 0) {
      return {ad_utility::MemorySize::megabytes(maxMemoryInMB_)};
    } else {
      return {std::nullopt};
    }
  }
};

/*
@brief Returns a lambda function, which calculates and returns $base^(x+row)$.
With $row$ being the single `size_t` argument of the function and $x$ being
$log_base(startingPoint)$ rounded up.
*/
auto createDefaultGrowthLambda(const size_t& base,
                               const size_t& startingPoint) {
  return [base, startingExponent{calculateNextWholeExponent(
                    base, startingPoint)}](const size_t& row) {
    return static_cast<size_t>(std::pow(base, startingExponent + row));
  };
}

/*
@brief Create and return a lambda, that returns true, iff:
- (Can be turned off) None of the benchmark measurements took to long.
- (Can be turned off) None of the generated `IdTable`s are to big.

@tparam SmallerTableMemorySizeFunction, BiggerTableMemorySizeFunction A lambda
function, that takes a `const size_t&` and return a `size_t`.

@param maxTime The maximum amount of time, a single function measurements is
allowed to take. If no value is given, it will not be tested.
@param maxMemoryInByte How much memory space a `IdTable` is allowed to have at
maximum. If no value is given, it will not be tested.
@param smallerTableMemorySizeFunction, biggerTableMemorySizeFunction These
functions should calculate/approximate the amount of memory, the bigger and
smaller `IdTable` take, in byte. The only parameter given is the row number in
the benchmark table.
@param resultTableAmountColumns How many columns the result of joining the
smaller and bigger `IdTable` has. Used for calculating/approximating the memory
space, that this result table takes up.
*/
template <invocableWithReturnType<size_t, const size_t&>
              SmallerTableMemorySizeFunction,
          invocableWithReturnType<size_t, const size_t&>
              BiggerTableMemorySizeFunction>
auto createDefaultStoppingLambda(
    const std::optional<float>& maxTime,
    const std::optional<size_t>& maxMemoryInByte,
    const SmallerTableMemorySizeFunction& smallerTableMemorySizeFunction,
    const BiggerTableMemorySizeFunction& biggerTableMemorySizeFunction,
    const size_t& resultTableAmountColumns) {
  return [maxTime, maxMemoryInByte, &smallerTableMemorySizeFunction,
          &biggerTableMemorySizeFunction,
          resultTableAmountColumns](const ResultTable& table) -> bool {
    // If the tables has no rows, that's an automatic pass.
    if (table.numRows() == 0) {
      return true;
    }

    // The row, that we are looking at.
    const size_t row = table.numRows() - 1;

    // Checks, if all tables don't take up to much memory space. Takes the
    // number of the current row in the benchmark table.
    auto checkMemorySizeOfTables =
        [&smallerTableMemorySizeFunction, &biggerTableMemorySizeFunction,
         &maxMemoryInByte, &resultTableAmountColumns,
         &table](const size_t& benchmarkTableRowNumber) {
          return (smallerTableMemorySizeFunction(benchmarkTableRowNumber) <=
                  maxMemoryInByte.value()) &&
                 (biggerTableMemorySizeFunction(benchmarkTableRowNumber) <=
                  maxMemoryInByte.value()) &&
                 (approximateMemoryNeededByIdTable(
                      std::stoull(table.getEntry<std::string>(
                          benchmarkTableRowNumber, 5)),
                      resultTableAmountColumns) <= maxMemoryInByte.value());
        };

    // Did any measurement take to long in the newest row? Or did any table have
    // to many rows?
    return (!maxTime.has_value() && !maxMemoryInByte.has_value()) ||
           ((maxTime.has_value() ? checkIfFunctionMeasurementOfRowUnderMaxtime(
                                       table, row, maxTime.value())
                                 : true) &&
            (maxMemoryInByte.has_value() ? checkMemorySizeOfTables(row)
                                         : true));
  };
}

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

    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        const std::string& tableName =
            absl::StrCat("Smaller table stays at ", smallerTableAmountRows_,
                         " rows, ratio to rows of bigger table grows.");

        /*
        We have to call different functions depending on if there was a max time
        limit to a single measurement, or a max memory usage limit for a single
        table.
        */
        ResultTable* table;
        if (!maxTimeSingleMeasurement().has_value() &&
            !maxMemory().has_value()) {
          /*
          We got the fixed amount of rows for the smaller table and the variable
          amount of rows for the bigger table. Those can be used to calculate
          the ratios needed for a benchmark table, that has those values.
          */
          const std::vector<size_t> ratioRows{ad_utility::transform(
              createExponentVectorUntilSize(10, minBiggerTableRows_,
                                            maxBiggerTableRows_),
              [this](const size_t& number) {
                return number / smallerTableAmountRows_;
              })};

          table = &makeBenchmarkTable(
              &results, tableName, overlapChance_, smallerTableSorted,
              biggerTableSorted, ratioRows, smallerTableAmountRows_,
              smallerTableAmountColumns_, biggerTableAmountColumns_);
        } else {
          // Returns the ratio used for the measurements in a given row.
          auto growthFunction = createDefaultGrowthLambda(
              10, minBiggerTableRows_ / smallerTableAmountRows_);

          table = &makeGrowingBenchmarkTable(
              &results, tableName,
              createDefaultStoppingLambda(
                  maxTimeSingleMeasurement(),
                  maxMemory().has_value()
                      ? std::optional{maxMemory().value().getBytes()}
                      : std::optional<size_t>{},
                  [this](const size_t&) {
                    return approximateMemoryNeededByIdTable(
                        smallerTableAmountRows_, smallerTableAmountColumns_);
                  },
                  [this, &growthFunction](const size_t& row) {
                    return approximateMemoryNeededByIdTable(
                        smallerTableAmountRows_ * growthFunction(row),
                        biggerTableAmountColumns_);
                  },
                  smallerTableAmountColumns_ + biggerTableAmountColumns_ - 1),
              overlapChance_, smallerTableSorted, biggerTableSorted,
              growthFunction, smallerTableAmountRows_,
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

    meta.addKeyValuePair("Value changing with every row", "ratioRows");
    meta.addKeyValuePair("overlapChance", overlapChance_);
    meta.addKeyValuePair("smallerTableAmountRows", smallerTableAmountRows_);
    meta.addKeyValuePair("smallerTableAmountColumns",
                         smallerTableAmountColumns_);
    meta.addKeyValuePair("biggerTableAmountColumns", biggerTableAmountColumns_);

    GeneralInterfaceImplementation::addExternallySetConfiguration(&meta);

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

          /*
          We have to call different functions depending on if there was a max
          time limit to a single measurement, or a max memory usage limit for a
          single table.
          */
          ResultTable* table;
          if (!maxTimeSingleMeasurement().has_value() &&
              !maxMemory().has_value()) {
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
            // Returns the amount of rows in the smaller `IdTable`, used for the
            // measurements in a given row.
            auto growthFunction =
                createDefaultGrowthLambda(10, minBiggerTableRows_ / ratioRows);

            table = &makeGrowingBenchmarkTable(
                &results, tableName,
                createDefaultStoppingLambda(
                    maxTimeSingleMeasurement(),
                    maxMemory().has_value()
                        ? std::optional{maxMemory().value().getBytes()}
                        : std::optional<size_t>{},
                    [this, &growthFunction](const size_t& row) {
                      return approximateMemoryNeededByIdTable(
                          growthFunction(row), smallerTableAmountColumns_);
                    },
                    [this, &growthFunction, &ratioRows](const size_t& row) {
                      return approximateMemoryNeededByIdTable(
                          growthFunction(row) * ratioRows,
                          biggerTableAmountColumns_);
                    },
                    smallerTableAmountColumns_ + biggerTableAmountColumns_ - 1),
                overlapChance_, smallerTableSorted, biggerTableSorted,
                ratioRows, growthFunction, smallerTableAmountColumns_,
                biggerTableAmountColumns_);
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
    meta.addKeyValuePair("overlapChance", overlapChance_);
    meta.addKeyValuePair("smallerTableAmountColumns",
                         smallerTableAmountColumns_);
    meta.addKeyValuePair("biggerTableAmountColumns", biggerTableAmountColumns_);

    GeneralInterfaceImplementation::addExternallySetConfiguration(&meta);

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

    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        constexpr std::string_view tableName =
            "Both tables always have the same amount of rows and that amount "
            "grows.";

        /*
        We have to call different functions depending on if there was a max time
        limit to a single measurement, or a max memory usage limit for a single
        table.
        */
        ResultTable* table;
        if (!maxTimeSingleMeasurement().has_value() &&
            !maxMemory().has_value()) {
          // Easier reading.
          const std::vector<size_t> smallerTableAmountRows{
              createExponentVectorUntilSize(10, minBiggerTableRows_,
                                            maxBiggerTableRows_)};

          table = &makeBenchmarkTable(
              &results, tableName, overlapChance_, smallerTableSorted,
              biggerTableSorted, static_cast<size_t>(1), smallerTableAmountRows,
              smallerTableAmountColumns_, biggerTableAmountColumns_);
        } else {
          // Returns the amount of rows in the smaller `IdTable`, used for the
          // measurements in a given row.
          auto growthFunction =
              createDefaultGrowthLambda(10, minBiggerTableRows_);

          table = &makeGrowingBenchmarkTable(
              &results, tableName,
              createDefaultStoppingLambda(
                  maxTimeSingleMeasurement(),
                  maxMemory().has_value()
                      ? std::optional{maxMemory().value().getBytes()}
                      : std::optional<size_t>{},
                  [this, &growthFunction](const size_t& row) {
                    return approximateMemoryNeededByIdTable(
                        growthFunction(row), smallerTableAmountColumns_);
                  },
                  [this, &growthFunction](const size_t& row) {
                    return approximateMemoryNeededByIdTable(
                        growthFunction(row), biggerTableAmountColumns_);
                  },
                  smallerTableAmountColumns_ + biggerTableAmountColumns_ - 1),
              overlapChance_, smallerTableSorted, biggerTableSorted,
              static_cast<size_t>(1), growthFunction,
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
    meta.addKeyValuePair("overlapChance", overlapChance_);
    meta.addKeyValuePair("ratioRows", 1);
    meta.addKeyValuePair("smallerTableAmountColumns",
                         smallerTableAmountColumns_);
    meta.addKeyValuePair("biggerTableAmountColumns", biggerTableAmountColumns_);

    GeneralInterfaceImplementation::addExternallySetConfiguration(&meta);

    return meta;
  }
};

// Registering the benchmarks
AD_REGISTER_BENCHMARK(BmSameSizeRowGrowth);
AD_REGISTER_BENCHMARK(BmOnlySmallerTableSizeChanges);
AD_REGISTER_BENCHMARK(BmOnlyBiggerTableSizeChanges);
}  // namespace ad_benchmark
