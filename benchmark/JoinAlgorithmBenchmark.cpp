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
#include <optional>
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
#include "../test/util/RandomTestHelpers.h"
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

using namespace std::string_literals;

namespace ad_benchmark {
/*
@brief Creates an overlap between the join columns of the IdTables, by
randomly overiding entries of the smaller table with entries of the bigger
table.

@param smallerTable The table, where join column entries will be
overwritten.
@param biggerTable The table, where join column entries will be copied from.
@param probabilityToCreateOverlap The height of the probability for any
join column entry of smallerTable to be overwritten by a random join column
entry of biggerTable.
@param randomSeed Seed for the random generators.
*/
static void createOverlapRandomly(IdTableAndJoinColumn* const smallerTable,
                                  const IdTableAndJoinColumn& biggerTable,
                                  const double probabilityToCreateOverlap,
                                  ad_utility::RandomSeed randomSeed) {
  // For easier reading.
  const size_t smallerTableJoinColumn = (*smallerTable).joinColumn;
  const size_t smallerTableNumberRows = (*smallerTable).idTable.numRows();

  // The probability for creating an overlap must be in (0,100], any other
  // values make no sense.
  AD_CONTRACT_CHECK(0 < probabilityToCreateOverlap &&
                    probabilityToCreateOverlap <= 100);

  // Is the bigger table actually bigger?
  AD_CONTRACT_CHECK(smallerTableNumberRows <= biggerTable.idTable.numRows());

  // Seeds for the random generators, so that things are less similiar.
  const std::array<ad_utility::RandomSeed, 2> seeds =
      createArrayOfRandomSeeds<2>(std::move(randomSeed));

  // Creating the generator for choosing a random row in the bigger table.
  ad_utility::SlowRandomIntGenerator<size_t> randomBiggerTableRow(
      0, biggerTable.idTable.numRows() - 1, seeds.at(0));

  // Generator for checking, if an overlap should be created.
  ad_utility::RandomDoubleGenerator randomDouble(0, 100, seeds.at(1));

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
The columns of the automaticly generated benchmark tables contain the following
informations:
- The parameter, that changes with every row..
- Time needed for sorting `IdTable`s.
- Time needed for merge/galloping join.
- Time needed for sorting and merge/galloping added togehter.
- Time needed for the hash join.
- How many rows the result of joining the tables has.
- How much faster the hash join is. For example: Two times faster.

The following global variables exist, in order to make the information about the
order of columns explicit.
*/
constexpr size_t CHANGING_PARAMTER_COLUMN_NUM = 0;
constexpr size_t TIME_FOR_SORTING_COLUMN_NUM = 1;
constexpr size_t TIME_FOR_MERGE_GALLOPING_JOIN_COLUMN_NUM = 2;
constexpr size_t TIME_FOR_SORTING_AND_MERGE_GALLOPING_JOIN_COLUMN_NUM = 3;
constexpr size_t TIME_FOR_HASH_JOIN_COLUMN_NUM = 4;
constexpr size_t NUM_ROWS_OF_JOIN_RESULT_COLUMN_NUM = 5;
constexpr size_t JOIN_ALGORITHM_SPEEDUP_COLUMN_NUM = 6;

/*
@brief Adds the function time measurements to a row of the benchmark table
in `makeGrowingBenchmarkTable`.
For an explanation of the parameters, see `makeGrowingBenchmarkTable`.
*/
static void addMeasurementsToRowOfBenchmarkTable(
    ResultTable* table, const size_t& row, const float overlap,
    ad_utility::RandomSeed randomSeed, const bool smallerTableSorted,
    const bool biggerTableSorted, const size_t& ratioRows,
    const size_t& smallerTableAmountRows,
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

  // Seeds for the random generators, so that things are less similiar between
  // the tables.
  const std::array<ad_utility::RandomSeed, 5> seeds =
      createArrayOfRandomSeeds<5>(std::move(randomSeed));

  // Now we create two randomly filled `IdTable`, which have no overlap, and
  // save them together with the information, where their join column is.
  IdTableAndJoinColumn smallerTable{
      createRandomlyFilledIdTable(
          smallerTableAmountRows, smallerTableAmountColumns,
          JoinColumnAndBounds{0, smallerTableJoinColumnLowerBound,
                              smallerTableJoinColumnUpperBound, seeds.at(0)},
          seeds.at(1)),
      0};
  IdTableAndJoinColumn biggerTable{
      createRandomlyFilledIdTable(
          smallerTableAmountRows * ratioRows, biggerTableAmountColumns,
          JoinColumnAndBounds{0, biggerTableJoinColumnLowerBound,
                              biggerTableJoinColumnUpperBound, seeds.at(2)},
          seeds.at(3)),
      0};

  // Creating overlap, if wanted.
  if (overlap > 0) {
    createOverlapRandomly(&smallerTable, biggerTable, overlap, seeds.at(4));
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
      row, TIME_FOR_HASH_JOIN_COLUMN_NUM,
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
      row, TIME_FOR_SORTING_COLUMN_NUM,
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
      row, TIME_FOR_MERGE_GALLOPING_JOIN_COLUMN_NUM,
      [&numberRowsOfResult, &smallerTable, &biggerTable, &joinLambda]() {
        numberRowsOfResult =
            useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda)
                .numRows();
      });

  // Adding the number of rows of the result.
  table->setEntry(row, NUM_ROWS_OF_JOIN_RESULT_COLUMN_NUM, numberRowsOfResult);
}

// `T` must be an invocable object, which can be invoked with `const size_t&`
// and returns an instance of `ReturnType`.
template <typename T, typename ReturnType>
concept growthFunction =
    ad_utility::RegularInvocableWithExactReturnType<T, ReturnType,
                                                    const size_t&>;

// Is `T` of the given type, or a function, that takes `size_t` and return
// the given type?
template <typename T, typename Type>
concept isTypeOrGrowthFunction =
    std::same_as<T, Type> || growthFunction<T, Type>;

// There must be exactly one growth function, that either returns a `size_t`, or
// a `float`.
template <typename... Ts>
concept exactlyOneGrowthFunction =
    ((growthFunction<Ts, size_t> || growthFunction<Ts, float>)+...) == 1;

/*
@brief Create a benchmark table for join algorithm, with the given
parameters for the IdTables, which will keep getting more rows, until the given
stop function decides, that there are enough rows. The rows will be the return
values of the parameter, you gave a function for, and the columns will be:
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
@param parameterName The names of the parameter, you gave a growth function for.
Will be set as the name of the column, which will show the returned values of
the growth function.
@param stopFunction Returns true, if the table benchmark should get a new row.
False, if there are enough rows and the created table should be returned.
Decides the final size of the benchmark table.
@param overlap The height of the probability for any join column entry of
smallerTable to be overwritten by a random join column entry of biggerTable.
@param randomSeed Seed for the random generators.
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
template <
    ad_utility::RegularInvocableWithExactReturnType<bool, const ResultTable&>
        StopFunction,
    isTypeOrGrowthFunction<float> T1, isTypeOrGrowthFunction<size_t> T2,
    isTypeOrGrowthFunction<size_t> T3, isTypeOrGrowthFunction<size_t> T4,
    isTypeOrGrowthFunction<size_t> T5, isTypeOrGrowthFunction<float> T6 = float,
    isTypeOrGrowthFunction<float> T7 = float>
requires exactlyOneGrowthFunction<T1, T2, T3, T4, T5, T6, T7>
static ResultTable& makeGrowingBenchmarkTable(
    BenchmarkResults* results, const std::string_view tableDescriptor,
    std::string parameterName, StopFunction stopFunction, const T1& overlap,
    ad_utility::RandomSeed randomSeed, const bool smallerTableSorted,
    const bool biggerTableSorted, const T2& ratioRows,
    const T3& smallerTableAmountRows, const T4& smallerTableAmountColumns,
    const T5& biggerTableAmountColumns,
    const T6& smallerTableJoinColumnSampleSizeRatio = 1.0,
    const T7& biggerTableJoinColumnSampleSizeRatio = 1.0) {
  // Is something a growth function?
  constexpr auto isGrowthFunction = []<typename T>() {
    /*
    We have to cheat a bit, because being a function is not something, that
    can easily be checked for to my knowledge. Instead, we simply check, if it's
    one of the limited variation of growth function, that we allow.
    */
    if constexpr (growthFunction<T, size_t> || growthFunction<T, float>) {
      return true;
    } else {
      return false;
    }
  };

  // Returns the first argument, that is a growth function.
  auto returnFirstGrowthFunction =
      [&isGrowthFunction]<typename... Ts>(Ts&... args) -> auto& {
    // Put them into a tuple, so that we can easly look them up.
    auto tup = std::tuple<Ts&...>{AD_FWD(args)...};

    // Get the index of the first growth function.
    constexpr static size_t idx =
        ad_utility::getIndexOfFirstTypeToPassCheck<isGrowthFunction, Ts...>();

    // Do we have a valid index?
    static_assert(idx < sizeof...(Ts),
                  "There was no growth function in this parameter pack.");

    return std::get<idx>(tup);
  };

  /*
  @brief Calls the growth function with the number of the next row to be
  created, if it's a function, and returns the result. Otherwise just returns
  the given `possibleGrowthFunction`.
  */
  auto returnOrCall = [&isGrowthFunction]<typename T>(
                          const T& possibleGrowthFunction,
                          const size_t nextRowNumber) {
    if constexpr (isGrowthFunction.template operator()<T>()) {
      return possibleGrowthFunction(nextRowNumber);
    } else {
      return possibleGrowthFunction;
    }
  };

  // For creating a new random seed for every new row.
  auto seedGenerator = [numberGenerator =
                            ad_utility::FastRandomIntGenerator<unsigned int>(
                                std::move(randomSeed))]() mutable {
    return ad_utility::RandomSeed::make(std::invoke(numberGenerator));
  };

  /*
  Now on to creating the benchmark table. Because we don't know, how many row
  names we will have, we just create a table without row names.
  */
  ResultTable& table = results->addTable(
      std::string{tableDescriptor}, {},
      {std::move(parameterName), "Time for sorting", "Merge/Galloping join",
       "Sorting + merge/galloping join", "Hash join",
       "Number of rows in resulting IdTable", "Speedup of hash join"});
  /*
  Adding measurements to the table, as long as the stop function doesn't say
  anything else.
  */
  while (stopFunction(table)) {
    // What's the row number of the next to be added row?
    const size_t rowNumber = table.numRows();

    // Add a new row without content.
    table.addRow();
    table.setEntry(rowNumber, CHANGING_PARAMTER_COLUMN_NUM,
                   returnFirstGrowthFunction(
                       overlap, ratioRows, smallerTableAmountRows,
                       smallerTableAmountColumns, biggerTableAmountColumns,
                       smallerTableJoinColumnSampleSizeRatio,
                       biggerTableJoinColumnSampleSizeRatio)(rowNumber));

    // Converting all our function parameters to non functions.
    addMeasurementsToRowOfBenchmarkTable(
        &table, rowNumber, returnOrCall(overlap, rowNumber),
        std::invoke(seedGenerator), smallerTableSorted, biggerTableSorted,
        returnOrCall(ratioRows, rowNumber),
        returnOrCall(smallerTableAmountRows, rowNumber),
        returnOrCall(smallerTableAmountColumns, rowNumber),
        returnOrCall(biggerTableAmountColumns, rowNumber),
        returnOrCall(smallerTableJoinColumnSampleSizeRatio, rowNumber),
        returnOrCall(biggerTableJoinColumnSampleSizeRatio, rowNumber));
  }

  // Adding together the time for sorting the `IdTables` and then joining
  // them using merge/galloping join.
  sumUpColumns(&table, TIME_FOR_SORTING_AND_MERGE_GALLOPING_JOIN_COLUMN_NUM,
               TIME_FOR_SORTING_COLUMN_NUM,
               TIME_FOR_MERGE_GALLOPING_JOIN_COLUMN_NUM);

  // Calculate, how much of a speedup the hash join algorithm has in comparison
  // to the merge/galloping join algrithm.
  calculateSpeedupOfColumn(&table, TIME_FOR_HASH_JOIN_COLUMN_NUM,
                           TIME_FOR_SORTING_AND_MERGE_GALLOPING_JOIN_COLUMN_NUM,
                           JOIN_ALGORITHM_SPEEDUP_COLUMN_NUM);

  // For more specific adjustments.
  return table;
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

  return checkTime(TIME_FOR_SORTING_COLUMN_NUM) &&
         checkTime(TIME_FOR_MERGE_GALLOPING_JOIN_COLUMN_NUM) &&
         checkTime(TIME_FOR_HASH_JOIN_COLUMN_NUM);
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
ad_utility::MemorySize approximateMemoryNeededByIdTable(
    const size_t& amountRows, const size_t& amountColumns) {
  /*
  The overhead can be, more or less, ignored. We are just concerned over
  the space needed for the entries.
  */
  constexpr size_t memoryPerIdTableEntryInByte = sizeof(IdTable::value_type);

  return ad_utility::MemorySize::bytes(amountRows * amountColumns *
                                       memoryPerIdTableEntryInByte);
}

/*
Partly implements the interface `BenchmarkInterface`, by:

- Providing the member variables, that most of the benchmark classes here set
using the `ConfigManager`.

- A default `getMetadata`, that adds the date and time, where the benchmark
measurements were taken.
*/
class GeneralInterfaceImplementation : public BenchmarkInterface {
  /*
  The max time for a single measurement and the max memory, that a single
  `IdTable` is allowed to take up.
  Both are set via `ConfigManager` and the user at runtime, but because their
  pure form contains coded information, mainly that `0` stands for `infinite`,
  access is regulated via getter, which also transform them into easier to
  understand forms.
  */
  float maxTimeSingleMeasurement_;
  std::string configVariableMaxMemory_;

  /*
  The random seed, that we use to create random seeds for our random
  generators.
  */
  size_t randomSeed_;

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

    decltype(auto) randomSeed = config.addOption(
        "randomSeed",
        "The seed used for random generators. Note: The default value is a "
        "non-deterministic random value, which changes with every execution.",
        &randomSeed_, static_cast<size_t>(std::random_device{}()));

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

    decltype(auto) maxMemoryInStringFormat = config.addOption(
        "maxMemory",
        "Max amount of memory that a `IdTable` is allowed to take up. `0` for "
        "unlimited memory. Example: 4kB, 8MB, 24 B, etc. ...",
        &configVariableMaxMemory_, "0 B"s);

    decltype(auto) maxTimeSingleMeasurement = config.addOption(
        "maxTimeSingleMeasurement",
        "The maximal amount of time, in seconds, any function "
        "measurement is allowed to take. `0` for unlimited time. Note: This "
        "can only be checked, after a measurement was taken.",
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
    Is `maxMemory` big enough, to even allow for one row of the smaller
    table, bigger table, or the table resulting from joining the smaller and
    bigger table?`
    If not, return an `ErrorMessage`.
    */
    auto checkIfMaxMemoryBigEnoughForOneRow =
        [](const ad_utility::MemorySize maxMemory,
           const std::string_view tableName, const size_t amountOfColumns)
        -> std::optional<ad_utility::ErrorMessage> {
      const ad_utility::MemorySize memoryNeededForOneRow =
          approximateMemoryNeededByIdTable(1, amountOfColumns);
      // Remember: `0` is for unlimited memory.
      if (maxMemory == 0_B || memoryNeededForOneRow <= maxMemory) {
        return std::nullopt;
      } else {
        return std::make_optional<ad_utility::ErrorMessage>(
            absl::StrCat("'maxMemory' (", maxMemory.asString(),
                         ") must be big enough, for at least one row "
                         "in the ",
                         tableName, ", which requires at least ",
                         memoryNeededForOneRow.asString(), "."));
      }
    };
    config.addValidator(
        [checkIfMaxMemoryBigEnoughForOneRow](std::string_view maxMemory,
                                             size_t smallerTableNumColumns) {
          return checkIfMaxMemoryBigEnoughForOneRow(
              ad_utility::MemorySize::parse(maxMemory), "smaller table",
              smallerTableNumColumns);
        },
        "'maxMemory' must be big enough for at least one row in the smaller "
        "table.",
        maxMemoryInStringFormat, smallerTableAmountColumns);
    config.addValidator(
        [checkIfMaxMemoryBigEnoughForOneRow](std::string_view maxMemory,
                                             size_t biggerTableNumColumns) {
          return checkIfMaxMemoryBigEnoughForOneRow(
              ad_utility::MemorySize::parse(maxMemory), "bigger table",
              biggerTableNumColumns);
        },
        "'maxMemory' must be big enough for at least one row in the bigger "
        "table.",
        maxMemoryInStringFormat, biggerTableAmountColumns);
    config.addValidator(
        [checkIfMaxMemoryBigEnoughForOneRow](std::string_view maxMemory,
                                             size_t smallerTableNumColumns,
                                             size_t biggerTableNumColumns) {
          return checkIfMaxMemoryBigEnoughForOneRow(
              ad_utility::MemorySize::parse(maxMemory),
              "result of joining the smaller and bigger table",
              smallerTableNumColumns + biggerTableNumColumns - 1);
        },
        "'maxMemory' must be big enough for at least one row in the result of "
        "joining the smaller and bigger table.",
        maxMemoryInStringFormat, smallerTableAmountColumns,
        biggerTableAmountColumns);

    // Is `smallerTableAmountRows` a valid value?
    config.addValidator(generateBiggerEqualLambda(1UL, true),
                        "'smallerTableAmountRows' must be at least 1.",
                        "'smallerTableAmountRows' must be at least 1.",
                        smallerTableAmountRows);

    // Is `smallerTableAmountRows` smaller than `minBiggerTableRows`?
    config.addValidator(
        lessEqualLambda,
        "'smallerTableAmountRows' must be smaller than, or equal to, "
        "'minBiggerTableRows'.",
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
        absl::StrCat(
            "Interessting measurement values show up at ",
            minBiggerTableRows.getConfigOption().getDefaultValueAsString(),
            ", or more, for 'minBiggerTableRows'"),
        minBiggerTableRows);

    // Is `minBiggerTableRows` smaller, or equal, to `maxBiggerTableRows`?
    config.addValidator(
        lessEqualLambda,
        "'minBiggerTableRows' must be smaller than, or equal to, "
        "'maxBiggerTableRows'.",
        "'minBiggerTableRows' must be smaller than, or equal to, "
        "'maxBiggerTableRows'.",
        minBiggerTableRows, maxBiggerTableRows);

    // Do we have at least 1 column?
    config.addValidator(generateBiggerEqualLambda(1UL, true),
                        "'smallerTableAmountColumns' must be at least 1.",
                        "'smallerTableAmountColumns' must be at least 1.",
                        smallerTableAmountColumns);
    config.addValidator(generateBiggerEqualLambda(1UL, true),
                        "'biggerTableAmountColumns' must be at least 1.",
                        "'biggerTableAmountColumns' must be at least 1.",
                        biggerTableAmountColumns);

    // Is `overlapChance_` bigger than 0?
    config.addValidator(generateBiggerEqualLambda(0UL, false),
                        "'overlapChance' must be bigger than 0.",
                        "'overlapChance' must be bigger than 0.",
                        overlapChance);

    /*
    Is `randomSeed_` smaller/equal than the max value for seeds?
    Note: The static cast is needed, because a random generator seed is always
    `unsigned int`:
    */
    config.addValidator(
        [maxSeed = static_cast<size_t>(ad_utility::RandomSeed::max().get())](
            const size_t seed) { return seed <= maxSeed; },
        absl::StrCat("'randomSeed' must be smaller than, or equal to, ",
                     ad_utility::RandomSeed::max().get(), "."),
        absl::StrCat("'randomSeed' must be smaller than, or equal to, ",
                     ad_utility::RandomSeed::max().get(), "."),
        randomSeed);

    // Is `maxTimeSingleMeasurement` a positive number?
    config.addValidator(
        generateBiggerEqualLambda(0UL, true),
        "'maxTimeSingleMeasurement' must be bigger to, or equal to, 0.",
        "'maxTimeSingleMeasurement' must be bigger to, or equal to, 0.",
        maxTimeSingleMeasurement);

    // Is the ratio of rows at least 10?
    config.addValidator(generateBiggerEqualLambda(10UL, true),
                        "'ratioRows' must be at least 10.",
                        "'ratioRows' must be at least 10.", ratioRows);
    config.addValidator(generateBiggerEqualLambda(10UL, true),
                        "'minRatioRows' must be at least 10.",
                        "'minRatioRows' must be at least 10.", minRatioRows);

    // Is `minRatioRows` smaller than `maxRatioRows`?
    config.addValidator(lessEqualLambda,
                        "'minRatioRows' must be smaller than, or equal to, "
                        "'maxRatioRows'.",
                        "'minRatioRows' must be smaller than, or equal to, "
                        "'maxRatioRows'.",
                        minRatioRows, maxRatioRows);
  }

  /*
  @brief Add metadata information, that is always interessting, if it has been
  set from outside:
  - "maxTimeSingleMeasurement"
  - "maxMemory"
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
        "maxMemory",
        ad_utility::MemorySize::parse(configVariableMaxMemory_).getBytes());
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
  @brief The maximum amount of memory, the bigger table, and by simple logic
  also the smaller table, is allowed to take up. Iff returns the same value as
  `maxMemory()`, if the `maxMemory` configuration option, interpreted as a
  `MemorySize`, wasn't set to `0`. (Which stand for infinite memory.)
  */
  ad_utility::MemorySize maxMemoryBiggerTable() const {
    return maxMemory().value_or(approximateMemoryNeededByIdTable(
        maxBiggerTableRows_, biggerTableAmountColumns_));
  }

  /*
  The maximum amount of memory, any table is allowed to take up.
  Returns the value of the `maxMemory` configuration option interpreted as a
  `MemorySize`, if it wasn't set to `0`. (Which stand for infinite memory.) In
  the case of `0`, returns an empty optional.
  */
  std::optional<ad_utility::MemorySize> maxMemory() const {
    ad_utility::MemorySize maxMemory{
        ad_utility::MemorySize::parse(configVariableMaxMemory_)};
    if (maxMemory != 0_B) {
      return {std::move(maxMemory)};
    } else {
      return {std::nullopt};
    }
  }

  /*
  Getter for the the random seed, that we use to create random seeds for our
  random generators.
  */
  ad_utility::RandomSeed randomSeed() const {
    /*
    Note: The static cast is needed, because a random generator seed is always
    `unsigned int`:
    */
    return ad_utility::RandomSeed::make(static_cast<unsigned int>(randomSeed_));
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
- None of the generated `IdTable`s are to big.

@tparam SmallerTableMemorySizeFunction, BiggerTableMemorySizeFunction A lambda
function, that takes a `const size_t&` and return a `MemorySize`.

@param maxTime The maximum amount of time, a single function measurements is
allowed to take. If no value is given, it will not be tested.
@param maxMemorySmallerTable, maxMemoryBiggerTable, maxMemoryJoinResultTable How
much memory space the `IdTable` is allowed to have at maximum. If no value is
given, it will  not be tested.
@param smallerTableMemorySizeFunction, biggerTableMemorySizeFunction These
functions should calculate/approximate the amount of memory, the bigger and
smaller `IdTable` take. The only parameter given is the row number in
the benchmark table.
@param resultTableAmountColumns How many columns the result of joining the
smaller and bigger `IdTable` has. Used for calculating/approximating the memory
space, that this result table takes up.
*/
template <ad_utility::RegularInvocableWithExactReturnType<
              ad_utility::MemorySize, const size_t&>
              SmallerTableMemorySizeFunction,
          ad_utility::RegularInvocableWithExactReturnType<
              ad_utility::MemorySize, const size_t&>
              BiggerTableMemorySizeFunction>
auto createDefaultStoppingLambda(
    const std::optional<float>& maxTime,
    const std::optional<ad_utility::MemorySize>& maxMemorySmallerTable,
    const std::optional<ad_utility::MemorySize>& maxMemoryBiggerTable,
    const std::optional<ad_utility::MemorySize>& maxMemoryJoinResultTable,
    const SmallerTableMemorySizeFunction& smallerTableMemorySizeFunction,
    const BiggerTableMemorySizeFunction& biggerTableMemorySizeFunction,
    const size_t& resultTableAmountColumns) {
  return [maxTime, maxMemorySmallerTable, maxMemoryBiggerTable,
          maxMemoryJoinResultTable, smallerTableMemorySizeFunction,
          biggerTableMemorySizeFunction,
          resultTableAmountColumns](const ResultTable& table) -> bool {
    // If the tables has no rows, that's an automatic pass.
    if (table.numRows() == 0) {
      return true;
    }

    // The row, that we are looking at.
    const size_t row = table.numRows() - 1;

    // Checks, if the table, described by the `memorySizeFunction`, doesn't take
    // up to much memory space.
    auto checkMemorySizeOfTable =
        [](const size_t& benchmarkTableRowNumber,
           ad_utility::RegularInvocableWithExactReturnType<
               ad_utility::MemorySize, const size_t&> auto memorySizeFunction,
           const std::optional<ad_utility::MemorySize> maxMemory) {
          return maxMemory.has_value()
                     ? memorySizeFunction(benchmarkTableRowNumber) <=
                           maxMemory.value()
                     : true;
        };

    /*
    Remember: We are currently deciding, if the current `ResultTable`
    should get a new row and we don't want to create `IdTable`s, that are
    to big.
    However, unlike the function measurement times, we CAN calculate the
    memory space taken up by a `IdTable`, without creating it.
    So, we check, if the smaller and bigger table of the NEXT row would be
    to big, instead of the smaller and bigger table of this row.

    Unfortunaly, that can't be done with the result of joining the two
    tables. For that, we would need the amount of rows in it, which we
    can't have, without having already created it.
    */
    return (maxTime.has_value() ? checkIfFunctionMeasurementOfRowUnderMaxtime(
                                      table, row, maxTime.value())
                                : true) &&
           checkMemorySizeOfTable(row + 1, smallerTableMemorySizeFunction,
                                  maxMemorySmallerTable) &&
           checkMemorySizeOfTable(row + 1, biggerTableMemorySizeFunction,
                                  maxMemoryBiggerTable) &&
           checkMemorySizeOfTable(
               row,
               [&resultTableAmountColumns, &table](const size_t& rowNumber) {
                 return approximateMemoryNeededByIdTable(
                     std::stoull(table.getEntry<std::string>(
                         rowNumber, NUM_ROWS_OF_JOIN_RESULT_COLUMN_NUM)),
                     resultTableAmountColumns);
               },
               maxMemoryJoinResultTable);
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

        // Returns the ratio used for the measurements in a given row.
        auto growthFunction = createDefaultGrowthLambda(
            10, minBiggerTableRows_ / smallerTableAmountRows_);

        // The lambda for signalling the benchmark table creation function, that
        // the table is finished.
        const ad_utility::MemorySize maxMemorySizeBiggeTable =
            maxMemoryBiggerTable();
        auto stoppingFunction = createDefaultStoppingLambda(
            maxTimeSingleMeasurement(), maxMemorySizeBiggeTable,
            maxMemorySizeBiggeTable, maxMemory(),
            [this](const size_t&) {
              return approximateMemoryNeededByIdTable(
                  smallerTableAmountRows_, smallerTableAmountColumns_);
            },
            [this, &growthFunction](const size_t& row) {
              return approximateMemoryNeededByIdTable(
                  smallerTableAmountRows_ * growthFunction(row),
                  biggerTableAmountColumns_);
            },
            smallerTableAmountColumns_ + biggerTableAmountColumns_ - 1);

        ResultTable& table = makeGrowingBenchmarkTable(
            &results, tableName, "Row ratio", stoppingFunction, overlapChance_,
            randomSeed(), smallerTableSorted, biggerTableSorted, growthFunction,
            smallerTableAmountRows_, smallerTableAmountColumns_,
            biggerTableAmountColumns_);

        // Add the metadata, that changes with every call and can't be
        // generalized.
        BenchmarkMetadata& meta = table.metadata();
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
    meta.addKeyValuePair("randomSeed", randomSeed().get());
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

          // Returns the amount of rows in the smaller `IdTable`, used for the
          // measurements in a given row.
          auto growthFunction =
              createDefaultGrowthLambda(10, minBiggerTableRows_ / ratioRows);

          // The lambda for signalling the benchmark table creation function,
          // that the table is finished.
          const ad_utility::MemorySize maxMemorySizeBiggeTable =
              maxMemoryBiggerTable();
          auto stoppingFunction = createDefaultStoppingLambda(
              maxTimeSingleMeasurement(), maxMemorySizeBiggeTable,
              maxMemorySizeBiggeTable, maxMemory(),
              [this, &growthFunction](const size_t& row) {
                return approximateMemoryNeededByIdTable(
                    growthFunction(row), smallerTableAmountColumns_);
              },
              [this, &growthFunction, &ratioRows](const size_t& row) {
                return approximateMemoryNeededByIdTable(
                    growthFunction(row) * ratioRows, biggerTableAmountColumns_);
              },
              smallerTableAmountColumns_ + biggerTableAmountColumns_ - 1);

          ResultTable& table = makeGrowingBenchmarkTable(
              &results, tableName, "Amount of rows in the smaller table",
              stoppingFunction, overlapChance_, randomSeed(),
              smallerTableSorted, biggerTableSorted, ratioRows, growthFunction,
              smallerTableAmountColumns_, biggerTableAmountColumns_);

          // Add the metadata, that changes with every call and can't be
          // generalized.
          BenchmarkMetadata& meta = table.metadata();
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
    meta.addKeyValuePair("randomSeed", randomSeed().get());
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

        // Returns the amount of rows in the smaller `IdTable`, used for the
        // measurements in a given row.
        auto growthFunction =
            createDefaultGrowthLambda(10, minBiggerTableRows_);

        // The lambda for signalling the benchmark table creation function,
        // that the table is finished.
        const ad_utility::MemorySize maxMemorySizeBiggeTable =
            maxMemoryBiggerTable();
        auto stoppingFunction = createDefaultStoppingLambda(
            maxTimeSingleMeasurement(), maxMemorySizeBiggeTable,
            maxMemorySizeBiggeTable, maxMemory(),
            [this, &growthFunction](const size_t& row) {
              return approximateMemoryNeededByIdTable(
                  growthFunction(row), smallerTableAmountColumns_);
            },
            [this, &growthFunction](const size_t& row) {
              return approximateMemoryNeededByIdTable(
                  growthFunction(row), biggerTableAmountColumns_);
            },
            smallerTableAmountColumns_ + biggerTableAmountColumns_ - 1);

        ResultTable& table = makeGrowingBenchmarkTable(
            &results, tableName, "Amount of rows", stoppingFunction,
            overlapChance_, randomSeed(), smallerTableSorted, biggerTableSorted,
            1UL, growthFunction, smallerTableAmountColumns_,
            biggerTableAmountColumns_);

        // Add the metadata, that changes with every call and can't be
        // generalized.
        BenchmarkMetadata& meta = table.metadata();
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
    meta.addKeyValuePair("randomSeed", randomSeed().get());
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
