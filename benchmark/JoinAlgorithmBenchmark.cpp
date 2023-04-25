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
#include <cstdio>
#include <ctime>
#include <type_traits>
#include <tuple>

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
    if constexpr (std::is_same_v<T, std::vector<float>>) {
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
                      &ratioRows, &smallerTableAmountRows,
                      &smallerTableAmountColumns, &biggerTableAmountColumns,
                      &smallerTableJoinColumnSampleSizeRatio,
                      &biggerTableJoinColumnSampleSizeRatio](
                         ResultTable* table) {
    BenchmarkMetadata& meta = table->metadata();

    meta.addKeyValuePair("overlap", overlap);
    meta.addKeyValuePair("smallerTableSorted", smallerTableSorted);
    meta.addKeyValuePair("biggerTableSorted", biggerTableSorted);
    meta.addKeyValuePair("ratioRows", ratioRows);
    meta.addKeyValuePair("smallerTableAmountRows", smallerTableAmountRows);
    meta.addKeyValuePair("smallerTableAmountColumns",
                         smallerTableAmountColumns);
    meta.addKeyValuePair("biggerTableAmountColumns", biggerTableAmountColumns);
    meta.addKeyValuePair("smallerTableJoinColumnSampleSizeRatio",
                         smallerTableJoinColumnSampleSizeRatio);
    meta.addKeyValuePair("biggerTableJoinColumnSampleSizeRatio",
                         biggerTableJoinColumnSampleSizeRatio);
  };

  // The lambdas for the join algorithms.
  auto hashJoinLambda = makeHashJoinLambda();
  auto joinLambda = makeJoinLambda();

  // Returns the first argument, that is a vector.
  auto returnFirstVector = []<typename... Ts>(Ts&... args)->auto&{
    // Put them into a tuple, so that we can easly look them up.
    auto tup = std::tuple<Ts&...>{AD_FWD(args)...};

    // Is something a vector?
    constexpr auto isVector = []<typename T>(){
      return ad_utility::isVector<std::decay_t<T>>;};

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
  auto returnEntry = []<typename T>(const T& possibleVector, const size_t pos){
    if constexpr(ad_utility::isVector<T>){
      return possibleVector.at(pos);
    } else {
      return possibleVector;
    }
  };

  // Now on to creating the benchmark table. First, we find out, which of our
  // arguments was the vector and use it to create the row names for the table.
  auto& vec = returnFirstVector(overlap, ratioRows, smallerTableAmountRows,
    smallerTableAmountColumns, biggerTableAmountColumns,
    smallerTableJoinColumnSampleSizeRatio,
    biggerTableJoinColumnSampleSizeRatio);

  // Then, we convert the content of `vec` to strings and add the table.
  ResultTable* table = &(records->addTable(std::string{tableDescriptor},
    ad_utility::transform(vec,
    [](const auto& entry) { return std::to_string(entry); }),
    {"Time for sorting", "Merge/Galloping join",
     "Sorting + merge/galloping join", "Hash join",
     "Number of rows in resulting IdTable", "Speedup of hash join"}));

  // Adding measurements to the table.
  for (size_t i = 0; i < vec.size(); i++){
    // All our function parameters as non vectors.
    const float sOverlap = returnEntry(overlap, i);
    const size_t sRatioRows = returnEntry(ratioRows, i);
    const size_t sSmallerTableAmountRows =
      returnEntry(smallerTableAmountRows, i);
    const size_t sSmallerTableAmountColumns =
      returnEntry(smallerTableAmountColumns, i);
    const size_t sBiggerTableAmountColumns =
      returnEntry(biggerTableAmountColumns, i);
    const float sSmallerTableJoinColumnSampleSizeRatio =
      returnEntry(smallerTableJoinColumnSampleSizeRatio, i);
    const float sBiggerTableJoinColumnSampleSizeRatio =
      returnEntry(biggerTableJoinColumnSampleSizeRatio, i);

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
            std::floor(static_cast<float>(sSmallerTableAmountRows) *
                       sSmallerTableJoinColumnSampleSizeRatio)) -
        1;
    const size_t biggerTableJoinColumnLowerBound =
        smallerTableJoinColumnUpperBound + 1;
    const size_t biggerTableJoinColumnUpperBound =
        biggerTableJoinColumnLowerBound +
        static_cast<size_t>(
            std::floor(static_cast<float>(sSmallerTableAmountRows) *
                       static_cast<float>(sRatioRows) *
                       sBiggerTableJoinColumnSampleSizeRatio)) -
        1;

    // Now we create two randomly filled `IdTable`, which have no overlap, and
    // save them together with the information, where their join column is.
    IdTableAndJoinColumn smallerTable{createRandomlyFilledIdTable(
      sSmallerTableAmountRows, sSmallerTableAmountColumns, 0,
      smallerTableJoinColumnLowerBound, smallerTableJoinColumnUpperBound), 0};
    IdTableAndJoinColumn biggerTable{createRandomlyFilledIdTable(
      sSmallerTableAmountRows * sRatioRows, sBiggerTableAmountColumns, 0,
      biggerTableJoinColumnLowerBound, biggerTableJoinColumnUpperBound), 0};

    // Creating overlap, if wanted.
    if (sOverlap > 0) {
      createOverlapRandomly(&smallerTable, biggerTable, sOverlap);
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
        i, 3,
        [&numberRowsOfResult, &smallerTable, &biggerTable, &hashJoinLambda]() {
          numberRowsOfResult = useJoinFunctionOnIdTables(
                                   smallerTable, biggerTable, hashJoinLambda)
                                   .numRows();
        });

    /*
    The sorting of the `IdTables`. That must be done before the
    merge/galloping, otherwise their algorithm won't result in a correct
    result.
    */
    table->addMeasurement(
        i, 0,
        [&smallerTable, &smallerTableSorted, &biggerTable,
        &biggerTableSorted]() {
          if (!smallerTableSorted) {
            sortIdTableByJoinColumnInPlace(smallerTable);
          }
          if (!biggerTableSorted){
            sortIdTableByJoinColumnInPlace(biggerTable);
          }
        });

    // The merge/galloping join.
    table->addMeasurement(
        i, 1,
        [&numberRowsOfResult, &smallerTable, &biggerTable, &joinLambda]() {
          numberRowsOfResult =
              useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda)
                  .numRows();
        });

    // Adding the number of rows of the result.
    table->setEntry(i, 4, std::to_string(numberRowsOfResult));
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
  addMetadata(table);
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

  // Amount of rows for the smaller `IdTable`, if we always use the amount.
  size_t smallerTableAmountRows_;

  // The maximum amount of rows, that the smaller `IdTable` should have.
  size_t maxSmallerTableRows_;

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

  // The maximal row ratio between the smaller and the bigger `IdTable`.
  size_t maxRatioRows_;

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
                                       static_cast<size_t>(256));

    setToValueInConfigurationOrDefault(
        maxSmallerTableRows_, "maxSmallerTableRows", static_cast<size_t>(2000));

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

    setToValueInConfigurationOrDefault(maxRatioRows_, "maxRatioRows",
                                       static_cast<size_t>(256));
  }

  BenchmarkMetadata getMetadata() const final {
    BenchmarkMetadata meta{};

    // The current point in time according to the system clock.
    std::time_t currentTimePoint =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    /*
    Converting the current time to a string of of format
    'day.month.year hour:minute:second'. Until the year 9999, this string
    should always be 20 characters long, iff you count the trailing null.
    */
    std::array<char, 20> timeAsString;
    std::strftime(timeAsString.data(), timeAsString.size(), "%d.%m.%Y %T",
                  std::localtime(&currentTimePoint));

    // Adding the formatted time to the metadata.
    meta.addKeyValuePair(
        "Point in time, in which taking the"
        " measurements was finished",
        timeAsString.data());

    return meta;
  }
};

// Create benchmark tables, where the smaller table stays at 2000 rows and
// the bigger tables keeps getting bigger. Amount of columns stays the same.
class BmOnlyBiggerTableSizeChanges final
    : public GeneralInterfaceImplementation {
 public:
  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    // Easier reading.
    const std::vector<size_t> ratioRows{
        createExponentVectorUntilSize(2, maxRatioRows_)};
    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        makeBenchmarkTable(
            &results,
            absl::StrCat("Smaller table stays at ", smallerTableAmountRows_,
                         " rows, ratio to rows of", " bigger table grows."),
            overlapChance_, smallerTableSorted, biggerTableSorted, ratioRows,
            smallerTableAmountRows_, smallerTableAmountColumns_,
            biggerTableAmountColumns_);
      }
    }

    return results;
  }
};

// Create benchmark tables, where the smaller table grows and the ratio
// between tables stays the same. As does the amount of columns.
class BmOnlySmallerTableSizeChanges final
    : public GeneralInterfaceImplementation {
 public:
  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    // Easier reading.
    const std::vector<size_t> smallerTableAmountRows{
        createExponentVectorUntilSize(2, maxSmallerTableRows_)};
    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        // We also make multiple tables for different row ratios.
        for (const size_t ratioRows :
             createExponentVectorUntilSize(2, maxRatioRows_)) {
          makeBenchmarkTable(
              &results,
              absl::StrCat("The amount of rows in ",
                           "the smaller table grows and the ratio, to the ",
                           "amount of rows in the bigger table, stays at ",
                           ratioRows, "."),
              overlapChance_, smallerTableSorted, biggerTableSorted, ratioRows,
              smallerTableAmountRows, smallerTableAmountColumns_,
              biggerTableAmountColumns_);
        }
      }
    }

    return results;
  }
};

// Create benchmark tables, where the tables are the same size and
// both just get more rows.
class BmSameSizeRowGrowth final : public GeneralInterfaceImplementation {
 public:
  BenchmarkResults runAllBenchmarks() override {
    BenchmarkResults results{};

    // Easier reading.
    const std::vector<size_t> smallerTableAmountRows{
        createExponentVectorUntilSize(2, maxSmallerTableRows_)};
    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}) {
      for (const bool biggerTableSorted : {false, true}) {
        makeBenchmarkTable(&results,
                           "Both tables always have the same amount"
                           "of rows and that amount grows.",
                           overlapChance_, smallerTableSorted,
                           biggerTableSorted, ratioRows_,
                           smallerTableAmountRows, smallerTableAmountColumns_,
                           biggerTableAmountColumns_);
      }
    }

    return results;
  }
};

// Registering the benchmarks
AD_REGISTER_BENCHMARK(BmUnsortedAndSortedIdTable);
AD_REGISTER_BENCHMARK(BmSameSizeRowGrowth);
AD_REGISTER_BENCHMARK(BmOnlySmallerTableSizeChanges);
AD_REGISTER_BENCHMARK(BmOnlyBiggerTableSizeChanges);
}  // namespace ad_benchmark
