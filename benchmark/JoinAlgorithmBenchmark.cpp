// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (January of 2023, schlegea@informatik.uni-freiburg.de)
// Author of the file this file is based on: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#include <cstdio>
#include <algorithm>
#include <concepts>

#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/ResultTable.h"
#include "infrastructure/BenchmarkMeasurementContainer.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/Random.h"
#include "engine/idTable/IdTable.h"
#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/util/IdTableHelperFunction.h"
#include "../test/util/IdTableHelpers.h"
#include "../test/util/JoinHelpers.h"

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
  AD_CONTRACT_CHECK(0 < probabilityToCreateOverlap && probabilityToCreateOverlap <= 100);

  // Is the bigger table actually bigger?
  AD_CONTRACT_CHECK(smallerTableNumberRows <= biggerTable.idTable.numRows());

  // Creating the generator for choosing a random row in the bigger table.
  SlowRandomIntGenerator<size_t> randomBiggerTableRow(0, biggerTable.idTable.numRows() - 1);

  // Generator for checking, if an overlap should be created.
  RandomDoubleGenerator randomDouble(0, 100);

  for(size_t i = 0; i < smallerTableNumberRows; i++) {
    // Only do anything, if the probability is right.
    if (randomDouble() <= probabilityToCreateOverlap) {
      // Create an overlap.
      (*smallerTable).idTable(i, smallerTableJoinColumn) =
        biggerTable.idTable(randomBiggerTableRow(), biggerTable.joinColumn);
    }
  }
}

// Benchmarks for unsorted and sorted tables, with and without overlapping values in
// IdTables. Done with normal join and hash join.
class BM_UnsortedAndSortedIdTable: public BenchmarkInterface {

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
    numberColumns = std::min(static_cast<size_t>(20),
      config.getValueByNestedKeys<size_t>("numberColumns").value_or(10));
  }

  BenchmarkResults runAllBenchmarks(){
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

    // Because it's easier to read/interpret, the benchmarks are entries in tables.
    auto& sortedIdTablesTable = results.addTable("Sorted IdTables",
        {"Merge/Galloping join", "Hashed join"},
        {"Overlapping join column entries", "Non-overlapping join column entries"});
    auto& unsortedIdTablesTable = results.addTable("Unsorted IdTables",
        {"Merge/Galloping join", "Hashed join"},
        {"Overlapping join column entries", "Non-overlapping join column entries"});

    // Benchmarking with non-overlapping IdTables.
 
    unsortedIdTablesTable.addMeasurement(1, 1, hashJoinLambdaWrapper);
    unsortedIdTablesTable.addMeasurement(0, 1, sortThenJoinLambdaWrapper);

    // Because the sortThenJoinLambdaWrapper sorts tables IN PLACE, a and b are now sorted.
    sortedIdTablesTable.addMeasurement(1, 1, hashJoinLambdaWrapper);
    sortedIdTablesTable.addMeasurement(0, 1, joinLambdaWrapper);

    // Benchmarking with overlapping IdTables.

    // We make the tables overlapping and then randomly shuffle them.
    createOverlapRandomly(&a, b, 10.0);

    randomShuffle(a.idTable.begin(), a.idTable.end());
    randomShuffle(b.idTable.begin(), b.idTable.end());

    unsortedIdTablesTable.addMeasurement(1, 0, hashJoinLambdaWrapper);
    unsortedIdTablesTable.addMeasurement(0, 0, sortThenJoinLambdaWrapper);

    // Because the sortThenJoinLambdaWrapper sorts tables IN PLACE, a and b are now sorted.
    sortedIdTablesTable.addMeasurement(1, 0, hashJoinLambdaWrapper);
    sortedIdTablesTable.addMeasurement(0, 0, joinLambdaWrapper);

    return results;
  }
};

template<typename T>
concept isSizeTVector = std::is_same<T, std::vector<size_t>>::value;

template<typename T>
concept isFloatVector = std::is_same<T, std::vector<float>>::value;

template<typename T, typename... objects>
concept areAllTheSameType = (std::is_same<T, objects>::value && ...);

template<typename vectorContent, typename T, typename... objects>
concept onlyFirstIsVector = std::is_same<T, std::vector<vectorContent>>::value
  && areAllTheSameType<vectorContent, objects...>;

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
template<typename T1, typename T2, typename T3, typename T4,
  typename TF, typename T5 = float, typename T6 = float>
requires ((onlyFirstIsVector<size_t, T1, T2, T3, T4> ||
      onlyFirstIsVector<size_t, T2, T1, T3, T4> ||
      onlyFirstIsVector<size_t, T3, T2, T1, T4> ||
      onlyFirstIsVector<size_t, T4, T2, T3, T1>) &&
    areAllTheSameType<float, TF, T5, T6>) ||
  ((onlyFirstIsVector<float, TF, T5, T6> ||
    onlyFirstIsVector<float, T5, TF, T6> ||
    onlyFirstIsVector<float, T6, T5, TF>) &&
    areAllTheSameType<size_t, T1, T2, T3, T4>)
static void makeBenchmarkTable(BenchmarkResults* records, const TF& overlap,
    const bool smallerTableSorted, const bool biggerTableSorted, const T1& ratioRows,
    const T2& smallerTableAmountRows, const T3& smallerTableAmountColumns,
    const T4& biggerTableAmountColumns,
    const T5& smallerTableJoinColumnSampleSizeRatio = 1.0,
    const T6& biggerTableJoinColumnSampleSizeRatio = 1.0) {
  // Checking, if smallerTableJoinColumnSampleSizeRatio and
  // biggerTableJoinColumnSampleSizeRatio are floats bigger than 0. Otherwise
  // , they don't make sense.
  auto biggerThanZero = []<typename T>(const T& object){
    if constexpr(std::is_same<T, std::vector<float>>::value) {
      std::ranges::for_each(object, [](float number){
        AD_CONTRACT_CHECK(number > 0);}, {});
    }else{
      // Object must be a float.
      AD_CONTRACT_CHECK(object > 0);
    }
  };
  biggerThanZero(smallerTableJoinColumnSampleSizeRatio);
  biggerThanZero(biggerTableJoinColumnSampleSizeRatio);

  /*
   * For converting of the template parameter argument to string at runtime.
   * We don't know, which one of the template parameter is a vector, and
   * which isn't, so we can reduce code duplication by having a function, that
   * converts both to strings for the creation of the benchmark table name
   * later.
   */
  auto templateParameterArgumentToString = []<typename T>(const T& object){
    // A number can be delegated, because there is already a 'translation'
    // function for that.
    if constexpr (std::is_same<T, size_t>::value ||
        std::is_same<T, float>::value){
      return std::to_string(object);
    } else {

      // Converting the vector is a bit more involved.
      std::stringstream sstream;
      sstream << "{";

      // Add every number to the stream.
      std::ranges::for_each(object,
          [&sstream](auto number){sstream << number << ", ";}, {});

      // Only return part of the output, because we have a ", " too much.
      return sstream.str().substr(0, sstream.str().length() - 2) + "}";
    }
  };

  // The name of the benchmark table, this function is creating. A bit ugly,
  // because std::vector needs special behaviour and we do not know,
  // which argument is the vector.
  std::stringstream tableDescriptor;
  tableDescriptor << "Benchmarks with a " <<
    templateParameterArgumentToString(overlap) << "\% chance for " <<
    "overlap betwenn " << (smallerTableSorted ? "" : "not ") <<
    "sorted smaller table, with " <<
    templateParameterArgumentToString(smallerTableAmountRows) <<
    " rows, " << templateParameterArgumentToString(smallerTableAmountColumns)
    << " columns and " <<
    templateParameterArgumentToString(smallerTableJoinColumnSampleSizeRatio) <<
    " sample size ratio, and " << (biggerTableSorted ? "" : "not ") <<
    "sorted bigger table, with " <<
    templateParameterArgumentToString(biggerTableAmountColumns) <<
    " columns , " << templateParameterArgumentToString(smallerTableAmountRows)
    << " * " << templateParameterArgumentToString(ratioRows) << " rows and "
    << templateParameterArgumentToString(biggerTableJoinColumnSampleSizeRatio)
    << " sample size ratio.";

  // The lambdas for the join algorithms.
  auto hashJoinLambda = makeHashJoinLambda();
  auto joinLambda = makeJoinLambda();

  // To reduce code duplication, the creation of the benchmark table is done
  // per lambda.
  auto createBenchmarkTable = [&tableDescriptor,
       &records]<typename VectorContentType>(
      const std::vector<VectorContentType>& unconvertedRowNames)->ResultTable&{
    // Creating the names for the rows for the benchmark table creation.
    std::vector<std::string> rowNames(unconvertedRowNames.size());
    std::ranges::transform(unconvertedRowNames, rowNames.begin(),
        [](const VectorContentType& entry){return std::to_string(entry);}, {});

    return records->addTable(tableDescriptor.str(), rowNames,
        {"Merge/Galloping join", "Hash join"});
  };

  // Setup for easier creation of the tables, that will be joined.
  IdTableAndJoinColumn smallerTable{makeIdTableFromVector({{}}), 0};
  IdTableAndJoinColumn biggerTable{makeIdTableFromVector({{}}), 0};
  auto replaceIdTables = [&smallerTableSorted, &biggerTableSorted,
       &smallerTable, &biggerTable](float overlap,
           size_t smallerTableAmountRows,
           size_t smallerTableAmountColumns,
           float smallerTableJoinColumnSampleSizeRatio, size_t ratioRows,
           size_t biggerTableAmountColumns,
           float biggerTableJoinColumnSampleSizeRatio){
         // For easier use, we only calculate the boundaries for the random
         // join column entry generators once.
         // Reminder: The $-1$ in the upper bounds is, because a range [a, b]
         // of natural numbers has $b - a + 1$ elements.
         const size_t smallerTableJoinColumnLowerBound = 0;
         const size_t smallerTableJoinColumnUpperBound = (smallerTableAmountRows
           * smallerTableJoinColumnSampleSizeRatio) - 1;
         const size_t biggerTableJoinColumnLowerBound =
           smallerTableJoinColumnUpperBound + 1;
         const size_t biggerTableJoinColumnUpperBound =
           biggerTableJoinColumnLowerBound + (smallerTableAmountRows *
               ratioRows * biggerTableJoinColumnSampleSizeRatio) - 1;

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
         if (smallerTableSorted){sortIdTableByJoinColumnInPlace(smallerTable);};
         if (biggerTableSorted){sortIdTableByJoinColumnInPlace(biggerTable);};
       };

  // Add the next row of join algorithm measurements to the benchmark table.
  auto addNextRowToBenchmarkTable = [i = 0,
  &hashJoinLambda, &smallerTableSorted, &biggerTableSorted, &joinLambda,
  &smallerTable, &biggerTable](ResultTable* table)mutable{
    // Hash join first, because merge/galloping join sorts all tables, if
    // needed, before joining them.
    table->addMeasurement(i, 1, [&](){
        useJoinFunctionOnIdTables(smallerTable, biggerTable, hashJoinLambda);});

    // The merge/galloping join may have to sort non, one, or both tables,
    // before using them. That decision shouldn't happen in the wrapper for the
    // call, to minimize overhead.
    if ((!smallerTableSorted) && (!biggerTableSorted)) {
      table->addMeasurement(i, 0, [&](){
          sortIdTableByJoinColumnInPlace(smallerTable);
          sortIdTableByJoinColumnInPlace(biggerTable);
          useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda);});
    } else if (!smallerTableSorted) {
      table->addMeasurement(i, 0, [&](){
          sortIdTableByJoinColumnInPlace(smallerTable);
          useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda);});
    } else if (!biggerTableSorted) {
      table->addMeasurement(i, 0, [&](){
          sortIdTableByJoinColumnInPlace(biggerTable);
          useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda);});
    } else {
      table->addMeasurement(i, 0, [&](){
          useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda);});
    }

    // The next call of the lambda, will be one row further.
    i++;
  };

  // We have to adjust a few things, based on which argument is the
  // vector. Fortunaly, we can do it so, that the uneeded parts get deleted
  // at compile time.
  if constexpr (std::is_same<T1, std::vector<size_t>>::value){
    ResultTable& table = createBenchmarkTable(ratioRows);
    for (const size_t& ratioRow : ratioRows) {
      replaceIdTables(overlap, smallerTableAmountRows,
          smallerTableAmountColumns, smallerTableJoinColumnSampleSizeRatio,
          ratioRow, biggerTableAmountColumns,
          biggerTableJoinColumnSampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
  } else if constexpr (std::is_same<T2, std::vector<size_t>>::value){
    ResultTable& table = createBenchmarkTable(smallerTableAmountRows);
    for (const size_t& smallerTableAmountRow : smallerTableAmountRows) {
      replaceIdTables(overlap, smallerTableAmountRow,
          smallerTableAmountColumns, smallerTableJoinColumnSampleSizeRatio,
          ratioRows, biggerTableAmountColumns,
          biggerTableJoinColumnSampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
  } else if constexpr (std::is_same<T3, std::vector<size_t>>::value){
    ResultTable& table = createBenchmarkTable(smallerTableAmountColumns);
    for (const size_t& smallerTableAmountColumn : smallerTableAmountColumns) {
      replaceIdTables(overlap, smallerTableAmountRows,
          smallerTableAmountColumn, smallerTableJoinColumnSampleSizeRatio,
          ratioRows, biggerTableAmountColumns,
          biggerTableJoinColumnSampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
  } else if constexpr (std::is_same<T4, std::vector<size_t>>::value){
    ResultTable& table = createBenchmarkTable(biggerTableAmountColumns);
    for (const size_t& biggerTableAmountColumn : biggerTableAmountColumns) {
      replaceIdTables(overlap, smallerTableAmountRows,
          smallerTableAmountColumns, smallerTableJoinColumnSampleSizeRatio,
          ratioRows, biggerTableAmountColumn,
          biggerTableJoinColumnSampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
  } else if constexpr (std::is_same<T4, std::vector<float>>::value){
    ResultTable& table = createBenchmarkTable(overlap);
    for (const size_t& overlapChance : overlap) {
      replaceIdTables(overlapChance, smallerTableAmountRows,
          smallerTableAmountColumns, smallerTableJoinColumnSampleSizeRatio,
          ratioRows, biggerTableAmountColumns,
          biggerTableJoinColumnSampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
  } else if constexpr (std::is_same<T5, std::vector<float>>::value) {
    ResultTable& table = createBenchmarkTable(smallerTableJoinColumnSampleSizeRatio);
    for (const size_t& sampleSizeRatio : smallerTableJoinColumnSampleSizeRatio){
      replaceIdTables(overlap, smallerTableAmountRows,
          smallerTableAmountColumns, sampleSizeRatio,
          ratioRows, biggerTableAmountColumns,
          biggerTableJoinColumnSampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
  } else if constexpr (std::is_same<T6, std::vector<float>>::value) {
    ResultTable& table = createBenchmarkTable(biggerTableJoinColumnSampleSizeRatio);
    for (const size_t& sampleSizeRatio : biggerTableJoinColumnSampleSizeRatio){
      replaceIdTables(overlap, smallerTableAmountRows,
          smallerTableAmountColumns, smallerTableJoinColumnSampleSizeRatio,
          ratioRows, biggerTableAmountColumns, sampleSizeRatio);
      addNextRowToBenchmarkTable(&table);
    }
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
static std::vector<size_t> createExponentVectorUntilSize(const size_t base,
    const size_t stoppingPoint) {
  std::vector<size_t> exponentVector{};

  // Calculating and adding the exponents.
  size_t currentExponent = 1; // base^0 = 1
  while (currentExponent <= stoppingPoint) {
    exponentVector.push_back(currentExponent);
    currentExponent *= base;
  }

  return exponentVector;
}

/*
Partly implements the interface `BenchmarkInterface`, by providing the member
variables, that most of the benchmark classes here set using the
`BenchmarkConfiguration` and delivers a default configuration parser, that sets them.
*/
class GeneralConfigurationOption: public BenchmarkInterface{
  protected:
  // The maximum amount of rows, that the smaller table should have.
  size_t maxSmallerTableRows;
  // The maximal row ratio between the smaller and the bigger table.
  size_t maxRatioRows;

  public:
  void parseConfiguration(const BenchmarkConfiguration& config) final{
    maxSmallerTableRows =
      config.getValueByNestedKeys<size_t>("maxSmallerTableRows").value_or(256);
    maxRatioRows =
      config.getValueByNestedKeys<size_t>("maxRatioRows").value_or(256);
  }
};

// Create benchmark tables, where the smaller table stays at 2000 rows and
// the bigger tables keeps getting bigger. Amount of columns stays the same.
class BM_OnlyBiggerTableSizeChanges final: public GeneralConfigurationOption{
  public:

  BenchmarkResults runAllBenchmarks() override{
    BenchmarkResults results{};

    // Easier reading.
    const std::vector<size_t> ratioRows{
      createExponentVectorUntilSize(2, maxRatioRows)};
    constexpr size_t smallerTableAmountRows{2000};
    constexpr size_t smallerTableAmountColumns{20};
    constexpr size_t biggerTableAmountColumns{20};
    constexpr float overlapChance{42.0};
    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}){
      for (const bool biggerTableSorted : {false, true}) {
        makeBenchmarkTable(&results, overlapChance, smallerTableSorted,
            biggerTableSorted, ratioRows, smallerTableAmountRows,
            smallerTableAmountColumns, biggerTableAmountColumns);
      }
    }

    return results;
  }
};

// Create benchmark tables, where the smaller table grows and the ratio
// between tables stays the same. As does the amount of columns.
class BM_OnlySmallerTableSizeChanges final: public GeneralConfigurationOption{
  public:

  BenchmarkResults runAllBenchmarks() override{
    BenchmarkResults results{};

    // Easier reading.
    const std::vector<size_t> smallerTableAmountRows{
      createExponentVectorUntilSize(2, maxSmallerTableRows)};
    constexpr size_t smallerTableAmountColumns{3};
    constexpr size_t biggerTableAmountColumns{3};
    constexpr float overlapChance{42.0};
    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}){
      for (const bool biggerTableSorted : {false, true}) {
        // We also make multiple tables for different row ratios.
        for (const size_t ratioRows: createExponentVectorUntilSize(2, maxRatioRows)){
          makeBenchmarkTable(&results, overlapChance, smallerTableSorted,
              biggerTableSorted, ratioRows, smallerTableAmountRows,
              smallerTableAmountColumns, biggerTableAmountColumns);
        }
      }
    }

    return results;
  }
};

// Create benchmark tables, where the tables are the same size and
// both just get more rows.
class BM_SameSizeRowGrowth final: public GeneralConfigurationOption{
  public:

  BenchmarkResults runAllBenchmarks() override{
    BenchmarkResults results{};

  // Easier reading.
  const std::vector<size_t> smallerTableAmountRows{
    createExponentVectorUntilSize(2, maxSmallerTableRows)};
    constexpr size_t smallerTableAmountColumns{3};
    constexpr size_t biggerTableAmountColumns{3};
    constexpr size_t ratioRows{1};
    constexpr float overlapChance{42.0};
    // Making a benchmark table for all combination of IdTables being sorted.
    for (const bool smallerTableSorted : {false, true}){
      for (const bool biggerTableSorted : {false, true}) {
        makeBenchmarkTable(&results, overlapChance, smallerTableSorted,
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
