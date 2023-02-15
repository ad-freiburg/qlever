// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (January of 2023, schlegea@informatik.uni-freiburg.de)
// Author of the file this file is based on: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#include <gtest/gtest.h>

#include <cstdio>
#include <algorithm>
#include <concepts>

#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "util/Forward.h"
#include "util/Random.h"
#include "engine/idTable/IdTable.h"
#include "../benchmark/Benchmark.h"
#include "../benchmark/util/IdTableHelperFunction.h"
#include "../test/util/IdTableHelpers.h"
#include "../test/util/JoinHelpers.h"

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
void createOverlapRandomly(IdTableAndJoinColumn* const smallerTable,
    const IdTableAndJoinColumn& biggerTable,
    const double probabilityToCreateOverlap) {
  // For easier reading.
  const size_t smallerTableJoinColumn = (*smallerTable).joinColumn;
  const size_t smallerTableNumberRows = (*smallerTable).idTable.numRows();

  // The probability for creating an overlap must be in (0,100], any other
  // values make no sense.
  AD_CHECK(0 < probabilityToCreateOverlap && probabilityToCreateOverlap <= 100);

  // Is the bigger table actually bigger?
  AD_CHECK(smallerTableNumberRows <= biggerTable.idTable.numRows());

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
void BM_UnsortedAndSortedIdTable(BenchmarkRecords* records) {
  // For easier changing of the IdTables size.
  const size_t NUMBER_ROWS = 10000;
  const size_t NUMBER_COLUMNS = 20; 

  auto hashJoinLambda = makeHashJoinLambda();
  auto joinLambda = makeJoinLambda();

  // Tables, that have no overlapping values in their join columns.
  IdTableAndJoinColumn a{
    createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 0, 10), 0};
  IdTableAndJoinColumn b{
    createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 20, 30), 0};

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
  records->addTable("Sorted IdTables", {"Merge/Galloping join", "Hashed join"},
      {"Overlapping join column entries", "Non-overlapping join column entries"});
  records->addTable("Unsorted IdTables", {"Merge/Galloping join", "Hashed join"},
      {"Overlapping join column entries", "Non-overlapping join column entries"});

  // Benchmarking with non-overlapping IdTables.
 
  records->addToExistingTable("Unsorted IdTables", 1, 1, hashJoinLambdaWrapper);
  records->addToExistingTable("Unsorted IdTables", 0, 1, sortThenJoinLambdaWrapper);

  // Because the sortThenJoinLambdaWrapper sorts tables IN PLACE, a and b are now sorted.
  records->addToExistingTable("Sorted IdTables", 1, 1, hashJoinLambdaWrapper);
  records->addToExistingTable("Sorted IdTables", 0, 1, joinLambdaWrapper);

  // Benchmarking with overlapping IdTables.

  // We make the tables overlapping and then randomly shuffle them.
  createOverlapRandomly(&a, b, 10.0);

  randomShuffle(a.idTable.begin(), a.idTable.end());
  randomShuffle(b.idTable.begin(), b.idTable.end());

  records->addToExistingTable("Unsorted IdTables", 1, 0, hashJoinLambdaWrapper);
  records->addToExistingTable("Unsorted IdTables", 0, 0, sortThenJoinLambdaWrapper);

  // Because the sortThenJoinLambdaWrapper sorts tables IN PLACE, a and b are now sorted.
  records->addToExistingTable("Sorted IdTables", 1, 0, hashJoinLambdaWrapper);
  records->addToExistingTable("Sorted IdTables", 0, 0, joinLambdaWrapper);
}

template<typename T1, typename T2, typename T3, typename T4>
concept onlyFirstIsVector = std::is_same<T1, std::vector<size_t>>::value &&
  std::is_same<T2, size_t>::value && std::is_same<T3, size_t>::value &&
  std::is_same<T4, size_t>::value;

/*
 * @brief Create a benchmark table for join algorithm, with the given
 *  parameters for the IdTables. The columns will be the algorithm and the
 *  rows will be the parameter, you gave a list for.
 *
 * @tparam T1, T2, T3, T4 Exactly on of those muste be a std::vector<size_t>
 *  and all others must be size_t. Typ inference should be able to handle it,
 *  if you give the right function arguments.
 *
 * @param records The BenchmarkRecords, in which you want to create a new
 *  benchmark table.
 * @param overlap Should the tables used for the benchmark table have any
 *  overlap concering the entries of their join columns?
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
 */
template<typename T1, typename T2, typename T3,typename T4>
requires onlyFirstIsVector<T1, T2, T3, T4> || onlyFirstIsVector<T2, T1, T3, T4>
  || onlyFirstIsVector<T3, T2, T1, T4> || onlyFirstIsVector<T4, T2, T3, T1>
void makeBenchmarkTable(BenchmarkRecords* records, const bool overlap,
    const bool smallerTableSorted, const bool biggerTableSorted, const T1& ratioRows,
    const T2& smallerTableAmountRows, const T3& smallerTableAmountColumns,
    const T4& biggerTableAmountColumns) {
  // For converting a std::vector<size_t>, or size_t, to string at runtime.
  // We don't know, which one of the four template parameter is a vector, and
  // wich isn't, so we can reduce code duplication by having a function, that
  // converts both to strings for the creation of the benchmark table name
  // later.
  auto size_tOrSize_tVectorToString = []<typename T>(const T& object){
    // A size_t can be delegated, because there is already a 'translation'
    // function for that.
    if constexpr (std::is_same<T, size_t>::value){
      return std::to_string(object);
    } else {

      // Converting the vector is a bit more involved.
      std::stringstream sstream;
      sstream << "{";

      // Add every number to the stream.
      std::ranges::for_each(object,
          [&sstream](size_t number){sstream << number << ", ";}, {});

      // Only return part of the output, because we have a ", " too much.
      return sstream.str().substr(0, sstream.str().length() - 2) + "}";
    }
  };

  // The name of the benchmark table, this function is creating. A bit ugly,
  // because std::vector<size_t> needs special behaviour and we do not know,
  // which argument is the vector.
  std::stringstream tableDescriptor;
  tableDescriptor << "Benchmarks with " << ((overlap) ? "" : "no ") <<
    "overlap betwenn " << (smallerTableSorted ? "" : "not ") <<
    "sorted smaller table, with " <<
    size_tOrSize_tVectorToString(smallerTableAmountRows) <<
    " rows and " << size_tOrSize_tVectorToString(smallerTableAmountColumns)
    << " columns, and " << (biggerTableSorted ? "" : "not ") <<
    "sorted bigger table, with " <<
    size_tOrSize_tVectorToString(biggerTableAmountColumns) <<
    " columns and " << size_tOrSize_tVectorToString(smallerTableAmountRows)
    << " * " << size_tOrSize_tVectorToString(ratioRows) << " rows.";

  // The lambdas for the join algorithms.
  auto hashJoinLambda = makeHashJoinLambda();
  auto joinLambda = makeJoinLambda();

  // To reduce code duplication, the creation of the benchmark table is done
  // per lambda.
  auto createBenchmarkTable = [&tableDescriptor, &records](
      const std::vector<size_t>& unconvertedRowNames){
    // Creating the names for the rows for the benchmark table creation.
    std::vector<std::string> rowNames(unconvertedRowNames.size());
    std::ranges::transform(unconvertedRowNames, rowNames.begin(),
        [](const size_t& number){return std::to_string(number);}, {});

    records->addTable(tableDescriptor.str(), rowNames,
        {"Merge/Galloping join", "Hash join"});
  };

  // Setup for easier creation of the tables, that will be joined.
  IdTableAndJoinColumn smallerTable{makeIdTableFromVector({{}}), 0};
  IdTableAndJoinColumn biggerTable{makeIdTableFromVector({{}}), 0};
  auto replaceIdTables = [&overlap, &smallerTableSorted, &biggerTableSorted,
       &smallerTable, &biggerTable](size_t smallerTableAmountRows,
           size_t smallerTableAmountColumns, size_t ratioRows,
           size_t biggerTableAmountColumns){
         // Replacing the old id tables with newly generated ones, based
         // on specification.
         smallerTable.idTable = createRandomlyFilledIdTable(
             smallerTableAmountRows, smallerTableAmountColumns, 0, 0, 500);
         biggerTable.idTable = createRandomlyFilledIdTable(
             smallerTableAmountRows * ratioRows, biggerTableAmountColumns,
             0, 501, 1'000'000'000);

         // Creating overlap, if wanted.
         if (overlap) {createOverlapRandomly(&smallerTable, biggerTable, 42.0);};

         // Sort the tables, if wanted.
         if (smallerTableSorted){sortIdTableByJoinColumnInPlace(smallerTable);};
         if (biggerTableSorted){sortIdTableByJoinColumnInPlace(biggerTable);};
       };

  // Add the next row of join algorithm measurements to the benchmark table.
  auto addNextRowToBenchmarkTable = [i = 0, &tableDescriptor, &records,
  &hashJoinLambda, &smallerTableSorted, &biggerTableSorted, &joinLambda,
  &smallerTable, &biggerTable]()mutable{
    // Hash join first, because merge/galloping join sorts all tables, if
    // needed, before joining them.
    records->addToExistingTable(tableDescriptor.str(), i, 1, [&](){
        useJoinFunctionOnIdTables(smallerTable, biggerTable, hashJoinLambda);});

    // The merge/galloping join may have to sort non, one, or both tables,
    // before using them. That decision shouldn't happen in the wrapper for the
    // call, to minimize overhead.
    if ((!smallerTableSorted) && (!biggerTableSorted)) {
      records->addToExistingTable(tableDescriptor.str(), i, 0, [&](){
          sortIdTableByJoinColumnInPlace(smallerTable);
          sortIdTableByJoinColumnInPlace(biggerTable);
          useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda);});
    } else if (!smallerTableSorted) {
      records->addToExistingTable(tableDescriptor.str(), i, 0, [&](){
          sortIdTableByJoinColumnInPlace(smallerTable);
          useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda);});
    } else if (!biggerTableSorted) {
      records->addToExistingTable(tableDescriptor.str(), i, 0, [&](){
          sortIdTableByJoinColumnInPlace(biggerTable);
          useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda);});
    } else {
      records->addToExistingTable(tableDescriptor.str(), i, 0, [&](){
          useJoinFunctionOnIdTables(smallerTable, biggerTable, joinLambda);});
    }

    // The next call of the lambda, will be one row further.
    i++;
  };

  // We have to adjust a few things, based on which argument is the
  // vector. Fortunaly, we can do it so, that the uneeded parts get deleted
  // at compile time.
  if constexpr (std::is_same<T1, std::vector<size_t>>::value){
    createBenchmarkTable(ratioRows);
    for (const size_t& ratioRow : ratioRows) {
      replaceIdTables(smallerTableAmountRows, smallerTableAmountColumns,
          ratioRow, biggerTableAmountColumns);
      addNextRowToBenchmarkTable();
    }
  } else if constexpr (std::is_same<T2, std::vector<size_t>>::value){
    createBenchmarkTable(smallerTableAmountRows);
    for (const size_t& smallerTableAmountRow : smallerTableAmountRows) {
      replaceIdTables(smallerTableAmountRow, smallerTableAmountColumns,
          ratioRows, biggerTableAmountColumns);
      addNextRowToBenchmarkTable();
    }
  } else if constexpr (std::is_same<T3, std::vector<size_t>>::value){
    createBenchmarkTable(smallerTableAmountColumns);
    for (const size_t& smallerTableAmountColumn : smallerTableAmountColumns) {
      replaceIdTables(smallerTableAmountRows, smallerTableAmountColumn,
          ratioRows, biggerTableAmountColumns);
      addNextRowToBenchmarkTable();
    }
  } else {
    createBenchmarkTable(biggerTableAmountColumns);
    for (const size_t& biggerTableAmountColumn : biggerTableAmountColumns) {
      replaceIdTables(smallerTableAmountRows, smallerTableAmountColumns,
          ratioRows, biggerTableAmountColumn);
      addNextRowToBenchmarkTable();
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
std::vector<size_t> createExponentVectorUntilSize(const size_t base,
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

// Create benchmark tables, where the smaller table stays at 2000 rows and
// the bigger tables keeps getting bigger. Amount of columns stays the same.
void BM_OnlyBiggerTableSizeChanges(BenchmarkRecords* records){
  // Easier reading.
  const std::vector<size_t> ratioRows{
    createExponentVectorUntilSize(2, 100'000)};
  constexpr size_t smallerTableAmountRows{2000};
  constexpr size_t smallerTableAmountColumns{20};
  constexpr size_t biggerTableAmountColumns{20};
  // Making a benchmark table for all combination of IdTables being sorted.
  for (const bool smallerTableSorted : {false, true}){
    for (const bool biggerTableSorted : {false, true}) {
      makeBenchmarkTable(records, false, smallerTableSorted, biggerTableSorted,
          ratioRows, smallerTableAmountRows, smallerTableAmountColumns,
          biggerTableAmountColumns);
    }
  }
}

// Create benchmark tables, where the smaller table grows and the ratio
// between tables stays the same. As does the amount of columns.
void BM_OnlySmallerTableSizeChanges(BenchmarkRecords* records){
  // Easier reading.
  const std::vector<size_t> smallerTableAmountRows{
    createExponentVectorUntilSize(2, 200'000)};
  constexpr size_t smallerTableAmountColumns{3};
  constexpr size_t biggerTableAmountColumns{3};
  // Making a benchmark table for all combination of IdTables being sorted.
  for (const bool smallerTableSorted : {false, true}){
    for (const bool biggerTableSorted : {false, true}) {
      // We also make multiple tables for different row ratios.
      for (const size_t ratioRows: createExponentVectorUntilSize(2, 1'000)){
        makeBenchmarkTable(records, false, smallerTableSorted,
            biggerTableSorted, ratioRows, smallerTableAmountRows,
            smallerTableAmountColumns, biggerTableAmountColumns);
      }
    }
  }
}

// Create benchmark tables, where the tables are the same size and
// both just get more rows.
void BM_SameSizeRowGrowth(BenchmarkRecords* records){
  // Easier reading.
  const std::vector<size_t> smallerTableAmountRows{
    createExponentVectorUntilSize(2, 200'000'000)};
  constexpr size_t smallerTableAmountColumns{3};
  constexpr size_t biggerTableAmountColumns{3};
  constexpr size_t ratioRows{1};
  // Making a benchmark table for all combination of IdTables being sorted.
  for (const bool smallerTableSorted : {false, true}){
    for (const bool biggerTableSorted : {false, true}) {
      makeBenchmarkTable(records, false, smallerTableSorted,
          biggerTableSorted, ratioRows, smallerTableAmountRows,
          smallerTableAmountColumns, biggerTableAmountColumns);
    }
  }
}

BenchmarkRegister temp{{BM_UnsortedAndSortedIdTable,
  BM_OnlyBiggerTableSizeChanges, BM_OnlySmallerTableSizeChanges}};
