// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (January of 2023, schlegea@informatik.uni-freiburg.de)
// Author of the file this file is based on: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#include <gtest/gtest.h>

#include <cstdio>
#include <algorithm>

#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "util/Forward.h"
#include "util/Random.h"
#include "engine/idTable/IdTable.h"
#include "../benchmark/Benchmark.h"

// Local helper function.
#include "../benchmark/util/IdTableHelperFunction.h"

// Non-local helper function.
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
        Engine::sort<NUMBER_COLUMNS>(&a.idTable, 0);
        Engine::sort<NUMBER_COLUMNS>(&b.idTable, 0);

        useJoinFunctionOnIdTables(a, b, joinLambda);
  };

  auto joinLambdaWrapper = [&]() {
    useJoinFunctionOnIdTables(a, b, joinLambda);
  };

  auto hashJoinLambdaWrapper = [&]() {
        useJoinFunctionOnIdTables(a, b, hashJoinLambda);
  };

  // Because it's easier to read/interpret, the benchmarks are entries in tables.
  records->addTable("Sorted IdTables", {"Merge join", "Hashed join"},
      {"Overlapping join column entries", "Non-overlapping join column entries"});
  records->addTable("Unsorted IdTables", {"Merge join", "Hashed join"},
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

BenchmarkRegister temp{{BM_UnsortedAndSortedIdTable}};
