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

// Benchmarks for unsorted and sorted tables, with and without overlapping values in
// IdTables. Done with normal join and hash join.
void BM_UnsortedAndSortedIdTable(BenchmarkRecords* records) {
  // For easier changing of the IdTables size.
  const size_t NUMBER_ROWS = 1000;
  const size_t NUMBER_COLUMNS = NUMBER_ROWS; 

  auto hashJoinLambda = makeHashJoinLambda();
  auto joinLambda = makeJoinLambda();

  // Tables, that have no overlapping values in their join columns.
  IdTableAndJoinColumn a{
    createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 0, 10), 0};
  IdTableAndJoinColumn b{
    createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 20, 30), 0};

  // We need to order the IdTables, before using the normal join function on
  // them. This is the projection function for that.
  auto projectionFunction = [](const auto& row) {
    return row[0];
  };

  // Lambda wrapper for the functions, that I measure.
  
  // Sorts the table IN PLACE and the uses the normal join on them.
  auto sortThenJoinLambdaWrapper = [&]() {
        // Sorting the tables after the join column.
        std::ranges::sort(a.idTable, {}, projectionFunction);
        std::ranges::sort(b.idTable, {}, projectionFunction);

        useJoinFunctionOnIdTables(a, b, joinLambda);
  };

  auto joinLambdaWrapper = [&]() {
    useJoinFunctionOnIdTables(a, b, joinLambda);
  };

  auto hashJoinLambdaWrapper = [&]() {
        useJoinFunctionOnIdTables(a, b, hashJoinLambda);
  };

  // Benchmarking with non-overlapping IdTables.
 
  records->addSingleMeasurement("Hashed join with non-overlapping unsorted IdTables", hashJoinLambdaWrapper);
  records->addSingleMeasurement("Normal join with non-overlapping unsorted IdTables", sortThenJoinLambdaWrapper);

  // Because the sortThenJoinLambdaWrapper sorts tables IN PLACE, a and b are now sorted.
  records->addSingleMeasurement("Hashed join with non-overlapping sorted IdTables", hashJoinLambdaWrapper);
  records->addSingleMeasurement("Normal join with non-overlapping sorted IdTables", joinLambdaWrapper);

  // Benchmarking with overlapping IdTables.
  // We make the tables overlapping and then randomly shuffle them.
  for (size_t i = 0; i*20 < NUMBER_ROWS; i++) {
    a.idTable(i*10, 0) = I(10);
    b.idTable(i*20, 0) = I(10);
  }
  randomShuffle(a.idTable.begin(), a.idTable.end());
  randomShuffle(b.idTable.begin(), b.idTable.end());

  records->addSingleMeasurement("Hashed join with overlapping unsorted IdTables", hashJoinLambdaWrapper);
  records->addSingleMeasurement("Normal join with overlapping unsorted IdTables", sortThenJoinLambdaWrapper);

  // Because the sortThenJoinLambdaWrapper sorts tables IN PLACE, a and b are now sorted.
  records->addSingleMeasurement("Hashed join with overlapping sorted IdTables", hashJoinLambdaWrapper);
  records->addSingleMeasurement("Normal join with overlapping sorted IdTables", joinLambdaWrapper);
}

BenchmarkRegister temp{{BM_UnsortedAndSortedIdTable}};
