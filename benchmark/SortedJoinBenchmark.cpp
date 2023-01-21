// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)
// Author of the file this file is based on: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdio>
#include <algorithm>

#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"
#include "../benchmark/Benchmark.h"

// Local helper function.
#include "../benchmark/util/IdTableHelperFunction.h"

// Non-local helper function.
#include "../test/util/IdTableHelpers.h"
#include "../test/util/JoinHelpers.h"

// Benchmarks for sorted tables, with and without overlapping values in
// IdTables. Done with normal join and hash join.
void BM_SortedIdTable(BenchmarkRecords* records) {
  // For easier changing of the IdTables size.
  const size_t NUMBER_ROWS = 1000;
  const size_t NUMBER_COLUMNS = NUMBER_ROWS; 
  
  auto hashJoinLambda = makeHashJoinLambda();
  auto joinLambda = makeJoinLambda();

  // Tables, that have overlapping values in their join columns.
  IdTableAndJoinColumn a{
    createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 0, 10), 0};
  IdTableAndJoinColumn b{
    createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 5, 15), 0};
  
  // Because overlap is not yet guaranteed, we put some in.
  for (size_t i = 0; i*20 < NUMBER_ROWS; i++) {
    a.idTable(i*10, 0) = I(10);
    b.idTable(i*20, 0) = I(10);
  }

  // Sorting the tables by the join column.
  auto sortIdTables = [&a, &b] () {
    Engine::sort<NUMBER_COLUMNS>(&a.idTable, 0);
    Engine::sort<NUMBER_COLUMNS>(&b.idTable, 0);
  };
  sortIdTables();
  
  // Lambda wrapper for the functions, that I measure.
  auto joinLambdaWrapper = [&]() {
    useJoinFunctionOnIdTables(a, b, joinLambda);
  };
  auto hashJoinLambdaWrapper = [&]() {
    useJoinFunctionOnIdTables(a, b, hashJoinLambda);
  };


  records->addSingleMeasurement("Normal join with overlapping IdTables", joinLambdaWrapper);
  records->addSingleMeasurement("Hashed join with overlapping IdTables", hashJoinLambdaWrapper);

  // Same thing, but non overlapping.
  a.idTable = createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 0, 10);
  b.idTable = createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 20, 30);
 
  sortIdTables();

  records->addSingleMeasurement("Normal join with non-overlapping IdTables", joinLambdaWrapper);
  records->addSingleMeasurement("Hashed join with non-overlapping IdTables", hashJoinLambdaWrapper);
}

BenchmarkRegister temp{{BM_SortedIdTable}};
