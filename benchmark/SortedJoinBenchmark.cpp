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
#include "util/Forward.h"
#include "engine/idTable/IdTable.h"
#include "../benchmark/Benchmark.h"

// Local helper function.
#include "../benchmark/util/IdTableHelperFunction.h"
#include "../benchmark/util/JoinHelperFunction.h"

// Benchmarks for sorted tables, with and without overlapping values in
// IdTables. Done with normal join and hash join.
void BM_SortedIdTable(BenchmarkRecords* records) {
  // For easier changing of the IdTables size.
  // Important: Apparently Join::join has an inbuild timeout checker, which
  // keeps getting triggerd by tables with a size between 500-600, or higher.
  // This leads to a segmentation fault, because it keeps trying to throw an
  // exception about the time out, but that exception tries to include
  // information about the query execution tree using a shared pointer. And, as
  // far as I can tell, that pointer was never actually initialized here, so
  // it points into memory, which we are not allowed to access. Which leads
  // to the seg fault.
  const size_t NUMBER_ROWS = 500;
  const size_t NUMBER_COLUMNS = 500; 
  
  Join J{Join::InvalidOnlyForTestingJoinTag{}};
  auto JoinLambda = [&J]<int A, int B, int C>(auto&&... args) {
    return J.join<A, B, C>(AD_FWD(args)...);
  };
  auto HashJoinLambda = [&J]<int A, int B, int C>(auto&&... args) {
    return J.hashJoin<A, B, C>(AD_FWD(args)...);
  };

  // Tables, that have overlapping values in their join columns.
  IdTable a = createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 0, 10);
  IdTable b = createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 5, 15);
  
  // Because overlap is not yet guaranteed, we put some in.
  for (size_t i = 0; i*20 < NUMBER_ROWS; i++) {
    a(i*10, 0) = I(10);
    b(i*20, 0) = I(10);
  }

  // Sorting the tables after the join column.
  auto sortIdTables = [&a, &b] () {
    auto sortFunction = [](const auto& row1, const auto& row2) {
      return row1[0] < row2[0];
    };
    std::sort(a.begin(), a.end(), sortFunction);
    std::sort(b.begin(), b.end(), sortFunction);
  };
  sortIdTables();
  
  // Lambda wrapper for the functions, that I measure.
  auto joinLambdaWrapper = [&]() {
    useJoinFunctionOnIdTables(a, 0, b, 0, JoinLambda);
  };
  auto hashJoinLambdaWrapper = [&]() {
    useJoinFunctionOnIdTables(a, 0, b, 0, HashJoinLambda);
  };


  records->measureTime("Normal join with overlapping IdTables", joinLambdaWrapper);
  records->measureTime("Hashed join with overlapping IdTables", hashJoinLambdaWrapper);

  // Same thing, but non overlapping.
  a = createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 0, 10);
  b = createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 20, 30);
 
  sortIdTables();

  records->measureTime("Normal join with non-overlapping IdTables", joinLambdaWrapper);
  records->measureTime("Hashed join with non-overlapping IdTables", hashJoinLambdaWrapper);
}

BenchmarkRegister temp{{BM_SortedIdTable}};
