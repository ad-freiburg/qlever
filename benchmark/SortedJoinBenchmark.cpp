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
  const size_t NUMBER_ROWS = 1000;
  const size_t NUMBER_COLUMNS = 1000; 
  
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
  for (size_t i = 1; i*5+1 <= NUMBER_ROWS - 1; i++) {
    size_t row = i * 5;
    a(row - 3, 0) = I(10);
    b(row + 1, 0) = I(10);
  }

  // Sorting the tables after the join column.
  auto sortFunction = [](const auto& row1, const auto& row2) {
    return row1[0] < row2[0];
  };
  std::sort(a.begin(), a.end(), sortFunction);
  std::sort(b.begin(), b.end(), sortFunction);


  records->measureTime("Normal join with overlapping IdTables", [&]() {
        useJoinFunctionOnIdTables(a, 0, b, 0, JoinLambda);
      }
    );
  records->measureTime("Hashed join with overlapping IdTables", [&]() {
        useJoinFunctionOnIdTables(a, 0, b, 0, HashJoinLambda);
      }
    );

  // Same thing, but non overlapping.
  a = createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 0, 10);
  b = createRandomlyFilledIdTable(NUMBER_ROWS, NUMBER_COLUMNS, 0, 20, 30);
  
  // Sorting the tables after the join column.
  std::sort(a.begin(), a.end(), sortFunction);
  std::sort(b.begin(), b.end(), sortFunction);
  
  records->measureTime("Normal join with non-overlapping IdTables", [&]() {
        useJoinFunctionOnIdTables(a, 0, b, 0, JoinLambda);
      }
    );
  records->measureTime("Hashed join with non-overlapping IdTables", [&]() {
        useJoinFunctionOnIdTables(a, 0, b, 0, HashJoinLambda);
      }
    );
}

BenchmarkRegister temp{{&BM_SortedIdTable}};
