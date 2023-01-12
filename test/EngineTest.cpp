// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Co-Author: Andre Schlegel (November of 2022,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <tuple>

#include "./util/GTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"
#include "util/Forward.h"
#include "util/Random.h"
#include "util/SourceLocation.h"

TEST(EngineTest, distinctTest) {
  IdTable inp{makeIdTableFromVector(
      {{1, 1, 3, 7}, {6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}})};

  IdTable result{4, allocator()};

  std::vector<size_t> keepIndices{{1, 2}};
  CALL_FIXED_SIZE(4, Engine::distinct, inp, keepIndices, &result);

  // For easier checking.
  IdTable expectedResult{
      makeIdTableFromVector({{1, 1, 3, 7}, {2, 2, 3, 5}, {3, 6, 5, 4}})};
  ASSERT_EQ(expectedResult, result);
}

TEST(JoinTest, optionalJoinTest) {
  IdTable a{makeIdTableFromVector(
      {{4, 1, 2}, {2, 1, 3}, {1, 1, 4}, {2, 2, 1}, {1, 3, 1}})};
  IdTable b{
      makeIdTableFromVector({{3, 3, 1}, {1, 8, 1}, {4, 2, 2}, {1, 1, 3}})};
  IdTable result{4, allocator()};
  vector<array<ColumnIndex, 2>> jcls{};
  jcls.push_back(array<ColumnIndex, 2>{{1, 2}});
  jcls.push_back(array<ColumnIndex, 2>{{2, 1}});

  // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
  // a have to equal those of column 2 of b and vice versa).
  int aWidth{static_cast<int>(a.numColumns())};
  int bWidth{static_cast<int>(b.numColumns())};
  int resultWidth{static_cast<int>(result.numColumns())};
  CALL_FIXED_SIZE((std::array{aWidth, bWidth, resultWidth}),
                  OptionalJoin::optionalJoin, a, b, false, true, jcls, &result);

  // For easier checking of the result.
  IdTable expectedResult{makeIdTableFromVector(
      {{4, 1, 2, 0}, {2, 1, 3, 3}, {1, 1, 4, 0}, {2, 2, 1, 0}, {1, 3, 1, 1}})};
  expectedResult(0, 3) = ID_NO_VALUE;
  expectedResult(2, 3) = ID_NO_VALUE;
  expectedResult(3, 3) = ID_NO_VALUE;

  ASSERT_EQ(expectedResult, result);

  // Test the optional join with variable sized data.
  IdTable va{makeIdTableFromVector(
      {{1, 2, 3, 4, 5, 6}, {1, 2, 3, 7, 5, 6}, {7, 6, 5, 4, 3, 2}})};

  IdTable vb{makeIdTableFromVector({{2, 3, 4}, {2, 3, 5}, {6, 7, 4}})};

  IdTable vresult{7, allocator()};
  jcls.clear();
  jcls.push_back(array<ColumnIndex, 2>{{1, 0}});
  jcls.push_back(array<ColumnIndex, 2>{{2, 1}});

  aWidth = va.numColumns();
  bWidth = vb.numColumns();
  resultWidth = vresult.numColumns();
  CALL_FIXED_SIZE((std::array{aWidth, bWidth, resultWidth}),
                  OptionalJoin::optionalJoin, va, vb, true, false, jcls,
                  &vresult);

  // For easier checking.
  expectedResult = makeIdTableFromVector({{1, 2, 3, 4, 5, 6, 4},
                                          {1, 2, 3, 4, 5, 6, 5},
                                          {1, 2, 3, 7, 5, 6, 4},
                                          {1, 2, 3, 7, 5, 6, 5},
                                          {0, 6, 7, 0, 0, 0, 4}});
  expectedResult(4, 0) = ID_NO_VALUE;
  expectedResult(4, 3) = ID_NO_VALUE;
  expectedResult(4, 4) = ID_NO_VALUE;
  expectedResult(4, 5) = ID_NO_VALUE;

  ASSERT_EQ(expectedResult, vresult);
}

TEST(EngineTest, hashJoinTest) {
  // Going through the tests of the normal lambda. The result for hashed join
  // is sorted, if the input was also sorted, so hashJoin can go through them,
  // without modifying them first.
  Join J{Join::InvalidOnlyForTestingJoinTag{}};
  auto HashJoinLambda = [&J]<int A, int B, int C>(auto&&... args) {
    return J.hashJoin<A, B, C>(AD_FWD(args)...);
  };
  auto JoinLambda = [&J]<int A, int B, int C>(auto&&... args) {
    return J.join<A, B, C>(AD_FWD(args)...);
  };
  goThroughSetOfTestsWithJoinFunction(createNormalJoinTestSet(), HashJoinLambda);

  /*
   * Create two sorted IdTables. Join them using the normal join and the
   * hashed join. Compare.
  */
  std::vector<std::vector<size_t>> a = {
      {34, 73, 92, 61, 18},
      {11, 80, 20, 43, 75},
      {96, 51, 40, 67, 23}
    };
 
  std::vector<std::vector<size_t>> b = {
      {34, 73, 92, 61, 18},
      {96, 2, 76, 87, 38},
      {96, 16, 27, 22, 38},
      {7, 51, 40, 67, 23}
    }; 
  
  // Saving the results and comparing them.
  IdTable result1 = useJoinFunctionOnIdTables(makeIdTableFromVector(a), 0, makeIdTableFromVector(b), 0, JoinLambda); // Normal.
  IdTable result2 = useJoinFunctionOnIdTables(makeIdTableFromVector(a), 0, makeIdTableFromVector(b), 0, HashJoinLambda); // Hash.
  ASSERT_EQ(result1, result2);

  // Checking if result1 and result2 are correct.
  compareIdTableWithSolution(result1, makeIdTableFromVector(std::vector<std::vector<size_t>>{
      {
        {34, 73, 92, 61, 18, 73, 92, 61, 18},
        {96, 51, 40, 67, 23, 2, 76, 87, 38},
        {96, 51, 40, 67, 23, 16, 27, 22, 38}
      }
    }),
    true,
    0
  );
  compareIdTableWithSolution(result2, makeIdTableFromVector(std::vector<std::vector<size_t>>{
      {
        {34, 73, 92, 61, 18, 73, 92, 61, 18},
        {96, 51, 40, 67, 23, 2, 76, 87, 38},
        {96, 51, 40, 67, 23, 16, 27, 22, 38}
      }
    }),
    true,
    0
  );

  // Random shuffle the tables, hash join them, sort the result and check,
  // if the result is still correct.
  randomShuffle(a.begin(), a.end());
  randomShuffle(b.begin(), b.end());
  result2 = useJoinFunctionOnIdTables(makeIdTableFromVector(a), 0, makeIdTableFromVector(b), 0, HashJoinLambda);

  // Sorting is done inside this function.
  compareIdTableWithSolution(result2, makeIdTableFromVector(std::vector<std::vector<size_t>>{
      {
        {34, 73, 92, 61, 18, 73, 92, 61, 18},
        {96, 51, 40, 67, 23, 2, 76, 87, 38},
        {96, 51, 40, 67, 23, 16, 27, 22, 38}
      }
    }),
    false
  );
};
