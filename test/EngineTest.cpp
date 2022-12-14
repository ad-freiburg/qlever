// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <tuple>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/OptionalJoin.h"
#include "util/Random.h"
#include "engine/QueryExecutionTree.h"
#include "util/Forward.h"
#include "util/SourceLocation.h"
#include "./util/GTestHelpers.h"
#include "engine/IdTable.h"

ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}

auto I = [](const auto& id) {
  return Id::makeFromVocabIndex(VocabIndex::make(id));
};

/*
 * Creates an IdTable for table described in talbeContent and returns it.
*/
template<size_t TABLE_WIDTH>
IdTable makeIdTableFromVector(std::vector<std::array<size_t, TABLE_WIDTH>> tableContent) {
  IdTable result(TABLE_WIDTH, allocator());

  // Copying the content into the table.
  for (const auto& row: tableContent) {
    const size_t backIndex = result.size();
    result.push_back();

    for (size_t c = 0; c < TABLE_WIDTH; c++) {
      result(backIndex, c) = I(row[c]);
    }
  }

  return result;
}

/*
 * @brief Tests, if the given IdTable has the same content as the sample
 * solution and, if the option was choosen, if the IdTable is sorted by
 * the join column.
 *
 * @tparam TABLE_WIDTH Just ignore it. Made to be infered from the function
 *  paramters.
 *
 * @param table The IdTable that should be tested.
 * @param sampleSolution The sample solution. Doesn't need to be sorted,
 *  or the same order of rows as the table.
 * @param testForSorted If this is true, it will also be tested, if the table
 *  is sorted by the join column.
 * @param jc The join column of the table.
 * @param t Ignore it. It's only here for being able to make better messages,
 *  if a IdTable fails the comparison.
*/
template<size_t TABLE_WIDTH>
void compareIdTableWithSolution(IdTable table, 
    std::vector<std::array<size_t, TABLE_WIDTH>> sampleSolution,
    const bool testForSorted = false,
    const size_t jc = 0,
    ad_utility::source_location t = ad_utility::source_location::current()
    ) {
  // For generating more informative messages, when failing the comparison.
  std::stringstream traceMessage;
  traceMessage << "compareIdTableWithSolution comparing IdTable\n";
  for (size_t row = 0; row < table.size(); row++) {
    for (size_t column = 0; column < table.cols(); column++) {
      traceMessage << table(row, column) << " ";
    }
    traceMessage << "\n";
  }
  traceMessage << "with IdTable described by vector\n";
  for (auto const& row: sampleSolution) {
    for (auto const& entry: row) {
      traceMessage << entry << " ";
    }
    traceMessage << "\n";
  }
  auto trace = generateLocationTrace(t, traceMessage.str());
  
  // Do the IdTable and sampleSolution have the same dimensions?
  ASSERT_EQ(table.size(), sampleSolution.size());
  ASSERT_EQ(table.cols(), TABLE_WIDTH);

  if (testForSorted) {
    // Is the table sorted by join column?
    auto oldEntry = table(0, jc);
    for (size_t i = 1; i < table.size(); i++) {
      ASSERT_TRUE(oldEntry <= table(i, jc));
      oldEntry = table(i, jc);
    }
  }

  // Sort both the table and the sampleSolution, so that both have a definite
  // form for comparison.
  auto sortFunction = [](
      const auto& element1,
      const auto& element2)
    {
      size_t i = 0;
      while (i < (TABLE_WIDTH - 1) && element1[i] == element2[i]) {
        i++;
      }
      return element1[i] < element2[i];
    };
  std::sort(table.begin(), table.end(), sortFunction);
  std::sort(sampleSolution.begin(), sampleSolution.end(), sortFunction);

  for (size_t row = 0; row < sampleSolution.size(); row++) {
    for (size_t column = 0; column < TABLE_WIDTH; column++) {
      ASSERT_EQ(I(sampleSolution[row][column]), table(row, column));
    }
  }
}

/*
 * @brief Converts the given vectors to IdTables, joins them together with the
 * given join function and returns the result.
 *
 * @tparam TABLE_A_WIDTH is the width of the first table.
 * @tparam TABLE_B_WIDTH is the width of the second table.
 * @tparam JOIN_FUNCTION is used to allow the transfer of any type of
 *  lambda function, that could contain a join function. You never have to
 *  actually specify this parameter , just let inference do its job.
 *
 * @param tableA, tableB hold the description of a table in form of vectors.
 * @param jcA, jcB are the join columns for the tables.
 * @param func the function, that will be used for joining the two tables
 *  together. Look into src/engine/Join.h for how it should look like.
 *
 * @returns tableA and tableB joined together in a IdTable.
 */
template<size_t TABLE_A_WIDTH, size_t TABLE_B_WIDTH, typename JOIN_FUNCTION>
IdTable useJoinFunctionOnVectorsTables(
        const std::vector<std::array<size_t, TABLE_A_WIDTH>>& tableA,
        const size_t jcA,
        const std::vector<std::array<size_t, TABLE_B_WIDTH>>& tableB,
        const size_t jcB,
        JOIN_FUNCTION func
        ) {
  IdTable a = makeIdTableFromVector(tableA);
  IdTable b = makeIdTableFromVector(tableB);
  
  constexpr int reswidth = TABLE_A_WIDTH + TABLE_B_WIDTH - 1;
  IdTable res(reswidth, allocator());
  
  CALL_FIXED_SIZE_3(TABLE_A_WIDTH, TABLE_B_WIDTH, reswidth, func.template operator(), a, jcA, b, jcB, &res);

  return res;
}

/*
 * @brief Goes through the sets of tests, converts the given vectors to
 * IdTables, joins them together with the given join function and
 * compares the results with the given sample solution.
 * However, because of technical limitations, all left/right/sample-solution
 * vectors MUST have the same amount of columns.
 *
 * @tparam TABLE_A_WIDTH is the width of the left tables.
 * @tparam TABLE_B_WIDTH is the width of the right tables.
 * @tparam TABLE_RESULT_WIDTH is the width of the sample solution tables.
 * @tparam How many test cases we have.
 * @tparam JOIN_FUNCTION is used to allow the transfer of any type of
 *  lambda function, that could contain a join function. You never have to
 *  actually specify this parameter, just let inference do its job.
 *
 * @param testSet is an array of tests, wrritten as tuples. The items in
 * the tuple have the following meaning:
 *  - First item: The left IdTable described through a vector of arrays.
 *  - Second item: The join column of the left IdTable.
 *  - Third item: The right IdTable described through a vector of arrays.
 *  - Fourth item: The join column of the right IdTable.
 *  - Fith item: The sample solution of the join function performed on both
 *    described IdTables. Once again, this IdTable is described through a
 *    vector of arrays.
 * @param func the function, that will be used for joining the two tables
 *  together. Look into src/engine/Join.h, or this file, for how it should look like.
 * @param testForSorted If this is true, then the result of func on the entries
 *  of testSet will also be tested on if they are sorted by their join column.
 */
template<size_t TABLE_A_WIDTH, size_t TABLE_B_WIDTH, size_t TABLE_RESULT_WIDTH, size_t NUMBER_OF_TESTS, typename JOIN_FUNCTION>
void goThroughSetOfTestsWithJoinFunction(
      const std::array<std::tuple< std::vector<std::array<size_t, TABLE_A_WIDTH>>, size_t, std::vector<std::array<size_t, TABLE_B_WIDTH>>, size_t, std::vector<std::array<size_t, TABLE_RESULT_WIDTH>>> , NUMBER_OF_TESTS> testSet,
      JOIN_FUNCTION func,
      const bool testForSorted = false,
      ad_utility::source_location t = ad_utility::source_location::current()
    ) {
  // For generating better messages, when failing a test.
  auto trace = generateLocationTrace(t, "goThroughSetOfTestsWithJoinFunction");

  for (size_t i = 0; i < NUMBER_OF_TESTS; i++) {
    // For easier reading.
    const std::vector<std::array<size_t, TABLE_A_WIDTH>>& tableAVector = std::get<0>(testSet[i]);
    const size_t jcA = std::get<1>(testSet[i]);
    const std::vector<std::array<size_t, TABLE_B_WIDTH>>& tableBVector = std::get<2>(testSet[i]);
    const size_t jcB = std::get<3>(testSet[i]);
    const std::vector<std::array<size_t, TABLE_RESULT_WIDTH>>& sampleSolutionTabletVector = std::get<4>(testSet[i]);
    const size_t sampleSolutionSize = sampleSolutionTabletVector.size();
    
    IdTable resultTable = useJoinFunctionOnVectorsTables(tableAVector, jcA, tableBVector, jcB, func);

    compareIdTableWithSolution(resultTable, sampleSolutionTabletVector, testForSorted, jcA);
    ASSERT_EQ(sampleSolutionSize, resultTable.size());
  }
}

/* 
 * @brief Creates and returns a set of join tests for the normal join function
 * in the formate described for goThroughSetofTestsWithJoinFunction.
 */
std::array<std::tuple< std::vector<std::array<size_t, 2>>, size_t, std::vector<std::array<size_t, 2>>, size_t, std::vector<std::array<size_t, 3>>> , 3> createNormalJoinTestSet() {
  std::array<std::tuple< std::vector<std::array<size_t, 2>>, size_t, std::vector<std::array<size_t, 2>>, size_t, std::vector<std::array<size_t, 3>>> , 3> myTestSet;

  myTestSet[0] = std::make_tuple(
      std::vector<std::array<size_t, 2>>{{{1, 1}, {1, 3}, {2, 1}, {2, 2}, {4, 1}}},
      0,
      std::vector<std::array<size_t, 2>>{{{1, 3}, {1, 8}, {3, 1}, {4, 2}}},
      0,
      std::vector<std::array<size_t, 3>>{{1, 1, 3}, {1, 1, 8}, {1, 3, 3}, {1, 3, 8}, {4, 1, 2}}
  );

  myTestSet[1] = std::make_tuple(
      std::vector<std::array<size_t, 2>>{{{1, 1}, {1, 3}, {2, 1}, {2, 2}, {4, 1}}},
      0,
      std::vector<std::array<size_t, 2>>{{{1, 3}, {1, 8}, {3, 1}, {4, 2}}},
      0,
      std::vector<std::array<size_t, 3>>{{1, 1, 3}, {1, 1, 8}, {1, 3, 3}, {1, 3, 8}, {4, 1, 2}, {400000, 200000, 200000}}
  );
  for (size_t i = 1; i <= 10000; ++i) {
    std::get<2>(myTestSet[1]).push_back({4 + i, 2 + i});
  }
  std::get<0>(myTestSet[1]).push_back({400000, 200000});
  std::get<2>(myTestSet[1]).push_back({400000, 200000});

  myTestSet[2] = std::make_tuple(
      std::vector<std::array<size_t, 2>>{},
      0,
      std::vector<std::array<size_t, 2>>{},
      0,
      std::vector<std::array<size_t, 3>>{{40000, 200000, 200000}, {4000001, 200000, 200000}}
  );
  for (size_t i = 1; i <= 10000; ++i) {
    std::get<0>(myTestSet[2]).push_back({4 + i, 2 + i});
  }
  std::get<0>(myTestSet[2]).push_back({40000, 200000});
  std::get<2>(myTestSet[2]).push_back({40000, 200000});

  for (size_t i = 1; i <= 10000; ++i) {
    std::get<0>(myTestSet[2]).push_back({40000 + i, 2 + i});
  }
  std::get<0>(myTestSet[2]).push_back({4000001, 200000});
  std::get<2>(myTestSet[2]).push_back({4000001, 200000});

  return myTestSet;
}

/* 
 * @brief The set of join tests for the normal join function had one special
 * case with different column sizes than the rest, so I had to create another
 * function just for creating that one.
 * How to read the formate is described in goThroughSetofTestsWithJoinFunction.
 */
std::array<std::tuple< std::vector<std::array<size_t, 2>>, size_t, std::vector<std::array<size_t, 1>>, size_t, std::vector<std::array<size_t, 2>>> , 1> createSpecialCaseOfNormalJoinTestSet() {
  std::array<std::tuple< std::vector<std::array<size_t, 2>>, size_t, std::vector<std::array<size_t, 1>>, size_t, std::vector<std::array<size_t, 2>>>, 1> myTestSet;

  myTestSet[0] = std::make_tuple(
      std::vector<std::array<size_t, 2>>{{{0, 1}, {0, 2}, {1, 3}, {1, 4}}},
      0,
      std::vector<std::array<size_t, 1>>{{{0}}},
      0,
      std::vector<std::array<size_t, 2>>{{0, 1}, {0, 2}}
  );

  return myTestSet;
}

TEST(EngineTest, joinTest) {
  Join J{Join::InvalidOnlyForTestingJoinTag{}};
  auto JoinLambda = [&J]<int A, int B, int C>(auto&&... args) {
    return J.join<A, B, C>(AD_FWD(args)...);
  };
  goThroughSetOfTestsWithJoinFunction(createNormalJoinTestSet(), JoinLambda, true);
  goThroughSetOfTestsWithJoinFunction(createSpecialCaseOfNormalJoinTestSet(), JoinLambda, true);
};

TEST(EngineTest, optionalJoinTest) {
  IdTable a = makeIdTableFromVector(std::vector<std::array<size_t, 3>>{{{4, 1, 2}, {2, 1, 3}, {1, 1, 4}, {2, 2, 1}, {1, 3, 1}}});
  IdTable b = makeIdTableFromVector(std::vector<std::array<size_t, 3>>{{{3, 3, 1}, {1, 8, 1}, {4, 2, 2}, {1, 1, 3}}});
  IdTable res(4, allocator());
  vector<array<ColumnIndex, 2>> jcls;
  jcls.push_back(array<ColumnIndex, 2>{{1, 2}});
  jcls.push_back(array<ColumnIndex, 2>{{2, 1}});

  // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
  // a have to equal those of column 2 of b and vice versa).
  int aWidth = a.cols();
  int bWidth = b.cols();
  int resWidth = res.cols();
  CALL_FIXED_SIZE_3(aWidth, bWidth, resWidth, OptionalJoin::optionalJoin, a, b,
                    false, true, jcls, &res);
  
  // For easier checking of the result.
  IdTable sampleSolution = makeIdTableFromVector(std::vector<std::array<size_t, 4>>{
          {4, 1, 2, 0},
          {2, 1, 3, 3},
          {1, 1, 4, 0},
          {2, 2, 1, 0},
          {1, 3, 1, 1}
        }
      );
  sampleSolution(0, 3) = ID_NO_VALUE;
  sampleSolution(2, 3) = ID_NO_VALUE;
  sampleSolution(3, 3) = ID_NO_VALUE;

  ASSERT_EQ(sampleSolution.size(), res.size());
  ASSERT_EQ(sampleSolution, res);

  // Test the optional join with variable sized data.
  IdTable va = makeIdTableFromVector(std::vector<std::array<size_t, 6>>{{{1, 2, 3, 4, 5, 6}, {1, 2, 3, 7, 5 ,6}, {7, 6, 5, 4, 3, 2}}});

  IdTable vb = makeIdTableFromVector(std::vector<std::array<size_t, 3>>{{{2, 3, 4}, {2, 3, 5}, {6, 7, 4}}});

  IdTable vres(7, allocator());
  jcls.clear();
  jcls.push_back(array<ColumnIndex, 2>{{1, 0}});
  jcls.push_back(array<ColumnIndex, 2>{{2, 1}});

  aWidth = va.cols();
  bWidth = vb.cols();
  resWidth = vres.cols();
  CALL_FIXED_SIZE_3(aWidth, bWidth, resWidth, OptionalJoin::optionalJoin, va,
                    vb, true, false, jcls, &vres);
  
  // For easier checking.
  sampleSolution = makeIdTableFromVector(std::vector<std::array<size_t, 7>>{
          {1, 2, 3, 4, 5, 6, 4},
          {1, 2, 3, 4, 5, 6, 5},
          {1, 2, 3, 7, 5, 6, 4},
          {1, 2, 3, 7, 5, 6, 5},
          {0, 6, 7, 0, 0, 0, 4}
        }
      );
  sampleSolution(4, 0) = ID_NO_VALUE;
  sampleSolution(4, 3) = ID_NO_VALUE;
  sampleSolution(4, 4) = ID_NO_VALUE;
  sampleSolution(4, 5) = ID_NO_VALUE;

  ASSERT_EQ(sampleSolution.size(), vres.size());
  ASSERT_EQ(sampleSolution.cols(), vres.cols());
  ASSERT_EQ(sampleSolution, vres);
}

TEST(EngineTest, distinctTest) {
  IdTable inp = makeIdTableFromVector(std::vector<std::array<size_t, 4>>{{{1, 1, 3, 7}, {6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}}});

  IdTable res(4, allocator());

  std::vector<size_t> keepIndices = {1, 2};
  CALL_FIXED_SIZE_1(4, Engine::distinct, inp, keepIndices, &res);
  
  // For easier checking.
  IdTable sampleSolution = makeIdTableFromVector(std::vector<std::array<size_t, 4>>{{{1, 1, 3, 7}, {2, 2, 3, 5}, {3, 6, 5, 4}}});
  ASSERT_EQ(sampleSolution.size(), res.size());
  ASSERT_EQ(sampleSolution, res);
}

TEST(EngineTest, hashJoinTest) {
  // Going through the tests of the normal lambda. Only possible, because
  // the result for hashed join is sorted, if the input was also sorted.
  Join J{Join::InvalidOnlyForTestingJoinTag{}};
  auto HashJoinLambda = [&J]<int A, int B, int C>(auto&&... args) {
    return J.hashJoin<A, B, C>(AD_FWD(args)...);
  };
  auto JoinLambda = [&J]<int A, int B, int C>(auto&&... args) {
    return J.join<A, B, C>(AD_FWD(args)...);
  };
  goThroughSetOfTestsWithJoinFunction(createNormalJoinTestSet(), HashJoinLambda);
  goThroughSetOfTestsWithJoinFunction(createSpecialCaseOfNormalJoinTestSet(), HashJoinLambda);


  /*
   * Create two sorted IdTables. Join them using the normal join and the
   * hashed join. Compare.
  */
  std::vector<std::array<size_t, 5>> a = {
      {34, 73, 92, 61, 18},
      {11, 80, 20, 43, 75},
      {96, 51, 40, 67, 23}
    };
 
  std::vector<std::array<size_t, 5>> b = {
      {34, 73, 92, 61, 18},
      {96, 2, 76, 87, 38},
      {96, 16, 27, 22, 38},
      {7, 51, 40, 67, 23}
    }; 
  
  // Saving the results and comparing them.
  IdTable result1 = useJoinFunctionOnVectorsTables(a, 0, b, 0, JoinLambda); // Normal.
  IdTable result2 = useJoinFunctionOnVectorsTables(a, 0, b, 0, HashJoinLambda); // Hash.
  ASSERT_EQ(result1, result2);

  // Checking if result1 and result2 are correct.
  compareIdTableWithSolution(result1, std::vector<std::array<size_t, 9>>{
      {
        {34, 73, 92, 61, 18, 73, 92, 61, 18},
        {96, 51, 40, 67, 23, 2, 76, 87, 38},
        {96, 51, 40, 67, 23, 16, 27, 22, 38}
      }
    },
    true,
    0
  );
  compareIdTableWithSolution(result2, std::vector<std::array<size_t, 9>>{
      {
        {34, 73, 92, 61, 18, 73, 92, 61, 18},
        {96, 51, 40, 67, 23, 2, 76, 87, 38},
        {96, 51, 40, 67, 23, 16, 27, 22, 38}
      }
    },
    true,
    0
  );

  // Random shuffle the tables, hash join them, sort the result and check,
  // if the result is still correct.
  randomShuffle(a.begin(), a.end());
  randomShuffle(b.begin(), b.end());
  result2 = useJoinFunctionOnVectorsTables(a, 0, b, 0, HashJoinLambda);

  // Sorting is done inside this function.
  compareIdTableWithSolution(result2, std::vector<std::array<size_t, 9>>{
      {
        {34, 73, 92, 61, 18, 73, 92, 61, 18},
        {96, 51, 40, 67, 23, 2, 76, 87, 38},
        {96, 51, 40, 67, 23, 16, 27, 22, 38}
      }
    },
    false
  );
};
