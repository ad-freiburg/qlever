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

#include "./util/AllocatorTestHelpers.h"
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

using ad_utility::testing::makeAllocator;
namespace {
/*
 * Does what it says on the tin: Save an IdTable with the corresponding
 * join column.
 */
struct IdTableAndJoinColumn {
  IdTable idTable;
  size_t joinColumn;
};

/*
 * A structure containing all information needed for a normal join test. A
 * normal join test is defined as two IdTables being joined, the resulting
 * IdTable tested, if it is sorted after the join column, or not, and this
 * IdTable is than compared with the expectedResult.
 */
struct JoinTestCase {
  IdTableAndJoinColumn leftInput;
  IdTableAndJoinColumn rightInput;
  IdTable expectedResult;
  bool resultMustBeSortedByJoinColumn;
};

/*
 * @brief Join two IdTables using the given join function and return
 * the result.
 *
 * @tparam JOIN_FUNCTION is used to allow the transfer of any type of
 *  lambda function, that could contain a join function. You never have to
 *  actually specify this parameter , just let inference do its job.
 *
 * @param tableA, tableB the tables with their join columns.
 * @param func the function, that will be used for joining the two tables
 *  together. Look into src/engine/Join.h for how it should look like.
 *
 * @returns tableA and tableB joined together in a IdTable.
 */
template <typename JOIN_FUNCTION>
IdTable useJoinFunctionOnIdTables(const IdTableAndJoinColumn& tableA,
                                  const IdTableAndJoinColumn& tableB,
                                  JOIN_FUNCTION func) {
  int resultWidth{static_cast<int>(tableA.idTable.numColumns() +
                                   tableB.idTable.numColumns() - 1)};
  IdTable result{static_cast<size_t>(resultWidth), makeAllocator()};

  // You need to use this special function for executing lambdas. The normal
  // function for functions won't work.
  // Additionaly, we need to cast the two size_t, because callFixedSize only
  // takes arrays of int.
  ad_utility::callFixedSize(
      (std::array{static_cast<int>(tableA.idTable.numColumns()),
                  static_cast<int>(tableB.idTable.numColumns()), resultWidth}),
      func, tableA.idTable, tableA.joinColumn, tableB.idTable,
      tableB.joinColumn, &result);

  return result;
}

/*
 * @brief Goes through the sets of tests, joins them together with the given
 * join function and compares the results with the given expected content.
 *
 * @tparam JOIN_FUNCTION is used to allow the transfer of any type of
 *  lambda function, that could contain a join function. You never have to
 *  actually specify this parameter, just let inference do its job.
 *
 * @param testSet is an array of JoinTestCases, that describe what tests to
 *  do. For an explanation, how to read JoinTestCase, see the definition.
 * @param func the function, that will be used for joining the two tables
 *  together. Look into src/engine/Join.h, or this file, for how it should look
 * like.
 */
template <typename JOIN_FUNCTION>
void goThroughSetOfTestsWithJoinFunction(
    const std::vector<JoinTestCase>& testSet, JOIN_FUNCTION func,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "goThroughSetOfTestsWithJoinFunction")};

  for (JoinTestCase const& test : testSet) {
    IdTable resultTable{
        useJoinFunctionOnIdTables(test.leftInput, test.rightInput, func)};

    compareIdTableWithExpectedContent(resultTable, test.expectedResult,
                                      test.resultMustBeSortedByJoinColumn,
                                      test.leftInput.joinColumn);
  }
}

void runTestCasesForAllJoinAlgorithms(
    std::vector<JoinTestCase> testSet,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating better messages, when failing a test.
  auto trace{generateLocationTrace(l, "runTestCasesForAllJoinAlgorithms")};

  // All normal join algorithm defined as lambda functions for easier handing
  // over to helper functions.
  Join J{Join::InvalidOnlyForTestingJoinTag{}};
  DISABLE_WARNINGS_CLANG_13
  auto hashJoinLambda = [&J]<int A, int B, int C>(auto&&... args) {
    ENABLE_WARNINGS_CLANG_13
    return J.hashJoin(AD_FWD(args)...);
  };
  DISABLE_WARNINGS_CLANG_13
  auto joinLambda = [&J]<int A, int B, int C>(auto&&... args) {
    ENABLE_WARNINGS_CLANG_13
    return J.join<A, B, C>(AD_FWD(args)...);
  };

  // For sorting IdTableAndJoinColumn by their join column.
  auto sortByJoinColumn = [](IdTableAndJoinColumn& idTableAndJC) {
    std::ranges::sort(idTableAndJC.idTable, {},
                      [&idTableAndJC](const auto& row) {
                        return row[idTableAndJC.joinColumn];
                      });
  };

  // Random shuffle both tables, run hashJoin, check result.
  std::ranges::for_each(testSet, [](JoinTestCase& testCase) {
    randomShuffle(testCase.leftInput.idTable.begin(),
                  testCase.leftInput.idTable.end());
    randomShuffle(testCase.rightInput.idTable.begin(),
                  testCase.rightInput.idTable.end());
    testCase.resultMustBeSortedByJoinColumn = false;
  });
  goThroughSetOfTestsWithJoinFunction(testSet, hashJoinLambda);

  // Sort the larger table by join column, run hashJoin, check result (this time
  // it's sorted).
  std::ranges::for_each(testSet, [&sortByJoinColumn](JoinTestCase& testCase) {
    IdTableAndJoinColumn& largerInputTable =
        (testCase.leftInput.idTable.size() >=
         testCase.rightInput.idTable.size())
            ? testCase.leftInput
            : testCase.rightInput;
    sortByJoinColumn(largerInputTable);
    testCase.resultMustBeSortedByJoinColumn = true;
  });
  goThroughSetOfTestsWithJoinFunction(testSet, hashJoinLambda);

  // Sort both tables, run merge join and hash join, check result. (Which has to
  // be sorted.)
  std::ranges::for_each(testSet, [&sortByJoinColumn](JoinTestCase& testCase) {
    sortByJoinColumn(testCase.leftInput);
    sortByJoinColumn(testCase.rightInput);
    testCase.resultMustBeSortedByJoinColumn = true;
  });
  goThroughSetOfTestsWithJoinFunction(testSet, joinLambda);
  goThroughSetOfTestsWithJoinFunction(testSet, hashJoinLambda);
}

/*
 * @brief Return a vector of JoinTestCase for testing with the join functions.
 */
std::vector<JoinTestCase> createJoinTestSet() {
  std::vector<JoinTestCase> myTestSet{};

  // For easier creation of IdTables and readability.
  VectorTable leftIdTable{{{1, 1}, {1, 3}, {2, 1}, {2, 2}, {4, 1}}};
  VectorTable rightIdTable{{{1, 3}, {1, 8}, {3, 1}, {4, 2}}};
  VectorTable expectedResult{
      {{1, 1, 3}, {1, 1, 8}, {1, 3, 3}, {1, 3, 8}, {4, 1, 2}}};
  myTestSet.push_back(
      JoinTestCase{IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
                   IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
                   makeIdTableFromVector(expectedResult), true});

  leftIdTable = {{1, 1}, {1, 3}, {2, 1}, {2, 2}, {4, 1}};
  rightIdTable = {{1, 3}, {1, 8}, {3, 1}, {4, 2}};
  expectedResult = {{1, 1, 3}, {1, 1, 8}, {1, 3, 3},
                    {1, 3, 8}, {4, 1, 2}, {400000, 200000, 200000}};
  for (int64_t i = 1; i <= 10000; ++i) {
    rightIdTable.push_back({4 + i, 2 + i});
  }
  leftIdTable.push_back({400000, 200000});
  rightIdTable.push_back({400000, 200000});
  myTestSet.push_back(
      JoinTestCase{IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
                   IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
                   makeIdTableFromVector(expectedResult), true});

  leftIdTable = {};
  rightIdTable = {};
  expectedResult = {{40000, 200000, 200000}, {4000001, 200000, 200000}};
  for (int64_t i = 1; i <= 10000; ++i) {
    leftIdTable.push_back({4 + i, 2 + i});
  }
  leftIdTable.push_back({40000, 200000});
  rightIdTable.push_back({40000, 200000});
  for (int64_t i = 1; i <= 10000; ++i) {
    leftIdTable.push_back({40000 + i, 2 + i});
  }
  leftIdTable.push_back({4000001, 200000});
  rightIdTable.push_back({4000001, 200000});
  myTestSet.push_back(
      JoinTestCase{IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
                   IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
                   makeIdTableFromVector(expectedResult), true});

  leftIdTable = {{0, 1}, {0, 2}, {1, 3}, {1, 4}};
  rightIdTable = {{0}};
  expectedResult = {{0, 1}, {0, 2}};
  myTestSet.push_back(
      JoinTestCase{IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
                   IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
                   makeIdTableFromVector(expectedResult), true});

  leftIdTable = {
      {34, 73, 92, 61, 18}, {11, 80, 20, 43, 75}, {96, 51, 40, 67, 23}};
  rightIdTable = {{34, 73, 92, 61, 18},
                  {96, 2, 76, 87, 38},
                  {96, 16, 27, 22, 38},
                  {7, 51, 40, 67, 23}};
  expectedResult = {{34, 73, 92, 61, 18, 73, 92, 61, 18},
                    {96, 51, 40, 67, 23, 2, 76, 87, 38},
                    {96, 51, 40, 67, 23, 16, 27, 22, 38}};
  myTestSet.push_back(
      JoinTestCase{IdTableAndJoinColumn{makeIdTableFromVector(leftIdTable), 0},
                   IdTableAndJoinColumn{makeIdTableFromVector(rightIdTable), 0},
                   makeIdTableFromVector(expectedResult), true});

  return myTestSet;
}
}  // namespace

TEST(JoinTest, joinTest) {
  runTestCasesForAllJoinAlgorithms(createJoinTestSet());
};
