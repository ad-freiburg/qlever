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
#include "./util/JoinHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"
#include "util/Forward.h"
#include "util/Random.h"
#include "util/SourceLocation.h"
#include "../test/IndexTestHelpers.h"

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
 * @brief Tests, whether the given IdTable has the same content as the sample
 * solution and, if the option was choosen, if the IdTable is sorted by
 * the join column.
 *
 * @param table The IdTable that should be tested.
 * @param expectedContent The sample solution. Doesn't need to be sorted,
 *  or the same order of rows as the table.
 * @param resultMustBeSortedByJoinColumn If this is true, it will also be
 * tested, if the table is sorted by the join column.
 * @param joinColumn The join column of the table.
 * @param l Ignore it. It's only here for being able to make better messages,
 *  if a IdTable fails the comparison.
 */
void compareIdTableWithExpectedContent(
    const IdTable& table, const IdTable& expectedContent,
    const bool resultMustBeSortedByJoinColumn = false,
    const size_t joinColumn = 0,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  // For generating more informative messages, when failing the comparison.
  std::stringstream traceMessage{};

  auto writeIdTableToStream = [&traceMessage](const IdTable& idTable) {
    std::ranges::for_each(idTable,
                          [&traceMessage](const auto& row) {
                            // TODO<C++23> Use std::views::join_with for both
                            // loops.
                            for (size_t i = 0; i < row.numColumns(); i++) {
                              traceMessage << row[i] << " ";
                            }
                            traceMessage << "\n";
                          },
                          {});
  };

  traceMessage << "compareIdTableWithExpectedContent comparing IdTable\n";
  writeIdTableToStream(table);
  traceMessage << "with IdTable \n";
  writeIdTableToStream(expectedContent);
  auto trace{generateLocationTrace(l, traceMessage.str())};

  // Because we compare tables later by sorting them, so that every table has
  // one definit form, we need to create local copies.
  IdTable localTable{table.clone()};
  IdTable localExpectedContent{expectedContent.clone()};

  if (resultMustBeSortedByJoinColumn) {
    // Is the table sorted by join column?
    ASSERT_TRUE(std::ranges::is_sorted(localTable.getColumn(joinColumn)));
  }

  // Sort both the table and the expectedContent, so that both have a definite
  // form for comparison.
  std::ranges::sort(localTable, std::ranges::lexicographical_compare);
  std::ranges::sort(localExpectedContent, std::ranges::lexicographical_compare);

  ASSERT_EQ(localTable, localExpectedContent);
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
  auto hashJoinLambda = makeHashJoinLambda();
  auto joinLambda = makeJoinLambda();

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
  for (size_t i = 1; i <= 10000; ++i) {
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
  for (size_t i = 1; i <= 10000; ++i) {
    leftIdTable.push_back({4 + i, 2 + i});
  }
  leftIdTable.push_back({40000, 200000});
  rightIdTable.push_back({40000, 200000});
  for (size_t i = 1; i <= 10000; ++i) {
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

TEST(JoinTest, joinTest) {
  runTestCasesForAllJoinAlgorithms(createJoinTestSet());
};
