//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/IdTableHelpers.h"
#include "engine/QueryExecutionTree.h"
#include "engine/TextLimit.h"
#include "engine/ValuesForTesting.h"
#include "util/IndexTestHelpers.h"

namespace {
TextLimit makeTextLimit(IdTable input, const size_t& n,
                        const ColumnIndex& textRecordColumn,
                        const ColumnIndex& entityColumn,
                        const ColumnIndex& scoreColumn) {
  auto qec = ad_utility::testing::getQec();
  qec->_textLimit = n;
  std::vector<std::optional<Variable>> vars;
  for (size_t i = 0; i < input.numColumns(); ++i) {
    vars.emplace_back("?" + std::to_string(i));
  }
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      ad_utility::testing::getQec(), std::move(input), vars);
  return TextLimit{qec, std::move(subtree), textRecordColumn, entityColumn,
                   scoreColumn};
}

bool testSorted(IdTable result, const ColumnIndex& textRecordColumn,
                const ColumnIndex& entityColumn,
                const ColumnIndex& scoreColumn) {
  for (size_t i = 1; i < result.size(); i++) {
    if (result[i][entityColumn] > result[i - 1][entityColumn] ||
        (result[i][entityColumn] == result[i - 1][entityColumn] &&
         result[i][scoreColumn] > result[i - 1][scoreColumn]) ||
        (result[i][entityColumn] == result[i - 1][entityColumn] &&
         result[i][scoreColumn] == result[i - 1][scoreColumn] &&
         result[i][textRecordColumn] > result[i - 1][textRecordColumn])) {
      return false;
    }
  }
  return true;
}
}  // namespace

// _____________________________________________________________________________
TEST(TextLimit, computeResult) {
  /*
  The indices written as a table:
  textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  7          | 1      | 1    | 2     | 1      | 5
  0          | 6      | 3    | 3     | 4      | 4
  5          | 1      | 1    | 2     | 3      | 27
  5          | 1      | 0    | 2     | 0      | 27
  19         | 1      | 4    | 1     | 7      | 9
  3          | 5      | 4    | 2     | 4      | 4
  2          | 5      | 0    | 6     | 7      | 5
  5          | 5      | 2    | 4     | 9      | 7
  2          | 4      | 1    | 5     | 6      | 19
  1          | 36     | 2    | 4     | 5      | 3
  4          | 0      | 1    | 7     | 8      | 6
  0          | 0      | 2    | 1     | 2      | 19
  */

  VectorTable input = {
      {7, 1, 1, 2, 1, 5},  {0, 6, 3, 3, 4, 4},  {5, 1, 1, 2, 3, 27},
      {5, 1, 0, 2, 0, 27}, {19, 1, 4, 1, 7, 9}, {3, 5, 4, 2, 4, 4},
      {2, 5, 0, 6, 7, 5},  {5, 5, 2, 4, 9, 7},  {2, 4, 1, 5, 6, 19},
      {1, 36, 2, 4, 5, 3}, {4, 0, 1, 7, 8, 6},  {0, 0, 2, 1, 2, 19}};
  IdTable inputTable = makeIdTableFromVector(input, &Id::makeFromInt);

  /*
  Written as a table sorted on entity, score, textRecord descending:
  textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  1          | 36     | 2    | 4     | 5      | 3
  0          | 6      | 3    | 3     | 4      | 4
  2          | 5      | 0    | 6     | 7      | 5
  5          | 5      | 2    | 4     | 9      | 7
  3          | 5      | 4    | 2     | 4      | 4
  2          | 4      | 1    | 5     | 6      | 19
  7          | 1      | 1    | 2     | 1      | 5
  5          | 1      | 1    | 2     | 3      | 27
  5          | 1      | 0    | 2     | 0      | 27
  19         | 1      | 4    | 1     | 7      | 9
  4          | 0      | 1    | 7     | 8      | 6
  0          | 0      | 2    | 1     | 2      | 19
  */

  // Test with limit 0.
  TextLimit textLimit0 = makeTextLimit(inputTable.clone(), 0, 0, 1, 3);
  ASSERT_EQ(textLimit0.getResultWidth(), 6);
  ASSERT_TRUE(textLimit0.knownEmptyResult());
  IdTable resultIdTable = textLimit0.getResult()->idTable().clone();

  ASSERT_EQ(resultIdTable.numRows(), 0);

  // Test with limit 1.
  TextLimit textLimit1 = makeTextLimit(inputTable.clone(), 1, 0, 1, 3);
  ASSERT_EQ(textLimit1.getResultWidth(), 6);
  resultIdTable = textLimit1.getResult()->idTable().clone();

  ASSERT_TRUE(testSorted(resultIdTable.clone(), 0, 1, 3));

  /*
  Expected result:
  textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  1          | 36     | 2    | 4     | 5      | 3
  0          | 6      | 3    | 3     | 4      | 4
  2          | 5      | 0    | 6     | 7      | 5
  2          | 4      | 1    | 5     | 6      | 19
  7          | 1      | 1    | 2     | 1      | 5
  4          | 0      | 1    | 7     | 8      | 6
  */
  VectorTable expected = {{1, 36, 2, 4, 5, 3}, {0, 6, 3, 3, 4, 4},
                          {2, 5, 0, 6, 7, 5},  {2, 4, 1, 5, 6, 19},
                          {7, 1, 1, 2, 1, 5},  {4, 0, 1, 7, 8, 6}};
  IdTable expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  compareIdTableWithExpectedContent(resultIdTable, expectedTable);

  // Test with limit 2.
  TextLimit textLimit2 = makeTextLimit(inputTable.clone(), 2, 0, 1, 3);
  ASSERT_EQ(textLimit2.getResultWidth(), 6);
  resultIdTable = textLimit2.getResult()->idTable().clone();

  ASSERT_TRUE(testSorted(resultIdTable.clone(), 0, 1, 3));

  /*
  Expected result:
  textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  1          | 36     | 2    | 4     | 5      | 3
  0          | 6      | 3    | 3     | 4      | 4
  2          | 5      | 0    | 6     | 7      | 5
  5          | 5      | 2    | 4     | 9      | 7
  2          | 4      | 1    | 5     | 6      | 19
  7          | 1      | 1    | 2     | 1      | 5
  5          | 1      | 1    | 2     | 3      | 27
  5          | 1      | 0    | 2     | 0      | 27
  4          | 0      | 1    | 7     | 8      | 6
  0          | 0      | 2    | 1     | 2      | 19
  */

  expected = {{1, 36, 2, 4, 5, 3}, {0, 6, 3, 3, 4, 4},  {2, 5, 0, 6, 7, 5},
              {5, 5, 2, 4, 9, 7},  {2, 4, 1, 5, 6, 19}, {7, 1, 1, 2, 1, 5},
              {5, 1, 1, 2, 3, 27}, {5, 1, 0, 2, 0, 27}, {4, 0, 1, 7, 8, 6},
              {0, 0, 2, 1, 2, 19}};
  expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  compareIdTableWithExpectedContent(resultIdTable, expectedTable);

  // Test with limit 19.
  TextLimit textLimit19 = makeTextLimit(inputTable.clone(), 19, 0, 1, 3);
  ASSERT_EQ(textLimit19.getResultWidth(), 6);
  resultIdTable = textLimit19.getResult()->idTable().clone();

  ASSERT_TRUE(testSorted(resultIdTable.clone(), 0, 1, 3));

  /*
  Expected result:
  textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  1          | 36     | 2    | 4     | 5      | 3
  0          | 6      | 3    | 3     | 4      | 4
  2          | 5      | 0    | 6     | 7      | 5
  5          | 5      | 2    | 4     | 9      | 7
  3          | 5      | 4    | 2     | 4      | 4
  2          | 4      | 1    | 5     | 6      | 19
  7          | 1      | 1    | 2     | 1      | 5
  5          | 1      | 1    | 2     | 3      | 27
  5          | 1      | 0    | 2     | 0      | 27
  19         | 1      | 4    | 1     | 7      | 9
  4          | 0      | 1    | 7     | 8      | 6
  0          | 0      | 2    | 1     | 2      | 19
  */

  expected = {{1, 36, 2, 4, 5, 3}, {0, 6, 3, 3, 4, 4},  {2, 5, 0, 6, 7, 5},
              {5, 5, 2, 4, 9, 7},  {3, 5, 4, 2, 4, 4},  {2, 4, 1, 5, 6, 19},
              {7, 1, 1, 2, 1, 5},  {5, 1, 1, 2, 3, 27}, {5, 1, 0, 2, 0, 27},
              {19, 1, 4, 1, 7, 9}, {4, 0, 1, 7, 8, 6},  {0, 0, 2, 1, 2, 19}};
  expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  compareIdTableWithExpectedContent(resultIdTable, expectedTable);

  /*
  New idTable:
  textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  7          | 1      | 1    | 2     | 1      | 5
  0          | 2      | 3    | 3     | 4      | 4
  5          | 2      | 1    | 2     | 3      | 27
  5          | 2      | 0    | 2     | 0      | 27
  19         | 2      | 4    | 1     | 7      | 9
  3          | 5      | 4    | 2     | 4      | 4

  ordered by entity, score, textRecord descending:
  textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  3          | 5      | 4    | 2     | 4      | 4
  0          | 2      | 3    | 3     | 4      | 4
  5          | 2      | 1    | 2     | 3      | 27
  5          | 2      | 0    | 2     | 0      | 27
  19         | 2      | 4    | 1     | 7      | 9
  7          | 1      | 1    | 2     | 1      | 5
  */

  input = {{7, 1, 1, 2, 1, 5},  {0, 2, 3, 3, 4, 4},  {5, 2, 1, 2, 3, 27},
           {5, 2, 0, 2, 0, 27}, {19, 2, 4, 1, 7, 9}, {3, 5, 4, 2, 4, 4}};
  inputTable = makeIdTableFromVector(input, &Id::makeFromInt);

  // Test with limit 3.
  TextLimit textLimit3 = makeTextLimit(inputTable.clone(), 3, 0, 1, 3);
  ASSERT_EQ(textLimit3.getResultWidth(), 6);
  resultIdTable = textLimit3.getResult()->idTable().clone();

  ASSERT_TRUE(testSorted(resultIdTable.clone(), 0, 1, 3));

  /*
  Expected result:
  textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  3          | 5      | 4    | 2     | 4      | 4
  0          | 2      | 3    | 3     | 4      | 4
  5          | 2      | 1    | 2     | 3      | 27
  5          | 2      | 0    | 2     | 0      | 27
  19         | 2      | 4    | 1     | 7      | 9
  7          | 1      | 1    | 2     | 1      | 5
  */

  expected = {{3, 5, 4, 2, 4, 4},  {0, 2, 3, 3, 4, 4},  {5, 2, 1, 2, 3, 27},
              {5, 2, 0, 2, 0, 27}, {19, 2, 4, 1, 7, 9}, {7, 1, 1, 2, 1, 5}};
  expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  compareIdTableWithExpectedContent(resultIdTable, expectedTable);
}

// _____________________________________________________________________________
TEST(TextLimit, BasicMemberFunctions) {
  VectorTable input{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}};
  IdTable inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  TextLimit textLimit = makeTextLimit(inputTable.clone(), 5, 0, 1, 2);
  ASSERT_EQ(textLimit.getResultWidth(), 3);
  ASSERT_EQ(textLimit.getCostEstimate(), 12);
  ASSERT_EQ(textLimit.getSizeEstimateBeforeLimit(), 4);
  ASSERT_FALSE(textLimit.knownEmptyResult());
  ASSERT_THAT(textLimit.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAre(
                  ::testing::Pair(Variable("?0"), makeAlwaysDefinedColumn(0)),
                  ::testing::Pair(Variable("?1"), makeAlwaysDefinedColumn(1)),
                  ::testing::Pair(Variable("?2"), makeAlwaysDefinedColumn(2))));

  inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  TextLimit textLimit2 = makeTextLimit(
      IdTable(3, ad_utility::testing::makeAllocator()), 5, 0, 1, 2);
  ASSERT_TRUE(textLimit2.knownEmptyResult());
}

// _____________________________________________________________________________
TEST(TextLimit, CacheKey) {
  VectorTable input{{1, 2, 3}, {1, 2, 3}, {1, 2, 3}};
  IdTable inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  TextLimit textLimit1 = makeTextLimit(inputTable.clone(), 4, 0, 1, 2);
  ASSERT_EQ(textLimit1.getDescriptor(), "TextLimit with limit n: 4");

  TextLimit textLimit2 = makeTextLimit(inputTable.clone(), 4, 0, 1, 2);
  // Every argument is the same.
  ASSERT_EQ(textLimit1.getCacheKey(), textLimit2.getCacheKey());

  TextLimit textLimit3 = makeTextLimit(inputTable.clone(), 5, 0, 1, 2);
  // The limit is different.
  ASSERT_NE(textLimit1.getCacheKey(), textLimit3.getCacheKey());

  TextLimit textLimit4 = makeTextLimit(inputTable.clone(), 4, 1, 1, 2);
  // The textRecordColumn is different.
  ASSERT_NE(textLimit1.getCacheKey(), textLimit4.getCacheKey());

  TextLimit textLimit5 = makeTextLimit(inputTable.clone(), 4, 0, 2, 2);
  // The entityColumn is different.
  ASSERT_NE(textLimit1.getCacheKey(), textLimit5.getCacheKey());

  TextLimit textLimit6 = makeTextLimit(inputTable.clone(), 4, 0, 1, 3);
  // The scoreColumn is different.
  ASSERT_NE(textLimit1.getCacheKey(), textLimit6.getCacheKey());

  input = {{1, 2, 3}, {1, 2, 3}, {1, 2, 4}};
  inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  TextLimit textLimit7 = makeTextLimit(inputTable.clone(), 4, 0, 1, 2);
  // The input is different.
  ASSERT_NE(textLimit1.getCacheKey(), textLimit7.getCacheKey());
}
