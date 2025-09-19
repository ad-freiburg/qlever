//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Nick Göckel <nick.goeckel@students.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/IdTableHelpers.h"
#include "engine/CartesianProductJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/TextLimit.h"
#include "engine/ValuesForTesting.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

namespace {
TextLimit makeTextLimit(IdTable input, const size_t& n,
                        const ColumnIndex& textRecordColumn,
                        const std::vector<ColumnIndex>& entityColumns,
                        const std::vector<ColumnIndex>& scoreColumns) {
  auto qec = ad_utility::testing::getQec();
  std::vector<std::optional<Variable>> vars;
  for (size_t i = 0; i < input.numColumns(); ++i) {
    vars.emplace_back("?" + std::to_string(i));
  }
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      ad_utility::testing::getQec(), std::move(input), vars);
  return TextLimit{
      qec,           n,           std::move(subtree), textRecordColumn,
      entityColumns, scoreColumns};
}

CartesianProductJoin makeJoin(IdTable input1, IdTable input2) {
  auto qec = ad_utility::testing::getQec();
  size_t j = 0;
  std::vector<std::shared_ptr<QueryExecutionTree>> valueOperations;
  std::vector<std::optional<Variable>> vars;
  for (size_t i = 0; i < input1.numColumns(); ++i) {
    vars.emplace_back("?" + std::to_string(j++));
  }
  valueOperations.push_back(ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(input1), std::move(vars)));

  vars.clear();
  for (size_t i = 0; i < input2.numColumns(); ++i) {
    vars.emplace_back("?" + std::to_string(j++));
  }
  valueOperations.push_back(ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(input2), std::move(vars)));

  return CartesianProductJoin{qec, std::move(valueOperations)};
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
  Written as a table sorted on entity ascending and score, textRecord
  descending: textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  4          | 0      | 1    | 7     | 8      | 6
  0          | 0      | 2    | 1     | 2      | 19
  7          | 1      | 1    | 2     | 1      | 5
  5          | 1      | 0    | 2     | 0      | 27
  5          | 1      | 1    | 2     | 3      | 27
  19         | 1      | 4    | 1     | 7      | 9
  2          | 4      | 1    | 5     | 6      | 19
  2          | 5      | 0    | 6     | 7      | 5
  5          | 5      | 2    | 4     | 9      | 7
  3          | 5      | 4    | 2     | 4      | 4
  0          | 6      | 3    | 3     | 4      | 4
  1          | 36     | 2    | 4     | 5      | 3
  */

  // Test with limit 0.
  TextLimit textLimit0 = makeTextLimit(inputTable.clone(), 0, 0, {1}, {3});
  ASSERT_EQ(textLimit0.getResultWidth(), 6);
  ASSERT_TRUE(textLimit0.knownEmptyResult());
  IdTable resultIdTable = textLimit0.getResult()->idTable().clone();

  ASSERT_EQ(resultIdTable.numRows(), 0);

  // Test with limit 1.
  TextLimit textLimit1 = makeTextLimit(inputTable.clone(), 1, 0, {1}, {3});
  /*
  Expected result:
  textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  4          | 0      | 1    | 7     | 8      | 6
  7          | 1      | 1    | 2     | 1      | 5
  2          | 4      | 1    | 5     | 6      | 19
  2          | 5      | 0    | 6     | 7      | 5
  0          | 6      | 3    | 3     | 4      | 4
  1          | 36     | 2    | 4     | 5      | 3
  */
  VectorTable expected = {{4, 0, 1, 7, 8, 6},  {7, 1, 1, 2, 1, 5},
                          {2, 4, 1, 5, 6, 19}, {2, 5, 0, 6, 7, 5},
                          {0, 6, 3, 3, 4, 4},  {1, 36, 2, 4, 5, 3}};
  resultIdTable = textLimit1.getResult()->idTable().clone();
  IdTable expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  compareIdTableWithExpectedContent(resultIdTable, expectedTable);

  // Test with limit 2.
  TextLimit textLimit2 = makeTextLimit(inputTable.clone(), 2, 0, {1}, {3});
  /*
  Expected result:
  textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  4          | 0      | 1    | 7     | 8      | 6
  0          | 0      | 2    | 1     | 2      | 19
  7          | 1      | 1    | 2     | 1      | 5
  5          | 1      | 1    | 2     | 3      | 27
  5          | 1      | 0    | 2     | 0      | 27
  2          | 4      | 1    | 5     | 6      | 19
  2          | 5      | 0    | 6     | 7      | 5
  5          | 5      | 2    | 4     | 9      | 7
  0          | 6      | 3    | 3     | 4      | 4
  1          | 36     | 2    | 4     | 5      | 3
  */
  expected = {{4, 0, 1, 7, 8, 6},  {0, 0, 2, 1, 2, 19}, {7, 1, 1, 2, 1, 5},
              {5, 1, 1, 2, 3, 27}, {5, 1, 0, 2, 0, 27}, {2, 4, 1, 5, 6, 19},
              {2, 5, 0, 6, 7, 5},  {5, 5, 2, 4, 9, 7},  {0, 6, 3, 3, 4, 4},
              {1, 36, 2, 4, 5, 3}};
  resultIdTable = textLimit2.getResult()->idTable().clone();
  expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  compareIdTableWithExpectedContent(resultIdTable, expectedTable);

  // Test with limit 19.
  TextLimit textLimit19 = makeTextLimit(inputTable.clone(), 19, 0, {1}, {3});
  /*
  Expected result:
  textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  4          | 0      | 1    | 7     | 8      | 6
  0          | 0      | 2    | 1     | 2      | 19
  7          | 1      | 1    | 2     | 1      | 5
  5          | 1      | 1    | 2     | 3      | 27
  5          | 1      | 0    | 2     | 0      | 27
  19         | 1      | 4    | 1     | 7      | 9
  2          | 4      | 1    | 5     | 6      | 19
  2          | 5      | 0    | 6     | 7      | 5
  5          | 5      | 2    | 4     | 9      | 7
  3          | 5      | 4    | 2     | 4      | 4
  0          | 6      | 3    | 3     | 4      | 4
  1          | 36     | 2    | 4     | 5      | 3
  */
  expected = {{4, 0, 1, 7, 8, 6},  {0, 0, 2, 1, 2, 19}, {7, 1, 1, 2, 1, 5},
              {5, 1, 1, 2, 3, 27}, {5, 1, 0, 2, 0, 27}, {19, 1, 4, 1, 7, 9},
              {2, 4, 1, 5, 6, 19}, {2, 5, 0, 6, 7, 5},  {5, 5, 2, 4, 9, 7},
              {3, 5, 4, 2, 4, 4},  {0, 6, 3, 3, 4, 4},  {1, 36, 2, 4, 5, 3}};
  resultIdTable = textLimit19.getResult()->idTable().clone();
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

  ordered by entity ascending and score, textRecord descending:
  textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  7          | 1      | 1    | 2     | 1      | 5
  0          | 2      | 3    | 3     | 4      | 4
  5          | 2      | 1    | 2     | 3      | 27
  5          | 2      | 0    | 2     | 0      | 27
  19         | 2      | 4    | 1     | 7      | 9
  3          | 5      | 4    | 2     | 4      | 4
  */

  input = {{7, 1, 1, 2, 1, 5},  {0, 2, 3, 3, 4, 4},  {5, 2, 1, 2, 3, 27},
           {5, 2, 0, 2, 0, 27}, {19, 2, 4, 1, 7, 9}, {3, 5, 4, 2, 4, 4}};
  inputTable = makeIdTableFromVector(input, &Id::makeFromInt);

  // Test with limit 3.
  TextLimit textLimit3 = makeTextLimit(inputTable.clone(), 3, 0, {1}, {3});
  /*
  Expected result:
  textRecord | entity | word | score | random | random2
  -----------------------------------------------------
  7          | 1      | 1    | 2     | 1      | 5
  0          | 2      | 3    | 3     | 4      | 4
  5          | 2      | 1    | 2     | 3      | 27
  5          | 2      | 0    | 2     | 0      | 27
  19         | 2      | 4    | 1     | 7      | 9
  3          | 5      | 4    | 2     | 4      | 4
  */
  expected = {{7, 1, 1, 2, 1, 5},  {0, 2, 3, 3, 4, 4},  {5, 2, 1, 2, 3, 27},
              {5, 2, 0, 2, 0, 27}, {19, 2, 4, 1, 7, 9}, {3, 5, 4, 2, 4, 4}};
  resultIdTable = textLimit3.getResult()->idTable().clone();
  expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  compareIdTableWithExpectedContent(resultIdTable, expectedTable);
}

// _____________________________________________________________________________
TEST(TextLimit, computeResultMultipleEntities) {
  /*
  The indices written as a table:
  textRecord | entity1 | entity2 | entity3 | word | score1 | score2 | score2
  -------------------------------------------------------------------------
  7          | 1       | 1       | 1       | 1    | 2      | 21     | 2
  0          | 6       | 7       | 6       | 3    | 5      | 1      | 3
  5          | 1       | 1       | 2       | 1    | 1      | 1      | 2
  5          | 1       | 1       | 1       | 0    | 1      | 4      | 2
  19         | 1       | 1       | 1       | 4    | 22     | 2      | 1
  3          | 5       | 3       | 8       | 4    | 4      | 3      | 2
  2          | 5       | 9       | 5       | 0    | 5      | 2      | 6
  5          | 5       | 23      | 17      | 2    | 6      | 6      | 4
  2          | 4       | 4       | 2       | 1    | 8      | 5      | 5
  1          | 36      | 36      | 36      | 2    | 7      | 4      | 4
  4          | 0       | 3       | 1       | 1    | 7      | 7      | 7
  0          | 0       | 1       | 3       | 2    | 4      | 3      | 1


  ordered by entity1, entity2, entity3 ascending and score1+score2+score3,
  textRecord descending: textRecord | entity1 | entity2 | entity3 | word |
  score1 | score2 | score2
  -------------------------------------------------------------------------
  0          | 0       | 1       | 3       | 2    | 4      | 3      | 1
  4          | 0       | 3       | 1       | 1    | 7      | 7      | 7
  19         | 1       | 1       | 1       | 4    | 22     | 2      | 1
  7          | 1       | 1       | 1       | 1    | 2      | 21     | 2
  5          | 1       | 1       | 1       | 0    | 1      | 4      | 2
  5          | 1       | 1       | 2       | 1    | 1      | 1      | 2
  2          | 4       | 4       | 2       | 1    | 8      | 5      | 5
  3          | 5       | 3       | 8       | 4    | 4      | 3      | 2
  2          | 5       | 9       | 5       | 0    | 5      | 2      | 6
  5          | 5       | 23      | 17      | 2    | 6      | 6      | 4
  0          | 6       | 7       | 6       | 3    | 5      | 1      | 3
  1          | 36      | 36      | 36      | 2    | 7      | 4      | 4
  */
  IdTable inputTable = makeIdTableFromVector({{7, 1, 1, 1, 1, 2, 21, 2},
                                              {0, 6, 7, 6, 3, 5, 1, 3},
                                              {5, 1, 1, 2, 1, 1, 1, 2},
                                              {5, 1, 1, 1, 0, 1, 4, 2},
                                              {19, 1, 1, 1, 4, 22, 2, 1},
                                              {3, 5, 3, 8, 4, 4, 3, 2},
                                              {2, 5, 9, 5, 0, 5, 2, 6},
                                              {5, 5, 23, 17, 2, 6, 6, 4},
                                              {2, 4, 4, 2, 1, 8, 5, 5},
                                              {1, 36, 36, 36, 2, 7, 4, 4},
                                              {4, 0, 3, 1, 1, 7, 7, 7},
                                              {0, 0, 1, 3, 2, 4, 3, 1}},
                                             &Id::makeFromInt);

  // Test TextLimit with limit 2.
  TextLimit textLimit2 =
      makeTextLimit(inputTable.clone(), 2, 0, {1, 2, 3}, {5, 6, 7});
  /*
  Expected result:
  textRecord | entity1 | entity2 | entity3 | word | score1 | score2 | score2
  -------------------------------------------------------------------------
  0          | 0       | 1       | 3       | 2    | 4      | 3      | 1
  4          | 0       | 3       | 1       | 1    | 7      | 7      | 7
  19         | 1       | 1       | 1       | 4    | 22     | 2      | 1
  7          | 1       | 1       | 1       | 1    | 2      | 21     | 2
  5          | 1       | 1       | 2       | 1    | 1      | 1      | 2
  2          | 4       | 4       | 2       | 1    | 8      | 5      | 5
  3          | 5       | 3       | 8       | 4    | 4      | 3      | 2
  2          | 5       | 9       | 5       | 0    | 5      | 2      | 6
  5          | 5       | 23      | 17      | 2    | 6      | 6      | 4
  0          | 6       | 7       | 6       | 3    | 5      | 1      | 3
  1          | 36      | 36      | 36      | 2    | 7      | 4      | 4
  */
  VectorTable expected = {{0, 0, 1, 3, 2, 4, 3, 1},   {4, 0, 3, 1, 1, 7, 7, 7},
                          {19, 1, 1, 1, 4, 22, 2, 1}, {7, 1, 1, 1, 1, 2, 21, 2},
                          {5, 1, 1, 2, 1, 1, 1, 2},   {2, 4, 4, 2, 1, 8, 5, 5},
                          {3, 5, 3, 8, 4, 4, 3, 2},   {2, 5, 9, 5, 0, 5, 2, 6},
                          {5, 5, 23, 17, 2, 6, 6, 4}, {0, 6, 7, 6, 3, 5, 1, 3},
                          {1, 36, 36, 36, 2, 7, 4, 4}};
  IdTable resultIdTable = textLimit2.getResult()->idTable().clone();
  IdTable expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  compareIdTableWithExpectedContent(resultIdTable, expectedTable);

  // Test two entity columns but three score columns. That is possible if there
  // is a fixed entity statement.
  TextLimit textLimitFixedEntity =
      makeTextLimit(inputTable.clone(), 1, 0, {1, 2}, {5, 6, 7});
  /*
  Expected result:
  textRecord | entity1 | entity2 | entity3 | word | score1 | score2 | score2
  -------------------------------------------------------------------------
  4          | 0       | 3       | 1       | 1    | 7      | 7      | 7
  0          | 0       | 1       | 3       | 2    | 4      | 3      | 1
  19         | 1       | 1       | 1       | 4    | 22     | 2      | 1
  2          | 4       | 4       | 2       | 1    | 8      | 5      | 5
  3          | 5       | 3       | 8       | 4    | 4      | 3      | 2
  2          | 5       | 9       | 5       | 0    | 5      | 2      | 6
  5          | 5       | 23      | 17      | 2    | 6      | 6      | 4
  0          | 6       | 7       | 6       | 3    | 5      | 1      | 3
  1          | 36      | 36      | 36      | 2    | 7      | 4      | 4
  */
  expected = {{4, 0, 3, 1, 1, 7, 7, 7},   {0, 0, 1, 3, 2, 4, 3, 1},
              {19, 1, 1, 1, 4, 22, 2, 1}, {2, 4, 4, 2, 1, 8, 5, 5},
              {3, 5, 3, 8, 4, 4, 3, 2},   {2, 5, 9, 5, 0, 5, 2, 6},
              {5, 5, 23, 17, 2, 6, 6, 4}, {0, 6, 7, 6, 3, 5, 1, 3},
              {1, 36, 36, 36, 2, 7, 4, 4}};
  resultIdTable = textLimitFixedEntity.getResult()->idTable().clone();
  expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  compareIdTableWithExpectedContent(resultIdTable, expectedTable);
}

// _____________________________________________________________________________
TEST(TextLimit, PositioningTest) {
  /*
  The first indices written as a table:
  textRecord1 | entity1 | word1 | score1 | random11 | random12
  -----------------------------------------------------
  7           | 1       | 1     | 2      | 1        | 5
  0           | 6       | 3     | 3      | 4        | 4
  5           | 1       | 1     | 2      | 3        | 27
  5           | 1       | 0     | 2      | 0        | 27
  19          | 1       | 4     | 1      | 7        | 9
  3           | 5       | 4     | 2      | 4        | 4
  2           | 5       | 0     | 6      | 7        | 5
  5           | 5       | 2     | 4      | 9        | 7
  2           | 4       | 1     | 5      | 6        | 19
  1           | 36      | 2     | 4      | 5        | 3
  4           | 0       | 1     | 7      | 8        | 6
  0           | 0       | 2     | 1      | 2        | 19
  */

  VectorTable input1 = {
      {7, 1, 1, 2, 1, 5},  {0, 6, 3, 3, 4, 4},  {5, 1, 1, 2, 3, 27},
      {5, 1, 0, 2, 0, 27}, {19, 1, 4, 1, 7, 9}, {3, 5, 4, 2, 4, 4},
      {2, 5, 0, 6, 7, 5},  {5, 5, 2, 4, 9, 7},  {2, 4, 1, 5, 6, 19},
      {1, 36, 2, 4, 5, 3}, {4, 0, 1, 7, 8, 6},  {0, 0, 2, 1, 2, 19}};
  IdTable inputTable1 = makeIdTableFromVector(input1, &Id::makeFromInt);

  /*
  The second indices written as a table:
  textRecord2 | entity2 | word2 | score2 | random21 | random22
  -----------------------------------------------------
  7           | 5       | 1     | 2      | 56       | 3
  0           | 2       | 8     | 12     | 0        | 3
  5           | 4       | 1     | 2      | 2        | 27
  5           | 2       | 3     | 1      | 0        | 7
  19          | 2       | 4     | 15     | 7        | 9
  3           | 5       | 4     | 2      | 8        | 3
  2           | 5       | 5     | 3      | 7        | 5
  */

  VectorTable input2 = {{7, 5, 1, 2, 56, 3},  {0, 2, 8, 12, 0, 3},
                        {5, 4, 1, 2, 2, 27},  {5, 2, 3, 1, 0, 7},
                        {19, 2, 4, 15, 7, 9}, {3, 5, 4, 2, 8, 3},
                        {2, 5, 5, 3, 7, 5}};
  IdTable inputTable2 = makeIdTableFromVector(input2, &Id::makeFromInt);

  // Test all 4 possible orders that the two textLimit operations can be applied
  // in. Test with limit 2.
  auto newColumnIndex = [](CartesianProductJoin c, size_t oldIndex) {
    return c.getExternallyVisibleVariableColumns()
        .at(Variable{"?" + std::to_string(oldIndex)})
        .columnIndex_;
  };

  // First Order: Apply textLimit1 and textLimit2 before applying the cartesian
  // join.
  TextLimit textLimit11 = makeTextLimit(inputTable1.clone(), 2, 0, {1}, {3});
  TextLimit textLimit12 = makeTextLimit(inputTable2.clone(), 2, 0, {1}, {3});

  // Join the two results.
  IdTable resultIdTable1 = textLimit11.getResult()->idTable().clone();
  IdTable resultIdTable2 = textLimit12.getResult()->idTable().clone();

  IdTable finalIdTableOrder1 =
      makeJoin(resultIdTable1.clone(), resultIdTable2.clone())
          .getResult()
          ->idTable()
          .clone();

  // Second Order: Apply textLimit1 and textLimit2 after applying the cartesian
  // join.
  CartesianProductJoin c2 = makeJoin(inputTable1.clone(), inputTable2.clone());
  IdTable intermediateResult = c2.getResult()->idTable().clone();

  TextLimit textLimit21 =
      makeTextLimit(intermediateResult.clone(), 2, newColumnIndex(c2, 0),
                    {newColumnIndex(c2, 1)}, {newColumnIndex(c2, 3)});
  resultIdTable1 = textLimit21.getResult()->idTable().clone();

  TextLimit textLimit22 =
      makeTextLimit(resultIdTable1.clone(), 2, newColumnIndex(c2, 6),
                    {newColumnIndex(c2, 7)}, {newColumnIndex(c2, 9)});
  IdTable finalIdTableOrder2 = textLimit22.getResult()->idTable().clone();

  // Third Order: Apply textLimit2 before applying the cartesian join and
  // textLimit1 after.
  TextLimit textLimit32 = makeTextLimit(inputTable2.clone(), 2, 0, {1}, {3});
  resultIdTable2 = textLimit32.getResult()->idTable().clone();

  CartesianProductJoin c3 =
      makeJoin(inputTable1.clone(), resultIdTable2.clone());
  intermediateResult = c3.getResult()->idTable().clone();

  TextLimit textLimit31 =
      makeTextLimit(intermediateResult.clone(), 2, newColumnIndex(c3, 0),
                    {newColumnIndex(c3, 1)}, {newColumnIndex(c3, 3)});
  IdTable finalIdTableOrder3 = textLimit31.getResult()->idTable().clone();

  // Fourth Order: Apply textLimit2 after applying the cartesian join and
  // textLimit1 before.
  TextLimit textLimit41 = makeTextLimit(inputTable1.clone(), 2, 0, {1}, {3});
  resultIdTable1 = textLimit41.getResult()->idTable().clone();

  CartesianProductJoin c4 =
      makeJoin(resultIdTable1.clone(), inputTable2.clone());
  intermediateResult = c4.getResult()->idTable().clone();

  TextLimit textLimit42 =
      makeTextLimit(intermediateResult.clone(), 2, newColumnIndex(c4, 6),
                    {newColumnIndex(c4, 7)}, {newColumnIndex(c4, 9)});
  IdTable finalIdTableOrder4 = textLimit42.getResult()->idTable().clone();

  // Compare the results of all 4 orders.
  compareIdTableWithExpectedContent(finalIdTableOrder1, finalIdTableOrder2);
  compareIdTableWithExpectedContent(finalIdTableOrder1, finalIdTableOrder3);
  compareIdTableWithExpectedContent(finalIdTableOrder1, finalIdTableOrder4);
}

// _____________________________________________________________________________
TEST(TextLimit, BasicMemberFunctions) {
  VectorTable input{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}};
  IdTable inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  TextLimit textLimit = makeTextLimit(inputTable.clone(), 5, 0, {1}, {2});
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
      IdTable(3, ad_utility::testing::makeAllocator()), 5, 0, {1}, {2});
  ASSERT_TRUE(textLimit2.knownEmptyResult());
}

// _____________________________________________________________________________
TEST(TextLimit, CacheKey) {
  VectorTable input{{1, 2, 3}, {1, 2, 3}, {1, 2, 3}};
  IdTable inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  TextLimit textLimit1 = makeTextLimit(inputTable.clone(), 4, 0, {1}, {2});
  ASSERT_EQ(textLimit1.getDescriptor(), "TextLimit with limit: 4");

  TextLimit textLimit2 = makeTextLimit(inputTable.clone(), 4, 0, {1}, {2});
  // Every argument is the same.
  ASSERT_EQ(textLimit1.getCacheKey(), textLimit2.getCacheKey());

  TextLimit textLimit3 = makeTextLimit(inputTable.clone(), 5, 0, {1}, {2});
  // The limit is different.
  ASSERT_NE(textLimit1.getCacheKey(), textLimit3.getCacheKey());

  TextLimit textLimit4 = makeTextLimit(inputTable.clone(), 4, 1, {1}, {2});
  // The textRecordColumn is different.
  ASSERT_NE(textLimit1.getCacheKey(), textLimit4.getCacheKey());

  TextLimit textLimit5 = makeTextLimit(inputTable.clone(), 4, 0, {2}, {2});
  // The entityColumn is different.
  ASSERT_NE(textLimit1.getCacheKey(), textLimit5.getCacheKey());

  TextLimit textLimit6 = makeTextLimit(inputTable.clone(), 4, 0, {1}, {3});
  // The scoreColumn is different.
  ASSERT_NE(textLimit1.getCacheKey(), textLimit6.getCacheKey());

  input = {{1, 2, 3}, {1, 2, 3}, {1, 2, 4}};
  inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  TextLimit textLimit7 = makeTextLimit(inputTable.clone(), 4, 0, {1}, {2});
  // The input is different.
  ASSERT_NE(textLimit1.getCacheKey(), textLimit7.getCacheKey());
}

// _____________________________________________________________________________
TEST(TextLimit, clone) {
  VectorTable input{{1, 2, 3}, {1, 2, 3}, {1, 2, 3}};
  IdTable inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  TextLimit textLimit = makeTextLimit(inputTable.clone(), 4, 0, {1}, {2});

  auto clone = textLimit.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(static_cast<Operation&>(textLimit), IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), textLimit.getDescriptor());
}
