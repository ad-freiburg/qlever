//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./printers/VariablePrinters.h"
#include "./printers/VariableToColumnMapPrinters.h"
#include "engine/VariableToColumnMap.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

class VariableToColumnMapTest : public ::testing::TestWithParam<bool> {};

// In the right input there are three columns (0, 3, 4) which are not
// represented by variables, but that will still be part of the result.
TEST_P(VariableToColumnMapTest, gapsInRightCols) {
  using V = Variable;
  bool keepJoinCols = GetParam();
  VariableToColumnMap leftCols;
  leftCols[V{"?x"}] = makeAlwaysDefinedColumn(0);
  leftCols[V{"?y"}] = makeAlwaysDefinedColumn(1);

  VariableToColumnMap rightCols;
  rightCols[V{"?x"}] = makeAlwaysDefinedColumn(1);
  rightCols[V{"?a"}] = makePossiblyUndefinedColumn(2);
  rightCols[V{"?b"}] = makeAlwaysDefinedColumn(5);

  auto joinCols = makeVarToColMapForJoinOperation(
      leftCols, rightCols, {{0, 1}}, BinOpType::Join, 2, keepJoinCols);
  VariableToColumnMap expected;
  if (keepJoinCols) {
    expected[V{"?x"}] = makeAlwaysDefinedColumn(0);
  }
  expected[V{"?y"}] = makeAlwaysDefinedColumn(keepJoinCols ? 1 : 0);
  expected[V{"?a"}] = makePossiblyUndefinedColumn(keepJoinCols ? 3 : 2);
  expected[V{"?b"}] = makeAlwaysDefinedColumn(keepJoinCols ? 6 : 5);
  EXPECT_THAT(joinCols, ::testing::UnorderedElementsAreArray(expected));
}

// In the left input there are three columns (1, 3) which are not represented by
// variables, but that will still be part of the result. The column `3` can only
// be inferred from the last argument to `makeVarToColMapForJoinOperation` which
// is the total number of columns in the left input.
TEST_P(VariableToColumnMapTest, gapsInLeftCols) {
  using V = Variable;
  bool keepJoinCols = GetParam();
  VariableToColumnMap leftCols;
  leftCols[V{"?x"}] = makeAlwaysDefinedColumn(2);
  leftCols[V{"?y"}] = makeAlwaysDefinedColumn(0);

  VariableToColumnMap rightCols;
  rightCols[V{"?x"}] = makePossiblyUndefinedColumn(0);
  rightCols[V{"?a"}] = makeAlwaysDefinedColumn(1);

  auto joinCols = makeVarToColMapForJoinOperation(
      leftCols, rightCols, {{2, 0}}, BinOpType::Join, 4, keepJoinCols);
  VariableToColumnMap expected;
  if (keepJoinCols) {
    expected[V{"?x"}] = makeAlwaysDefinedColumn(2);
  }
  expected[V{"?y"}] = makeAlwaysDefinedColumn(0);
  expected[V{"?a"}] = makeAlwaysDefinedColumn(keepJoinCols ? 4 : 3);
  EXPECT_THAT(joinCols, ::testing::UnorderedElementsAreArray(expected));
}

// A test where the join columns and non-join columns are interleaved in the
// left result to test some corner cases when the join columns are dropped.
TEST_P(VariableToColumnMapTest, mixedJoinAndNonJoinColumns) {
  using V = Variable;
  bool keepJoinCols = GetParam();
  VariableToColumnMap leftCols;
  leftCols[V{"?y"}] = makeAlwaysDefinedColumn(0);
  leftCols[V{"?a"}] = makeAlwaysDefinedColumn(1);
  leftCols[V{"?x"}] = makeAlwaysDefinedColumn(2);
  leftCols[V{"?b"}] = makeAlwaysDefinedColumn(3);

  VariableToColumnMap rightCols;
  rightCols[V{"?x"}] = makePossiblyUndefinedColumn(0);
  rightCols[V{"?y"}] = makeAlwaysDefinedColumn(1);
  rightCols[V{"?c"}] = makeAlwaysDefinedColumn(2);

  auto joinCols = makeVarToColMapForJoinOperation(
      leftCols, rightCols, {{2, 0}, {0, 1}}, BinOpType::Join, 4, keepJoinCols);
  VariableToColumnMap expected;
  if (keepJoinCols) {
    expected[V{"?y"}] = makeAlwaysDefinedColumn(0);
  }
  expected[V{"?a"}] = makeAlwaysDefinedColumn(keepJoinCols ? 1 : 0);
  if (keepJoinCols) {
    expected[V{"?x"}] = makeAlwaysDefinedColumn(2);
  }
  // Two join columns before `?b` so it has to be shifted by two when the join
  // columns are dropped.
  expected[V{"?b"}] = makeAlwaysDefinedColumn(keepJoinCols ? 3 : 1);

  // Same for `?c`.
  expected[V{"?c"}] = makeAlwaysDefinedColumn(keepJoinCols ? 4 : 2);
  EXPECT_THAT(joinCols, ::testing::UnorderedElementsAreArray(expected));
}

// Test the status of `mightBeUndefined` for join columns when the corresponding
// columns in one or both of the inputs might be undefined.
TEST_P(VariableToColumnMapTest, undefinedJoinColumn) {
  auto keepJoinCols = GetParam();
  using V = Variable;
  VariableToColumnMap leftCols;
  leftCols[V{"?x"}] = makeAlwaysDefinedColumn(0);
  leftCols[V{"?y"}] = makePossiblyUndefinedColumn(1);
  leftCols[V{"?z"}] = makePossiblyUndefinedColumn(2);

  VariableToColumnMap rightCols;
  rightCols[V{"?x"}] = makePossiblyUndefinedColumn(0);
  rightCols[V{"?y"}] = makeAlwaysDefinedColumn(2);
  rightCols[V{"?z"}] = makePossiblyUndefinedColumn(1);

  auto joinCols = makeVarToColMapForJoinOperation(
      leftCols, rightCols, {{0, 0}, {1, 2}, {2, 1}}, BinOpType::Join, 3,
      keepJoinCols);
  VariableToColumnMap expected;
  if (keepJoinCols) {
    expected[V{"?x"}] = makeAlwaysDefinedColumn(0);
    expected[V{"?y"}] = makeAlwaysDefinedColumn(1);
    expected[V{"?z"}] = makePossiblyUndefinedColumn(2);
  }
  EXPECT_THAT(joinCols, ::testing::UnorderedElementsAreArray(expected));
}

// Test all the combinations for possibly undefined and always defined columns
// that might occur with OPTIONAL joins.
TEST_P(VariableToColumnMapTest, optionalJoin) {
  bool keepJoinCols = GetParam();
  using V = Variable;
  VariableToColumnMap leftCols;
  leftCols[V{"?x"}] = makeAlwaysDefinedColumn(0);
  leftCols[V{"?y"}] = makePossiblyUndefinedColumn(1);

  VariableToColumnMap rightCols;
  rightCols[V{"?x"}] = makePossiblyUndefinedColumn(0);
  rightCols[V{"?y"}] = makeAlwaysDefinedColumn(2);
  rightCols[V{"?a"}] = makeAlwaysDefinedColumn(1);

  auto joinCols =
      makeVarToColMapForJoinOperation(leftCols, rightCols, {{0, 0}, {1, 2}},
                                      BinOpType::OptionalJoin, 2, keepJoinCols);
  VariableToColumnMap expected;
  if (keepJoinCols) {
    expected[V{"?x"}] = makeAlwaysDefinedColumn(0);
    expected[V{"?y"}] = makePossiblyUndefinedColumn(1);
  }
  expected[V{"?a"}] = makePossiblyUndefinedColumn(keepJoinCols ? 2 : 0);
  EXPECT_THAT(joinCols, ::testing::UnorderedElementsAreArray(expected));
}

// Instantiate the test suite with true and false
INSTANTIATE_TEST_SUITE_P(VariableToColumnMap,            // Instance name
                         VariableToColumnMapTest,        // Test suite name
                         ::testing::Values(true, false)  // Parameters
);
