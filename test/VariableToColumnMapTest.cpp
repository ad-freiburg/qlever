//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./printers/VariablePrinters.h"
#include "./printers/VariableToColumnMapPrinters.h"
#include "engine/VariableToColumnMap.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

TEST(VariableToColumnMap, gapsInRightCols) {
  using V = Variable;
  VariableToColumnMap leftCols;
  leftCols[V{"?x"}] = makeAlwaysDefinedColumn(0);
  leftCols[V{"?y"}] = makeAlwaysDefinedColumn(1);

  VariableToColumnMap rightCols;
  rightCols[V{"?x"}] = makeAlwaysDefinedColumn(1);
  rightCols[V{"?a"}] = makePossiblyUndefinedColumn(2);
  rightCols[V{"?b"}] = makeAlwaysDefinedColumn(5);

  auto joinCols = makeVarToColMapForJoinOperation(leftCols, rightCols, {{0, 1}},
                                                  BinOpType::Join, 2);
  VariableToColumnMap expected;
  expected[V{"?x"}] = makeAlwaysDefinedColumn(0);
  expected[V{"?y"}] = makeAlwaysDefinedColumn(1);
  expected[V{"?a"}] = makePossiblyUndefinedColumn(3);
  expected[V{"?b"}] = makeAlwaysDefinedColumn(6);
  EXPECT_THAT(joinCols, ::testing::UnorderedElementsAreArray(expected));
}

TEST(VariableToColumnMap, gapsInLeftCols) {
  using V = Variable;
  VariableToColumnMap leftCols;
  leftCols[V{"?x"}] = makeAlwaysDefinedColumn(3);
  leftCols[V{"?y"}] = makeAlwaysDefinedColumn(0);

  VariableToColumnMap rightCols;
  rightCols[V{"?x"}] = makePossiblyUndefinedColumn(0);
  rightCols[V{"?a"}] = makeAlwaysDefinedColumn(1);

  auto joinCols = makeVarToColMapForJoinOperation(leftCols, rightCols, {{3, 0}},
                                                  BinOpType::Join, 4);
  VariableToColumnMap expected;
  expected[V{"?x"}] = makeAlwaysDefinedColumn(3);
  expected[V{"?y"}] = makeAlwaysDefinedColumn(0);
  expected[V{"?a"}] = makeAlwaysDefinedColumn(4);
  EXPECT_THAT(joinCols, ::testing::UnorderedElementsAreArray(expected));
}

TEST(VariableToColumnMap, undefinedJoinColumn) {
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
      leftCols, rightCols, {{0, 0}, {1, 2}, {2, 1}}, BinOpType::Join, 3);
  VariableToColumnMap expected;
  expected[V{"?x"}] = makeAlwaysDefinedColumn(0);
  expected[V{"?y"}] = makeAlwaysDefinedColumn(1);
  expected[V{"?z"}] = makePossiblyUndefinedColumn(2);
  EXPECT_THAT(joinCols, ::testing::UnorderedElementsAreArray(expected));
}

TEST(VariableToColumnMap, optionalJoin) {
  using V = Variable;
  VariableToColumnMap leftCols;
  leftCols[V{"?x"}] = makeAlwaysDefinedColumn(0);
  leftCols[V{"?y"}] = makePossiblyUndefinedColumn(1);

  VariableToColumnMap rightCols;
  rightCols[V{"?x"}] = makePossiblyUndefinedColumn(0);
  rightCols[V{"?y"}] = makeAlwaysDefinedColumn(2);
  rightCols[V{"?a"}] = makeAlwaysDefinedColumn(1);

  auto joinCols = makeVarToColMapForJoinOperation(
      leftCols, rightCols, {{0, 0}, {1, 2}}, BinOpType::OptionalJoin, 2);
  VariableToColumnMap expected;
  expected[V{"?x"}] = makeAlwaysDefinedColumn(0);
  expected[V{"?y"}] = makePossiblyUndefinedColumn(1);
  expected[V{"?a"}] = makePossiblyUndefinedColumn(2);
  EXPECT_THAT(joinCols, ::testing::UnorderedElementsAreArray(expected));
}
