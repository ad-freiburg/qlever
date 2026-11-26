//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <array>
#include <vector>

#include "./printers/VariablePrinters.h"
#include "./printers/VariableToColumnMapPrinters.h"
#include "engine/VariableToColumnMap.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/VariableToColumnMapTestHelpers.h"
#include "util/serializer/ByteBufferSerializer.h"

using JoinColumns = std::vector<std::array<ColumnIndex, 2>>;

class VariableToColumnMapTest : public ::testing::TestWithParam<bool> {};

// In the right input there are three columns (0, 3, 4) which are not
// represented by variables, but that will still be part of the result.
TEST_P(VariableToColumnMapTest, gapsInRightCols) {
  using V = Variable;
  bool keepJoinCols = GetParam();
  auto leftCols =
      makeVariableToColumnMap({{V{"?x"}, makeAlwaysDefinedColumn(0)},
                               {V{"?y"}, makeAlwaysDefinedColumn(1)}});

  auto rightCols =
      makeVariableToColumnMap({{V{"?x"}, makeAlwaysDefinedColumn(1)},
                               {V{"?a"}, makePossiblyUndefinedColumn(2)},
                               {V{"?b"}, makeAlwaysDefinedColumn(5)}});

  auto joinCols =
      makeVarToColMapForJoinOperation(leftCols, rightCols, JoinColumns{{0, 1}},
                                      BinOpType::Join, 2, keepJoinCols);
  auto expected = makeVariableToColumnMap();
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
  auto leftCols =
      makeVariableToColumnMap({{V{"?x"}, makeAlwaysDefinedColumn(2)},
                               {V{"?y"}, makeAlwaysDefinedColumn(0)}});

  auto rightCols =
      makeVariableToColumnMap({{V{"?x"}, makePossiblyUndefinedColumn(0)},
                               {V{"?a"}, makeAlwaysDefinedColumn(1)}});

  auto joinCols =
      makeVarToColMapForJoinOperation(leftCols, rightCols, JoinColumns{{2, 0}},
                                      BinOpType::Join, 4, keepJoinCols);
  auto expected = makeVariableToColumnMap();
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
  auto leftCols =
      makeVariableToColumnMap({{V{"?y"}, makeAlwaysDefinedColumn(0)},
                               {V{"?a"}, makeAlwaysDefinedColumn(1)},
                               {V{"?x"}, makeAlwaysDefinedColumn(2)},
                               {V{"?b"}, makeAlwaysDefinedColumn(3)}});

  auto rightCols =
      makeVariableToColumnMap({{V{"?x"}, makePossiblyUndefinedColumn(0)},
                               {V{"?y"}, makeAlwaysDefinedColumn(1)},
                               {V{"?c"}, makeAlwaysDefinedColumn(2)}});

  auto joinCols = makeVarToColMapForJoinOperation(
      leftCols, rightCols, JoinColumns{{2, 0}, {0, 1}}, BinOpType::Join, 4,
      keepJoinCols);
  auto expected = makeVariableToColumnMap();
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
  auto leftCols =
      makeVariableToColumnMap({{V{"?x"}, makeAlwaysDefinedColumn(0)},
                               {V{"?y"}, makePossiblyUndefinedColumn(1)},
                               {V{"?z"}, makePossiblyUndefinedColumn(2)}});

  auto rightCols =
      makeVariableToColumnMap({{V{"?x"}, makePossiblyUndefinedColumn(0)},
                               {V{"?y"}, makeAlwaysDefinedColumn(2)},
                               {V{"?z"}, makePossiblyUndefinedColumn(1)}});

  auto joinCols = makeVarToColMapForJoinOperation(
      leftCols, rightCols, JoinColumns{{0, 0}, {1, 2}, {2, 1}}, BinOpType::Join,
      3, keepJoinCols);
  auto expected = makeVariableToColumnMap();
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
  auto leftCols =
      makeVariableToColumnMap({{V{"?x"}, makeAlwaysDefinedColumn(0)},
                               {V{"?y"}, makePossiblyUndefinedColumn(1)}});

  auto rightCols =
      makeVariableToColumnMap({{V{"?x"}, makePossiblyUndefinedColumn(0)},
                               {V{"?y"}, makeAlwaysDefinedColumn(2)},
                               {V{"?a"}, makeAlwaysDefinedColumn(1)}});

  auto joinCols = makeVarToColMapForJoinOperation(
      leftCols, rightCols, JoinColumns{{0, 0}, {1, 2}}, BinOpType::OptionalJoin,
      2, keepJoinCols);
  auto expected = makeVariableToColumnMap();
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

// _____________________________________________________________________________
TEST(ColumnIndexAndTypeInfo, serialization) {
  ColumnIndexAndTypeInfo c1 = makeAlwaysDefinedColumn(42);
  ColumnIndexAndTypeInfo c2 = makePossiblyUndefinedColumn(46);
  using namespace ad_utility::serialization;
  ByteBufferWriteSerializer writer;
  writer | c1;
  writer | c2;

  ColumnIndexAndTypeInfo c3;
  ColumnIndexAndTypeInfo c4;
  ByteBufferReadSerializer reader{std::move(writer).data()};
  reader | c3;
  reader | c4;
  EXPECT_EQ(c1, c3);
  EXPECT_EQ(c2, c4);
}

// _____________________________________________________________________________
TEST(VariableToColumnMap, serializeDeterministically) {
  using V = Variable;
  using namespace ad_utility::serialization;
  auto map =
      makeVariableToColumnMap({{V{"?x"}, makeAlwaysDefinedColumn(0)},
                               {V{"?a"}, makePossiblyUndefinedColumn(1)},
                               {V{"?zebra"}, makeAlwaysDefinedColumn(2)}});

  ByteBufferWriteSerializer writer;
  serializeDeterministically(writer, map);

  // The map is read back with exactly the same contents.
  ByteBufferReadSerializer reader{writer.data()};
  auto readMap = makeVariableToColumnMap();
  // The previous contents of the map are overwritten, not added to.
  readMap[V{"?notInTheSerializedMap"}] = makeAlwaysDefinedColumn(17);
  serializeDeterministically(reader, readMap);
  EXPECT_THAT(readMap, ::testing::UnorderedElementsAreArray(map));
}

// _____________________________________________________________________________
TEST(VariableToColumnMap,
     serializeDeterministicallyIsIndependentOfInsertionOrder) {
  using V = Variable;
  using namespace ad_utility::serialization;
  // The same entries, but inserted in the opposite order, such that the
  // iteration order of the underlying hash map may differ.
  auto map1 =
      makeVariableToColumnMap({{V{"?x"}, makeAlwaysDefinedColumn(0)},
                               {V{"?a"}, makePossiblyUndefinedColumn(1)},
                               {V{"?zebra"}, makeAlwaysDefinedColumn(2)}});
  auto map2 =
      makeVariableToColumnMap({{V{"?zebra"}, makeAlwaysDefinedColumn(2)},
                               {V{"?a"}, makePossiblyUndefinedColumn(1)},
                               {V{"?x"}, makeAlwaysDefinedColumn(0)}});

  auto serialize = [](const VariableToColumnMap& map) {
    ByteBufferWriteSerializer writer;
    serializeDeterministically(writer, map);
    return std::move(writer).data();
  };
  EXPECT_EQ(serialize(map1), serialize(map2));
}
