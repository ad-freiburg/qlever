// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "index/GraphComputation.h"

namespace {
using ad_utility::testing::makeAllocator;
using ad_utility::testing::VocabId;

// Create an `IdTable` with the given number of columns and no rows.
IdTable emptyBlock(size_t numColumns) {
  return IdTable{numColumns, makeAllocator()};
}
}  // namespace

// _____________________________________________________________________________
TEST(GraphComputation, hasDuplicateTriplesEmptyBlock) {
  EXPECT_FALSE(hasDuplicateTriples(emptyBlock(3)));
  EXPECT_FALSE(hasDuplicateTriples(emptyBlock(4)));
  EXPECT_FALSE(hasDuplicateTriples(emptyBlock(7)));
}

// _____________________________________________________________________________
TEST(GraphComputation, hasDuplicateTriplesSingleRow) {
  EXPECT_FALSE(hasDuplicateTriples(makeIdTableFromVector({{1, 2, 3, 4}})));
}

// _____________________________________________________________________________
TEST(GraphComputation, hasDuplicateTriplesNoDuplicates) {
  EXPECT_FALSE(hasDuplicateTriples(
      makeIdTableFromVector({{1, 2, 3, 0}, {1, 2, 4, 0}, {1, 3, 3, 1}})));
}

// _____________________________________________________________________________
TEST(GraphComputation, hasDuplicateTriplesAtFirstPosition) {
  EXPECT_TRUE(hasDuplicateTriples(
      makeIdTableFromVector({{1, 2, 3, 0}, {1, 2, 3, 1}, {1, 2, 4, 0}})));
}

// _____________________________________________________________________________
TEST(GraphComputation, hasDuplicateTriplesAtLastPosition) {
  EXPECT_TRUE(hasDuplicateTriples(
      makeIdTableFromVector({{1, 2, 3, 0}, {1, 2, 4, 0}, {1, 2, 4, 1}})));
}

// _____________________________________________________________________________
TEST(GraphComputation, hasDuplicateTriplesDifferInThirdColumn) {
  // The first two columns agree, but the third column differs, so these are
  // not duplicates.
  EXPECT_FALSE(hasDuplicateTriples(
      makeIdTableFromVector({{1, 2, 3, 0}, {1, 2, 4, 0}, {1, 2, 5, 0}})));
  // The same, but with an additional matching pair in the first two columns
  // only.
  EXPECT_FALSE(hasDuplicateTriples(
      makeIdTableFromVector({{1, 2, 3, 0}, {1, 2, 4, 0}, {2, 2, 4, 0}})));
}

// _____________________________________________________________________________
TEST(GraphComputation, hasDuplicateTriplesIgnoresGraphColumn) {
  // The rows only differ in the graph column, which must be ignored, so these
  // count as duplicates.
  EXPECT_TRUE(
      hasDuplicateTriples(makeIdTableFromVector({{1, 2, 3, 0}, {1, 2, 3, 1}})));
}

// _____________________________________________________________________________
TEST(GraphComputation, hasDuplicateTriplesIgnoresAdditionalPayload) {
  // Columns beyond the graph column are additional payload and must also be
  // ignored.
  EXPECT_TRUE(hasDuplicateTriples(makeIdTableFromVector(
      {{1, 2, 3, 0, 17, 18}, {1, 2, 3, 0, 19, 20}, {1, 2, 4, 0, 21, 22}})));
  EXPECT_FALSE(hasDuplicateTriples(makeIdTableFromVector(
      {{1, 2, 3, 0, 17, 18}, {1, 2, 4, 0, 17, 18}, {1, 2, 5, 0, 17, 18}})));
}

// _____________________________________________________________________________
TEST(GraphComputation, hasDuplicateTriplesLargeBlockWithoutDuplicates) {
  VectorTable table;
  for (int64_t i = 0; i < 1000; ++i) {
    table.push_back({1, 2, i, i % 3});
  }
  EXPECT_FALSE(hasDuplicateTriples(makeIdTableFromVector(table)));
  // Duplicate the last row, then there is a duplicate at the very end.
  table.push_back({1, 2, 999, 2});
  EXPECT_TRUE(hasDuplicateTriples(makeIdTableFromVector(table)));
}

// _____________________________________________________________________________
TEST(GraphComputation, getGraphInfoOnlyOneGraph) {
  // If there is only one graph, then the duplicate check is not performed at
  // all, because duplicates can only occur across different graphs.
  auto [hasDuplicates, graphs] = getGraphInfo(
      makeIdTableFromVector({{1, 2, 3, 0}, {1, 2, 3, 0}, {1, 2, 4, 0}}));
  EXPECT_FALSE(hasDuplicates);
  ASSERT_TRUE(graphs.has_value());
  EXPECT_THAT(graphs.value(), ::testing::ElementsAre(VocabId(0)));
}

// _____________________________________________________________________________
TEST(GraphComputation, getGraphInfoMultipleGraphs) {
  {
    auto [hasDuplicates, graphs] = getGraphInfo(
        makeIdTableFromVector({{1, 2, 3, 0}, {1, 2, 3, 1}, {1, 2, 4, 0}}));
    EXPECT_TRUE(hasDuplicates);
    ASSERT_TRUE(graphs.has_value());
    EXPECT_THAT(graphs.value(), ::testing::ElementsAre(VocabId(0), VocabId(1)));
  }
  {
    auto [hasDuplicates, graphs] = getGraphInfo(
        makeIdTableFromVector({{1, 2, 3, 0}, {1, 2, 4, 1}, {1, 2, 5, 0}}));
    EXPECT_FALSE(hasDuplicates);
    ASSERT_TRUE(graphs.has_value());
    EXPECT_THAT(graphs.value(), ::testing::ElementsAre(VocabId(0), VocabId(1)));
  }
}

// _____________________________________________________________________________
TEST(GraphComputation, getGraphInfoEmptyBlock) {
  auto [hasDuplicates, graphs] = getGraphInfo(emptyBlock(4));
  EXPECT_FALSE(hasDuplicates);
  ASSERT_TRUE(graphs.has_value());
  EXPECT_THAT(graphs.value(), ::testing::IsEmpty());
}
