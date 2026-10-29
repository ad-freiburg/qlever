// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/OperationTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/QueryExecutionTree.h"
#include "engine/RowNumber.h"

namespace {
using Vars = std::vector<std::optional<Variable>>;
using namespace ad_utility::testing;
using namespace ::testing;

// Return a `RowNumber` with the given `offset` that binds `?n` on a child with
// the variables `?a` and `?b`, sorted by `?b`, which yields the two blocks
// `{{1, 2}, {3, 4}}` and `{{5, 6}}` of integers.
RowNumber makeRowNumber(int64_t offset) {
  auto qec = getQec();
  qec->clearCacheUnpinnedOnly();
  std::vector<IdTable> children;
  children.push_back(makeIdTableFromVector({{1, 2}, {3, 4}}, IntId));
  children.push_back(makeIdTableFromVector({{5, 6}}, IntId));
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(children), Vars{Variable{"?a"}, Variable{"?b"}}, false,
      std::vector<ColumnIndex>{1});
  return {qec, std::move(child), Variable{"?n"}, offset};
}

// Test that the row numbers are prepended as the first column, both for a
// materialized and a lazy result, and that they continue across blocks.
TEST(RowNumber, computeResult) {
  // The row numbers start at the `offset`.
  auto withOffset = makeRowNumber(1);
  auto materialized = withOffset.computeResultOnlyForTesting(false);
  ASSERT_TRUE(materialized.isFullyMaterialized());
  EXPECT_THAT(
      materialized.idTableView(),
      matchesIdTableFromVector({{1, 1, 2}, {2, 3, 4}, {3, 5, 6}}, IntId));

  // The row numbers of a lazy result continue in the second block.
  auto withoutOffset = makeRowNumber(0);
  auto lazy = withoutOffset.computeResultOnlyForTesting(true);
  ASSERT_FALSE(lazy.isFullyMaterialized());
  std::vector<IdTable> blocks;
  for (auto& [table, vocab] : lazy.idTables()) {
    blocks.push_back(std::move(table));
  }
  EXPECT_THAT(
      blocks,
      ElementsAre(matchesIdTableFromVector({{0, 1, 2}, {1, 3, 4}}, IntId),
                  matchesIdTableFromVector({{2, 5, 6}}, IntId)));
}

// Test that the result is sorted by the row number column and then by the
// sort columns of the child, and that the child's columns are shifted by one.
TEST(RowNumber, resultSortedOnAndVarToColMap) {
  auto rowNumber = makeRowNumber(0);
  EXPECT_THAT(rowNumber.getResultSortedOn(), ElementsAre(0, 2));
  EXPECT_THAT(
      rowNumber.getExternallyVisibleVariableColumns(),
      UnorderedElementsAre(Pair(Variable{"?n"}, makeAlwaysDefinedColumn(0)),
                           Pair(Variable{"?a"}, makeAlwaysDefinedColumn(1)),
                           Pair(Variable{"?b"}, makeAlwaysDefinedColumn(2))));
  EXPECT_EQ(rowNumber.getResultWidth(), 3);
  EXPECT_EQ(rowNumber.getMultiplicity(0), 1.0f);
}

// Test that any order that starts with the row number column is provided
// without sorting, and that other orders are not.
TEST(RowNumber, makeSortedTree) {
  auto rowNumber = makeRowNumber(0);

  // The row numbers are unique, so they can be followed by any other column.
  auto sorted = rowNumber.makeSortedTree({0, 1});
  ASSERT_TRUE(sorted.has_value());
  EXPECT_THAT(sorted.value()->getRootOperation()->getResultSortedOn(),
              ElementsAre(0, 1));
  EXPECT_THAT(
      sorted.value()->getResult()->idTableView(),
      matchesIdTableFromVector({{0, 1, 2}, {1, 3, 4}, {2, 5, 6}}, IntId));

  // An order that does not start with the row number column requires a `Sort`.
  EXPECT_FALSE(rowNumber.makeSortedTree({1, 0}).has_value());

  // Requesting an order that the result already has is a contract violation.
  EXPECT_ANY_THROW(rowNumber.makeSortedTree({0, 2}));
}

// Test that a negative offset and sort columns that do not start with the row
// number column are rejected.
TEST(RowNumber, invalidArguments) {
  auto qec = getQec();
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}, IntId), Vars{Variable{"?a"}});
  EXPECT_ANY_THROW(RowNumber(qec, child, Variable{"?n"}, -1));
  EXPECT_ANY_THROW(RowNumber(qec, child, Variable{"?n"}, 0, {1}));
}

// Test that cloning yields a deep copy.
TEST(RowNumber, clone) {
  auto rowNumber = makeRowNumber(3);
  auto clone = rowNumber.clone();
  ASSERT_TRUE(clone != nullptr);
  EXPECT_THAT(*clone, IsDeepCopy(rowNumber));
}

}  // namespace
