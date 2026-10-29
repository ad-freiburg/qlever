// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
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
// sort columns of the child, that the row number column is prepended to the
// columns of the child, and that the row numbers are unique.
TEST(RowNumber, sortedOnAndColumns) {
  // A row number `?n` on a child with `?a` and `?b`, sorted by `?b`.
  auto rowNumber = makeRowNumber(0);

  // The result is sorted by `?n` and then by `?b`.
  EXPECT_THAT(rowNumber.getResultSortedOn(), ElementsAre(0, 2));

  // The columns of `?a` and `?b` are shifted by one.
  EXPECT_EQ(rowNumber.getResultWidth(), 3);
  EXPECT_THAT(
      rowNumber.getExternallyVisibleVariableColumns(),
      UnorderedElementsAre(Pair(Variable{"?n"}, makeAlwaysDefinedColumn(0)),
                           Pair(Variable{"?a"}, makeAlwaysDefinedColumn(1)),
                           Pair(Variable{"?b"}, makeAlwaysDefinedColumn(2))));

  // Each row number occurs only once.
  EXPECT_EQ(rowNumber.getMultiplicity(0), 1.0f);
}

// Test that the cache key depends on the offset, but not on the sort columns
// (which don't change the result).
TEST(RowNumber, cacheKey) {
  // A different offset yields a different key.
  auto rowNumber = makeRowNumber(0);
  EXPECT_NE(rowNumber.getCacheKey(), makeRowNumber(1).getCacheKey());

  // A different order of the same result yields the same key.
  EXPECT_EQ(rowNumber.getCacheKey(),
            rowNumber.makeSortedTree({0, 1}).value()->getCacheKey());
}

// Test that row numbers up to the maximum integer value are allowed, and that
// larger row numbers throw.
TEST(RowNumber, maxRowNumber) {
  // The last of the three rows gets the maximum integer value.
  auto rowNumber = makeRowNumber(Id::maxInt - 2);
  auto result = rowNumber.computeResultOnlyForTesting(false);
  EXPECT_EQ(result.idTableView()(2, 0), Id::makeFromInt(Id::maxInt));

  // The last row would exceed the maximum integer value.
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeRowNumber(Id::maxInt - 1).computeResultOnlyForTesting(false),
      HasSubstr("exceed the maximum integer value"));
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

// Test that a negative offset, sort columns that do not start with the row
// number column, and a row number variable of the child are rejected.
TEST(RowNumber, invalidArguments) {
  // A child with a single column `?a`.
  auto qec = getQec();
  auto child = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}, IntId), Vars{Variable{"?a"}});

  // The offset must not be negative.
  EXPECT_ANY_THROW(RowNumber(qec, child, Variable{"?n"}, -1));

  // The sort columns must start with the row number column.
  EXPECT_ANY_THROW(RowNumber(qec, child, Variable{"?n"}, 0, {1}));

  // The row number variable must not be a variable of the child.
  EXPECT_ANY_THROW(RowNumber(qec, child, Variable{"?a"}));
}

// Test that cloning yields a deep copy.
TEST(RowNumber, clone) {
  auto rowNumber = makeRowNumber(3);
  auto clone = rowNumber.clone();
  ASSERT_TRUE(clone != nullptr);
  EXPECT_THAT(*clone, IsDeepCopy(rowNumber));
}

}  // namespace
