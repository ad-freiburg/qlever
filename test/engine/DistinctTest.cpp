//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/OperationTestHelpers.h"
#include "engine/Distinct.h"
#include "engine/NeutralElementOperation.h"
#include "engine/StripColumns.h"

using ad_utility::testing::makeAllocator;
using V = Variable;

namespace {
// Convert a generator to a vector for easier comparison in assertions
std::vector<IdTable> toVector(Result::LazyResult generator) {
  std::vector<IdTable> result;
  for (auto& [table, vocab] : generator) {
    // IMPORTANT: The `vocab` will go out of scope here, but the tests don't use
    // any vocabulary so this is fine.
    result.push_back(std::move(table));
  }
  return result;
}

// _____________________________________________________________________________
Distinct makeDistinct(const std::vector<ColumnIndex>& keepIndices) {
  auto* qec = ad_utility::testing::getQec();
  return {qec,
          ad_utility::makeExecutionTree<ValuesForTesting>(
              qec, std::vector<IdTable>{},
              std::vector<std::optional<Variable>>{Variable{"?x"}}),
          keepIndices};
}
}  // namespace

TEST(Distinct, CacheKey) {
  // The cache key has to change when the subtree changes or when the
  // `keepIndices` (the distinct variables) change.
  auto qec = ad_utility::testing::getQec();
  auto d = ad_utility::makeExecutionTree<Distinct>(
      ad_utility::testing::getQec(),
      ad_utility::makeExecutionTree<NeutralElementOperation>(qec),
      std::vector<ColumnIndex>{0, 1});
  Distinct d2(ad_utility::testing::getQec(),
              ad_utility::makeExecutionTree<NeutralElementOperation>(qec), {0});
  Distinct d3(ad_utility::testing::getQec(), d, {0});

  EXPECT_NE(d->getCacheKey(), d2.getCacheKey());
  EXPECT_NE(d->getCacheKey(), d3.getCacheKey());
  EXPECT_NE(d2.getCacheKey(), d3.getCacheKey());
}

// _____________________________________________________________________________
TEST(Distinct, distinct) {
  IdTable input{makeIdTableFromVector(
      {{1, 1, 3, 7}, {6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}})};

  Distinct distinct = makeDistinct({1, 2});
  IdTable result = distinct.outOfPlaceDistinctForTesting(input);

  IdTable expectedResult{
      makeIdTableFromVector({{1, 1, 3, 7}, {2, 2, 3, 5}, {3, 6, 5, 4}})};
  ASSERT_EQ(expectedResult, result);
}

// _____________________________________________________________________________
TEST(Distinct, testChunkEdgeCases) {
  Distinct distinct = makeDistinct({0});
  IdTable input{1, ad_utility::testing::makeAllocator()};
  IdTable::row_type row{1};

  {
    input.resize(1);
    row[0] = Id::makeFromInt(0);
    ql::ranges::fill(input, row);
    IdTable result = distinct.outOfPlaceDistinctForTesting(input);

    ASSERT_EQ(makeIdTableFromVector({{0}}, &Id::makeFromInt), result);
  }

  {
    input.resize(Distinct::CHUNK_SIZE + 1);
    row[0] = Id::makeFromInt(0);
    ql::ranges::fill(input, row);
    IdTable result = distinct.outOfPlaceDistinctForTesting(input);

    ASSERT_EQ(makeIdTableFromVector({{0}}, &Id::makeFromInt), result);
  }

  {
    input.resize(Distinct::CHUNK_SIZE + 1);
    row[0] = Id::makeFromInt(0);
    ql::ranges::fill(input, row);
    input.at(Distinct::CHUNK_SIZE, 0) = Id::makeFromInt(1);
    IdTable result = distinct.outOfPlaceDistinctForTesting(input);

    ASSERT_EQ(makeIdTableFromVector({{0}, {1}}, &Id::makeFromInt), result);
  }

  {
    input.resize(2 * Distinct::CHUNK_SIZE);
    row[0] = Id::makeFromInt(0);
    ql::ranges::fill(input, row);
    IdTable result = distinct.outOfPlaceDistinctForTesting(input);

    ASSERT_EQ(makeIdTableFromVector({{0}}, &Id::makeFromInt), result);
  }

  {
    input.resize(2 * Distinct::CHUNK_SIZE + 2);
    row[0] = Id::makeFromInt(0);
    ql::ranges::fill(input, row);
    input.at(2 * Distinct::CHUNK_SIZE + 1, 0) = Id::makeFromInt(1);
    IdTable result = distinct.outOfPlaceDistinctForTesting(input);

    ASSERT_EQ(makeIdTableFromVector({{0}, {1}}, &Id::makeFromInt), result);
  }
}

// _____________________________________________________________________________
TEST(Distinct, distinctWithEmptyInput) {
  IdTable input{1, makeAllocator()};
  Distinct distinct = makeDistinct({});
  IdTable result = distinct.outOfPlaceDistinctForTesting(input);
  ASSERT_EQ(input, result);
}

// _____________________________________________________________________________
TEST(Distinct, nonLazy) {
  IdTable input{makeIdTableFromVector(
      {{1, 1, 3, 7}, {6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}})};

  auto qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(input),
      std::vector<std::optional<Variable>>{
          {V{"?a"}, V{"?b"}, V{"?c"}, V{"?d"}}},
      false, std::vector<ColumnIndex>{1, 2}, LocalVocab{}, std::nullopt, true);

  Distinct distinct{qec, std::move(values), {1, 2}};

  {
    auto result =
        distinct.getResult(false, ComputationMode::FULLY_MATERIALIZED);
    ASSERT_TRUE(result->isFullyMaterialized());
    EXPECT_EQ(
        result->idTableView(),
        makeIdTableFromVector({{1, 1, 3, 7}, {2, 2, 3, 5}, {3, 6, 5, 4}}));
  }

  {
    auto result = distinct.getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
    ASSERT_TRUE(result->isFullyMaterialized());
    EXPECT_EQ(
        result->idTableView(),
        makeIdTableFromVector({{1, 1, 3, 7}, {2, 2, 3, 5}, {3, 6, 5, 4}}));
  }
}

// _____________________________________________________________________________
TEST(Distinct, nonLazyWithLazyInputs) {
  std::vector<IdTable> idTables{};
  idTables.push_back(makeIdTableFromVector({{1, 1, 3, 7}}));
  idTables.push_back(makeIdTableFromVector(
      {{6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}}));

  auto qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(idTables),
      std::vector<std::optional<Variable>>{
          {V{"?a"}, V{"?b"}, V{"?c"}, V{"?d"}}},
      false, std::vector<ColumnIndex>{1, 2});

  Distinct distinct{qec, std::move(values), {1, 2}};

  auto result = distinct.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(result->isFullyMaterialized());
  EXPECT_EQ(result->idTableView(),
            makeIdTableFromVector({{1, 1, 3, 7}, {2, 2, 3, 5}, {3, 6, 5, 4}}));
}

// _____________________________________________________________________________
TEST(Distinct, lazyWithLazyInputs) {
  std::vector<IdTable> idTables{};
  idTables.push_back(makeIdTableFromVector({{1, 1, 3, 7}}));
  idTables.push_back(makeIdTableFromVector(
      {{6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}}));
  idTables.push_back(makeIdTableFromVector({{2, 6, 5, 2}}));
  idTables.push_back(IdTable{4, ad_utility::makeUnlimitedAllocator<Id>()});
  idTables.push_back(makeIdTableFromVector(
      {{6, 7, 0, 6}, {2, 7, 1, 5}, {3, 7, 2, 4}, {1, 7, 3, 1}}));
  idTables.push_back(makeIdTableFromVector(
      {{6, 7, 4, 6}, {2, 7, 4, 5}, {3, 7, 4, 4}, {1, 7, 4, 1}}));

  auto qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(idTables),
      std::vector<std::optional<Variable>>{
          {V{"?a"}, V{"?b"}, V{"?c"}, V{"?d"}}},
      false, std::vector<ColumnIndex>{1, 2});

  Distinct distinct{qec, std::move(values), {1, 2}};

  auto result = distinct.getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
  ASSERT_FALSE(result->isFullyMaterialized());

  auto m = matchesIdTable;
  using ::testing::ElementsAre;
  EXPECT_THAT(
      toVector(result->idTables()),
      ElementsAre(
          m(makeIdTableFromVector({{1, 1, 3, 7}})),
          m(makeIdTableFromVector({{2, 2, 3, 5}, {3, 6, 5, 4}})),
          m(makeIdTableFromVector(
              {{6, 7, 0, 6}, {2, 7, 1, 5}, {3, 7, 2, 4}, {1, 7, 3, 1}})),
          m(makeIdTableFromVector({{6, 7, 4, 6}}))));
}

// _____________________________________________________________________________
TEST(Distinct, clone) {
  auto qec = ad_utility::testing::getQec();
  Distinct distinct{ad_utility::testing::getQec(),
                    ad_utility::makeExecutionTree<NeutralElementOperation>(qec),
                    std::vector<ColumnIndex>{0, 1}};

  auto clone = distinct.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(distinct, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), distinct.getDescriptor());
}

// _____________________________________________________________________________
TEST(Distinct, isDistinctBy) {
  using Vars = std::vector<std::optional<Variable>>;
  using SC = std::vector<ColumnIndex>;
  auto* qec = ad_utility::testing::getQec();

  // A `Distinct` on `{0, 1}` produces rows that are distinct wrt `{0, 1}` and
  // any superset thereof, but not wrt a set that misses one of these columns.
  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1, 7}, {0, 1, 8}, {2, 3, 9}}),
      Vars{Variable{"?x"}, Variable{"?y"}, Variable{"?z"}});
  auto distinct =
      ad_utility::makeExecutionTree<Distinct>(qec, values, SC{0, 1});
  const auto& op = *distinct->getRootOperation();

  EXPECT_TRUE(op.isDistinctBy(SC{0, 1}));
  EXPECT_TRUE(op.isDistinctBy(SC{0, 1, 2}));
  EXPECT_TRUE(op.isDistinctBy(SC{2, 1, 0}));
  EXPECT_FALSE(op.isDistinctBy(SC{0, 2}));
  EXPECT_FALSE(op.isDistinctBy(SC{0}));
}

// Test that `makeTreeWithStrippedColumns` strips the subtree to the requested
// variables plus the variables the `DISTINCT` compares on, translates
// `keepIndices_` accordingly, and adds a `StripColumns` for the compared
// variables that the parent did not request.
TEST(Distinct, makeTreeWithStrippedColumns) {
  using ::testing::ElementsAre;
  using ::testing::Key;
  using ::testing::SizeIs;
  using ::testing::UnorderedElementsAre;
  // A subtree with the variables `?a`, `?b`, `?c`, `?d`, sorted on `?b`.
  auto qec = ad_utility::testing::getQec();
  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec,
      makeIdTableFromVector(
          {{6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}}),
      std::vector<std::optional<V>>{V{"?a"}, V{"?b"}, V{"?c"}, V{"?d"}});
  auto strip = [&](std::vector<ColumnIndex> keepIndices,
                   std::set<V> requestedVariables) {
    Distinct distinct{qec, values, keepIndices};
    auto tree = distinct.makeTreeWithStrippedColumns(requestedVariables);
    AD_CONTRACT_CHECK(tree.has_value() && tree.value() != nullptr);
    return std::move(tree).value();
  };

  // DISTINCT on `?b`, only `?b` requested: the result has only `?b`, the
  // `Distinct` now compares on column 0 and computes the distinct values.
  {
    auto tree = strip({1}, {V{"?b"}});
    auto distinct =
        std::dynamic_pointer_cast<Distinct>(tree->getRootOperation());
    ASSERT_TRUE(distinct);
    EXPECT_THAT(tree->getVariableColumns(), UnorderedElementsAre(Key(V{"?b"})));
    EXPECT_EQ(tree->getVariableColumn(V{"?b"}), 0u);
    EXPECT_THAT(distinct->getDistinctColumns(), ElementsAre(0));
    EXPECT_EQ(tree->getResult(false)->idTableView(),
              makeIdTableFromVector({{1}, {2}, {6}}));
  }

  // DISTINCT on `?b` and `?d`, `?a`, `?b` and `?d` requested: `?c` is stripped
  // and the columns are renumbered.
  {
    auto tree = strip({1, 3}, {V{"?a"}, V{"?b"}, V{"?d"}});
    auto distinct =
        std::dynamic_pointer_cast<Distinct>(tree->getRootOperation());
    ASSERT_TRUE(distinct);
    EXPECT_EQ(tree->getVariableColumn(V{"?a"}), 0u);
    EXPECT_EQ(tree->getVariableColumn(V{"?b"}), 1u);
    EXPECT_EQ(tree->getVariableColumn(V{"?d"}), 2u);
    EXPECT_THAT(distinct->getDistinctColumns(), ElementsAre(1, 2));
  }

  // A requested variable that the subtree does not have is ignored.
  {
    auto tree = strip({1}, {V{"?a"}, V{"?b"}, V{"?d"}, V{"?notIncluded"}});
    EXPECT_THAT(tree->getVariableColumns(),
                UnorderedElementsAre(Key(V{"?a"}), Key(V{"?b"}), Key(V{"?d"})));
  }

  // DISTINCT on `?b`, only `?c` requested: the `Distinct` keeps `?b` and `?c`,
  // a `StripColumns` on top removes `?b`.
  {
    auto tree = strip({1}, {V{"?c"}});
    auto stripColumns =
        std::dynamic_pointer_cast<StripColumns>(tree->getRootOperation());
    ASSERT_TRUE(stripColumns);
    EXPECT_THAT(tree->getVariableColumns(), UnorderedElementsAre(Key(V{"?c"})));
    const auto* child = stripColumns->getChildren().at(0);
    auto distinct =
        std::dynamic_pointer_cast<Distinct>(child->getRootOperation());
    ASSERT_TRUE(distinct);
    EXPECT_THAT(child->getVariableColumns(),
                UnorderedElementsAre(Key(V{"?b"}), Key(V{"?c"})));
    EXPECT_THAT(distinct->getDistinctColumns(),
                ElementsAre(child->getVariableColumn(V{"?b"})));
    EXPECT_EQ(tree->getResult(false)->idTableView(),
              makeIdTableFromVector({{3}, {3}, {5}}));
  }

  // All variables requested: nothing is stripped and the indices stay.
  {
    auto tree = strip({0}, {V{"?a"}, V{"?b"}, V{"?c"}, V{"?d"}});
    auto distinct =
        std::dynamic_pointer_cast<Distinct>(tree->getRootOperation());
    ASSERT_TRUE(distinct);
    EXPECT_THAT(tree->getVariableColumns(), SizeIs(4));
    EXPECT_THAT(distinct->getDistinctColumns(), ElementsAre(0));
  }
}
