//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/OperationTestHelpers.h"
#include "engine/Distinct.h"
#include "engine/NeutralElementOperation.h"

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
  IdTable result = distinct.outOfPlaceDistinct<4>(input);

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
    IdTable result = distinct.outOfPlaceDistinct<1>(input);

    ASSERT_EQ(makeIdTableFromVector({{0}}, &Id::makeFromInt), result);
  }

  {
    input.resize(Distinct::CHUNK_SIZE + 1);
    row[0] = Id::makeFromInt(0);
    ql::ranges::fill(input, row);
    IdTable result = distinct.outOfPlaceDistinct<1>(input);

    ASSERT_EQ(makeIdTableFromVector({{0}}, &Id::makeFromInt), result);
  }

  {
    input.resize(Distinct::CHUNK_SIZE + 1);
    row[0] = Id::makeFromInt(0);
    ql::ranges::fill(input, row);
    input.at(Distinct::CHUNK_SIZE, 0) = Id::makeFromInt(1);
    IdTable result = distinct.outOfPlaceDistinct<1>(input);

    ASSERT_EQ(makeIdTableFromVector({{0}, {1}}, &Id::makeFromInt), result);
  }

  {
    input.resize(2 * Distinct::CHUNK_SIZE);
    row[0] = Id::makeFromInt(0);
    ql::ranges::fill(input, row);
    IdTable result = distinct.outOfPlaceDistinct<1>(input);

    ASSERT_EQ(makeIdTableFromVector({{0}}, &Id::makeFromInt), result);
  }

  {
    input.resize(2 * Distinct::CHUNK_SIZE + 2);
    row[0] = Id::makeFromInt(0);
    ql::ranges::fill(input, row);
    input.at(2 * Distinct::CHUNK_SIZE + 1, 0) = Id::makeFromInt(1);
    IdTable result = distinct.outOfPlaceDistinct<1>(input);

    ASSERT_EQ(makeIdTableFromVector({{0}, {1}}, &Id::makeFromInt), result);
  }
}

// _____________________________________________________________________________
TEST(Distinct, distinctWithEmptyInput) {
  IdTable input{1, makeAllocator()};
  Distinct distinct = makeDistinct({});
  IdTable result = distinct.outOfPlaceDistinct<1>(input);
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
        result->idTable(),
        makeIdTableFromVector({{1, 1, 3, 7}, {2, 2, 3, 5}, {3, 6, 5, 4}}));
  }

  {
    auto result = distinct.getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
    ASSERT_TRUE(result->isFullyMaterialized());
    EXPECT_EQ(
        result->idTable(),
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
  EXPECT_EQ(result->idTable(),
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
      toVector(std::move(result->idTables())),
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
