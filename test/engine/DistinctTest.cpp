//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/Distinct.h"
#include "engine/NeutralElementOperation.h"

using ad_utility::testing::makeAllocator;
using V = Variable;

namespace {
// Convert a generator to a vector for easier comparison in assertions
std::vector<IdTable> toVector(cppcoro::generator<IdTable> generator) {
  std::vector<IdTable> result;
  for (auto& table : generator) {
    result.push_back(std::move(table));
  }
  return result;
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

  std::vector<ColumnIndex> keepIndices{{1, 2}};
  IdTable result = CALL_FIXED_SIZE(4, Distinct::distinct, std::move(input),
                                   keepIndices, std::nullopt);

  // For easier checking.
  IdTable expectedResult{
      makeIdTableFromVector({{1, 1, 3, 7}, {2, 2, 3, 5}, {3, 6, 5, 4}})};
  ASSERT_EQ(expectedResult, result);
}

// _____________________________________________________________________________
TEST(Distinct, distinctWithEmptyInput) {
  IdTable input{1, makeAllocator()};
  IdTable result = CALL_FIXED_SIZE(1, Distinct::distinct, input.clone(),
                                   std::vector<ColumnIndex>{}, std::nullopt);
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
      ElementsAre(m(makeIdTableFromVector({{1, 1, 3, 7}})),
                  m(makeIdTableFromVector({{2, 2, 3, 5}, {3, 6, 5, 4}}))));
}
