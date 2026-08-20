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

// _____________________________________________________________________________
TEST(Distinct, makeTreeWithStrippedColumns) {
  IdTable input{makeIdTableFromVector(
      {{6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}})};

  auto qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(input),
      std::vector<std::optional<Variable>>{
          {V{"?a"}, V{"?b"}, V{"?c"}, V{"?d"}}});

  // Test case 1: Distinct keeps the original column 1 (?b).
  // makeTreeWithStrippedColumns has no additional variables.
  // Therefore, only ?b should remain, now at column index 0.
  {
    Distinct distinct{qec, values, {1}};
    std::optional<std::shared_ptr<QueryExecutionTree>> resultTree =
        distinct.makeTreeWithStrippedColumns(std::set<Variable>{});
    ASSERT_TRUE(resultTree.has_value());
    ASSERT_TRUE((*resultTree) != nullptr);
    const VariableToColumnMap& v2cMap = (*resultTree)->getVariableColumns();

    // check whether resultTree contains one column with name ?b
    EXPECT_EQ(v2cMap.size(), 1);
    EXPECT_TRUE(v2cMap.contains(Variable{"?b"}));
    EXPECT_FALSE(v2cMap.contains(V{"?a"}));
    EXPECT_FALSE(v2cMap.contains(V{"?c"}));
    EXPECT_FALSE(v2cMap.contains(V{"?d"}));

    // After stripping, ?b should have index 0 instead of index 1 as before.
    ColumnIndex resultColumnIndex = v2cMap.at(Variable{"?b"}).columnIndex_;
    EXPECT_EQ(resultColumnIndex, 0);
  }

  // Test case 2: Distinct keeps the original columns 1, 3 (?b, ?d).
  // makeTreeWithStrippedColumns has the additional variables ?a and ?d.
  // Therefore, ?a, ?b and ?d should remain.
  {
    Distinct distinct{qec, values, {1, 3}};
    std::optional<std::shared_ptr<QueryExecutionTree>> resultTree =
        distinct.makeTreeWithStrippedColumns(
            std::set<Variable>{Variable{"?a"}, Variable{"?d"}});
    ASSERT_TRUE(resultTree.has_value());
    ASSERT_TRUE((*resultTree) != nullptr);
    const VariableToColumnMap& v2cMap = (*resultTree)->getVariableColumns();

    // check whether resultTree contains three columns with name ?a, ?b and ?d
    EXPECT_EQ(v2cMap.size(), 3);
    EXPECT_TRUE(v2cMap.contains(Variable{"?b"}));
    EXPECT_TRUE(v2cMap.contains(V{"?a"}));
    EXPECT_FALSE(v2cMap.contains(V{"?c"}));
    EXPECT_TRUE(v2cMap.contains(V{"?d"}));

    // Check the new indexes (?d should have index 2 instead of index 3 as
    // before the stripping)
    ColumnIndex resultColumnIndex = v2cMap.at(Variable{"?a"}).columnIndex_;
    EXPECT_EQ(resultColumnIndex, 0);
    resultColumnIndex = v2cMap.at(Variable{"?b"}).columnIndex_;
    EXPECT_EQ(resultColumnIndex, 1);
    resultColumnIndex = v2cMap.at(Variable{"?d"}).columnIndex_;
    EXPECT_EQ(resultColumnIndex, 2);
  }

  // Test case 3: Distinct keeps the original column 1 (?b).
  // makeTreeWithStrippedColumns has the additional variable ?a, ?d and the
  // variable "?notIncluded", which has to be ignored by the function.
  // Therefore, only ?a, ?b and ?d should remain.
  {
    Distinct distinct{qec, values, {1}};
    std::optional<std::shared_ptr<QueryExecutionTree>> resultTree =
        distinct.makeTreeWithStrippedColumns(std::set<Variable>{
            Variable{"?d"}, Variable{"?notIncluded"}, Variable{"?a"}});
    ASSERT_TRUE(resultTree.has_value());
    ASSERT_TRUE((*resultTree) != nullptr);
    const VariableToColumnMap& v2cMap = (*resultTree)->getVariableColumns();

    // check whether resultTree contains three columns with name ?a, ?b and ?d
    EXPECT_EQ(v2cMap.size(), 3);
    EXPECT_TRUE(v2cMap.contains(Variable{"?b"}));
    EXPECT_TRUE(v2cMap.contains(V{"?a"}));
    EXPECT_FALSE(v2cMap.contains(V{"?c"}));
    EXPECT_TRUE(v2cMap.contains(V{"?d"}));
    EXPECT_FALSE(v2cMap.contains(V{"?notIncluded"}));

    // Check the new indexes (?d should have index 2 instead of index 3 as
    // before the stripping)
    ColumnIndex resultColumnIndex = v2cMap.at(Variable{"?a"}).columnIndex_;
    EXPECT_EQ(resultColumnIndex, 0);
    resultColumnIndex = v2cMap.at(Variable{"?b"}).columnIndex_;
    EXPECT_EQ(resultColumnIndex, 1);
    resultColumnIndex = v2cMap.at(Variable{"?d"}).columnIndex_;
    EXPECT_EQ(resultColumnIndex, 2);
  }

  // Test case 4: Distinct keeps no original columns.
  // makeTreeWithStrippedColumns has the additional variable ?c.
  // Therefore, only ?c should remain with index 0.
  {
    Distinct distinct{qec, values, {}};
    std::optional<std::shared_ptr<QueryExecutionTree>> resultTree =
        distinct.makeTreeWithStrippedColumns(
            std::set<Variable>{Variable{"?c"}});
    ASSERT_TRUE(resultTree.has_value());
    ASSERT_TRUE((*resultTree) != nullptr);
    const VariableToColumnMap& v2cMap = (*resultTree)->getVariableColumns();

    // check whether resultTree contains three columns with name ?a, ?b and ?d
    EXPECT_EQ(v2cMap.size(), 1);
    EXPECT_FALSE(v2cMap.contains(Variable{"?b"}));
    EXPECT_FALSE(v2cMap.contains(V{"?a"}));
    EXPECT_TRUE(v2cMap.contains(V{"?c"}));
    EXPECT_FALSE(v2cMap.contains(V{"?d"}));

    // Check the new index
    ColumnIndex resultColumnIndex = v2cMap.at(Variable{"?c"}).columnIndex_;
    EXPECT_EQ(resultColumnIndex, 0);
  }

  // Test case 5: Distinct keeps the original column 0 (?a).
  // makeTreeWithStrippedColumns has the additional variable ?a, ?b and ?c.
  // Therefore, ?a, ?b and ?c should remain. This function tests the case when
  // the keepindices_ are already included in variables.
  {
    Distinct distinct{qec, values, {0}};
    std::optional<std::shared_ptr<QueryExecutionTree>> resultTree =
        distinct.makeTreeWithStrippedColumns(
            std::set<Variable>{Variable{"?c"}, Variable{"?b"}, Variable{"?a"}});
    ASSERT_TRUE(resultTree.has_value());
    ASSERT_TRUE((*resultTree) != nullptr);
    const VariableToColumnMap& v2cMap = (*resultTree)->getVariableColumns();

    // check whether resultTree contains three columns with name ?a, ?b and ?d
    EXPECT_EQ(v2cMap.size(), 3);
    EXPECT_TRUE(v2cMap.contains(Variable{"?b"}));
    EXPECT_TRUE(v2cMap.contains(V{"?a"}));
    EXPECT_TRUE(v2cMap.contains(V{"?c"}));
    EXPECT_FALSE(v2cMap.contains(V{"?d"}));

    // Check the new index
    ColumnIndex resultColumnIndex = v2cMap.at(Variable{"?a"}).columnIndex_;
    EXPECT_EQ(resultColumnIndex, 0);
    resultColumnIndex = v2cMap.at(Variable{"?b"}).columnIndex_;
    EXPECT_EQ(resultColumnIndex, 1);
    resultColumnIndex = v2cMap.at(Variable{"?c"}).columnIndex_;
    EXPECT_EQ(resultColumnIndex, 2);
  }

  // Test case 6: Distinct keeps no original columns and
  // makeTreeWithStrippedColumns has no additional variables. Therefore no
  // columns should remain. (never the case)
  {
    Distinct distinct{qec, values, {}};
    std::optional<std::shared_ptr<QueryExecutionTree>> resultTree =
        distinct.makeTreeWithStrippedColumns(std::set<Variable>{});
    ASSERT_TRUE(resultTree.has_value());
    ASSERT_TRUE((*resultTree) != nullptr);
    const VariableToColumnMap& v2cMap = (*resultTree)->getVariableColumns();

    // check whether resultTree contains three columns with name ?a, ?b and ?d
    EXPECT_EQ(v2cMap.size(), 0);
  }

  // Test case 7: Check whether the new Distinct Operation has updated its
  // keepIndices_ (but keeps same number of keepIndices_).
  {
    // Check whether original keepIndices_ contains 1 as indicated in the
    // constructor
    Distinct distinct{qec, values, {1}};
    std::vector<ColumnIndex> originalKeepIndices =
        distinct.getDistinctColumns();
    EXPECT_EQ(originalKeepIndices.size(), 1);
    EXPECT_EQ(originalKeepIndices[0], 1);

    // Check whether the new Distinct operation has updated its keepIndices_
    // according to the variables of makeTreeWithStrippedColumns().
    std::optional<std::shared_ptr<QueryExecutionTree>> resultTree =
        distinct.makeTreeWithStrippedColumns(
            std::set<Variable>{Variable{"?d"}, Variable{"?notIncluded"}});
    ASSERT_TRUE(resultTree.has_value());
    ASSERT_TRUE((*resultTree) != nullptr);

    auto rootOperation = (*resultTree)->getRootOperation();
    Distinct* distinctOperation = dynamic_cast<Distinct*>(rootOperation.get());
    ASSERT_TRUE(distinctOperation != nullptr);
    std::vector<ColumnIndex> newKeepIndices =
        distinctOperation->getDistinctColumns();
    EXPECT_EQ(newKeepIndices.size(), 1);
    EXPECT_EQ(newKeepIndices[0], 0);
  }
}
