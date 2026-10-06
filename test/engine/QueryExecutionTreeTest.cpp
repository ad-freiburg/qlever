// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/CartesianProductJoin.h"
#include "engine/Distinct.h"
#include "engine/IndexScan.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Sort.h"
#include "engine/StripColumns.h"
#include "engine/Union.h"

using namespace ad_utility::testing;

// _____________________________________________________________________________
TEST(QueryExecutionTree, getVariableColumn) {
  auto qec = getQec();
  auto x = Variable{"?x"};
  auto y = Variable{"?y"};
  auto qet = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{3}}),
      std::vector<std::optional<Variable>>{x});
  EXPECT_EQ(qet->getVariableColumn(x), 0u);
  EXPECT_THAT(qet->getVariableColumnOrNullopt(x), ::testing::Optional(0u));
  EXPECT_EQ(qet->getVariableColumnOrNullopt(y), std::nullopt);
  EXPECT_ANY_THROW(qet->getVariableColumn(y));
}

// _____________________________________________________________________________
TEST(QueryExecutionTree, sortedUnionSpecialCase) {
  using Var = Variable;
  using Vars = std::vector<std::optional<Variable>>;
  auto* qec = ad_utility::testing::getQec();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}), Vars{Var{"?a"}});

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0}}), Vars{Var{"?a"}});

  auto sortedTree = QueryExecutionTree::createSortedTree(
      ad_utility::makeExecutionTree<Union>(qec, leftT, rightT), {0});

  // Ensure no `Sort` is added on top
  EXPECT_TRUE(std::dynamic_pointer_cast<Union>(sortedTree->getRootOperation()));

  qec->getQueryTreeCache().clearAll();
  auto result = sortedTree->getResult(false);
  EXPECT_EQ(result->idTableView(), makeIdTableFromVector({{0}, {1}}));
}

// _____________________________________________________________________________
TEST(QueryExecutionTree, createSortedTreeAnyPermutation) {
  using Vars = std::vector<std::optional<Variable>>;
  Vars vars{std::nullopt, std::nullopt, std::nullopt};
  using SC = std::vector<ColumnIndex>;
  auto* qec = getQec();

  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1, 2}}), vars, false, SC{0, 1, 2});

  EXPECT_EQ(QueryExecutionTree::createSortedTreeAnyPermutation(values, {0, 1}),
            values);

  EXPECT_EQ(
      QueryExecutionTree::createSortedTreeAnyPermutation(values, {1, 0, 2}),
      values);

  {
    auto sortedTree =
        QueryExecutionTree::createSortedTreeAnyPermutation(values, {2});
    EXPECT_NE(sortedTree, values);
    auto castTree =
        std::dynamic_pointer_cast<Sort>(sortedTree->getRootOperation());
    ASSERT_TRUE(castTree);
    EXPECT_EQ(castTree->getResultSortedOn(), (SC{2}));
  }

  {
    auto valuesNotSorted = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{0, 1, 2}}), vars);
    auto sortedTree = QueryExecutionTree::createSortedTreeAnyPermutation(
        valuesNotSorted, {0, 1});
    EXPECT_NE(sortedTree, valuesNotSorted);
    auto castTree =
        std::dynamic_pointer_cast<Sort>(sortedTree->getRootOperation());
    ASSERT_TRUE(castTree);
    EXPECT_EQ(castTree->getResultSortedOn(), (SC{0, 1}));
  }
}

// _____________________________________________________________________________
TEST(QueryExecutionTree, createDistinctTreeReturnsInputWhenAlreadyDistinct) {
  using Vars = std::vector<std::optional<Variable>>;
  using SC = std::vector<ColumnIndex>;
  auto* qec = getQec();

  // When the root operation is already distinct wrt `distinctIndices`, the
  // `DISTINCT` is a no-op and `createDistinctTree` returns the tree unchanged.
  // A `LIMIT 1` makes any operation distinct wrt any columns.
  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0}, {1}}), Vars{Variable{"?x"}});
  values->applyLimitOffset(LimitOffsetClause{._limit = 1});

  EXPECT_EQ(QueryExecutionTree::createDistinctTree(values, SC{0}), values);
  EXPECT_EQ(QueryExecutionTree::createDistinctTree(values, SC{}), values);
}

// _____________________________________________________________________________
TEST(QueryExecutionTree, createDistinctTreeFallbackAddsDistinct) {
  using Vars = std::vector<std::optional<Variable>>;
  using SC = std::vector<ColumnIndex>;
  auto* qec = getQec();

  // A generic operation that is not known to be distinct (and cannot push the
  // `DISTINCT` down) simply gets a `Distinct` on top.
  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}, {0, 1}, {2, 3}}),
      Vars{Variable{"?x"}, Variable{"?y"}});

  auto tree = QueryExecutionTree::createDistinctTree(values, SC{0, 1});
  auto distinct = std::dynamic_pointer_cast<Distinct>(tree->getRootOperation());
  ASSERT_TRUE(distinct);
  EXPECT_EQ(distinct->getDistinctColumns(), (SC{0, 1}));
}

// _____________________________________________________________________________
TEST(QueryExecutionTree, createDistinctTreeEmptyIndicesUsesLimitOne) {
  using Vars = std::vector<std::optional<Variable>>;
  using SC = std::vector<ColumnIndex>;
  auto* qec = getQec();

  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0}, {1}, {2}}), Vars{Variable{"?x"}});

  // `DISTINCT` over zero columns keeps at most one row and is realized as a
  // `LIMIT 1`, not as a `Distinct`.
  auto tree = QueryExecutionTree::createDistinctTree(values, SC{});
  EXPECT_FALSE(std::dynamic_pointer_cast<Distinct>(tree->getRootOperation()));
  EXPECT_EQ(tree->getRootOperation()->getLimitOffset()._limit, 1u);

  // The input tree is cloned, not mutated.
  EXPECT_FALSE(values->getRootOperation()->getLimitOffset()._limit.has_value());

  EXPECT_EQ(tree->getResult(false)->idTableView(),
            makeIdTableFromVector({{0}}));
}

// _____________________________________________________________________________
TEST(QueryExecutionTree, createDistinctTreeKeepsHiddenVariablesHidden) {
  using Var = Variable;
  using Vars = std::vector<std::optional<Variable>>;
  using SC = std::vector<ColumnIndex>;
  auto* qec = getQec();
  Var a{"?a"};

  // Regression test for #3563: The `DISTINCT` is pushed into the
  // `CartesianProductJoin`, but the variables that were hidden by a subquery
  // must remain hidden.
  auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}, {0, 2}, {1, 3}}),
      Vars{a, Var{"?hidden"}});
  auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{5}, {6}}), Vars{Var{"?b"}});
  auto cartesian = ad_utility::makeExecutionTree<CartesianProductJoin>(
      qec, CartesianProductJoin::Children{left, right});
  cartesian->getRootOperation()->setSelectedVariablesForSubquery({a});

  auto tree = QueryExecutionTree::createDistinctTree(
      cartesian, SC{cartesian->getVariableColumn(a)});
  ASSERT_TRUE(std::dynamic_pointer_cast<CartesianProductJoin>(
      tree->getRootOperation()));
  EXPECT_THAT(tree->getVariableColumns(),
              ::testing::UnorderedElementsAre(::testing::Key(a)));

  // `?a` is deduplicated to `{0, 1}`, the hidden columns don't matter.
  auto result = tree->getResult(false);
  auto column = result->idTableView().getColumn(tree->getVariableColumn(a));
  auto expected = makeIdTableFromVector({{0}, {1}});
  EXPECT_THAT(std::vector<Id>(column.begin(), column.end()),
              ::testing::UnorderedElementsAre(expected(0, 0), expected(1, 0)));
}

// _____________________________________________________________________________
TEST(QueryExecutionTree, limitAndOffsetIsPropagatedWhenStrippingColumns) {
  using Vars = std::vector<std::optional<Variable>>;
  Vars vars{std::nullopt, std::nullopt, std::nullopt};
  using TC = TripleComponent;
  auto* qec = getQec();

  LimitOffsetClause limitOffset{2, 3};

  // `IndexScan` natively supports stripping columns.
  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{TC{Variable{"?s"}}, TC{Variable{"?p"}},
                         TC{Variable{"?o"}}});
  indexScan->applyLimitOffset(limitOffset);

  // `ValuesForTesting` doesn't support stripping columns natively.
  auto valuesForTesting = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1, 2}}),
      Vars{Variable{"?s"}, Variable{"?p"}, Variable{"?o"}});
  valuesForTesting->applyLimitOffset(limitOffset);

  auto strippedIndex = QueryExecutionTree::makeTreeWithStrippedColumns(
      indexScan, {Variable{"?s"}});
  EXPECT_EQ(strippedIndex->getRootOperation()->getLimitOffset(), limitOffset);
  EXPECT_TRUE(
      std::dynamic_pointer_cast<IndexScan>(strippedIndex->getRootOperation()));

  auto strippedValues = QueryExecutionTree::makeTreeWithStrippedColumns(
      valuesForTesting, {Variable{"?s"}});
  EXPECT_TRUE(
      strippedValues->getRootOperation()->getLimitOffset().isUnconstrained());
  ASSERT_TRUE(std::dynamic_pointer_cast<StripColumns>(
      strippedValues->getRootOperation()));
  EXPECT_EQ(strippedValues->getRootOperation()
                ->getChildren()
                .at(0)
                ->getRootOperation()
                ->getLimitOffset(),
            limitOffset);
}

// _____________________________________________________________________________
TEST(QueryExecutionTree, limitAndOffsetIsPropagatedWhenCreatingSortedTree) {
  using Var = Variable;
  using Vars = std::vector<std::optional<Variable>>;
  auto* qec = getQec();

  LimitOffsetClause limitOffset{2, 3};

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}), Vars{Var{"?a"}});
  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0}}), Vars{Var{"?a"}});

  // `Union` natively supports creating a sorted variant of itself, which
  // replaces the root operation and therefore has to restore the limit.
  auto unionTree = ad_utility::makeExecutionTree<Union>(qec, leftT, rightT);
  unionTree->applyLimitOffset(limitOffset);

  auto sortedTree = QueryExecutionTree::createSortedTree(unionTree, {0});
  ASSERT_TRUE(std::dynamic_pointer_cast<Union>(sortedTree->getRootOperation()));
  EXPECT_EQ(sortedTree->getRootOperation()->getLimitOffset(), limitOffset);

  // `ValuesForTesting` doesn't support this natively, so an additional `Sort`
  // is added on top and the limit stays where it is.
  auto valuesForTesting = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}, {0}}), Vars{Var{"?a"}});
  valuesForTesting->applyLimitOffset(limitOffset);

  auto sortedValues =
      QueryExecutionTree::createSortedTree(valuesForTesting, {0});
  ASSERT_TRUE(
      std::dynamic_pointer_cast<Sort>(sortedValues->getRootOperation()));
  EXPECT_TRUE(
      sortedValues->getRootOperation()->getLimitOffset().isUnconstrained());
  EXPECT_EQ(valuesForTesting->getRootOperation()->getLimitOffset(),
            limitOffset);
}

// _____________________________________________________________________________
TEST(QueryExecutionTree, createSortedTreeKeepsHiddenVariablesHidden) {
  using Var = Variable;
  using Vars = std::vector<std::optional<Variable>>;
  using SC = std::vector<ColumnIndex>;
  auto* qec = getQec();
  VariableToColumnMap expectedVariables{
      {Var{"?a"}, makeAlwaysDefinedColumn(0)}};

  // Regression test for #3569: The root operation re-sorts itself, but the
  // variables that were hidden by a subquery must remain hidden.
  auto makeValues = [qec](IdTable table) {
    return ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(table), Vars{Var{"?a"}, Var{"?b"}});
  };
  auto unionTree = ad_utility::makeExecutionTree<Union>(
      qec, makeValues(makeIdTableFromVector({{1, 2}})),
      makeValues(makeIdTableFromVector({{0, 3}})), SC{1});
  unionTree->getRootOperation()->setSelectedVariablesForSubquery({Var{"?a"}});

  auto sortedUnion = QueryExecutionTree::createSortedTree(unionTree, {0});
  ASSERT_TRUE(
      std::dynamic_pointer_cast<Union>(sortedUnion->getRootOperation()));
  EXPECT_EQ(sortedUnion->getVariableColumns(), expectedVariables);
  qec->getQueryTreeCache().clearAll();
  EXPECT_EQ(sortedUnion->getResult(false)->idTableView(),
            makeIdTableFromVector({{0, 3}, {1, 2}}));

  // The same holds for `Sort`.
  auto sortTree = ad_utility::makeExecutionTree<Sort>(
      qec, makeValues(makeIdTableFromVector({{1, 0}, {0, 1}})), SC{1});
  sortTree->getRootOperation()->setSelectedVariablesForSubquery({Var{"?a"}});

  auto sortedSort = QueryExecutionTree::createSortedTree(sortTree, {0});
  ASSERT_TRUE(std::dynamic_pointer_cast<Sort>(sortedSort->getRootOperation()));
  EXPECT_EQ(sortedSort->getVariableColumns(), expectedVariables);
}

// _____________________________________________________________________________
TEST(QueryExecutionTree, strippingColumnsIsNoOpWhenAllVariablesAreKept) {
  using Vars = std::vector<std::optional<Variable>>;
  Vars vars{std::nullopt, std::nullopt, std::nullopt};
  auto* qec = getQec();

  auto valuesForTesting = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      Vars{Variable{"?x1"}, Variable{"?x2"}});

  EXPECT_EQ(QueryExecutionTree::makeTreeWithStrippedColumns(
                valuesForTesting, {Variable{"?x1"}, Variable{"?x2"}}),
            valuesForTesting);

  EXPECT_EQ(QueryExecutionTree::makeTreeWithStrippedColumns(
                valuesForTesting,
                {Variable{"?x1"}, Variable{"?x2"}, Variable{"?x3"}}),
            valuesForTesting);

  auto valuesForTestingNoVars = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{}}), Vars{});

  EXPECT_EQ(QueryExecutionTree::makeTreeWithStrippedColumns(
                valuesForTestingNoVars, {}),
            valuesForTestingNoVars);

  EXPECT_EQ(QueryExecutionTree::makeTreeWithStrippedColumns(
                valuesForTestingNoVars, {Variable{"?x"}}),
            valuesForTestingNoVars);
}

// _____________________________________________________________________________
TEST(QueryExecutionTree,
     limitAndOffsetIsNotPropagatedRecursivelyWhenStrippingColumns) {
  using TC = TripleComponent;
  auto* qec = getQec();

  LimitOffsetClause limitOffset{2, 3};

  auto indexScan = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::Enum::PSO,
      SparqlTripleSimple{TC{Variable{"?s"}}, TC{Variable{"?p"}},
                         TC{Variable{"?o"}}});

  auto sort = ad_utility::makeExecutionTree<Sort>(qec, indexScan,
                                                  std::vector<ColumnIndex>{0});

  sort->applyLimitOffset(limitOffset);
  EXPECT_TRUE(
      indexScan->getRootOperation()->getLimitOffset().isUnconstrained());
  EXPECT_EQ(sort->getRootOperation()
                ->getChildren()
                .at(0)
                ->getRootOperation()
                ->getLimitOffset(),
            limitOffset);

  auto strippedValues =
      QueryExecutionTree::makeTreeWithStrippedColumns(sort, {Variable{"?s"}});
  EXPECT_TRUE(
      indexScan->getRootOperation()->getLimitOffset().isUnconstrained());

  // The test only makes sense if `Sort´ can handle stripping columns itself.
  EXPECT_TRUE(
      std::dynamic_pointer_cast<Sort>(strippedValues->getRootOperation()));
  EXPECT_EQ(strippedValues->getRootOperation()->getLimitOffset(), limitOffset);

  auto childOperation = strippedValues->getRootOperation()
                            ->getChildren()
                            .at(0)
                            ->getRootOperation();
  EXPECT_TRUE(std::dynamic_pointer_cast<IndexScan>(childOperation));
  EXPECT_EQ(childOperation->getLimitOffset(), limitOffset);
}

// _____________________________________________________________________________
TEST(QueryExecutionTree, constructorRequiresQecAndRootOperation) {
  auto* qec = getQec();
  auto operation = std::make_shared<ValuesForTesting>(
      qec, makeIdTableFromVector({{3}}),
      std::vector<std::optional<Variable>>{Variable{"?x"}});
  AD_EXPECT_THROW_WITH_MESSAGE(
      QueryExecutionTree(nullptr, operation),
      ::testing::HasSubstr("Assertion `qec_ != nullptr` failed."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      QueryExecutionTree(qec, nullptr),
      ::testing::HasSubstr("Assertion `rootOperation_ != nullptr` failed."));
}
