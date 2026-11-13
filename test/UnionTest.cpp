// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "./engine/ValuesForTesting.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/SortedUnionImpl.h"
#include "engine/operation/IndexScan.h"
#include "engine/operation/NeutralElementOperation.h"
#include "engine/operation/OptionalJoin.h"
#include "engine/operation/Sort.h"
#include "engine/operation/Union.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "global/Id.h"
#include "util/IndexTestHelpers.h"
#include "util/LocalVocabEntryTestHelpers.h"
#include "util/OperationTestHelpers.h"

using ad_utility::testing::colIndices;
using ad_utility::testing::localVocabEntryFromStringRepresentation;

namespace {
auto V = ad_utility::testing::VocabId;
using Vars = std::vector<std::optional<Variable>>;

// Create a `ValuesForTesting` with the given `table` and `vars`, which is
// sorted by all its columns.
std::shared_ptr<QueryExecutionTree> makeSortedValues(QueryExecutionContext* qec,
                                                     IdTable table, Vars vars) {
  std::vector<ColumnIndex> sortedOn(vars.size());
  std::iota(sortedOn.begin(), sortedOn.end(), 0);
  return ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(table), std::move(vars), false, std::move(sortedOn));
}

// Check that the `UNION` of `left` and `right` sorted by `targetOrder` yields
// `expected`, computed fully materialized and lazily.
void testSortedUnion(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> left,
    std::shared_ptr<QueryExecutionTree> right,
    std::vector<ColumnIndex> targetOrder, const IdTable& expected,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  Union unionOperation{
      qec, std::move(left), std::move(right),
      qlm::vector<ColumnIndex>{targetOrder.begin(), targetOrder.end(),
                               qec->getAllocator()}};
  for (auto mode : {ComputationMode::FULLY_MATERIALIZED,
                    ComputationMode::LAZY_IF_SUPPORTED}) {
    SCOPED_TRACE(mode == ComputationMode::FULLY_MATERIALIZED
                     ? "fully materialized"
                     : "lazy if supported");
    qec->getQueryTreeCache().clearAll();
    auto result = unionOperation.getResult(true, mode);
    auto actual =
        result->isFullyMaterialized()
            ? result->idTableView().clone()
            : aggregateTables(result->idTables(), expected.numColumns()).first;
    EXPECT_EQ(actual, expected);
  }
}

// Create a `UNION` sorted by `targetOrder` of a left child with the columns
// `?a ?b` and a right child with the columns `?a ?c ?d`, where `?c` can be
// UNDEF and all other columns are always defined. The result columns are
// `?a ?b ?c ?d`.
Union makeUnionForDecidingColumns(QueryExecutionContext* qec,
                                  std::vector<ColumnIndex> targetOrder) {
  using Var = Variable;
  auto right = makeIdTableFromVector({{1, 0, 3}});
  right(0, 1) = Id::makeUndefined();
  return Union{qec,
               makeSortedValues(qec, makeIdTableFromVector({{1, 2}}),
                                Vars{Var{"?a"}, Var{"?b"}}),
               makeSortedValues(qec, std::move(right),
                                Vars{Var{"?a"}, Var{"?c"}, Var{"?d"}}),
               qlm::vector<ColumnIndex>{targetOrder.begin(), targetOrder.end(),
                                        qec->getAllocator()}};
}

using SortedUnionRange = std::vector<Result::IdTableVocabPair>;

// Create a `SortedUnionRange` that consists of a single block with the given
// `table`.
SortedUnionRange makeSortedUnionRange(IdTable table) {
  SortedUnionRange range;
  range.emplace_back(std::move(table), LocalVocab{});
  return range;
}

// Create a `SortedUnionImpl` that merges the single-column inputs `left` and
// `right`, which both have to be sorted.
auto makeSortedUnion(SortedUnionRange left, SortedUnionRange right,
                     bool requestLaziness,
                     const ad_utility::AllocatorWithLimit<Id>& allocator,
                     ad_utility::SharedCancellationHandle handle) {
  // `SortedUnionImpl` copies the column origins and the target order, so they
  // don't have to outlive it.
  std::vector<std::array<size_t, 2>> columnOrigins{{0, 0}};
  std::vector<ColumnIndex> targetOrder{0};
  return sortedUnion::SortedUnionImpl{
      sortedUnion::IterationData{nullptr, std::move(left), {0}},
      sortedUnion::IterationData{nullptr, std::move(right), {0}},
      requestLaziness,
      columnOrigins,
      allocator,
      ql::span<const ColumnIndex>{targetOrder},
      [](IdTable table, const std::vector<ColumnIndex>&) { return table; },
      std::move(handle)};
}
}  // namespace

// A simple test for computing a union.
TEST(Union, computeUnion) {
  auto* qec = ad_utility::testing::getQec();
  IdTable left = makeIdTableFromVector({{V(1)}, {V(2)}, {V(3)}});
  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, left.clone(), Vars{Variable{"?x"}});

  IdTable right = makeIdTableFromVector({{V(4), V(5)}, {V(6), V(7)}});
  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, right.clone(), Vars{Variable{"?u"}, Variable{"?x"}});

  Union u{qec, leftT, rightT};
  auto resultTable = u.computeResultOnlyForTesting();
  const auto& result = resultTable.idTableView();

  auto U = Id::makeUndefined();
  auto expected = makeIdTableFromVector(
      {{V(1), U}, {V(2), U}, {V(3), U}, {V(5), V(4)}, {V(7), V(6)}});
  ASSERT_EQ(result, expected);
}

// A test with large inputs to test the chunked writing that is caused by the
// timeout checks.
TEST(Union, computeUnionLarge) {
  auto* qec = ad_utility::testing::getQec();
  VectorTable leftInput, rightInput, expected;
  size_t numInputsL = 1'500'000u;
  size_t numInputsR = 5;
  size_t numInputs = numInputsL + numInputsR;
  auto U = Id::makeUndefined();
  leftInput.reserve(numInputsL);
  expected.reserve(numInputs);
  for (size_t i = 0; i < numInputsL; ++i) {
    leftInput.push_back(std::vector<IntOrId>{V(i)});
    expected.push_back(std::vector<IntOrId>{V(i), U});
  }
  for (size_t i = 0; i < numInputsR; ++i) {
    rightInput.push_back(std::vector<IntOrId>{V(i + 425)});
    expected.push_back(std::vector<IntOrId>{U, V(i + 425)});
  }
  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector(leftInput), Vars{Variable{"?x"}});

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector(rightInput), Vars{Variable{"?u"}});

  Union u{qec, leftT, rightT};
  auto resultTable = u.computeResultOnlyForTesting();
  const auto& result = resultTable.idTableView();

  ASSERT_EQ(result, makeIdTableFromVector(expected));
}

// _____________________________________________________________________________
TEST(Union, computeUnionLazy) {
  auto runTest = [](bool nonLazyChildren, bool invisibleSubtreeColumns,
                    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(loc);
    auto* qec = ad_utility::testing::getQec();
    qec->getQueryTreeCache().clearAll();
    auto leftT = [&]() {
      if (!invisibleSubtreeColumns) {
        IdTable left = makeIdTableFromVector({{V(1)}, {V(2)}, {V(3)}});
        return ad_utility::makeExecutionTree<ValuesForTesting>(
            qec, std::move(left), Vars{Variable{"?x"}}, false, colIndices({}),
            LocalVocab{}, std::nullopt, nonLazyChildren);
      } else {
        // With the `invisibleSubtreeColumns==true` case we test the case, that
        // the input contains variables that are not visible because of a
        // subquery. This case was previously buggy and triggered an assertion.
        IdTable left = makeIdTableFromVector(
            {{V(1), V(3)}, {V(2), V(27)}, {V(3), V(123)}});
        auto tree = ad_utility::makeExecutionTree<ValuesForTesting>(
            qec, std::move(left), Vars{Variable{"?x"}, Variable{"?invisible"}},
            false, colIndices({}), LocalVocab{}, std::nullopt, nonLazyChildren);
        tree->getRootOperation()->setSelectedVariablesForSubquery(
            {Variable{"?x"}});
        return tree;
      }
    }();

    IdTable right = makeIdTableFromVector({{V(4), V(5)}, {V(6), V(7)}});
    auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(right), Vars{Variable{"?u"}, Variable{"?x"}}, false,
        colIndices({}), LocalVocab{}, std::nullopt, nonLazyChildren);

    Union u{qec, std::move(leftT), std::move(rightT)};
    auto resultTable = u.computeResultOnlyForTesting(true);
    ASSERT_FALSE(resultTable.isFullyMaterialized());
    auto result = resultTable.idTables();

    auto U = Id::makeUndefined();
    auto expected1 = makeIdTableFromVector({{V(1), U}, {V(2), U}, {V(3), U}});
    auto expected2 = makeIdTableFromVector({{V(5), V(4)}, {V(7), V(6)}});

    auto iterator = result.begin();
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(iterator->idTable_, expected1);

    ++iterator;
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(iterator->idTable_, expected2);

    ASSERT_EQ(++iterator, result.end());
  };

  runTest(false, false);
  runTest(false, true);
  runTest(true, false);
  runTest(true, true);
}

// _____________________________________________________________________________
TEST(Union, ensurePermutationIsAppliedCorrectly) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 2, 3, 4, 5}}),
      Vars{Var{"?a"}, Var{"?b"}, Var{"?c"}, Var{"?d"}, Var{"?e"}});

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{6, 7, 8}}),
      Vars{Var{"?b"}, Var{"?a"}, Var{"?e"}});

  Union u{qec, std::move(leftT), std::move(rightT)};

  {
    qec->getQueryTreeCache().clearAll();
    auto resultTable = u.computeResultOnlyForTesting(true);
    ASSERT_FALSE(resultTable.isFullyMaterialized());
    auto result = resultTable.idTables();

    auto U = Id::makeUndefined();
    auto expected1 = makeIdTableFromVector({{1, 2, 3, 4, 5}});
    auto expected2 = makeIdTableFromVector({{V(7), V(6), U, U, V(8)}});

    auto iterator = result.begin();
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(iterator->idTable_, expected1);

    ++iterator;
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(iterator->idTable_, expected2);

    ASSERT_EQ(++iterator, result.end());
  }

  {
    qec->getQueryTreeCache().clearAll();
    auto resultTable = u.computeResultOnlyForTesting();
    ASSERT_TRUE(resultTable.isFullyMaterialized());

    auto U = Id::makeUndefined();
    auto expected =
        makeIdTableFromVector({{1, 2, 3, 4, 5}, {V(7), V(6), U, U, V(8)}});
    EXPECT_EQ(resultTable.idTableView(), expected);
  }
}

// _____________________________________________________________________________
TEST(Union, inputWithZeroColumns) {
  auto* qec = ad_utility::testing::getQec();
  auto leftT = ad_utility::makeExecutionTree<NeutralElementOperation>(qec);
  auto rightT = ad_utility::makeExecutionTree<NeutralElementOperation>(qec);

  Union u{qec, std::move(leftT), std::move(rightT)};

  {
    qec->getQueryTreeCache().clearAll();
    auto resultTable = u.computeResultOnlyForTesting(true);
    ASSERT_FALSE(resultTable.isFullyMaterialized());
    auto result = resultTable.idTables();

    auto expected1 = makeIdTableFromVector({{}});

    auto iterator = result.begin();
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(iterator->idTable_, expected1);

    ++iterator;
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(iterator->idTable_, expected1);

    ASSERT_EQ(++iterator, result.end());
  }

  {
    qec->getQueryTreeCache().clearAll();
    auto resultTable = u.computeResultOnlyForTesting();
    ASSERT_TRUE(resultTable.isFullyMaterialized());

    auto expected = makeIdTableFromVector({{}, {}});
    EXPECT_EQ(resultTable.idTableView(), expected);
  }
}

// _____________________________________________________________________________
TEST(Union, clone) {
  auto* qec = ad_utility::testing::getQec();

  Union unionOperation{
      qec, ad_utility::makeExecutionTree<NeutralElementOperation>(qec),
      ad_utility::makeExecutionTree<NeutralElementOperation>(qec)};

  auto clone = unionOperation.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(unionOperation, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), unionOperation.getDescriptor());
}

// _____________________________________________________________________________
TEST(Union, cheapMergeIfOrderNotImportant) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 2}}), Vars{Var{"?a"}, Var{"?b"}}, false,
      colIndices({0, 1}));

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 0}, {2, 4}}), Vars{Var{"?a"}, Var{"?b"}},
      false, colIndices({0, 1}));
  Union unionOperation{qec, std::move(leftT), std::move(rightT)};
  EXPECT_TRUE(unionOperation.resultSortedOn().empty());
  {
    qec->getQueryTreeCache().clearAll();
    auto result =
        unionOperation.getResult(true, ComputationMode::LAZY_IF_SUPPORTED);
    EXPECT_FALSE(result->isFullyMaterialized());
    auto idTables = result->idTables();
    auto expected1 = makeIdTableFromVector({{1, 2}});
    auto expected2 = makeIdTableFromVector({{0, 0}, {2, 4}});

    auto iterator = idTables.begin();
    ASSERT_NE(iterator, idTables.end());
    ASSERT_EQ(iterator->idTable_, expected1);

    ++iterator;
    ASSERT_NE(iterator, idTables.end());
    ASSERT_EQ(iterator->idTable_, expected2);

    ASSERT_EQ(++iterator, idTables.end());
  }
}

// _____________________________________________________________________________
TEST(Union, sortedMerge) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  auto U = Id::makeUndefined();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 2, 4}}),
      Vars{Var{"?a"}, Var{"?b"}, Var{"?c"}}, false, colIndices({0, 1, 2}));

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{4, 1}, {8, 2}}), Vars{Var{"?c"}, Var{"?a"}},
      false, colIndices({1, 0}));
  Union unionOperation{qec, std::move(leftT), std::move(rightT),
                       colIndices({0, 1, 2})};
  EXPECT_THAT(unionOperation.resultSortedOn(), ::testing::ElementsAre(0, 1, 2));
  {
    qec->getQueryTreeCache().clearAll();
    auto result =
        unionOperation.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected = makeIdTableFromVector({{1, U, 4}, {1, 2, 4}, {2, U, 8}});
    EXPECT_EQ(result->idTableView(), expected);
  }
  {
    qec->getQueryTreeCache().clearAll();
    auto result =
        unionOperation.getResult(true, ComputationMode::LAZY_IF_SUPPORTED);
    auto expected = makeIdTableFromVector({{1, U, 4}, {1, 2, 4}, {2, U, 8}});
    auto idTables = result->idTables();
    auto it = idTables.begin();
    ASSERT_NE(it, idTables.end());
    EXPECT_EQ(it->idTable_, expected);

    ASSERT_EQ(++it, idTables.end());
  }
}

// Test the sorted merge when a sort column is missing in one of the children.
// Such a column is UNDEF for all rows of that child. If the row of the other
// child is also UNDEF in that column, the next sort column has to decide.
TEST(Union, sortedMergeWithColumnMissingOnOneSide) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  auto U = Id::makeUndefined();

  // A child with the columns `?a ?b ?c`, where `?b` is UNDEF, and a child with
  // the columns `?a ?c` only.
  auto withB = [&]() {
    auto table = makeIdTableFromVector({{1, 0, 5}});
    table(0, 1) = U;
    return makeSortedValues(qec, std::move(table),
                            Vars{Var{"?a"}, Var{"?b"}, Var{"?c"}});
  };
  auto withoutB = [&](int64_t c) {
    return makeSortedValues(qec, makeIdTableFromVector({{1, c}}),
                            Vars{Var{"?a"}, Var{"?c"}});
  };

  // The column `?b` is missing in the left child (result columns `?a ?c ?b`),
  // the sort order is `?a ?b ?c`.
  testSortedUnion(qec, withoutB(7), withB(), {0, 2, 1},
                  makeIdTableFromVector({{1, 5, U}, {1, 7, U}}));

  // The column `?b` is missing in the right child (result columns `?a ?b ?c`).
  testSortedUnion(qec, withB(), withoutB(7), {0, 1, 2},
                  makeIdTableFromVector({{1, U, 5}, {1, U, 7}}));
}

// Test the sorted merge when the FIRST sort column is missing in one of the
// children and can be UNDEF in the other child. The children must then be
// merged, a simple concatenation is not sorted.
TEST(Union, sortedMergeWithFirstSortColumnMissingOnOneSide) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  auto U = Id::makeUndefined();

  // A child with the columns `?a ?c`, where `?a` is UNDEF in the first row,
  // and a child with the column `?c` only.
  auto withA = [&]() {
    auto table = makeIdTableFromVector({{0, 5}, {1, 2}});
    table(0, 0) = U;
    return makeSortedValues(qec, std::move(table), Vars{Var{"?a"}, Var{"?c"}});
  };
  auto withoutA = [&](int64_t c) {
    return makeSortedValues(qec, makeIdTableFromVector({{c}}), Vars{Var{"?c"}});
  };

  // The column `?a` is missing in the left child (result columns `?c ?a`), the
  // sort order is `?a ?c`.
  testSortedUnion(qec, withoutA(7), withA(), {1, 0},
                  makeIdTableFromVector({{5, U}, {7, U}, {2, 1}}));

  // The column `?a` is missing in the right child (result columns `?a ?c`).
  testSortedUnion(qec, withA(), withoutA(7), {0, 1},
                  makeIdTableFromVector({{U, 5}, {U, 7}, {1, 2}}));
}

// Test which columns are deciding (missing in one child and never UNDEF in the
// other child), and that the comparator of the merge stops after the first
// deciding column of the sort order.
TEST(Union, isDecidingColumnAndComparatorOrder) {
  using ::testing::ElementsAre;
  auto* qec = ad_utility::testing::getQec();

  // The column `?a` is present in both children, `?c` is missing in the left
  // child but can be UNDEF in the right child, so neither is deciding. The
  // columns `?b` and `?d` are missing in one child and always defined in the
  // other, so they are deciding.
  auto unionOperation = makeUnionForDecidingColumns(qec, {});
  EXPECT_FALSE(unionOperation.isDecidingColumn(0));
  EXPECT_TRUE(unionOperation.isDecidingColumn(1));
  EXPECT_FALSE(unionOperation.isDecidingColumn(2));
  EXPECT_TRUE(unionOperation.isDecidingColumn(3));

  // Without a deciding column, the comparator uses the complete sort order.
  EXPECT_THAT(makeUnionForDecidingColumns(qec, {0, 2}).comparatorOrder(),
              ElementsAre(0, 2));

  // With a deciding column, the comparator stops right after it.
  EXPECT_THAT(makeUnionForDecidingColumns(qec, {0, 2, 3, 1}).comparatorOrder(),
              ElementsAre(0, 2, 3));
  EXPECT_THAT(makeUnionForDecidingColumns(qec, {1, 0, 2, 3}).comparatorOrder(),
              ElementsAre(1));
}

// Test when the concatenation of the two children is already sorted, so that
// the merge can be skipped.
TEST(Union, isSortedByConcatenation) {
  auto* qec = ad_utility::testing::getQec();
  auto isSorted = [qec](std::vector<ColumnIndex> targetOrder) {
    return makeUnionForDecidingColumns(qec, std::move(targetOrder))
        .isSortedByConcatenation();
  };

  // Without a sort order, any concatenation is fine.
  EXPECT_TRUE(isSorted({}));

  // The first sort column `?d` is UNDEF for all rows of the left child and
  // never UNDEF in the right child, so the left child comes first.
  EXPECT_TRUE(isSorted({3, 0}));

  // The first sort column `?a` is present in both children.
  EXPECT_FALSE(isSorted({0, 3}));

  // The first sort column `?c` is missing in the left child, but can be UNDEF
  // in the right child.
  EXPECT_FALSE(isSorted({2, 0}));

  // The first sort column `?b` is missing in the RIGHT child and never UNDEF in
  // the left child. The constructor of `Union` then swaps the two children, so
  // the concatenation is sorted as well.
  EXPECT_TRUE(isSorted({1, 0}));
}

// Test the sorted merge and the concatenation when a sort column is deciding,
// that is, missing in one of the children and never UNDEF in the other child.
// The following sort columns then do not matter.
TEST(Union, sortedMergeWithDecidingColumn) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  auto U = Id::makeUndefined();

  // A child with the columns `?a ?b ?c`, where `?b` is always defined, and a
  // child with the columns `?a ?c` only. The values of `?c` are chosen such
  // that a comparison of `?c` would give the wrong order.
  auto withB = [&]() {
    return makeSortedValues(qec, makeIdTableFromVector({{1, 4, 0}, {2, 4, 5}}),
                            Vars{Var{"?a"}, Var{"?b"}, Var{"?c"}});
  };
  auto withoutB = [&]() {
    return makeSortedValues(qec, makeIdTableFromVector({{1, 9}, {2, 0}}),
                            Vars{Var{"?a"}, Var{"?c"}});
  };

  // The column `?b` is missing in the left child (result columns `?a ?c ?b`),
  // the sort order is `?a ?b ?c`.
  testSortedUnion(
      qec, withoutB(), withB(), {0, 2, 1},
      makeIdTableFromVector({{1, 9, U}, {1, 0, 4}, {2, 0, U}, {2, 5, 4}}));

  // The column `?b` is missing in the right child (result columns `?a ?b ?c`).
  testSortedUnion(
      qec, withB(), withoutB(), {0, 1, 2},
      makeIdTableFromVector({{1, U, 9}, {1, 4, 0}, {2, U, 0}, {2, 4, 5}}));

  // The FIRST sort column `?b` is missing in the left child (result columns
  // `?a ?c ?b`), so the children are simply concatenated.
  testSortedUnion(
      qec, withoutB(), withB(), {2, 0, 1},
      makeIdTableFromVector({{1, 9, U}, {2, 0, U}, {1, 0, 4}, {2, 5, 4}}));
}

// _____________________________________________________________________________
TEST(Union, sortedMergeWithOneSideNonLazy) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{2}}), Vars{Var{"?a"}}, false,
      colIndices({0}), LocalVocab{}, std::nullopt, true);

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0}, {1}}), Vars{Var{"?a"}}, false,
      colIndices({0}));
  Union unionOperation{qec, std::move(leftT), std::move(rightT),
                       colIndices({0})};
  auto expected = makeIdTableFromVector({{0}, {1}, {2}});
  {
    qec->getQueryTreeCache().clearAll();
    auto result =
        unionOperation.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    EXPECT_EQ(result->idTableView(), expected);
  }
  {
    qec->getQueryTreeCache().clearAll();
    auto result =
        unionOperation.getResult(true, ComputationMode::LAZY_IF_SUPPORTED);
    auto idTables = result->idTables();
    auto it = idTables.begin();
    ASSERT_NE(it, idTables.end());
    EXPECT_EQ(it->idTable_, makeIdTableFromVector({{0}, {1}}));

    ++it;
    ASSERT_NE(it, idTables.end());
    EXPECT_EQ(it->idTable_, makeIdTableFromVector({{2}}));

    ASSERT_EQ(++it, idTables.end());
  }
}

// _____________________________________________________________________________
TEST(Union, sortedMergeWithLocalVocab) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();

  LocalVocab vocab1;
  vocab1.getIndexAndAddIfNotContained(localVocabEntryFromStringRepresentation(
      "\"Test1\"", qec->getLocalVocabContext()));

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}, {2}, {4}}), Vars{Var{"?a"}}, false,
      colIndices({0}), vocab1.clone());

  LocalVocab vocab2;
  vocab2.getIndexAndAddIfNotContained(localVocabEntryFromStringRepresentation(
      "\"Test2\"", qec->getLocalVocabContext()));
  std::vector<IdTable> tables;
  tables.push_back(makeIdTableFromVector({{0}}));
  tables.push_back(makeIdTableFromVector({{3}}));
  tables.push_back(makeIdTableFromVector({{5}}));

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(tables), Vars{Var{"?a"}}, false, colIndices({0}),
      vocab2.clone());
  {
    qec->getQueryTreeCache().clearAll();
    Union unionOperation{qec, leftT, rightT, colIndices({0})};
    auto result =
        unionOperation.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected = makeIdTableFromVector({{0}, {1}, {2}, {3}, {4}, {5}});
    EXPECT_EQ(result->idTableView(), expected);
    EXPECT_THAT(result->localVocab().getAllWordsForTesting(),
                ::testing::IsSupersetOf(vocab1.getAllWordsForTesting()));
    EXPECT_THAT(result->localVocab().getAllWordsForTesting(),
                ::testing::IsSupersetOf(vocab2.getAllWordsForTesting()));
  }
  {
    qec->getQueryTreeCache().clearAll();
    Union unionOperation{qec, std::move(leftT), std::move(rightT),
                         colIndices({0})};
    auto result =
        unionOperation.getResult(true, ComputationMode::LAZY_IF_SUPPORTED);
    auto idTables = result->idTables();

    auto it = idTables.begin();
    ASSERT_NE(it, idTables.end());
    EXPECT_EQ(it->idTable_, makeIdTableFromVector({{0}, {1}, {2}, {3}, {4}}));
    EXPECT_THAT(it->localVocab_.getAllWordsForTesting(),
                ::testing::IsSupersetOf(vocab1.getAllWordsForTesting()));
    EXPECT_THAT(it->localVocab_.getAllWordsForTesting(),
                ::testing::IsSupersetOf(vocab2.getAllWordsForTesting()));

    ++it;
    ASSERT_NE(it, idTables.end());
    EXPECT_EQ(it->idTable_, makeIdTableFromVector({{5}}));
    EXPECT_EQ(it->localVocab_.getAllWordsForTesting(),
              vocab2.getAllWordsForTesting());

    ASSERT_EQ(++it, idTables.end());
  }
}

// _____________________________________________________________________________
TEST(Union, cacheKeyDiffersForDifferentOrdering) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  auto U = Id::makeUndefined();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 4}}), Vars{Var{"?a"}, Var{"?b"}}, false,
      colIndices({0, 1}));

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 8}}), Vars{Var{"?a"}, Var{"?c"}}, false,
      colIndices({0, 1}));
  Union unionOperation1{qec, leftT, rightT, colIndices({0, 1, 2})};
  Union unionOperation2{qec, std::move(leftT), std::move(rightT),
                        colIndices({0, 2, 1})};

  EXPECT_NE(unionOperation1.getCacheKey(), unionOperation2.getCacheKey());
  EXPECT_EQ(unionOperation1.getChildren().at(0)->getCacheKey(),
            unionOperation2.getChildren().at(0)->getCacheKey());
  EXPECT_EQ(unionOperation1.getChildren().at(1)->getCacheKey(),
            unionOperation2.getChildren().at(1)->getCacheKey());

  qec->getQueryTreeCache().clearAll();
  {
    auto result =
        unionOperation1.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected = makeIdTableFromVector({{1, U, 8}, {1, 4, U}});
    EXPECT_EQ(result->idTableView(), expected);
  }
  {
    auto result =
        unionOperation2.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected = makeIdTableFromVector({{1, 4, U}, {1, U, 8}});
    EXPECT_EQ(result->idTableView(), expected);
  }
}

// _____________________________________________________________________________
TEST(Union, cacheKeyPreventsAmbiguity) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();

  // Construct the following two operations (for the check that follows):
  //
  // { VALUES ?a { 1 } INTERNAL SORT BY ?a } UNION { VALUES ?a { 1 } }
  //
  // { VALUES ?a { 1 } } UNION { VALUES ?a { 1 } } INTERNAL SORT BY ?a
  //
  auto values1 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}), Vars{Var{"?a"}});

  auto values2 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}), Vars{Var{"?a"}});
  auto sort = ad_utility::makeExecutionTree<Sort>(qec, values1->clone(),
                                                  colIndices({0}));
  Union operation1{qec, std::move(sort), values2};
  Sort operation2{qec,
                  ad_utility::makeExecutionTree<Union>(qec, std::move(values1),
                                                       std::move(values2)),
                  colIndices({0})};

  // Check that the two cache keys are different (which was not the case before
  // #1933).
  EXPECT_NE(operation1.getCacheKey(), operation2.getCacheKey());
}

// _____________________________________________________________________________
TEST(Union, cacheKeyStoresColumnMapping) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();

  {
    auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{1, 4}}), Vars{Var{"?a"}, Var{"?b"}});

    auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{2, 8}}), Vars{Var{"?a"}, Var{"?c"}});

    auto rightTHidden = rightT->clone();
    rightTHidden->getRootOperation()->setSelectedVariablesForSubquery(
        {Variable{"?a"}});

    Union unionOperation1{qec, leftT, std::move(rightT)};
    Union unionOperation2{qec, std::move(leftT), std::move(rightTHidden)};

    EXPECT_NE(unionOperation1.getCacheKey(), unionOperation2.getCacheKey());
  }

  {
    auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{1, 4}}), Vars{Var{"?a"}, Var{"?b"}});

    auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{2, 8}}), Vars{Var{"?a"}, Var{"?b"}});

    auto rightTSwapped = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{2, 8}}), Vars{Var{"?b"}, Var{"?a"}});

    Union unionOperation1{qec, leftT, std::move(rightT)};
    Union unionOperation2{qec, std::move(leftT), std::move(rightTSwapped)};

    EXPECT_NE(unionOperation1.getCacheKey(), unionOperation2.getCacheKey());
  }
}

// _____________________________________________________________________________
// We use a trick to merge two children where the first sort column is not
// present in both children. This test checks that the trick works correctly.
TEST(Union, testEfficientMerge) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  auto U = Id::makeUndefined();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}), Vars{Var{"?a"}}, false,
      colIndices({0}));

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{2}}), Vars{Var{"?b"}}, false,
      colIndices({0}));

  {
    qec->getQueryTreeCache().clearAll();
    Union unionOperation{qec, leftT, rightT, colIndices({0, 1})};
    // Check if children were swapped
    EXPECT_EQ(unionOperation.getChildren().at(0)->getCacheKey(),
              rightT->getCacheKey());
    EXPECT_EQ(unionOperation.getChildren().at(1)->getCacheKey(),
              leftT->getCacheKey());

    auto result =
        unionOperation.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected = makeIdTableFromVector({{U, 2}, {1, U}});
    EXPECT_EQ(result->idTableView(), expected);
  }
  {
    qec->getQueryTreeCache().clearAll();
    Union unionOperation{qec, leftT, rightT, colIndices({1, 0})};
    // Ensure children were not swapped
    EXPECT_EQ(unionOperation.getChildren().at(0)->getCacheKey(),
              leftT->getCacheKey());
    EXPECT_EQ(unionOperation.getChildren().at(1)->getCacheKey(),
              rightT->getCacheKey());

    auto result =
        unionOperation.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected = makeIdTableFromVector({{1, U}, {U, 2}});
    EXPECT_EQ(result->idTableView(), expected);
  }
}

// _____________________________________________________________________________
TEST(Union, createSortedVariantWorksProperly) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  auto U = Id::makeUndefined();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 2, 4}}),
      Vars{Var{"?a"}, Var{"?b"}, Var{"?c"}});

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 4}, {2, 8}}), Vars{Var{"?a"}, Var{"?d"}});
  Union unionOperation{qec, std::move(leftT), std::move(rightT)};
  EXPECT_TRUE(unionOperation.resultSortedOn().empty());

  {
    qec->getQueryTreeCache().clearAll();
    auto tree = unionOperation.makeSortedTree(colIndices({0, 1, 2, 3}));
    ASSERT_TRUE(tree.has_value());
    auto variant = tree.value()->getRootOperation();
    EXPECT_THAT(variant->getResultSortedOn(),
                ::testing::ElementsAre(0, 1, 2, 3));
    EXPECT_THAT(
        variant->getChildren().at(0)->getRootOperation()->getResultSortedOn(),
        ::testing::ElementsAre(0, 1, 2));
    EXPECT_THAT(
        variant->getChildren().at(1)->getRootOperation()->getResultSortedOn(),
        ::testing::ElementsAre(0, 1));
    auto result = variant->getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected =
        makeIdTableFromVector({{1, U, U, 4}, {1, 2, 4, U}, {2, U, U, 8}});
    EXPECT_EQ(result->idTableView(), expected);
  }
  {
    qec->getQueryTreeCache().clearAll();
    auto tree = unionOperation.makeSortedTree(colIndices({0, 3, 1, 2}));
    ASSERT_TRUE(tree.has_value());
    auto variant = tree.value()->getRootOperation();
    EXPECT_THAT(variant->getResultSortedOn(),
                ::testing::ElementsAre(0, 3, 1, 2));
    EXPECT_THAT(
        variant->getChildren().at(0)->getRootOperation()->getResultSortedOn(),
        ::testing::ElementsAre(0, 1, 2));
    EXPECT_THAT(
        variant->getChildren().at(1)->getRootOperation()->getResultSortedOn(),
        ::testing::ElementsAre(0, 1));
    auto result = variant->getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected =
        makeIdTableFromVector({{1, 2, 4, U}, {1, U, U, 4}, {2, U, U, 8}});
    EXPECT_EQ(result->idTableView(), expected);
  }
  {
    qec->getQueryTreeCache().clearAll();
    EXPECT_THROW(unionOperation.makeSortedTree({}), ad_utility::Exception);
  }
}

// _____________________________________________________________________________
TEST(Union, checkChunkSizeSplitsProperly) {
  using Var = Variable;
  using ::testing::Each;
  auto* qec = ad_utility::testing::getQec();

  IdTable reference{1, qec->getAllocator()};
  reference.resize(Union::chunkSize + (Union::chunkSize / 2) + 1);
  ql::ranges::fill(reference.getColumn(0), Id::makeFromInt(42));
  // Make sure we compute the expensive way
  reference.getColumn(0).back() = Id::makeFromInt(1337);

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, reference.clone(), Vars{Var{"?a"}}, false, colIndices({0}));

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(reference), Vars{Var{"?a"}}, false, colIndices({0}));

  Union unionOperation{qec, std::move(leftT), std::move(rightT),
                       colIndices({0})};

  qec->getQueryTreeCache().clearAll();
  auto result =
      unionOperation.getResult(true, ComputationMode::LAZY_IF_SUPPORTED);
  auto idTables = result->idTables();

  auto it = idTables.begin();
  ASSERT_NE(it, idTables.end());
  EXPECT_EQ(it->idTable_.size(), Union::chunkSize);
  EXPECT_THAT(it->idTable_.getColumn(0), Each(Id::makeFromInt(42)));

  ++it;
  ASSERT_NE(it, idTables.end());
  EXPECT_EQ(it->idTable_.size(), Union::chunkSize);
  EXPECT_THAT(it->idTable_.getColumn(0), Each(Id::makeFromInt(42)));

  ++it;
  ASSERT_NE(it, idTables.end());
  EXPECT_EQ(it->idTable_.size(), Union::chunkSize);
  EXPECT_THAT(it->idTable_.getColumn(0), Each(Id::makeFromInt(42)));

  ++it;
  ASSERT_NE(it, idTables.end());
  EXPECT_EQ(it->idTable_.size(), 2);
  EXPECT_THAT(it->idTable_.getColumn(0), Each(Id::makeFromInt(1337)));

  ++it;
  EXPECT_EQ(it, idTables.end());
}

// _____________________________________________________________________________
TEST(Union, columnOriginatesFromGraphOrUndef) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();

  IdTable reference{2, qec->getAllocator()};

  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, reference.clone(), Vars{Var{"?a"}, Var{"?d"}}, false,
      colIndices({0, 1}));

  auto index = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::PSO,
      SparqlTripleSimple{Variable{"?a"}, Variable{"?b"}, Variable{"?c"}});

  Union union1{qec, values, values};
  EXPECT_FALSE(union1.columnOriginatesFromGraphOrUndef(Var{"?a"}));
  EXPECT_FALSE(union1.columnOriginatesFromGraphOrUndef(Var{"?d"}));
  EXPECT_THROW(union1.columnOriginatesFromGraphOrUndef(Var{"?notExisting"}),
               ad_utility::Exception);

  Union union2{qec, values, index};
  EXPECT_FALSE(union2.columnOriginatesFromGraphOrUndef(Var{"?a"}));
  EXPECT_FALSE(union2.columnOriginatesFromGraphOrUndef(Var{"?b"}));
  EXPECT_TRUE(union2.columnOriginatesFromGraphOrUndef(Var{"?c"}));
  EXPECT_FALSE(union2.columnOriginatesFromGraphOrUndef(Var{"?d"}));
  EXPECT_THROW(union2.columnOriginatesFromGraphOrUndef(Var{"?notExisting"}),
               ad_utility::Exception);

  Union union3{qec, index, values};
  EXPECT_FALSE(union3.columnOriginatesFromGraphOrUndef(Var{"?a"}));
  EXPECT_FALSE(union3.columnOriginatesFromGraphOrUndef(Var{"?b"}));
  EXPECT_TRUE(union3.columnOriginatesFromGraphOrUndef(Var{"?c"}));
  EXPECT_FALSE(union3.columnOriginatesFromGraphOrUndef(Var{"?d"}));
  EXPECT_THROW(union3.columnOriginatesFromGraphOrUndef(Var{"?notExisting"}),
               ad_utility::Exception);

  Union union4{qec, index, index};
  EXPECT_TRUE(union4.columnOriginatesFromGraphOrUndef(Var{"?a"}));
  EXPECT_FALSE(union4.columnOriginatesFromGraphOrUndef(Var{"?b"}));
  EXPECT_TRUE(union4.columnOriginatesFromGraphOrUndef(Var{"?c"}));
  EXPECT_THROW(union4.columnOriginatesFromGraphOrUndef(Var{"?notExisting"}),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(Union, getCostEstimate) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  IdTable oneColumn{1, qec->getAllocator()};
  oneColumn.resize(100);
  ql::ranges::fill(oneColumn.getColumn(0), Id::makeUndefined());
  // The two columns are always defined, so that a sorted `UNION` whose first
  // sort column is missing in the other child can skip the merge.
  IdTable twoColumns{2, qec->getAllocator()};
  twoColumns.resize(100);
  ql::ranges::fill(twoColumns.getColumn(0), Id::makeFromInt(1));
  ql::ranges::fill(twoColumns.getColumn(1), Id::makeFromInt(1));

  auto valuesA = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, oneColumn.clone(), Vars{Var{"?a"}}, false, colIndices({0}));

  auto valuesB = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, oneColumn.clone(), Vars{Var{"?b"}}, false, colIndices({0}));

  auto valuesAb = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, twoColumns.clone(), Vars{Var{"?a"}, Var{"?b"}}, false,
      colIndices({0, 1}));

  Union sortedUnionWithUndefCol{qec, valuesB->clone(), valuesAb->clone(),
                                colIndices({1})};
  Union sortedUnionWithWrongUndefCol{qec, valuesA->clone(), valuesAb->clone(),
                                     colIndices({0})};

  // Second option is way cheaper because the faster implementation can be used.
  EXPECT_LT(sortedUnionWithUndefCol.getCostEstimate(),
            sortedUnionWithWrongUndefCol.getCostEstimate());

  Union unsortedUnionWithSingleVar{qec, valuesA->clone(), valuesA->clone()};
  Union unsortedUnionWithDifferentVars{qec, valuesA->clone(), valuesB->clone()};
  Union sortedUnionWithSingleVar{qec, valuesA->clone(), valuesA->clone(),
                                 colIndices({0})};
  Union sortedUnionWithTwoVars{qec, valuesAb->clone(), valuesAb->clone(),
                               colIndices({0, 1})};

  EXPECT_LT(unsortedUnionWithSingleVar.getCostEstimate(),
            unsortedUnionWithDifferentVars.getCostEstimate());
  EXPECT_LT(unsortedUnionWithSingleVar.getCostEstimate(),
            sortedUnionWithSingleVar.getCostEstimate());
  EXPECT_LT(sortedUnionWithSingleVar.getCostEstimate(),
            sortedUnionWithTwoVars.getCostEstimate());
  sortedUnionWithSingleVar.getCostEstimate();
  sortedUnionWithTwoVars.getCostEstimate();

  IdTable oneColumnSmall{1, qec->getAllocator()};
  oneColumn.resize(2);
  ql::ranges::fill(oneColumn.getColumn(0), Id::makeUndefined());

  auto valuesSmall = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, oneColumn.clone(), Vars{Var{"?a"}}, false, colIndices({0}));
  Union unsortedUnionSmall{qec, valuesSmall->clone(), valuesSmall->clone()};
  // Union should never be free.
  EXPECT_GT(unsortedUnionSmall.getCostEstimate(),
            valuesSmall->getCostEstimate() * 2);
}

// _____________________________________________________________________________
// Pushing a `LIMIT` into the children can change the algorithm and with it the
// sort order of an `OptionalJoin` inside them, see the caution note on
// `Operation::applyLimitOffset`. Check that the sort order which the merging
// implementation requires of the children is restored in that case.
TEST(Union, limitPushdownRestoresSortOrderOfChildren) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  // An `OptionalJoin` on `?a` that is sorted on `?a`, but switches to the index
  // nested loop join (which doesn't preserve the order of its left input) as
  // soon as its left input becomes smaller than its right input.
  auto makeOptionalJoin = [qec]() {
    auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec,
        makeIdTableFromVector(
            {{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {6, 6}}),
        Vars{Var{"?a"}, Var{"?b"}}, false, colIndices({0}));
    // Deliberately unsorted, so that the `OptionalJoin` wraps it in a `Sort`,
    // which is a precondition for the index nested loop join.
    auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{2, 20}, {1, 10}, {0, 0}}),
        Vars{Var{"?a"}, Var{"?c"}});
    return ad_utility::makeExecutionTree<OptionalJoin>(qec, std::move(left),
                                                       std::move(right));
  };
  auto other = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{7, 70}, {8, 80}}),
      Vars{Var{"?a"}, Var{"?d"}}, false, colIndices({0}));

  // The result of the union has to be sorted on `?a`, which is its column 0.
  Union unionOperation{qec, makeOptionalJoin(), std::move(other),
                       colIndices({0})};
  auto expectChildrenAreSorted =
      [&unionOperation](ad_utility::source_location loc =
                            AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(loc);
        for (const auto* child : unionOperation.getChildren()) {
          EXPECT_TRUE(child->getRootOperation()->isSortedBy(colIndices({0})));
        }
      };
  // Both children are already sorted, so the constructor added no `Sort`.
  expectChildrenAreSorted();

  // The limit is small enough to make the left input of the `OptionalJoin`
  // smaller than its right input.
  unionOperation.applyLimitOffset({2}, false);
  expectChildrenAreSorted();
}

// _____________________________________________________________________________
TEST(Union, limitAndOffsetArePushedDownToChildren) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  // The left child yields 1 to 5 and the right child yields 6 to 10, so the
  // union yields the numbers 1 to 10 in that order.
  auto makeUnion = [qec]() {
    auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{1}, {2}, {3}, {4}, {5}}), Vars{Var{"?a"}});
    auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{6}, {7}, {8}, {9}, {10}}),
        Vars{Var{"?a"}});
    return Union{qec, std::move(leftT), std::move(rightT)};
  };
  auto expectChildLimits =
      [](Union& unionOperation, std::optional<uint64_t> limit,
         ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(loc);
        for (const auto* child : unionOperation.getChildren()) {
          EXPECT_EQ(child->getRootOperation()->getLimitOffset(),
                    LimitOffsetClause{limit});
        }
      };
  auto expectResult = [qec](Union& unionOperation, const IdTable& expected,
                            ad_utility::source_location loc =
                                AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(loc);
    qec->getQueryTreeCache().clearAll();
    EXPECT_EQ(unionOperation.getResult(false)->idTableView(), expected);
  };

  {
    // Both children only have to supply `limit + offset` rows.
    auto unionOperation = makeUnion();
    unionOperation.applyLimitOffset({2, 3}, false);
    expectChildLimits(unionOperation, 5);
    // The `Union` still has to apply the `LIMIT`/`OFFSET` to its own result.
    expectResult(unionOperation, makeIdTableFromVector({{4}, {5}}));
  }
  {
    // A `LIMIT`/`OFFSET` that is applied on top of a previous one (which
    // happens for nested subqueries) must not shrink the limit of the children
    // too much. Here the result consists of the rows 5 and 6 of the union, so
    // the children still have to supply 7 rows.
    auto unionOperation = makeUnion();
    unionOperation.applyLimitOffset({10, 5}, false);
    expectChildLimits(unionOperation, 15);
    unionOperation.applyLimitOffset({2, 0}, false);
    expectChildLimits(unionOperation, 7);
    expectResult(unionOperation, makeIdTableFromVector({{6}, {7}}));
  }
  {
    // Adding up the limit and the offset must not overflow.
    auto unionOperation = makeUnion();
    unionOperation.applyLimitOffset({std::numeric_limits<uint64_t>::max(), 1},
                                    false);
    expectChildLimits(unionOperation, std::nullopt);
  }
  {
    // Without a limit there is no bound that could be pushed down.
    auto unionOperation = makeUnion();
    unionOperation.applyLimitOffset({std::nullopt, 8}, false);
    expectChildLimits(unionOperation, std::nullopt);
    expectResult(unionOperation, makeIdTableFromVector({{9}, {10}}));
  }
}

// Test that `SortedUnionImpl` requires a cancellation handle.
TEST(Union, sortedUnionImplRequiresCancellationHandle) {
  auto makeUnion = [](ad_utility::SharedCancellationHandle handle) {
    makeSortedUnion({}, {}, false, ad_utility::testing::makeAllocator(),
                    std::move(handle));
  };
  EXPECT_NO_THROW(
      makeUnion(std::make_shared<ad_utility::CancellationHandle<>>()));
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeUnion(nullptr),
      ::testing::HasSubstr("cancellationHandle_ != nullptr"));
}

// Test that a lazy `SortedUnionImpl` doesn't reserve memory for a full chunk
// up front, which would fail with a tight memory limit and many concurrent
// unions, even if the inputs are tiny (see issue #3898).
TEST(Union, lazySortedUnionImplDoesNotReserveFullChunk) {
  // A full chunk of a single column would need 8 MB.
  auto allocator =
      ad_utility::testing::makeAllocator(ad_utility::MemorySize::megabytes(1));
  auto sortedUnion = makeSortedUnion(
      makeSortedUnionRange(makeIdTableFromVector({{1}, {3}})),
      makeSortedUnionRange(makeIdTableFromVector({{2}})), true, allocator,
      std::make_shared<ad_utility::CancellationHandle<>>());
  EXPECT_EQ(
      aggregateTables(Result::LazyResult{std::move(sortedUnion)}, 1).first,
      makeIdTableFromVector({{1}, {2}, {3}}));
}

// Test that the non-lazy merge of a sorted union checks for cancellation
// after every `Union::chunkSize` merged rows.
TEST(Union, sortedUnionImplChecksForCancellationWhileMerging) {
  auto* qec = ad_utility::testing::getQec();
  // The inputs interleave, so the merge loop (and not the final append of the
  // remaining rows) produces more than `Union::chunkSize` rows.
  size_t numRows = Union::chunkSize / 2 + 1;
  auto makeInput = [qec, numRows](size_t offset) {
    IdTable table{1, qec->getAllocator()};
    table.resize(numRows);
    for (size_t i = 0; i < numRows; ++i) {
      table(i, 0) = V(2 * i + offset);
    }
    return makeSortedUnionRange(std::move(table));
  };
  auto makeUnion = [qec,
                    &makeInput](ad_utility::SharedCancellationHandle handle) {
    return makeSortedUnion(makeInput(0), makeInput(1), false,
                           qec->getAllocator(), std::move(handle));
  };

  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  auto result = makeUnion(handle).get();
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->idTable_.size(), 2 * numRows);
  EXPECT_TRUE(ql::ranges::is_sorted(result->idTable_.getColumn(0)));

  handle->cancel(ad_utility::CancellationState::MANUAL);
  EXPECT_THROW(makeUnion(handle).get(), ad_utility::CancellationException);
}
