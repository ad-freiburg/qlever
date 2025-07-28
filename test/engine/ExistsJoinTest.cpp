// Copyright 2024 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/OperationTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "engine/ExistsJoin.h"
#include "engine/IndexScan.h"
#include "engine/JoinHelpers.h"
#include "engine/QueryExecutionTree.h"
#include "engine/sparqlExpressions/ExistsExpression.h"

using namespace ad_utility::testing;

namespace {
auto V = VocabId;
constexpr auto U = Id::makeUndefined();
constexpr auto T = Id::makeFromBool(true);
constexpr auto F = Id::makeFromBool(false);
constexpr auto I = Id::makeFromInt;

// Helper function that computes an `ExistsJoin` of the given `left` and
// `right` and checks that the result columns is equal to `expectedAsBool`.
// The first `numJoinColumns` columns of both `leftInput` and `rightInput` are
// used as join columns.
//
void testExistsFromIdTable(
    IdTable left, IdTable right, std::vector<bool> expectedAsBool,
    size_t numJoinColumns,
    ad_utility::source_location loc = ad_utility::source_location::current()) {
  auto g = generateLocationTrace(loc);
  AD_CORRECTNESS_CHECK(left.numRows() == expectedAsBool.size());
  AD_CORRECTNESS_CHECK(left.numColumns() >= numJoinColumns);
  AD_CORRECTNESS_CHECK(right.numColumns() >= numJoinColumns);

  // Randomly permute the columns of the `input` and return the permutation that
  // was applied
  auto permuteColumns = [](auto& table) {
    auto colsView = ad_utility::integerRange(table.numColumns());
    std::vector<ColumnIndex> permutation;
    ql::ranges::copy(colsView, std::back_inserter(permutation));
    table.setColumnSubset(permutation);
    return permutation;
  };
  // Permute the columns.
  auto leftPermutation = permuteColumns(left);
  auto rightPermutation = permuteColumns(right);

  // We have to make the deep copy of `left` for the expected result at exactly
  // this point: The permutation of the columns (above) also affects the
  // expected result, while the permutation of the rows (which will be applied
  // below) doesn't affect it, as the `ExistsJoin` internally sorts its inputs.
  IdTable expected = left.clone();

  if (numJoinColumns > 0) {
    // Randomly shuffle the inputs, to ensure that the `existsJoin` correctly
    // pre-sorts its inputs.
    ad_utility::randomShuffle(left.begin(), left.end());
    ad_utility::randomShuffle(right.begin(), right.end());
  }

  auto qec = getQec();
  using V = Variable;
  using Vars = std::vector<std::optional<Variable>>;

  // Helper lambda `makeChild` that turns a `VectorTable` input into a
  // `QueryExecutionTree` with a `ValuesForTesting` operation.
  auto joinCol = [](size_t i) { return V{absl::StrCat("?joinCol_", i)}; };
  auto nonJoinCol = [i = 0]() mutable {
    return V{absl::StrCat("?nonJoinCol_", i++)};
  };

  auto makeChild = [&](const IdTable& input, const auto& columnPermutation) {
    Vars vars;
    for (auto colIdx : columnPermutation) {
      if (colIdx < numJoinColumns) {
        vars.push_back(joinCol(colIdx));
      } else {
        vars.push_back(nonJoinCol());
      }
    }
    return ad_utility::makeExecutionTree<ValuesForTesting>(qec, input.clone(),
                                                           vars);
  };

  // Compute the `ExistsJoin` and check the result.
  auto exists = ExistsJoin{qec, makeChild(left, leftPermutation),
                           makeChild(right, rightPermutation), V{"?exists"}};
  EXPECT_EQ(exists.getResultWidth(), left.numColumns() + 1);
  auto res = exists.computeResultOnlyForTesting();
  const auto& table = res.idTable();
  ASSERT_EQ(table.numRows(), left.size());
  expected.addEmptyColumn();
  ql::ranges::transform(expectedAsBool, expected.getColumn(2).begin(),
                        &Id::makeFromBool);
  EXPECT_THAT(table, matchesIdTable(expected));
}

// Same as the function above, but conveniently takes `VectorTable`s instead of
// `IdTable`s.
void testExists(
    const VectorTable& leftInput, const VectorTable& rightInput,
    std::vector<bool> expectedAsBool, size_t numJoinColumns,
    ad_utility::source_location loc = ad_utility::source_location::current()) {
  auto left = makeIdTableFromVector(leftInput);
  auto right = makeIdTableFromVector(rightInput);
  testExistsFromIdTable(std::move(left), std::move(right),
                        std::move(expectedAsBool), numJoinColumns,
                        std::move(loc));
}
// Helper function to test exists join implementations.
void testExistsJoin(std::vector<IdTable> leftTables,
                    std::vector<IdTable> rightTables,
                    const std::vector<IdTable>& expectedResult,
                    bool singleVar = false,
                    ad_utility::source_location location =
                        ad_utility::source_location::current()) {
  auto g = generateLocationTrace(location);
  auto qec = ad_utility::testing::getQec();

  auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(leftTables),
      singleVar ? std::vector<std::optional<Variable>>{Variable{"?x"}}
                : std::vector<std::optional<Variable>>{Variable{"?x"},
                                                       Variable{"?y"}},
      false, std::vector<ColumnIndex>{0});
  auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(rightTables),
      singleVar ? std::vector<std::optional<Variable>>{Variable{"?x"}}
                : std::vector<std::optional<Variable>>{Variable{"?x"},
                                                       Variable{"?z"}},
      false, std::vector<ColumnIndex>{0});
  ExistsJoin existsJoin{qec, left, right, Variable{"?exists"}};

  {
    qec->getQueryTreeCache().clearAll();

    auto result = existsJoin.computeResultOnlyForTesting(true);

    ASSERT_FALSE(result.isFullyMaterialized());

    std::vector<IdTable> actualResult;

    for (auto& [idTable, _] : result.idTables()) {
      actualResult.push_back(std::move(idTable));
    }

    // Provide nicer error messages
    EXPECT_EQ(actualResult.size(), expectedResult.size());
    EXPECT_EQ(actualResult, expectedResult);
  }

  {
    qec->getQueryTreeCache().clearAll();

    auto result = existsJoin.computeResultOnlyForTesting(false);

    ASSERT_TRUE(result.isFullyMaterialized());

    IdTable expected{existsJoin.getResultWidth(), qec->getAllocator()};

    for (const IdTable& idTable : expectedResult) {
      ASSERT_EQ(idTable.numColumns(), existsJoin.getResultWidth());
      expected.insertAtEnd(idTable);
    }

    EXPECT_EQ(result.idTable(), expected);
  }
}

}  // namespace

TEST(Exists, computeResult) {
  auto alloc = ad_utility::testing::makeAllocator();
  // Single join column.
  testExists({{3, 6}, {4, 7}, {5, 8}}, {{3, 15}, {3, 19}, {5, 37}},
             {true, false, true}, 1);

  // No join column.
  testExists({{3, 6}, {4, 7}, {5, 8}}, {{3, 15}, {3, 19}}, {true, true, true},
             0);
  testExistsFromIdTable(makeIdTableFromVector({{3, 6}, {4, 7}, {5, 8}}),
                        IdTable{2, alloc}, {false, false, false}, 0);

  // Single join column with one UNDEF (which always matches).
  auto U = Id::makeUndefined();
  testExists({{U, 13}, {3, 6}, {4, 7}, {5, 8}}, {{3, 15}, {3, 19}, {5, 37}},
             {true, true, false, true}, 1);
  testExists({{3, 6}, {4, 7}, {5, 8}}, {{U, 15}}, {true, true, true}, 1);

  // Two join columns.
  testExists({{3, 6}, {4, 7}, {5, 8}}, {{3, 15}, {3, 19}, {5, 37}},
             {false, false, false}, 2);
  testExists({{3, 6}, {4, 7}, {5, 8}},
             {{3, 6, 11}, {3, 19, 7}, {4, 8, 0}, {5, 8, 37}},
             {true, false, true}, 2);

  // Two join columns with UNDEFs in each column.
  testExists({{2, 2}, {3, U}, {4, 8}, {5, 8}},
             {{U, 8}, {3, 15}, {3, 19}, {5, U}, {5, 37}},
             {false, true, true, true}, 2);
  testExists({{U, U}}, {{13, 17}}, {true}, 2);
  testExists({{13, 17}, {25, 38}}, {{U, U}}, {true, true}, 2);

  // Empty inputs
  testExistsFromIdTable(IdTable(2, alloc),
                        makeIdTableFromVector({{U, U}, {3, 7}}), {}, 1);
  testExistsFromIdTable(makeIdTableFromVector({{U, U}, {3, 7}}),
                        IdTable(2, alloc), {false, false}, 2);
}

// _____________________________________________________________________________
TEST(Exists, clone) {
  auto* qec = getQec();
  ExistsJoin existsJoin{
      qec,
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, makeIdTableFromVector({{0, 1}}),
          std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}}),
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, makeIdTableFromVector({{0, 1}}),
          std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}}),
      Variable{"?z"}};

  auto clone = existsJoin.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(existsJoin, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), existsJoin.getDescriptor());
}

// _____________________________________________________________________________
TEST(Exists, testGeneratorIsForwardedForDistinctColumnsTrueCase) {
  auto* qec = getQec();
  qec->getQueryTreeCache().clearAll();
  ExistsJoin existsJoin{
      qec,
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, makeIdTableFromVector({{0, 1}}),
          std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}}),
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, makeIdTableFromVector({{2, 4}}),
          std::vector<std::optional<Variable>>{Variable{"?c"}, Variable{"?d"}}),
      Variable{"?z"}};

  auto result = existsJoin.computeResultOnlyForTesting(true);
  ASSERT_FALSE(result.isFullyMaterialized());

  auto idTables = result.idTables();
  auto it = idTables.begin();
  ASSERT_NE(it, idTables.end());
  EXPECT_EQ(it->idTable_,
            makeIdTableFromVector({{V(0), V(1), Id::makeFromBool(true)}}));

  EXPECT_EQ(++it, idTables.end());
}

// _____________________________________________________________________________
TEST(Exists, testGeneratorIsForwardedForDistinctColumnsFalseCase) {
  auto* qec = getQec();
  qec->getQueryTreeCache().clearAll();
  ExistsJoin existsJoin{
      qec,
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, makeIdTableFromVector({{0, 1}}),
          std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}}),
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, IdTable{2, qec->getAllocator()},
          std::vector<std::optional<Variable>>{Variable{"?c"}, Variable{"?d"}}),
      Variable{"?z"}};

  auto result = existsJoin.computeResultOnlyForTesting(true);
  ASSERT_FALSE(result.isFullyMaterialized());

  auto idTables = result.idTables();
  auto it = idTables.begin();
  ASSERT_NE(it, idTables.end());
  EXPECT_EQ(it->idTable_,
            makeIdTableFromVector({{V(0), V(1), Id::makeFromBool(false)}}));

  EXPECT_EQ(++it, idTables.end());
}

// _____________________________________________________________________________
TEST(ExistsJoin, lazyExistsJoin) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector({{U, V(10), T}, {V(1), V(11), F}}));
  expected.push_back(
      makeIdTableFromVector({{V(2), V(12), T}, {V(3), V(13), T}}));
  expected.push_back(
      makeIdTableFromVector({{V(4), V(14), F}, {V(5), V(15), F}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector({{U, V(10)}, {V(1), V(11)}}));
  leftTables.push_back(makeIdTableFromVector({{2, 12}, {3, 13}}));
  leftTables.push_back(makeIdTableFromVector({{4, 14}, {5, 15}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector({{V(2), V(22)}}));
  rightTables.push_back(makeIdTableFromVector({{3, 23}}));

  testExistsJoin(std::move(leftTables), std::move(rightTables),
                 std::move(expected));
}

// _____________________________________________________________________________
TEST(ExistsJoin, lazyExistsJoinWithUndefRight) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector({{U, V(10), T}, {V(1), V(11), T}}));
  expected.push_back(
      makeIdTableFromVector({{V(2), V(12), T}, {V(3), V(13), T}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector({{U, V(10)}, {V(1), V(11)}}));
  leftTables.push_back(makeIdTableFromVector({{2, 12}, {3, 13}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector({{U, V(20)}, {V(2), V(22)}}));

  testExistsJoin(std::move(leftTables), std::move(rightTables),
                 std::move(expected));
}

// _____________________________________________________________________________
TEST(ExistsJoin, lazyExistsJoinWithUndefLeft) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector(
      {{U, V(10), T}, {V(1), V(11), T}, {V(2), V(12), F}, {V(3), V(13), T}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector(
      {{U, V(10)}, {V(1), V(11)}, {V(2), V(12)}, {V(3), V(13)}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(
      makeIdTableFromVector({{V(1), V(101)}, {V(3), V(303)}}));

  testExistsJoin(std::move(leftTables), std::move(rightTables),
                 std::move(expected));
}

// _____________________________________________________________________________
TEST(ExistsJoin, lazyExistsJoinWithUndefLeftInSeparateTable) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector({{U, V(10), T}}));
  expected.push_back(makeIdTableFromVector(
      {{V(1), V(11), T}, {V(2), V(12), F}, {V(3), V(13), T}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector({{U, V(10)}}));
  leftTables.push_back(makeIdTableFromVector({{1, 11}, {2, 12}, {3, 13}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector({{1, 101}, {3, 303}}));

  testExistsJoin(std::move(leftTables), std::move(rightTables),
                 std::move(expected));
}

// _____________________________________________________________________________
TEST(ExistsJoin, lazyExistsJoinFastForwardsCorrectlyOnEmptyRight) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector({{U, V(10), F}}));
  expected.push_back(makeIdTableFromVector(
      {{V(1), V(11), F}, {V(2), V(12), F}, {V(3), V(13), F}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector({{U, V(10)}}));
  leftTables.push_back(makeIdTableFromVector({{1, 11}, {2, 12}, {3, 13}}));
  std::vector<IdTable> rightTables;

  testExistsJoin(std::move(leftTables), std::move(rightTables),
                 std::move(expected));
}

// _____________________________________________________________________________
TEST(ExistsJoin, lazyExistsJoinSkipsEmptyTablesOnTheRight) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector(
      {{V(1), V(11), F}, {V(2), V(12), F}, {V(3), V(13), F}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector({{1, 11}, {2, 12}, {3, 13}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(IdTable{2, leftTables.back().getAllocator()});

  testExistsJoin(std::move(leftTables), std::move(rightTables),
                 std::move(expected));
}

// _____________________________________________________________________________
TEST(ExistsJoin, lazyExistsJoinWithOneMaterializedTable) {
  auto qec = ad_utility::testing::getQec();

  {
    auto expected =
        makeIdTableFromVector({{U, 10, T}, {1, 11, F}, {2, 12, T}, {3, 13, F}});
    std::vector<IdTable> rightTables;
    rightTables.push_back(makeIdTableFromVector({{2, 22}}));

    auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{U, V(10)}, {1, 11}, {2, 12}, {3, 13}}),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}},
        false, std::vector<ColumnIndex>{0}, LocalVocab{}, std::nullopt, true);
    auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(rightTables),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?z"}},
        false, std::vector<ColumnIndex>{0});
    ExistsJoin existsJoin{qec, left, right, Variable{"?exists"}};

    qec->getQueryTreeCache().clearAll();

    auto result = existsJoin.computeResultOnlyForTesting(true);

    ASSERT_FALSE(result.isFullyMaterialized());

    auto lazyResult = result.idTables();
    auto it = lazyResult.begin();
    ASSERT_NE(it, lazyResult.end());

    EXPECT_EQ(it->idTable_, expected);

    EXPECT_EQ(++it, lazyResult.end());
  }

  {
    auto expected0 = makeIdTableFromVector({{U, 10, T}, {1, 11, F}});
    auto expected1 = makeIdTableFromVector({{2, 12, T}, {3, 13, F}});
    std::vector<IdTable> leftTables;
    leftTables.push_back(makeIdTableFromVector({{U, V(10)}, {V(1), V(11)}}));
    leftTables.push_back(makeIdTableFromVector({{2, 12}, {3, 13}}));

    auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(leftTables),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}},
        false, std::vector<ColumnIndex>{0});
    auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{V(2), V(22)}}),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?z"}},
        false, std::vector<ColumnIndex>{0}, LocalVocab{}, std::nullopt, true);
    ExistsJoin existsJoin{qec, left, right, Variable{"?exists"}};

    qec->getQueryTreeCache().clearAll();

    auto result = existsJoin.computeResultOnlyForTesting(true);

    ASSERT_FALSE(result.isFullyMaterialized());

    auto lazyResult = result.idTables();
    auto it = lazyResult.begin();
    ASSERT_NE(it, lazyResult.end());

    EXPECT_EQ(it->idTable_, expected0);
    ++it;
    ASSERT_NE(it, lazyResult.end());
    EXPECT_EQ(it->idTable_, expected1);

    EXPECT_EQ(++it, lazyResult.end());
  }
}

// _____________________________________________________________________________
TEST(ExistsJoin, lazyExistsJoinWithJoinColumnAtNonZeroIndex) {
  auto qec = ad_utility::testing::getQec();

  auto expected =
      makeIdTableFromVector({{10, U, T}, {11, 1, F}, {12, 2, T}, {13, 3, F}});
  std::vector<IdTable> leftTables;
  leftTables.push_back(
      makeIdTableFromVector({{V(10), U}, {11, 1}, {12, 2}, {13, 3}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector({{22, 2}}));

  auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(leftTables),
      std::vector<std::optional<Variable>>{Variable{"?y"}, Variable{"?x"}},
      false, std::vector<ColumnIndex>{1});
  auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(rightTables),
      std::vector<std::optional<Variable>>{Variable{"?z"}, Variable{"?x"}},
      false, std::vector<ColumnIndex>{1});
  ExistsJoin existsJoin{qec, left, right, Variable{"?exists"}};

  qec->getQueryTreeCache().clearAll();

  auto result = existsJoin.computeResultOnlyForTesting(true);

  ASSERT_FALSE(result.isFullyMaterialized());

  auto lazyResult = result.idTables();
  auto it = lazyResult.begin();
  ASSERT_NE(it, lazyResult.end());

  EXPECT_EQ(it->idTable_, expected);

  EXPECT_EQ(++it, lazyResult.end());
}

// _____________________________________________________________________________
TEST(ExistsJoin, lazyExistsJoinExceedingChunkSize) {
  {
    std::vector<IdTable> expected;
    expected.push_back(
        makeIdTableFromVector({{I(1), T}, {I(2), T}, {I(3), F}}));

    std::vector<IdTable> leftTables;
    leftTables.push_back(makeIdTableFromVector({{1}, {2}, {3}}, I));
    std::vector<IdTable> rightTables;
    rightTables.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, I(1)));
    rightTables.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, I(2)));

    testExistsJoin(std::move(leftTables), std::move(rightTables),
                   std::move(expected), true);
  }
  {
    auto addColumnFilledWith = [](bool value, IdTable& table) {
      table.addEmptyColumn();
      ql::ranges::fill(table.getColumn(table.numColumns() - 1),
                       Id::makeFromBool(value));
    };

    std::vector<IdTable> expected;
    expected.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, I(1)));
    addColumnFilledWith(true, expected.back());
    expected.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, I(2)));
    addColumnFilledWith(false, expected.back());
    expected.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, I(3)));
    addColumnFilledWith(true, expected.back());

    std::vector<IdTable> leftTables;
    leftTables.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, I(1)));
    leftTables.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, I(2)));
    leftTables.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, I(3)));
    std::vector<IdTable> rightTables;
    rightTables.push_back(makeIdTableFromVector({{1}, {3}}, I));

    testExistsJoin(std::move(leftTables), std::move(rightTables),
                   std::move(expected), true);
  }
}

// _____________________________________________________________________________
TEST(ExistsJoin, repeatingMatchesDontProduceDuplicates) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector({{0, 10, F}, {1, 11, T}}));
  expected.push_back(makeIdTableFromVector({{1, 110, T}}));
  expected.push_back(makeIdTableFromVector({{1, 111, T}}));
  expected.push_back(makeIdTableFromVector({{1, 12, T}, {2, 13, F}}));
  expected.push_back(makeIdTableFromVector({{2, 14, F}, {2, 15, F}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector({{0, 10}, {1, 11}}));
  leftTables.push_back(makeIdTableFromVector({{1, 110}}));
  leftTables.push_back(makeIdTableFromVector({{1, 111}}));
  leftTables.push_back(makeIdTableFromVector({{1, 12}, {2, 13}}));
  leftTables.push_back(makeIdTableFromVector({{2, 14}, {2, 15}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector({{1, 21}}));
  rightTables.push_back(makeIdTableFromVector({{1, 22}}));
  rightTables.push_back(makeIdTableFromVector({{3, 23}}));

  testExistsJoin(std::move(leftTables), std::move(rightTables),
                 std::move(expected));
}

// _____________________________________________________________________________
TEST(Exists, columnOriginatesFromGraphOrUndef) {
  using ad_utility::triple_component::Iri;
  auto* qec = getQec();
  auto values1 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}});
  auto values2 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?c"}});
  auto index = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::POS,
      SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                         Iri::fromIriref("<c>")});

  ExistsJoin existJoin1{qec, values1, values1, Variable{"?z"}};
  EXPECT_FALSE(existJoin1.columnOriginatesFromGraphOrUndef(Variable{"?a"}));
  EXPECT_FALSE(existJoin1.columnOriginatesFromGraphOrUndef(Variable{"?b"}));
  EXPECT_FALSE(existJoin1.columnOriginatesFromGraphOrUndef(Variable{"?z"}));
  EXPECT_THROW(
      existJoin1.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
      ad_utility::Exception);

  ExistsJoin existJoin2{qec, values1, values2, Variable{"?z"}};
  EXPECT_FALSE(existJoin2.columnOriginatesFromGraphOrUndef(Variable{"?a"}));
  EXPECT_FALSE(existJoin2.columnOriginatesFromGraphOrUndef(Variable{"?b"}));
  EXPECT_FALSE(existJoin2.columnOriginatesFromGraphOrUndef(Variable{"?z"}));
  EXPECT_THROW(existJoin2.columnOriginatesFromGraphOrUndef(Variable{"?c"}),
               ad_utility::Exception);
  EXPECT_THROW(
      existJoin2.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
      ad_utility::Exception);

  ExistsJoin existJoin3{qec, index, values1, Variable{"?z"}};
  EXPECT_TRUE(existJoin3.columnOriginatesFromGraphOrUndef(Variable{"?a"}));
  EXPECT_THROW(existJoin3.columnOriginatesFromGraphOrUndef(Variable{"?b"}),
               ad_utility::Exception);
  EXPECT_FALSE(existJoin3.columnOriginatesFromGraphOrUndef(Variable{"?z"}));
  EXPECT_THROW(
      existJoin3.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
      ad_utility::Exception);

  ExistsJoin existJoin4{qec, values1, index, Variable{"?z"}};
  EXPECT_FALSE(existJoin4.columnOriginatesFromGraphOrUndef(Variable{"?a"}));
  EXPECT_FALSE(existJoin4.columnOriginatesFromGraphOrUndef(Variable{"?b"}));
  EXPECT_FALSE(existJoin4.columnOriginatesFromGraphOrUndef(Variable{"?z"}));
  EXPECT_THROW(
      existJoin4.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
      ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(Exists, addExistsJoinsToSubtreeDoesntCollideForHiddenVariables) {
  auto* qec = getQec();

  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}});

  ParsedQuery query;
  query._rootGraphPattern._graphPatterns.push_back(
      parsedQuery::BasicGraphPattern{
          {SparqlTriple{TripleComponent{Variable{"?a"}}, iri("<something>"),
                        TripleComponent{Variable{"?b"}}}}});
  // Only add ?a to see if ?b remains hidden.
  query.selectClause().addVisibleVariable(Variable{"?a"});

  sparqlExpression::SparqlExpressionPimpl pimpl{
      std::make_shared<sparqlExpression::ExistsExpression>(std::move(query)),
      "dummy"};

  auto tree = ExistsJoin::addExistsJoinsToSubtree(
      pimpl, std::move(subtree), qec,
      std::make_shared<ad_utility::CancellationHandle<>>());

  const ExistsJoin& existsJoin =
      *std::dynamic_pointer_cast<ExistsJoin>(tree->getRootOperation());

  // Even though both variables match, only one of them should be joined.
  EXPECT_THAT(existsJoin.joinColumns_,
              ::testing::ElementsAre(std::array<ColumnIndex, 2>{0, 0}));
}

// _____________________________________________________________________________
TEST(Exists, cacheKeyDiffersForDifferentJoinColumns) {
  auto* qec = getQec();

  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}});

  ParsedQuery query;
  query._rootGraphPattern._graphPatterns.push_back(
      parsedQuery::BasicGraphPattern{
          {SparqlTriple{TripleComponent{Variable{"?a"}}, iri("<something>"),
                        TripleComponent{Variable{"?b"}}}}});

  query.selectClause().addVisibleVariable(Variable{"?a"});

  sparqlExpression::SparqlExpressionPimpl pimpl1{
      std::make_shared<sparqlExpression::ExistsExpression>(query), "dummy"};

  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();

  auto tree1 =
      ExistsJoin::addExistsJoinsToSubtree(pimpl1, subtree, qec, handle);

  query.selectClause().addVisibleVariable(Variable{"?b"});

  sparqlExpression::SparqlExpressionPimpl pimpl2{
      std::make_shared<sparqlExpression::ExistsExpression>(std::move(query)),
      "dummy"};

  auto tree2 = ExistsJoin::addExistsJoinsToSubtree(pimpl2, std::move(subtree),
                                                   qec, std::move(handle));

  EXPECT_NE(tree1->getCacheKey(), tree2->getCacheKey());
}
