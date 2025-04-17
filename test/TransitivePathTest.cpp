// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//         Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include <gmock/gmock.h>

#include <limits>
#include <memory>

#include "./util/IdTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "engine/QueryExecutionTree.h"
#include "engine/TransitivePathBase.h"
#include "engine/ValuesForTesting.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

using ad_utility::testing::getQec;
namespace {
auto V = ad_utility::testing::VocabId;
using Vars = std::vector<std::optional<Variable>>;

}  // namespace

// The first bool indicates if binary search should be used (true) or hash map
// based search (false). The second bool indicates if the result should be
// requested lazily.
class TransitivePathTest
    : public testing::TestWithParam<std::tuple<bool, bool>> {
 public:
  [[nodiscard]] static std::pair<std::shared_ptr<TransitivePathBase>,
                                 QueryExecutionContext*>
  makePath(IdTable input, Vars vars, TransitivePathSide left,
           TransitivePathSide right, size_t minDist, size_t maxDist,
           std::optional<std::string> turtleInput = std::nullopt) {
    bool useBinSearch = std::get<0>(GetParam());
    auto qec = getQec(std::move(turtleInput));
    auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(input), vars);
    return {TransitivePathBase::makeTransitivePath(
                qec, std::move(subtree), std::move(left), std::move(right),
                minDist, maxDist, useBinSearch),
            qec};
  }

  // ___________________________________________________________________________
  [[nodiscard]] static std::shared_ptr<TransitivePathBase> makePathUnbound(
      IdTable input, Vars vars, TransitivePathSide left,
      TransitivePathSide right, size_t minDist, size_t maxDist,
      std::optional<std::string> turtleInput = std::nullopt) {
    auto [T, qec] =
        makePath(std::move(input), vars, std::move(left), std::move(right),
                 minDist, maxDist, std::move(turtleInput));
    return T;
  }

  // Create bound transitive path with a side table that is either a single
  // table or multiple ones.
  [[nodiscard]] static std::shared_ptr<TransitivePathBase> makePathBound(
      bool isLeft, IdTable input, Vars vars,
      std::variant<IdTable, std::vector<IdTable>> sideTable,
      size_t sideTableCol, Vars sideVars, TransitivePathSide left,
      TransitivePathSide right, size_t minDist, size_t maxDist,
      bool forceFullyMaterialized = false) {
    auto [T, qec] = makePath(std::move(input), vars, std::move(left),
                             std::move(right), minDist, maxDist);
    auto operation =
        std::holds_alternative<IdTable>(sideTable)
            ? ad_utility::makeExecutionTree<ValuesForTesting>(
                  qec, std::move(std::get<IdTable>(sideTable)), sideVars, false,
                  std::vector<ColumnIndex>{sideTableCol}, LocalVocab{},
                  std::nullopt, forceFullyMaterialized)
            : ad_utility::makeExecutionTree<ValuesForTesting>(
                  qec, std::move(std::get<std::vector<IdTable>>(sideTable)),
                  sideVars, false, std::vector<ColumnIndex>{sideTableCol});
    auto boundPath = isLeft ? T->bindLeftSide(operation, sideTableCol)
                            : T->bindRightSide(operation, sideTableCol);

    EXPECT_TRUE(boundPath->isBoundOrId());
    return boundPath;
  }

  // ___________________________________________________________________________
  static std::vector<IdTable> split(const IdTable& idTable) {
    std::vector<IdTable> result;
    for (const auto& row : idTable) {
      result.emplace_back(idTable.numColumns(), idTable.getAllocator());
      result.back().push_back(row);
    }
    return result;
  }

  // ___________________________________________________________________________
  static bool requestLaziness() { return std::get<1>(GetParam()); }

  // ___________________________________________________________________________
  void assertResultMatchesIdTable(const Result& result, const IdTable& expected,
                                  ad_utility::source_location loc =
                                      ad_utility::source_location::current()) {
    auto t = generateLocationTrace(loc);
    using ::testing::UnorderedElementsAreArray;
    ASSERT_NE(result.isFullyMaterialized(), requestLaziness());
    if (requestLaziness()) {
      const auto& [idTable, localVocab] =
          aggregateTables(std::move(result.idTables()), expected.numColumns());
      EXPECT_THAT(idTable, UnorderedElementsAreArray(expected));
    } else {
      EXPECT_THAT(result.idTable(), UnorderedElementsAreArray(expected));
    }
  }

  // Call testCase three times with differing arguments. This is used to test
  // scenarios where the same input table is delivered in different splits
  // either wrapped within a generator or as a single table.
  static void runTestWithForcedSideTableScenarios(
      const std::invocable<std::variant<IdTable, std::vector<IdTable>>,
                           bool> auto& testCase,
      IdTable idTable,
      ad_utility::source_location loc =
          ad_utility::source_location::current()) {
    auto trace = generateLocationTrace(loc);
    testCase(idTable.clone(), false);
    testCase(split(idTable), false);
    testCase(idTable.clone(), true);
  }
};

// _____________________________________________________________________________
TEST_P(TransitivePathTest, idToId) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {1, 3}, {2, 3}});

  auto expected = makeIdTableFromVector({{0, 3}});

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, V(3), 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, std::numeric_limits<size_t>::max());

  EXPECT_TRUE(T->isBoundOrId());

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, idToVar) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {1, 3}, {2, 3}});

  auto expected = makeIdTableFromVector({{0, 1}, {0, 2}, {0, 3}});

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, std::numeric_limits<size_t>::max());

  EXPECT_TRUE(T->isBoundOrId());

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, varToId) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {1, 3}, {2, 3}});

  auto expected = makeIdTableFromVector({
      {2, 3},
      {1, 3},
      {0, 3},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, V(3), 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, std::numeric_limits<size_t>::max());

  EXPECT_TRUE(T->isBoundOrId());

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, idToVarMinLengthZero) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {1, 3}, {2, 3}});

  auto expected = makeIdTableFromVector({{0, 0}, {0, 1}, {0, 2}, {0, 3}});

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 0, std::numeric_limits<size_t>::max());

  EXPECT_TRUE(T->isBoundOrId());

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, varToIdMinLengthZero) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {1, 3}, {2, 3}});

  auto expected = makeIdTableFromVector({
      {3, 3},
      {2, 3},
      {1, 3},
      {0, 3},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, V(3), 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 0, std::numeric_limits<size_t>::max());

  EXPECT_TRUE(T->isBoundOrId());

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, varTovar) {
  auto sub = makeIdTableFromVector({
      {0, 1},
      {1, 2},
      {1, 3},
      {2, 3},
  });

  auto expected = makeIdTableFromVector({
      {0, 1},
      {0, 2},
      {0, 3},
      {1, 2},
      {1, 3},
      {2, 3},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, std::numeric_limits<size_t>::max());

  EXPECT_FALSE(T->isBoundOrId());

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, unlimitedMaxLength) {
  auto sub = makeIdTableFromVector({{0, 2},
                                    {2, 4},
                                    {4, 7},
                                    {0, 7},
                                    {3, 3},
                                    {7, 0},
                                    // Disconnected component.
                                    {V(10), V(11)}});

  auto expected = makeIdTableFromVector({{0, 2},
                                         {0, 4},
                                         {0, 7},
                                         {0, 0},
                                         {2, 4},
                                         {2, 7},
                                         {2, 0},
                                         {2, 2},
                                         {4, 7},
                                         {4, 0},
                                         {4, 2},
                                         {4, 4},
                                         {3, 3},
                                         {7, 0},
                                         {7, 2},
                                         {7, 4},
                                         {7, 7},
                                         {V(10), V(11)}});

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, std::numeric_limits<size_t>::max());

  EXPECT_FALSE(T->isBoundOrId());

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, idToLeftBound) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {1, 3}, {2, 3}, {3, 4}});

  auto leftOpTable = makeIdTableFromVector({
      {0, 1},
      {0, 2},
      {0, 3},
  });

  auto expected = makeIdTableFromVector({
      {1, 4, 0},
      {2, 4, 0},
      {3, 4, 0},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, V(4), 1);
  runTestWithForcedSideTableScenarios(
      [&](auto tableVariant, bool forceFullyMaterialized) {
        auto T = makePathBound(
            true, sub.clone(), {Variable{"?start"}, Variable{"?target"}},
            std::move(tableVariant), 1, {Variable{"?x"}, Variable{"?start"}},
            left, right, 0, std::numeric_limits<size_t>::max(),
            forceFullyMaterialized);

        auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
        assertResultMatchesIdTable(resultTable, expected);
      },
      leftOpTable.clone());
  runTestWithForcedSideTableScenarios(
      [&](auto tableVariant, bool forceFullyMaterialized) {
        auto T = makePathBound(
            true, sub.clone(), {Variable{"?start"}, Variable{"?target"}},
            std::move(tableVariant), 1, {std::nullopt, Variable{"?start"}},
            left, right, 0, std::numeric_limits<size_t>::max(),
            forceFullyMaterialized);

        auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
        assertResultMatchesIdTable(resultTable, expected);
      },
      std::move(leftOpTable));
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, idToRightBound) {
  auto sub = makeIdTableFromVector({
      {0, 1},
      {1, 2},
      {1, 3},
      {2, 3},
      {3, 4},
  });

  auto rightOpTable = makeIdTableFromVector({
      {2, 5},
      {3, 5},
      {4, 5},
  });

  auto expected = makeIdTableFromVector({
      {0, 2, 5},
      {0, 3, 5},
      {0, 4, 5},
  });

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  runTestWithForcedSideTableScenarios(
      [&](auto tableVariant, bool forceFullyMaterialized) {
        auto T = makePathBound(
            false, sub.clone(), {Variable{"?start"}, Variable{"?target"}},
            std::move(tableVariant), 0, {Variable{"?target"}, Variable{"?x"}},
            left, right, 0, std::numeric_limits<size_t>::max(),
            forceFullyMaterialized);

        auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
        assertResultMatchesIdTable(resultTable, expected);
      },
      rightOpTable.clone());
  runTestWithForcedSideTableScenarios(
      [&](auto tableVariant, bool forceFullyMaterialized) {
        auto T = makePathBound(
            false, sub.clone(), {Variable{"?start"}, Variable{"?target"}},
            std::move(tableVariant), 0, {Variable{"?target"}, std::nullopt},
            left, right, 0, std::numeric_limits<size_t>::max(),
            forceFullyMaterialized);

        auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
        assertResultMatchesIdTable(resultTable, expected);
      },
      std::move(rightOpTable));
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, leftBoundToVar) {
  auto sub = makeIdTableFromVector({
      {1, 2},
      {2, 3},
      {2, 4},
      {3, 4},
  });

  auto leftOpTable = makeIdTableFromVector({
      {0, 1},
      {0, 2},
      {0, 3},
  });

  auto expected = makeIdTableFromVector({
      {1, 1, 0},
      {1, 2, 0},
      {1, 3, 0},
      {1, 4, 0},
      {2, 2, 0},
      {2, 3, 0},
      {2, 4, 0},
      {3, 3, 0},
      {3, 4, 0},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  runTestWithForcedSideTableScenarios(
      [&](auto tableVariant, bool forceFullyMaterialized) {
        auto T = makePathBound(
            true, sub.clone(), {Variable{"?start"}, Variable{"?target"}},
            std::move(tableVariant), 1, {Variable{"?x"}, Variable{"?start"}},
            left, right, 0, std::numeric_limits<size_t>::max(),
            forceFullyMaterialized);

        auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
        assertResultMatchesIdTable(resultTable, expected);
      },
      std::move(leftOpTable));
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, rightBoundToVar) {
  auto sub = makeIdTableFromVector({
      {1, 2},
      {2, 3},
      {2, 4},
      {3, 4},
  });

  auto rightOpTable = makeIdTableFromVector({
      {2, 5},
      {3, 5},
      {4, 5},
  });

  auto expected = makeIdTableFromVector({
      {1, 2, 5},
      {1, 3, 5},
      {1, 4, 5},
      {2, 2, 5},
      {2, 3, 5},
      {2, 4, 5},
      {3, 3, 5},
      {3, 4, 5},
      {4, 4, 5},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  runTestWithForcedSideTableScenarios(
      [&](auto tableVariant, bool forceFullyMaterialized) {
        auto T = makePathBound(
            false, sub.clone(), {Variable{"?start"}, Variable{"?target"}},
            std::move(tableVariant), 0, {Variable{"?target"}, Variable{"?x"}},
            left, right, 0, std::numeric_limits<size_t>::max(),
            forceFullyMaterialized);

        auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
        assertResultMatchesIdTable(resultTable, expected);
      },
      std::move(rightOpTable));
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, startNodesWithNoMatchesRightBound) {
  auto sub = makeIdTableFromVector({
      {1, 2},
      {3, 4},
  });

  auto rightOpTable = makeIdTableFromVector({
      {2, 5},
      {3, 6},
      {4, 7},
  });

  auto expected = makeIdTableFromVector({
      {1, 2, 5},
      {3, 4, 7},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathBound(
      false, sub.clone(), {Variable{"?start"}, Variable{"?target"}},
      split(rightOpTable), 0, {Variable{"?target"}, Variable{"?x"}}, left,
      right, 1, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, emptySideTable) {
  auto sub = makeIdTableFromVector({
      {1, 2},
      {3, 4},
  });

  auto expected = makeIdTableFromVector({});

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathBound(true, sub.clone(),
                         {Variable{"?start"}, Variable{"?target"}},
                         std::vector<IdTable>{}, 0, {Variable{"?start"}}, left,
                         right, 0, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, startNodesWithNoMatchesLeftBound) {
  auto sub = makeIdTableFromVector({
      {1, 2},
      {3, 4},
  });

  auto leftOpTable = makeIdTableFromVector({
      {2, 5},
      {3, 6},
      {4, 7},
  });

  auto expected = makeIdTableFromVector({
      {3, 4, 6},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathBound(
      true, sub.clone(), {Variable{"?start"}, Variable{"?target"}},
      split(leftOpTable), 0, {Variable{"?start"}, Variable{"?x"}}, left, right,
      1, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, maxLength2FromVariable) {
  auto sub = makeIdTableFromVector({
      {0, 2},
      {2, 4},
      {4, 7},
      {0, 7},
      {3, 3},
      {7, 0},
      // Disconnected component.
      {10, 11},
  });

  auto expected = makeIdTableFromVector({{0, 2},
                                         {0, 4},
                                         {0, 7},
                                         {0, 0},
                                         {2, 4},
                                         {2, 7},
                                         {4, 7},
                                         {4, 0},
                                         {3, 3},
                                         {7, 0},
                                         {7, 2},
                                         {7, 7},
                                         {10, 11}});

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, 2);

  EXPECT_FALSE(T->isBoundOrId());
  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, maxLength2FromId) {
  auto sub = makeIdTableFromVector({
      {0, 2},
      {2, 4},
      {4, 7},
      {0, 7},
      {3, 3},
      {7, 0},
      // Disconnected component.
      {10, 11},
  });

  auto expected = makeIdTableFromVector({
      {7, 0},
      {7, 2},
      {7, 7},
  });

  TransitivePathSide left(std::nullopt, 0, V(7), 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, 2);

  EXPECT_TRUE(T->isBoundOrId());
  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, maxLength2ToId) {
  auto sub = makeIdTableFromVector({
      {0, 2},
      {2, 4},
      {4, 7},
      {0, 7},
      {3, 3},
      {7, 0},
      // Disconnected component.
      {10, 11},
  });

  auto expected = makeIdTableFromVector({
      {0, 2},
      {7, 2},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, V(2), 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, 2);

  EXPECT_TRUE(T->isBoundOrId());
  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, zeroLength) {
  std::string index = "<a> a 0 , 1 , 2 , 4 , 7 , 10 , 11 .";

  auto sub = makeIdTableFromVector(
      {
          {0, 2},
          {2, 4},
          {4, 7},
          {0, 7},
          {10, 11},
      },
      Id::makeFromInt);

  auto expected = makeIdTableFromVector({{0, 0},
                                         {0, 2},
                                         {0, 4},
                                         {0, 7},
                                         {1, 1},
                                         {2, 2},
                                         {2, 4},
                                         {2, 7},
                                         {4, 4},
                                         {4, 7},
                                         {10, 10},
                                         {10, 11},
                                         {7, 7},
                                         {11, 11}},
                                        Id::makeFromInt);
  // Add missing subject <a> before comparing
  expected.emplace_back();
  expected.at(expected.size() - 1, 0) =
      Id::makeFromVocabIndex(VocabIndex::make(0));
  expected.at(expected.size() - 1, 1) =
      Id::makeFromVocabIndex(VocabIndex::make(0));

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathUnbound(
      std::move(sub), {Variable{"?start"}, Variable{"?target"}}, left, right, 0,
      std::numeric_limits<size_t>::max(), std::move(index));

  EXPECT_FALSE(T->isBoundOrId());

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, zeroLengthWithLiteralsNotInIndex) {
  std::string index = "<a> a 0 , 1 , 2 , 4 .";

  auto sub = makeIdTableFromVector(
      {
          {0, 2},
          {2, 4},
      },
      Id::makeFromInt);

  auto expected = IdTable{2, ad_utility::testing::makeAllocator()};

  {
    TransitivePathSide left(std::nullopt, 0, 1337, 0);
    TransitivePathSide right(std::nullopt, 1, 1337, 1);
    auto T = makePathUnbound(
        sub.clone(), {Variable{"?start"}, Variable{"?target"}}, left, right, 0,
        std::numeric_limits<size_t>::max(), index);

    EXPECT_TRUE(T->isBoundOrId());

    auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
    assertResultMatchesIdTable(resultTable, expected);
  }

  {
    TransitivePathSide left(std::nullopt, 0, 1337, 0);
    TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
    auto T = makePathUnbound(
        sub.clone(), {Variable{"?start"}, Variable{"?target"}}, left, right, 0,
        std::numeric_limits<size_t>::max(), index);

    EXPECT_TRUE(T->isBoundOrId());

    auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
    assertResultMatchesIdTable(resultTable, expected);
  }

  {
    TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
    TransitivePathSide right(std::nullopt, 1, 1337, 1);
    auto T = makePathUnbound(
        std::move(sub), {Variable{"?start"}, Variable{"?target"}}, left, right,
        0, std::numeric_limits<size_t>::max(), std::move(index));

    EXPECT_TRUE(T->isBoundOrId());

    auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
    assertResultMatchesIdTable(resultTable, expected);
  }
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, literalsNotInIndex) {
  std::string index = "<a> a 0 , 1 , 2 , 4 .";

  auto sub = makeIdTableFromVector(
      {
          {0, 2},
          {2, 4},
      },
      Id::makeFromInt);

  auto expected = IdTable{2, ad_utility::testing::makeAllocator()};

  {
    TransitivePathSide left(std::nullopt, 0, 1337, 0);
    TransitivePathSide right(std::nullopt, 1, 1337, 1);
    auto T = makePathUnbound(
        sub.clone(), {Variable{"?start"}, Variable{"?target"}}, left, right, 1,
        std::numeric_limits<size_t>::max(), index);

    EXPECT_TRUE(T->isBoundOrId());

    auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
    assertResultMatchesIdTable(resultTable, expected);
  }

  {
    TransitivePathSide left(std::nullopt, 0, 1337, 0);
    TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
    auto T = makePathUnbound(
        sub.clone(), {Variable{"?start"}, Variable{"?target"}}, left, right, 1,
        std::numeric_limits<size_t>::max(), index);

    EXPECT_TRUE(T->isBoundOrId());

    auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
    assertResultMatchesIdTable(resultTable, expected);
  }

  {
    TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
    TransitivePathSide right(std::nullopt, 1, 1337, 1);
    auto T = makePathUnbound(
        std::move(sub), {Variable{"?start"}, Variable{"?target"}}, left, right,
        1, std::numeric_limits<size_t>::max(), std::move(index));

    EXPECT_TRUE(T->isBoundOrId());

    auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
    assertResultMatchesIdTable(resultTable, expected);
  }
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, literalsNotInIndexButInDeltaTriples) {
  using ad_utility::triple_component::Literal;
  std::string index = "<a> a 0 , 1 , 2 , 4 .";
  std::string literal = "my-literal";

  // Simulate entries in the delta triples by using entries that are not in the
  // index
  LocalVocab localVocab;
  auto id = Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
      LocalVocabEntry{Literal::literalWithoutQuotes(literal)}));
  auto sub = makeIdTableFromVector({
      {id, id},
  });

  IdTable expected = sub.clone();

  TripleComponent reference = Literal::literalWithoutQuotes(literal);

  {
    TransitivePathSide left(std::nullopt, 0, reference, 0);
    TransitivePathSide right(std::nullopt, 1, reference, 1);
    auto T = makePathUnbound(
        sub.clone(), {Variable{"?start"}, Variable{"?target"}}, left, right, 1,
        std::numeric_limits<size_t>::max(), index);

    EXPECT_TRUE(T->isBoundOrId());

    auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
    assertResultMatchesIdTable(resultTable, expected);
  }

  {
    TransitivePathSide left(std::nullopt, 0, reference, 0);
    TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
    auto T = makePathUnbound(
        sub.clone(), {Variable{"?start"}, Variable{"?target"}}, left, right, 1,
        std::numeric_limits<size_t>::max(), index);

    EXPECT_TRUE(T->isBoundOrId());

    auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
    assertResultMatchesIdTable(resultTable, expected);
  }

  {
    TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
    TransitivePathSide right(std::nullopt, 1, reference, 1);
    auto T = makePathUnbound(
        std::move(sub), {Variable{"?start"}, Variable{"?target"}}, left, right,
        1, std::numeric_limits<size_t>::max(), std::move(index));

    EXPECT_TRUE(T->isBoundOrId());

    auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
    assertResultMatchesIdTable(resultTable, expected);
  }
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, clone) {
  auto sub = makeIdTableFromVector({{0, 2}});

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  {
    auto transitivePath =
        makePathUnbound(sub.clone(), {Variable{"?start"}, Variable{"?target"}},
                        left, right, 0, std::numeric_limits<size_t>::max());

    EXPECT_FALSE(transitivePath->isBoundOrId());

    auto clone = transitivePath->clone();
    ASSERT_TRUE(clone);
    EXPECT_THAT(*transitivePath, IsDeepCopy(*clone));
    EXPECT_EQ(clone->getDescriptor(), transitivePath->getDescriptor());
  }
  {
    auto transitivePath = makePathBound(
        false, std::move(sub), {Variable{"?start"}, Variable{"?target"}},
        sub.clone(), 0, {Variable{"?start"}, Variable{"?other"}}, left, right,
        0, std::numeric_limits<size_t>::max());

    auto clone = transitivePath->clone();
    ASSERT_TRUE(clone);
    EXPECT_THAT(*transitivePath, IsDeepCopy(*clone));
    EXPECT_EQ(clone->getDescriptor(), transitivePath->getDescriptor());
  }
  {
    auto transitivePath = makePathBound(
        true, std::move(sub), {Variable{"?start"}, Variable{"?target"}},
        sub.clone(), 0, {Variable{"?target"}, Variable{"?other"}}, left, right,
        0, std::numeric_limits<size_t>::max());

    auto clone = transitivePath->clone();
    ASSERT_TRUE(clone);
    EXPECT_THAT(*transitivePath, IsDeepCopy(*clone));
    EXPECT_EQ(clone->getDescriptor(), transitivePath->getDescriptor());
  }
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, sameVariableOnBothSidesBound) {
  auto sub = makeIdTableFromVector({
      {1, 2},
      {2, 1},
      {3, 4},
      {4, 3},
  });

  auto sideTable = makeIdTableFromVector({
      {1},
      {2},
  });

  auto expected = makeIdTableFromVector({
      {1, 1},
      {2, 2},
  });

  {
    TransitivePathSide left(std::nullopt, 0, Variable{"?var"}, 0);
    TransitivePathSide right(std::nullopt, 1, Variable{"?var"}, 1);
    auto T = makePathBound(false, sub.clone(),
                           {Variable{"?internal1"}, Variable{"?internal2"}},
                           split(sideTable), 0, {Variable{"?var"}}, left, right,
                           1, std::numeric_limits<size_t>::max());

    auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
    assertResultMatchesIdTable(resultTable, expected);
  }
  {
    TransitivePathSide left(std::nullopt, 0, Variable{"?var"}, 0);
    TransitivePathSide right(std::nullopt, 1, Variable{"?var"}, 1);
    auto T = makePathBound(true, sub.clone(),
                           {Variable{"?internal1"}, Variable{"?internal2"}},
                           split(sideTable), 0, {Variable{"?var"}}, left, right,
                           1, std::numeric_limits<size_t>::max());

    auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
    assertResultMatchesIdTable(resultTable, expected);
  }
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, sameVariableOnBothSidesUnbound) {
  auto sub = makeIdTableFromVector({
      {1, 2},
      {2, 1},
      {3, 4},
      {4, 3},
  });

  auto expected = makeIdTableFromVector({
      {1, 1},
      {2, 2},
      {3, 3},
      {4, 4},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?var"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?var"}, 1);
  auto T = makePathUnbound(std::move(sub),
                           {Variable{"?internal1"}, Variable{"?internal2"}},
                           left, right, 1, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, sameVariableResultsInDifferentCacheKey) {
  auto sub = makeIdTableFromVector({
      {1, 2},
  });

  TransitivePathSide left{std::nullopt, 0, Variable{"?var"}, 0};
  TransitivePathSide right1{std::nullopt, 1, Variable{"?var"}, 1};
  TransitivePathSide right2{std::nullopt, 1, Variable{"?other"}, 1};
  auto T1 = makePathUnbound(
      std::move(sub), {Variable{"?internal1"}, Variable{"?internal2"}}, left,
      right1, 1, std::numeric_limits<size_t>::max());
  auto T2 = makePathUnbound(
      std::move(sub), {Variable{"?internal1"}, Variable{"?internal2"}}, left,
      right2, 1, std::numeric_limits<size_t>::max());

  EXPECT_NE(T1->getCacheKey(), T2->getCacheKey());
}

// _____________________________________________________________________________
INSTANTIATE_TEST_SUITE_P(
    TransitivePathTestSuite, TransitivePathTest,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()),
    [](const testing::TestParamInfo<std::tuple<bool, bool>>& info) {
      std::string result = std::get<0>(info.param) ? "TransitivePathBinSearch"
                                                   : "TransitivePathHashMap";
      result += std::get<1>(info.param) ? "Lazy" : "FullyMaterialized";
      return result;
    });
