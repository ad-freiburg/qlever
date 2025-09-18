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
#include "engine/TransitivePathBinSearch.h"
#include "engine/TransitivePathHashMap.h"
#include "engine/ValuesForTesting.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

using ad_utility::testing::getQec;
namespace {
auto V = ad_utility::testing::VocabId;
using Vars = std::vector<std::optional<Variable>>;
auto U = Id::makeUndefined();
using namespace ::testing;
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
           std::optional<std::string> turtleInput = std::nullopt,
           const std::optional<Variable>& graphVariable = std::nullopt) {
    bool useBinSearch = std::get<0>(GetParam());
    ad_utility::testing::TestIndexConfig config;
    config.turtleInput = std::move(turtleInput);
    config.indexType = graphVariable.has_value() ? qlever::Filetype::NQuad
                                                 : qlever::Filetype::Turtle;
    auto qec = getQec(std::move(config));
    // Clear the cache to avoid crosstalk between tests.
    qec->clearCacheUnpinnedOnly();
    auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(input), vars);
    return {TransitivePathBase::makeTransitivePath(
                qec, std::move(subtree), std::move(left), std::move(right),
                minDist, maxDist, useBinSearch, {}, graphVariable),
            qec};
  }

  // ___________________________________________________________________________
  [[nodiscard]] static std::shared_ptr<TransitivePathBase> makePathUnbound(
      IdTable input, Vars vars, TransitivePathSide left,
      TransitivePathSide right, size_t minDist, size_t maxDist,
      std::optional<std::string> turtleInput = std::nullopt,
      const std::optional<Variable>& graphVariable = std::nullopt) {
    auto [T, qec] =
        makePath(std::move(input), vars, std::move(left), std::move(right),
                 minDist, maxDist, std::move(turtleInput), graphVariable);
    return T;
  }

  // Create bound transitive path with a side table that is either a single
  // table or multiple ones.
  [[nodiscard]] static std::shared_ptr<TransitivePathBase> makePathBound(
      bool isLeft, IdTable input, Vars vars,
      std::variant<IdTable, std::vector<IdTable>> sideTable,
      size_t sideTableCol, Vars sideVars, TransitivePathSide left,
      TransitivePathSide right, size_t minDist, size_t maxDist,
      bool forceFullyMaterialized = false,
      const std::optional<Variable>& graphVariable = std::nullopt,
      std::optional<std::string> turtleInput = std::nullopt) {
    auto [T, qec] =
        makePath(std::move(input), vars, std::move(left), std::move(right),
                 minDist, maxDist, std::move(turtleInput), graphVariable);
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
  static void assertResultMatchesIdTable(
      const Result& result, const IdTable& expected,
      ad_utility::source_location loc =
          ad_utility::source_location::current()) {
    auto t = generateLocationTrace(loc);
    using ::testing::UnorderedElementsAreArray;
    ASSERT_NE(result.isFullyMaterialized(), requestLaziness());
    if (requestLaziness()) {
      const auto& [idTable, localVocab] =
          aggregateTables(result.idTables(), expected.numColumns());
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
TEST_P(TransitivePathTest, knownEmptyResult) {
  auto nonEmptySub = makeIdTableFromVector({{1, 1}});
  IdTable emptySub{2, nonEmptySub.getAllocator()};
  Vars vars = {Variable{"?start"}, Variable{"?target"}};

  TransitivePathSide fixedLeft{std::nullopt, 0, V(0), 0};
  TransitivePathSide fixedRight{std::nullopt, 1, V(3), 1};

  {
    auto T = makePathUnbound(emptySub.clone(), vars, fixedLeft, fixedRight, 1,
                             std::numeric_limits<size_t>::max());
    EXPECT_TRUE(T->isBoundOrId());
    EXPECT_TRUE(T->knownEmptyResult());
  }
  {
    auto T = makePathUnbound(nonEmptySub.clone(), vars, fixedLeft, fixedRight,
                             1, std::numeric_limits<size_t>::max());
    EXPECT_TRUE(T->isBoundOrId());
    EXPECT_FALSE(T->knownEmptyResult());
  }

  TransitivePathSide left{std::nullopt, 0, Variable{"?x"}, 0};
  TransitivePathSide right{std::nullopt, 1, Variable{"?y"}, 1};

  {
    auto T = makePathUnbound(emptySub.clone(), vars, left, right, 1,
                             std::numeric_limits<size_t>::max());
    EXPECT_FALSE(T->isBoundOrId());
    EXPECT_TRUE(T->knownEmptyResult());
  }
  {
    auto T = makePathUnbound(nonEmptySub.clone(), vars, left, right, 1,
                             std::numeric_limits<size_t>::max());
    EXPECT_FALSE(T->isBoundOrId());
    EXPECT_FALSE(T->knownEmptyResult());
  }

  for (bool isLeft : {true, false}) {
    {
      auto T = makePathBound(isLeft, emptySub.clone(), vars,
                             nonEmptySub.clone(), 0, vars, left, right, 0,
                             std::numeric_limits<size_t>::max());
      EXPECT_TRUE(T->isBoundOrId());
      EXPECT_FALSE(T->knownEmptyResult());
    }
    {
      auto T = makePathBound(isLeft, nonEmptySub.clone(), vars,
                             nonEmptySub.clone(), 0, vars, left, right, 0,
                             std::numeric_limits<size_t>::max());
      EXPECT_TRUE(T->isBoundOrId());
      EXPECT_FALSE(T->knownEmptyResult());
    }

    {
      auto T = makePathBound(isLeft, emptySub.clone(), vars, emptySub.clone(),
                             0, vars, left, right, 0,
                             std::numeric_limits<size_t>::max());
      EXPECT_TRUE(T->isBoundOrId());
      EXPECT_TRUE(T->knownEmptyResult());
    }
    {
      auto T = makePathBound(isLeft, nonEmptySub.clone(), vars,
                             emptySub.clone(), 0, vars, left, right, 0,
                             std::numeric_limits<size_t>::max());
      EXPECT_TRUE(T->isBoundOrId());
      EXPECT_TRUE(T->knownEmptyResult());
    }
  }
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
TEST_P(TransitivePathTest, boundToVarWithUndef) {
  auto sub = makeIdTableFromVector({
      {1, 2},
      {2, 3},
      {2, 4},
      {3, 4},
  });

  auto rightOpTable = makeIdTableFromVector({
      {Id::makeUndefined(), 10},
      {2, 11},
  });

  auto expected = makeIdTableFromVector({
      {1, 2, 10},
      {1, 3, 10},
      {1, 4, 10},
      {2, 3, 10},
      {2, 4, 10},
      {3, 4, 10},
      {2, 3, 11},
      {2, 4, 11},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathBound(
      true, std::move(sub), {Variable{"?internal1"}, Variable{"?internal2"}},
      std::move(rightOpTable), 0, {Variable{"?start"}, Variable{"?other"}},
      left, right, 1, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, boundToVarWithUndefWithGraph) {
  auto sub = makeIdTableFromVector({
      {1, 2, 100},
      {2, 3, 100},
      {2, 4, 101},
      {3, 4, 101},
  });

  auto rightOpTable = makeIdTableFromVector({
      {Id::makeUndefined(), 10},
      {2, 11},
  });

  auto expected = makeIdTableFromVector({
      {1, 2, 10, 100},
      {1, 3, 10, 100},
      {2, 3, 10, 100},
      {2, 4, 10, 101},
      {3, 4, 10, 101},
      {2, 3, 11, 100},
      {2, 4, 11, 101},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathBound(
      true, std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}},
      std::move(rightOpTable), 0, {Variable{"?start"}, Variable{"?other"}},
      left, right, 1, std::numeric_limits<size_t>::max(), false,
      Variable{"?g"});

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, boundToVarWithUndefGraph) {
  auto sub = makeIdTableFromVector({
      {1, 2, 100},
      {2, 3, 100},
      {2, 4, 101},
      {3, 4, 101},
  });

  auto rightOpTable = makeIdTableFromVector({
      {1, 11, 100},
      {2, 10, Id::makeUndefined()},
  });

  auto expected = makeIdTableFromVector({
      {1, 2, 11, 100},
      {1, 3, 11, 100},
      {2, 3, 10, 100},
      {2, 4, 10, 101},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathBound(
      true, std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}},
      std::move(rightOpTable), 0,
      {Variable{"?start"}, Variable{"?other"}, Variable{"?g"}}, left, right, 1,
      std::numeric_limits<size_t>::max(), false, Variable{"?g"});

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
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
  // Note: the entries in this local vocab are destroyed when this test is done.
  // It is therefore crucial that the `makePath...` functions clear the cache,
  // s.t. subsequent tests do not read results with a dangling local vocab from
  // the cache (Currently the indexes used for testing are `static` which should
  // be changed in the future).
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
TEST_P(TransitivePathTest, columnOriginatesFromGraphOrUndef) {
  auto sub = makeIdTableFromVector({{0, 2}});

  {
    TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
    TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
    auto T = makePathBound(
        false, sub.clone(), {Variable{"?internal1"}, Variable{"?internal2"}},
        sub.clone(), 0, {Variable{"?start"}, Variable{"?other"}}, left, right,
        0, std::numeric_limits<size_t>::max());

    EXPECT_TRUE(T->columnOriginatesFromGraphOrUndef(Variable{"?start"}));
    EXPECT_TRUE(T->columnOriginatesFromGraphOrUndef(Variable{"?target"}));
    EXPECT_FALSE(T->columnOriginatesFromGraphOrUndef(Variable{"?other"}));
    EXPECT_THROW(T->columnOriginatesFromGraphOrUndef(Variable{"?internal1"}),
                 ad_utility::Exception);
    EXPECT_THROW(T->columnOriginatesFromGraphOrUndef(Variable{"?internal2"}),
                 ad_utility::Exception);
    EXPECT_THROW(T->columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
                 ad_utility::Exception);
  }

  {
    TransitivePathSide left(std::nullopt, 0, 1, 0);
    TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
    auto T = makePathUnbound(
        std::move(sub), {Variable{"?internal1"}, Variable{"?internal2"}}, left,
        right, 0, std::numeric_limits<size_t>::max());

    EXPECT_TRUE(T->columnOriginatesFromGraphOrUndef(Variable{"?target"}));
    EXPECT_THROW(T->columnOriginatesFromGraphOrUndef(Variable{"?internal1"}),
                 ad_utility::Exception);
    EXPECT_THROW(T->columnOriginatesFromGraphOrUndef(Variable{"?internal2"}),
                 ad_utility::Exception);
    EXPECT_THROW(T->columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
                 ad_utility::Exception);
  }
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, unboundGraphVariable) {
  auto sub = makeIdTableFromVector({
      {1, 2, 1},
      {3, 4, 1},
      {1, 2, 2},
      {2, 1, 2},
      {3, 4, 2},
      {4, 3, 2},
      {2, 1, 3},
      {4, 3, 3},
  });

  auto expected = makeIdTableFromVector({
      {1, 2, 1},
      {3, 4, 1},
      {1, 1, 2},
      {1, 2, 2},
      {2, 1, 2},
      {2, 2, 2},
      {3, 3, 2},
      {3, 4, 2},
      {4, 3, 2},
      {4, 4, 2},
      {2, 1, 3},
      {4, 3, 3},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathUnbound(
      std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}}, left,
      right, 1, std::numeric_limits<size_t>::max(), std::nullopt,
      {Variable{"?g"}});

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, unboundGraphVariableEmptyPath) {
  auto sub = makeIdTableFromVector({
      {0, 1, 0},
      {2, 3, 0},
      {0, 1, 1},
      {1, 0, 1},
      {2, 3, 1},
      {3, 2, 1},
      {1, 0, 2},
      {3, 2, 2},
  });

  auto expected = makeIdTableFromVector({
      {0, 1, 0},
      {0, 0, 0},
      {0, 0, 2},
      {2, 3, 0},
      {2, 2, 0},
      {2, 2, 2},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathUnbound(
      std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}}, left,
      right, 0, std::numeric_limits<size_t>::max(),
      "<a> <b> <c> <a> . <a> <b> <c> <c> .", {Variable{"?g"}});

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, graphVariableBoundToNonGraphOperation) {
  auto sub = makeIdTableFromVector({
      {0, 1, 0},
      {2, 3, 0},
      {0, 1, 1},
      {1, 0, 1},
      {2, 3, 1},
      {3, 2, 1},
      {1, 0, 2},
      {3, 2, 2},
  });

  auto side = makeIdTableFromVector({
      {0, 10},
      {0, 10},
      {2, 20},
  });

  auto expected = makeIdTableFromVector({
      {0, 1, 10, 0},
      {0, 0, 10, 1},
      {0, 1, 10, 1},
      {0, 1, 10, 0},
      {0, 0, 10, 1},
      {0, 1, 10, 1},
      {2, 2, 20, 1},
      {2, 3, 20, 0},
      {2, 3, 20, 1},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathBound(
      true, std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}},
      std::move(side), 0, {Variable{"?start"}, Variable{"?other"}}, left, right,
      1, std::numeric_limits<size_t>::max(), false, {Variable{"?g"}},
      "<a> <b> <c> <a> . <a> <b> <d> <b> .");

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, graphVariableBoundToNonGraphOperationEmptyPath) {
  auto sub = makeIdTableFromVector({
      {0, 1, 0},
      {2, 3, 0},
      {0, 1, 1},
      {1, 0, 1},
      {2, 3, 1},
      {3, 2, 1},
      {1, 0, 2},
      {3, 2, 2},
  });

  auto side = makeIdTableFromVector({
      {0, 10},
      {0, 10},
      {2, 20},
  });

  auto expected = makeIdTableFromVector({
      {0, 0, 10, 0},
      {0, 1, 10, 0},
      {0, 0, 10, 1},
      {0, 1, 10, 1},
      {0, 0, 10, 0},
      {0, 1, 10, 0},
      {0, 0, 10, 1},
      {0, 1, 10, 1},
      {2, 2, 20, 0},
      {2, 3, 20, 0},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathBound(
      true, std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}},
      std::move(side), 0, {Variable{"?start"}, Variable{"?other"}}, left, right,
      0, std::numeric_limits<size_t>::max(), false, {Variable{"?g"}},
      "<a> <b> <c> <a> . <a> <b> <d> <b> .");

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, graphVariableBoundToGraphOperation) {
  auto sub = makeIdTableFromVector({
      {0, 1, 0},
      {2, 3, 0},
      {0, 1, 1},
      {1, 0, 1},
      {2, 3, 1},
      {3, 2, 1},
      {1, 0, 2},
      {3, 2, 2},
  });

  // Graph with index 3 does not exist
  auto side = makeIdTableFromVector({
      {0, 1},
      {0, 1},
      {0, 3},
      {1, 3},
      {2, 2},
      {2, 3},
      {3, 3},
  });

  auto expected = makeIdTableFromVector({
      {0, 1, 1},
      {0, 0, 1},
      {0, 1, 1},
      {0, 0, 1},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathBound(
      true, std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}},
      std::move(side), 0, {Variable{"?start"}, Variable{"?g"}}, left, right, 1,
      std::numeric_limits<size_t>::max(), false, {Variable{"?g"}});

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest,
       graphVariableBoundToGraphOperationGraphVariableBothSides) {
  auto sub = makeIdTableFromVector({
      {0, 1, 0},
      {1, 0, 0},
      {2, 3, 0},
      {0, 1, 1},
      {1, 0, 1},
      {2, 3, 1},
      {3, 2, 1},
      {1, 0, 2},
      {3, 2, 2},
  });

  auto side = makeIdTableFromVector({
      {0, 10},
      {0, 10},
      {2, 20},
  });

  auto expected = makeIdTableFromVector({
      {0, 0, 10, 0},
      {0, 0, 10, 0},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?g"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?g"}, 1);
  auto T = makePathBound(
      true, std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}},
      std::move(side), 0, {Variable{"?g"}, Variable{"?other"}}, left, right, 1,
      std::numeric_limits<size_t>::max(), false, {Variable{"?g"}},
      "<a> <b> <c> <a> . <a> <b> <d> <b> .");

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest,
       graphVariableBoundToGraphOperationGraphVariableBothSidesEmptyPath) {
  auto sub = makeIdTableFromVector({
      {0, 1, 0},
      {2, 3, 0},
      {0, 1, 1},
      {1, 0, 1},
      {2, 3, 1},
      {3, 2, 1},
      {1, 0, 2},
      {3, 2, 2},
  });

  auto side = makeIdTableFromVector({
      {0, 10},
      {0, 10},
      {2, 20},
  });

  auto expected = makeIdTableFromVector({
      {0, 0, 10, 0},
      {0, 0, 10, 0},
      {2, 2, 20, 2},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?g"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?g"}, 1);
  auto T = makePathBound(
      true, std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}},
      std::move(side), 0, {Variable{"?g"}, Variable{"?other"}}, left, right, 0,
      std::numeric_limits<size_t>::max(), false, {Variable{"?g"}},
      "<a> <b> <c> <a> . <a> <b> <c> <c> .");

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest,
       unboundGraphVariableToGraphOperationGraphVariableBothSides) {
  auto sub = makeIdTableFromVector({
      {1, 2, 1},
      {3, 4, 1},
      {1, 2, 2},
      {2, 1, 2},
      {3, 4, 2},
      {4, 3, 2},
      {2, 1, 3},
      {4, 3, 3},
  });

  auto expected = makeIdTableFromVector({
      {2, 2, 2},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?g"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?g"}, 1);
  auto T = makePathUnbound(
      std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}}, left,
      right, 1, std::numeric_limits<size_t>::max(), std::nullopt,
      {Variable{"?g"}});

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest,
       unboundGraphVariableToGraphOperationGraphVariableSingleSide) {
  auto sub = makeIdTableFromVector({
      {1, 2, 1},
      {3, 4, 1},
      {1, 2, 2},
      {2, 1, 2},
      {3, 4, 2},
      {4, 3, 2},
      {2, 1, 3},
      {4, 3, 3},
  });

  auto expected = makeIdTableFromVector({
      {1, 2, 1},
      {2, 1, 2},
      {2, 2, 2},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?g"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?free"}, 1);
  auto T = makePathUnbound(
      std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}}, left,
      right, 1, std::numeric_limits<size_t>::max(), std::nullopt,
      {Variable{"?g"}});

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest,
       unboundGraphVariableToGraphOperationGraphVariableSingleOtherSide) {
  auto sub = makeIdTableFromVector({
      {1, 2, 1},
      {3, 4, 1},
      {1, 2, 2},
      {2, 1, 2},
      {3, 4, 2},
      {4, 3, 2},
      {2, 1, 3},
      {4, 3, 3},
  });

  auto expected = makeIdTableFromVector({
      {1, 2, 2},
      {2, 2, 2},
      {4, 3, 3},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?free"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?g"}, 1);
  auto T = makePathUnbound(
      std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}}, left,
      right, 1, std::numeric_limits<size_t>::max(), std::nullopt,
      {Variable{"?g"}});

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest,
       graphVariableBoundToGraphOperationGraphVariableSingleSide) {
  auto sub = makeIdTableFromVector({
      {0, 1, 0},
      {2, 3, 0},
      {0, 1, 1},
      {1, 0, 1},
      {2, 3, 1},
      {3, 2, 1},
      {1, 0, 2},
      {3, 2, 2},
  });

  auto side = makeIdTableFromVector({
      {0, 10},
      {0, 10},
      {2, 20},
  });

  auto expected = makeIdTableFromVector({
      {0, 1, 10, 0},
      {0, 1, 10, 0},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?g"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathBound(
      true, std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}},
      std::move(side), 0, {Variable{"?g"}, Variable{"?other"}}, left, right, 1,
      std::numeric_limits<size_t>::max(), false, {Variable{"?g"}},
      "<a> <b> <c> <a> . <a> <b> <d> <b> .");

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest,
       graphVariableBoundToGraphOperationGraphVariableSingleOtherSide) {
  auto sub = makeIdTableFromVector({
      {0, 1, 0},
      {2, 3, 0},
      {0, 1, 1},
      {1, 0, 1},
      {2, 3, 1},
      {3, 2, 1},
      {1, 0, 2},
      {3, 2, 2},
  });

  auto side = makeIdTableFromVector({
      {0, 1, 10},
      {2, 0, 20},
  });

  auto expected = makeIdTableFromVector({
      {0, 1, 10, 1},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?g"}, 1);
  auto T = makePathBound(
      true, std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}},
      std::move(side), 0,
      {Variable{"?start"}, Variable{"?g"}, Variable{"?other"}}, left, right, 1,
      std::numeric_limits<size_t>::max(), false, {Variable{"?g"}},
      "<a> <b> <c> <a> . <a> <b> <d> <b> .");

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest,
       graphVariableBoundToGraphOperationGraphVariableSingleSideEmptyPath) {
  auto sub = makeIdTableFromVector({
      {0, 1, 0},
      {2, 3, 0},
      {0, 1, 1},
      {1, 0, 1},
      {2, 3, 1},
      {3, 2, 1},
      {1, 0, 2},
      {3, 2, 2},
  });

  auto side = makeIdTableFromVector({
      {0, 10},
      {0, 10},
      {2, 20},
  });

  auto expected = makeIdTableFromVector({
      {0, 0, 10, 0},
      {0, 1, 10, 0},
      {0, 0, 10, 0},
      {0, 1, 10, 0},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?g"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathBound(
      true, std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}},
      std::move(side), 0, {Variable{"?g"}, Variable{"?other"}}, left, right, 0,
      std::numeric_limits<size_t>::max(), false, {Variable{"?g"}},
      "<a> <b> <c> <a> . <a> <b> <d> <b> .");

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, graphVariableConstrainedByTwoIris) {
  auto sub = makeIdTableFromVector({
      {1, 2, 1},
      {2, 1, 1},
      {1, 2, 2},
      {2, 1, 2},
      {1, 2, 3},
      {2, 1, 3},
  });

  auto expected = makeIdTableFromVector({
      {1, 1, 1},
      {1, 1, 2},
      {1, 1, 3},
  });

  TransitivePathSide left(std::nullopt, 0, V(1), 0);
  TransitivePathSide right(std::nullopt, 1, V(1), 1);
  auto T = makePathUnbound(
      std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}}, left,
      right, 1, std::numeric_limits<size_t>::max(), std::nullopt,
      {Variable{"?g"}});

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, graphVariableConstrainedByTwoIrisEmptyPath) {
  auto sub = makeIdTableFromVector({
      {0, 1, 0},
      {1, 0, 0},
      {0, 1, 1},
      {1, 0, 1},
      {0, 1, 2},
      {1, 0, 2},
  });

  auto expected = makeIdTableFromVector({
      {0, 0, 0},
      {0, 0, 2},
  });

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, V(0), 1);
  auto T = makePathUnbound(
      std::move(sub),
      {Variable{"?internal1"}, Variable{"?internal2"}, Variable{"?g"}}, left,
      right, 0, std::numeric_limits<size_t>::max(),
      "<a> <b> <c> <a> . <a> <b> <c> <c> .", {Variable{"?g"}});

  auto resultTable = T->computeResultOnlyForTesting(requestLaziness());
  assertResultMatchesIdTable(resultTable, expected);
}

// _____________________________________________________________________________
TEST_P(TransitivePathTest, sortOrderGuaranteesWithBoundOperation) {
  auto sub = makeIdTableFromVector({
      {0, 1},
      {1, 1},
  });

  auto side = makeIdTableFromVector({
      {0, 10},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);

  auto [path, qec] =
      makePath(std::move(sub), {Variable{"?internal1"}, Variable{"?internal2"}},
               left, right, 1, std::numeric_limits<size_t>::max());
  EXPECT_THAT(path->resultSortedOn(), ::testing::ElementsAre());

  {
    auto operation = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, side.clone(),
        std::vector<std::optional<Variable>>{Variable{"?start"},
                                             Variable{"?other"}});
    auto boundPath = path->bindLeftSide(operation, 0);

    EXPECT_THAT(boundPath->resultSortedOn(), ::testing::ElementsAre());
  }

  {
    auto operation = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, side.clone(),
        std::vector<std::optional<Variable>>{Variable{"?start"},
                                             Variable{"?other"}},
        false, std::vector<ColumnIndex>{1});
    auto boundPath = path->bindLeftSide(operation, 0);

    EXPECT_THAT(boundPath->resultSortedOn(), ::testing::ElementsAre());
  }

  {
    auto operation = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, side.clone(),
        std::vector<std::optional<Variable>>{Variable{"?start"},
                                             Variable{"?other"}},
        false, std::vector<ColumnIndex>{0});
    auto boundPath = path->bindLeftSide(operation, 0);

    EXPECT_THAT(boundPath->resultSortedOn(), ::testing::ElementsAre(0));
  }

  {
    auto operation = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec,
        makeIdTableFromVector({
            {Id::makeUndefined(), 10},
        }),
        std::vector<std::optional<Variable>>{Variable{"?start"},
                                             Variable{"?other"}},
        false, std::vector<ColumnIndex>{0});
    auto boundPath = path->bindLeftSide(operation, 0);

    EXPECT_THAT(boundPath->resultSortedOn(), ::testing::ElementsAre());
  }
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

// _____________________________________________________________________________
TEST(TransitivePathBinSearch, successors) {
  auto input =
      makeIdTableFromVector({{0, 10}, {1, 10}, {2, 11}, {2, 12}, {3, 13}});
  BinSearchMap binSearchMap{input.getColumn(0), input.getColumn(1)};
  EXPECT_THAT(binSearchMap.successors(V(0)), ElementsAre(V(10)));
  EXPECT_THAT(binSearchMap.successors(V(1)), ElementsAre(V(10)));
  EXPECT_THAT(binSearchMap.successors(V(2)), ElementsAre(V(11), V(12)));
  EXPECT_THAT(binSearchMap.successors(V(3)), ElementsAre(V(13)));
}

// _____________________________________________________________________________
TEST(TransitivePathBinSearch, successorsWithGraph) {
  auto input = makeIdTableFromVector(
      {{0, 10, 100}, {1, 10, 100}, {2, 11, 100}, {2, 12, 101}, {3, 13, 101}});
  BinSearchMap binSearchMap{input.getColumn(0), input.getColumn(1),
                            input.getColumn(2)};
  binSearchMap.setGraphId(V(100));
  EXPECT_THAT(binSearchMap.successors(V(0)), ElementsAre(V(10)));
  EXPECT_THAT(binSearchMap.successors(V(1)), ElementsAre(V(10)));
  EXPECT_THAT(binSearchMap.successors(V(2)), ElementsAre(V(11)));
  EXPECT_THAT(binSearchMap.successors(V(3)), ElementsAre());

  binSearchMap.setGraphId(V(101));
  EXPECT_THAT(binSearchMap.successors(V(0)), ElementsAre());
  EXPECT_THAT(binSearchMap.successors(V(1)), ElementsAre());
  EXPECT_THAT(binSearchMap.successors(V(2)), ElementsAre(V(12)));
  EXPECT_THAT(binSearchMap.successors(V(3)), ElementsAre(V(13)));

  binSearchMap.setGraphId(V(102));
  EXPECT_THAT(binSearchMap.successors(V(0)), ElementsAre());
  EXPECT_THAT(binSearchMap.successors(V(1)), ElementsAre());
  EXPECT_THAT(binSearchMap.successors(V(2)), ElementsAre());
  EXPECT_THAT(binSearchMap.successors(V(3)), ElementsAre());
}

namespace {
// _____________________________________________________________________________
HashMapWrapper::MapOfMaps columnsToMap(
    const ad_utility::AllocatorWithLimit<Id>& allocator,
    ql::span<const Id> startCol, ql::span<const Id> targetCol,
    ql::span<const Id> graphCol) {
  HashMapWrapper::MapOfMaps edgesWithGraph{allocator};
  for (size_t i = 0; i < startCol.size(); i++) {
    auto it1 = edgesWithGraph.try_emplace(graphCol[i], allocator).first;
    auto it2 = it1->second.try_emplace(startCol[i], allocator).first;
    it2->second.insert(targetCol[i]);
  }
  return edgesWithGraph;
}

// _____________________________________________________________________________
HashMapWrapper::Map columnsToMap(
    const ad_utility::AllocatorWithLimit<Id>& allocator,
    ql::span<const Id> startCol, ql::span<const Id> targetCol) {
  HashMapWrapper::Map edges{allocator};

  for (size_t i = 0; i < startCol.size(); i++) {
    auto [it, _] = edges.try_emplace(startCol[i], allocator);
    it->second.insert(targetCol[i]);
  }
  return edges;
}
}  // namespace

// _____________________________________________________________________________
TEST(TransitivePathHashMap, successors) {
  auto input =
      makeIdTableFromVector({{0, 10}, {1, 10}, {2, 11}, {2, 12}, {3, 13}});
  HashMapWrapper hashMapWrapper{
      columnsToMap(input.getAllocator(), input.getColumn(0),
                   input.getColumn(1)),
      input.getAllocator()};
  EXPECT_THAT(hashMapWrapper.successors(V(0)), UnorderedElementsAre(V(10)));
  EXPECT_THAT(hashMapWrapper.successors(V(1)), UnorderedElementsAre(V(10)));
  EXPECT_THAT(hashMapWrapper.successors(V(2)),
              UnorderedElementsAre(V(11), V(12)));
  EXPECT_THAT(hashMapWrapper.successors(V(3)), UnorderedElementsAre(V(13)));
}
TEST(TransitivePathHashMap, successorsWithGraph) {
  auto input = makeIdTableFromVector(
      {{0, 10, 100}, {1, 10, 100}, {2, 11, 100}, {2, 12, 101}, {3, 13, 101}});
  HashMapWrapper hashMapWrapper{
      columnsToMap(input.getAllocator(), input.getColumn(0), input.getColumn(1),
                   input.getColumn(2)),
      input.getAllocator()};
  hashMapWrapper.setGraphId(V(100));
  EXPECT_THAT(hashMapWrapper.successors(V(0)), UnorderedElementsAre(V(10)));
  EXPECT_THAT(hashMapWrapper.successors(V(1)), UnorderedElementsAre(V(10)));
  EXPECT_THAT(hashMapWrapper.successors(V(2)), UnorderedElementsAre(V(11)));
  EXPECT_THAT(hashMapWrapper.successors(V(3)), UnorderedElementsAre());

  hashMapWrapper.setGraphId(V(101));
  EXPECT_THAT(hashMapWrapper.successors(V(0)), UnorderedElementsAre());
  EXPECT_THAT(hashMapWrapper.successors(V(1)), UnorderedElementsAre());
  EXPECT_THAT(hashMapWrapper.successors(V(2)), UnorderedElementsAre(V(12)));
  EXPECT_THAT(hashMapWrapper.successors(V(3)), UnorderedElementsAre(V(13)));

  hashMapWrapper.setGraphId(V(102));
  EXPECT_THAT(hashMapWrapper.successors(V(0)), UnorderedElementsAre());
  EXPECT_THAT(hashMapWrapper.successors(V(1)), UnorderedElementsAre());
  EXPECT_THAT(hashMapWrapper.successors(V(2)), UnorderedElementsAre());
  EXPECT_THAT(hashMapWrapper.successors(V(3)), UnorderedElementsAre());
}

// _____________________________________________________________________________
TEST(TransitivePathBinSearch, getEquivalentIdAndMatchingGraphs) {
  auto input =
      makeIdTableFromVector({{0, 10}, {1, 10}, {2, 11}, {2, 12}, {3, 13}});
  BinSearchMap binSearchMap{input.getColumn(0), input.getColumn(1)};
  EXPECT_THAT(
      binSearchMap.getEquivalentIdAndMatchingGraphs(U),
      ElementsAre(Pair(V(0), U), Pair(V(1), U), Pair(V(2), U), Pair(V(3), U)));
  EXPECT_THAT(binSearchMap.getEquivalentIdAndMatchingGraphs(V(0)),
              ElementsAre(Pair(V(0), U)));
  EXPECT_THAT(binSearchMap.getEquivalentIdAndMatchingGraphs(V(1)),
              ElementsAre(Pair(V(1), U)));
  EXPECT_THAT(binSearchMap.getEquivalentIdAndMatchingGraphs(V(2)),
              ElementsAre(Pair(V(2), U)));
  EXPECT_THAT(binSearchMap.getEquivalentIdAndMatchingGraphs(V(3)),
              ElementsAre(Pair(V(3), U)));
  EXPECT_THAT(binSearchMap.getEquivalentIdAndMatchingGraphs(V(4)),
              ElementsAre());
}

// _____________________________________________________________________________
TEST(TransitivePathBinSearch, getEquivalentIdAndMatchingGraphsWithGraphs) {
  auto input = makeIdTableFromVector({{0, 10, 100},
                                      {1, 10, 100},
                                      {2, 11, 100},
                                      {0, 10, 101},
                                      {2, 12, 101},
                                      {3, 13, 101},
                                      {3, 14, 101}});
  BinSearchMap binSearchMap{input.getColumn(0), input.getColumn(1),
                            input.getColumn(2)};
  // The active graph should be ignored
  for (Id graphId : {U, V(100), V(101)}) {
    // Don't explicitly set undefined, this works fine because by default no
    // active graph is set.
    if (!graphId.isUndefined()) {
      binSearchMap.setGraphId(graphId);
    }
    EXPECT_THAT(binSearchMap.getEquivalentIdAndMatchingGraphs(U),
                ElementsAre(Pair(V(0), V(100)), Pair(V(1), V(100)),
                            Pair(V(2), V(100)), Pair(V(0), V(101)),
                            Pair(V(2), V(101)), Pair(V(3), V(101))));
    EXPECT_THAT(binSearchMap.getEquivalentIdAndMatchingGraphs(V(0)),
                ElementsAre(Pair(V(0), V(100)), Pair(V(0), V(101))));
    EXPECT_THAT(binSearchMap.getEquivalentIdAndMatchingGraphs(V(1)),
                ElementsAre(Pair(V(1), V(100))));
    EXPECT_THAT(binSearchMap.getEquivalentIdAndMatchingGraphs(V(2)),
                ElementsAre(Pair(V(2), V(100)), Pair(V(2), V(101))));
    EXPECT_THAT(binSearchMap.getEquivalentIdAndMatchingGraphs(V(3)),
                ElementsAre(Pair(V(3), V(101))));
    EXPECT_THAT(binSearchMap.getEquivalentIdAndMatchingGraphs(V(4)),
                ElementsAre());
  }
}

// _____________________________________________________________________________
TEST(TransitivePathHashMap, getEquivalentIdAndMatchingGraphs) {
  auto input =
      makeIdTableFromVector({{0, 10}, {1, 10}, {2, 11}, {2, 12}, {3, 13}});
  HashMapWrapper hashMapWrapper{
      columnsToMap(input.getAllocator(), input.getColumn(0),
                   input.getColumn(1)),
      input.getAllocator()};
  EXPECT_THAT(hashMapWrapper.getEquivalentIdAndMatchingGraphs(U),
              UnorderedElementsAre(Pair(V(0), U), Pair(V(1), U), Pair(V(2), U),
                                   Pair(V(3), U)));
  EXPECT_THAT(hashMapWrapper.getEquivalentIdAndMatchingGraphs(V(0)),
              UnorderedElementsAre(Pair(V(0), U)));
  EXPECT_THAT(hashMapWrapper.getEquivalentIdAndMatchingGraphs(V(1)),
              UnorderedElementsAre(Pair(V(1), U)));
  EXPECT_THAT(hashMapWrapper.getEquivalentIdAndMatchingGraphs(V(2)),
              UnorderedElementsAre(Pair(V(2), U)));
  EXPECT_THAT(hashMapWrapper.getEquivalentIdAndMatchingGraphs(V(3)),
              UnorderedElementsAre(Pair(V(3), U)));
  EXPECT_THAT(hashMapWrapper.getEquivalentIdAndMatchingGraphs(V(4)),
              UnorderedElementsAre());
}

// _____________________________________________________________________________
TEST(TransitivePathHashMap, getEquivalentIdAndMatchingGraphsWithGraphs) {
  auto input = makeIdTableFromVector({{0, 10, 100},
                                      {1, 10, 100},
                                      {2, 11, 100},
                                      {0, 10, 101},
                                      {2, 12, 101},
                                      {3, 13, 101},
                                      {3, 14, 101}});
  HashMapWrapper hashMapWrapper{
      columnsToMap(input.getAllocator(), input.getColumn(0), input.getColumn(1),
                   input.getColumn(2)),
      input.getAllocator()};
  // The active graph should be ignored
  for (Id graphId : {U, V(100), V(101)}) {
    // Don't explicitly set undefined, this works fine because by default no
    // active graph is set.
    if (!graphId.isUndefined()) {
      hashMapWrapper.setGraphId(graphId);
    }
    EXPECT_THAT(hashMapWrapper.getEquivalentIdAndMatchingGraphs(U),
                UnorderedElementsAre(Pair(V(0), V(100)), Pair(V(1), V(100)),
                                     Pair(V(2), V(100)), Pair(V(0), V(101)),
                                     Pair(V(2), V(101)), Pair(V(3), V(101))));
    EXPECT_THAT(hashMapWrapper.getEquivalentIdAndMatchingGraphs(V(0)),
                UnorderedElementsAre(Pair(V(0), V(100)), Pair(V(0), V(101))));
    EXPECT_THAT(hashMapWrapper.getEquivalentIdAndMatchingGraphs(V(1)),
                UnorderedElementsAre(Pair(V(1), V(100))));
    EXPECT_THAT(hashMapWrapper.getEquivalentIdAndMatchingGraphs(V(2)),
                UnorderedElementsAre(Pair(V(2), V(100)), Pair(V(2), V(101))));
    EXPECT_THAT(hashMapWrapper.getEquivalentIdAndMatchingGraphs(V(3)),
                UnorderedElementsAre(Pair(V(3), V(101))));
    EXPECT_THAT(hashMapWrapper.getEquivalentIdAndMatchingGraphs(V(4)),
                UnorderedElementsAre());
  }
}
