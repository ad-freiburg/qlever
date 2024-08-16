// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//         Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>
#include <memory>

#include "./util/IdTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "engine/QueryExecutionTree.h"
#include "engine/TransitivePathBase.h"
#include "engine/ValuesForTesting.h"
#include "gtest/gtest.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"

using ad_utility::testing::getQec;
namespace {
auto V = ad_utility::testing::VocabId;
using Vars = std::vector<std::optional<Variable>>;

}  // namespace

class TransitivePathTest : public testing::TestWithParam<bool> {
 public:
  [[nodiscard]] static std::pair<std::shared_ptr<TransitivePathBase>,
                                 QueryExecutionContext*>
  makePath(IdTable input, Vars vars, TransitivePathSide left,
           TransitivePathSide right, size_t minDist, size_t maxDist) {
    bool useBinSearch = GetParam();
    auto qec = getQec();
    auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(input), vars);
    return {TransitivePathBase::makeTransitivePath(
                qec, std::move(subtree), std::move(left), std::move(right),
                minDist, maxDist, useBinSearch),
            qec};
  }

  [[nodiscard]] static std::shared_ptr<TransitivePathBase> makePathUnbound(
      IdTable input, Vars vars, TransitivePathSide left,
      TransitivePathSide right, size_t minDist, size_t maxDist) {
    auto [T, qec] = makePath(std::move(input), vars, std::move(left),
                             std::move(right), minDist, maxDist);
    return T;
  }

  [[nodiscard]] static std::shared_ptr<TransitivePathBase> makePathLeftBound(
      IdTable input, Vars vars, IdTable sideTable, size_t sideTableCol,
      Vars sideVars, TransitivePathSide left, TransitivePathSide right,
      size_t minDist, size_t maxDist) {
    auto [T, qec] = makePath(std::move(input), vars, std::move(left),
                             std::move(right), minDist, maxDist);
    auto leftOp = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(sideTable), sideVars);
    return T->bindLeftSide(leftOp, sideTableCol);
  }

  [[nodiscard]] static std::shared_ptr<TransitivePathBase> makePathRightBound(
      IdTable input, Vars vars, IdTable sideTable, size_t sideTableCol,
      Vars sideVars, TransitivePathSide left, TransitivePathSide right,
      size_t minDist, size_t maxDist) {
    auto [T, qec] = makePath(std::move(input), vars, std::move(left),
                             std::move(right), minDist, maxDist);
    auto rightOp = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(sideTable), sideVars);
    return T->bindRightSide(rightOp, sideTableCol);
  }
};

TEST_P(TransitivePathTest, idToId) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {1, 3}, {2, 3}});

  auto expected = makeIdTableFromVector({{0, 3}});

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, V(3), 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST_P(TransitivePathTest, idToVar) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {1, 3}, {2, 3}});

  auto expected = makeIdTableFromVector({{0, 1}, {0, 2}, {0, 3}});

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

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

  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST_P(TransitivePathTest, idToVarMinLengthZero) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {1, 3}, {2, 3}});

  auto expected = makeIdTableFromVector({{0, 0}, {0, 1}, {0, 2}, {0, 3}});

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 0, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

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

  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

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

  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

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

  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

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
  {
    auto T = makePathLeftBound(
        sub.clone(), {Variable{"?start"}, Variable{"?target"}},
        leftOpTable.clone(), 1, {Variable{"?x"}, Variable{"?start"}}, left,
        right, 0, std::numeric_limits<size_t>::max());

    auto resultTable = T->computeResultOnlyForTesting();
    ASSERT_THAT(resultTable.idTable(),
                ::testing::UnorderedElementsAreArray(expected));
  }
  {
    auto T = makePathLeftBound(
        std::move(sub), {Variable{"?start"}, Variable{"?target"}},
        std::move(leftOpTable), 1, {std::nullopt, Variable{"?start"}},
        std::move(left), std::move(right), 0,
        std::numeric_limits<size_t>::max());

    auto resultTable = T->computeResultOnlyForTesting();
    ASSERT_THAT(resultTable.idTable(),
                ::testing::UnorderedElementsAreArray(expected));
  }
}

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
  {
    auto T = makePathRightBound(
        sub.clone(), {Variable{"?start"}, Variable{"?target"}},
        rightOpTable.clone(), 0, {Variable{"?target"}, Variable{"?x"}}, left,
        right, 0, std::numeric_limits<size_t>::max());

    auto resultTable = T->computeResultOnlyForTesting();
    ASSERT_THAT(resultTable.idTable(),
                ::testing::UnorderedElementsAreArray(expected));
  }
  {
    auto T = makePathRightBound(
        std::move(sub), {Variable{"?start"}, Variable{"?target"}},
        std::move(rightOpTable), 0, {Variable{"?target"}, std::nullopt},
        std::move(left), std::move(right), 0,
        std::numeric_limits<size_t>::max());

    auto resultTable = T->computeResultOnlyForTesting();
    ASSERT_THAT(resultTable.idTable(),
                ::testing::UnorderedElementsAreArray(expected));
  }
}

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
  {
    auto T = makePathLeftBound(
        std::move(sub), {Variable{"?start"}, Variable{"?target"}},
        std::move(leftOpTable), 1, {Variable{"?x"}, Variable{"?start"}},
        std::move(left), std::move(right), 0,
        std::numeric_limits<size_t>::max());

    auto resultTable = T->computeResultOnlyForTesting();
    ASSERT_THAT(resultTable.idTable(),
                ::testing::UnorderedElementsAreArray(expected));
  }
}

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
  auto T = makePathRightBound(
      std::move(sub), {Variable{"?start"}, Variable{"?target"}},
      std::move(rightOpTable), 0, {Variable{"?target"}, Variable{"?x"}},
      std::move(left), std::move(right), 0, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

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
  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

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
  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

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
  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST_P(TransitivePathTest, zeroLengthException) {
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

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 0, std::numeric_limits<size_t>::max());
  AD_EXPECT_THROW_WITH_MESSAGE(
      T->computeResultOnlyForTesting(),
      ::testing::ContainsRegex("This query might have to evaluate the empty "
                               "path, which is currently "
                               "not supported"));
}

INSTANTIATE_TEST_SUITE_P(TransitivePathTestSuite, TransitivePathTest,
                         testing::Bool(),
                         [](const testing::TestParamInfo<bool>& info) {
                           return info.param ? "TransitivePathBinSearch"
                                             : "TransitivePathHashMap";
                         });
