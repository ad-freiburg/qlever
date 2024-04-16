// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>
#include <memory>

#include "./util/AllocatorTestHelpers.h"
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
using ad_utility::testing::makeAllocator;
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
  auto sub = makeIdTableFromVector(
      {{V(0), V(1)}, {V(1), V(2)}, {V(1), V(3)}, {V(2), V(3)}});

  auto expected = makeIdTableFromVector({{V(0), V(3)}});

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
  auto sub = makeIdTableFromVector(
      {{V(0), V(1)}, {V(1), V(2)}, {V(1), V(3)}, {V(2), V(3)}});

  auto expected =
      makeIdTableFromVector({{V(0), V(1)}, {V(0), V(2)}, {V(0), V(3)}});

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
  auto sub = makeIdTableFromVector(
      {{V(0), V(1)}, {V(1), V(2)}, {V(1), V(3)}, {V(2), V(3)}});

  auto expected = makeIdTableFromVector({
      {V(2), V(3)},
      {V(1), V(3)},
      {V(0), V(3)},
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
  auto sub = makeIdTableFromVector(
      {{V(0), V(1)}, {V(1), V(2)}, {V(1), V(3)}, {V(2), V(3)}});

  auto expected = makeIdTableFromVector(
      {{V(0), V(0)}, {V(0), V(1)}, {V(0), V(2)}, {V(0), V(3)}});

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
  auto sub = makeIdTableFromVector(
      {{V(0), V(1)}, {V(1), V(2)}, {V(1), V(3)}, {V(2), V(3)}});

  auto expected = makeIdTableFromVector({
      {V(3), V(3)},
      {V(2), V(3)},
      {V(1), V(3)},
      {V(0), V(3)},
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
      {V(0), V(1)},
      {V(1), V(2)},
      {V(1), V(3)},
      {V(2), V(3)},
  });

  auto expected = makeIdTableFromVector({
      {V(0), V(1)},
      {V(0), V(2)},
      {V(0), V(3)},
      {V(1), V(2)},
      {V(1), V(3)},
      {V(2), V(3)},
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
  auto sub = makeIdTableFromVector({{V(0), V(2)},
                                    {V(2), V(4)},
                                    {V(4), V(7)},
                                    {V(0), V(7)},
                                    {V(3), V(3)},
                                    {V(7), V(0)},
                                    // Disconnected component.
                                    {V(10), V(11)}});

  auto expected = makeIdTableFromVector({{V(0), V(2)},
                                         {V(0), V(4)},
                                         {V(0), V(7)},
                                         {V(0), V(0)},
                                         {V(2), V(4)},
                                         {V(2), V(7)},
                                         {V(2), V(0)},
                                         {V(2), V(2)},
                                         {V(4), V(7)},
                                         {V(4), V(0)},
                                         {V(4), V(2)},
                                         {V(4), V(4)},
                                         {V(3), V(3)},
                                         {V(7), V(0)},
                                         {V(7), V(2)},
                                         {V(7), V(4)},
                                         {V(7), V(7)},
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
  auto sub = makeIdTableFromVector(
      {{V(0), V(1)}, {V(1), V(2)}, {V(1), V(3)}, {V(2), V(3)}, {V(3), V(4)}});

  auto leftOpTable = makeIdTableFromVector({
      {V(0), V(1)},
      {V(0), V(2)},
      {V(0), V(3)},
  });

  auto expected = makeIdTableFromVector({
      {V(1), V(4), V(0)},
      {V(2), V(4), V(0)},
      {V(3), V(4), V(0)},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, V(4), 1);
  auto T = makePathLeftBound(
      std::move(sub), {Variable{"?start"}, Variable{"?target"}},
      std::move(leftOpTable), 1, {Variable{"?x"}, Variable{"?start"}},
      std::move(left), std::move(right), 0, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST_P(TransitivePathTest, idToRightBound) {
  auto sub = makeIdTableFromVector({
      {V(0), V(1)},
      {V(1), V(2)},
      {V(1), V(3)},
      {V(2), V(3)},
      {V(3), V(4)},
  });

  auto rightOpTable = makeIdTableFromVector({
      {V(2), V(5)},
      {V(3), V(5)},
      {V(4), V(5)},
  });

  auto expected = makeIdTableFromVector({
      {V(0), V(2), V(5)},
      {V(0), V(3), V(5)},
      {V(0), V(4), V(5)},
  });

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathRightBound(
      std::move(sub), {Variable{"?start"}, Variable{"?target"}},
      std::move(rightOpTable), 0, {Variable{"?target"}, Variable{"?x"}},
      std::move(left), std::move(right), 0, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST_P(TransitivePathTest, leftBoundToVar) {
  auto sub = makeIdTableFromVector({
      {V(1), V(2)},
      {V(2), V(3)},
      {V(2), V(4)},
      {V(3), V(4)},
  });

  auto leftOpTable = makeIdTableFromVector({
      {V(0), V(1)},
      {V(0), V(2)},
      {V(0), V(3)},
  });

  auto expected = makeIdTableFromVector({
      {V(1), V(1), V(0)},
      {V(1), V(2), V(0)},
      {V(1), V(3), V(0)},
      {V(1), V(4), V(0)},
      {V(2), V(2), V(0)},
      {V(2), V(3), V(0)},
      {V(2), V(4), V(0)},
      {V(3), V(3), V(0)},
      {V(3), V(4), V(0)},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathLeftBound(
      std::move(sub), {Variable{"?start"}, Variable{"?target"}},
      std::move(leftOpTable), 1, {Variable{"?x"}, Variable{"?start"}},
      std::move(left), std::move(right), 0, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST_P(TransitivePathTest, rightBoundToVar) {
  auto sub = makeIdTableFromVector({
      {V(1), V(2)},
      {V(2), V(3)},
      {V(2), V(4)},
      {V(3), V(4)},
  });

  auto rightOpTable = makeIdTableFromVector({
      {V(2), V(5)},
      {V(3), V(5)},
      {V(4), V(5)},
  });

  auto expected = makeIdTableFromVector({
      {V(1), V(2), V(5)},
      {V(1), V(3), V(5)},
      {V(1), V(4), V(5)},
      {V(2), V(2), V(5)},
      {V(2), V(3), V(5)},
      {V(2), V(4), V(5)},
      {V(3), V(3), V(5)},
      {V(3), V(4), V(5)},
      {V(4), V(4), V(5)},
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
      {V(0), V(2)},
      {V(2), V(4)},
      {V(4), V(7)},
      {V(0), V(7)},
      {V(3), V(3)},
      {V(7), V(0)},
      // Disconnected component.
      {V(10), V(11)},
  });

  auto expected = makeIdTableFromVector({{V(0), V(2)},
                                         {V(0), V(4)},
                                         {V(0), V(7)},
                                         {V(0), V(0)},
                                         {V(2), V(4)},
                                         {V(2), V(7)},
                                         {V(4), V(7)},
                                         {V(4), V(0)},
                                         {V(3), V(3)},
                                         {V(7), V(0)},
                                         {V(7), V(2)},
                                         {V(7), V(7)},
                                         {V(10), V(11)}});

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
      {V(0), V(2)},
      {V(2), V(4)},
      {V(4), V(7)},
      {V(0), V(7)},
      {V(3), V(3)},
      {V(7), V(0)},
      // Disconnected component.
      {V(10), V(11)},
  });

  auto expected = makeIdTableFromVector({
      {V(7), V(0)},
      {V(7), V(2)},
      {V(7), V(7)},
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
      {V(0), V(2)},
      {V(2), V(4)},
      {V(4), V(7)},
      {V(0), V(7)},
      {V(3), V(3)},
      {V(7), V(0)},
      // Disconnected component.
      {V(10), V(11)},
  });

  auto expected = makeIdTableFromVector({
      {V(0), V(2)},
      {V(7), V(2)},
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
      {V(0), V(2)},
      {V(2), V(4)},
      {V(4), V(7)},
      {V(0), V(7)},
      {V(3), V(3)},
      {V(7), V(0)},
      // Disconnected component.
      {V(10), V(11)},
  });

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 0, std::numeric_limits<size_t>::max());
  AD_EXPECT_THROW_WITH_MESSAGE(
      T->computeResultOnlyForTesting(),
      ::testing::ContainsRegex(
          "This query might have to evalute the empty path, which is currently "
          "not supported"));
}

INSTANTIATE_TEST_SUITE_P(TransitivePathTestSuite, TransitivePathTest,
                         testing::Bool(),
                         [](const testing::TestParamInfo<bool>& info) {
                           return info.param ? "TransitivePathBinSearch"
                                             : "TransitivePathHashMap";
                         });
