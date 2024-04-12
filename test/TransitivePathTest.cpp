// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

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
#include "util/IndexTestHelpers.h"

using ad_utility::testing::getQec;
using ad_utility::testing::makeAllocator;
namespace {
auto V = ad_utility::testing::VocabId;
using Vars = std::vector<std::optional<Variable>>;

// First sort both of the inputs and then ASSERT their equality. Needed for
// results of the TransitivePath operations which have a non-deterministic
// order because of the hash maps which are used internally.
void assertSameUnorderedContent(const IdTable& a, const IdTable& b) {
  auto aCpy = a.clone();
  auto bCpy = b.clone();
  ASSERT_EQ(a.numColumns(), b.numColumns());
  auto sorter = [](const auto& rowFromA, const auto& rowFromB) {
    for (size_t i = 0; i < rowFromA.numColumns(); ++i) {
      if (rowFromA[i] != rowFromB[i]) {
        return rowFromA[i] < rowFromB[i];
      }
    }
    // equal means "not smaller"
    return false;
  };
  std::sort(aCpy.begin(), aCpy.end(), sorter);
  std::sort(bCpy.begin(), bCpy.end(), sorter);
  ASSERT_EQ(aCpy, bCpy);
}
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
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(1)});
  sub.push_back({V(1), V(2)});
  sub.push_back({V(1), V(3)});
  sub.push_back({V(2), V(3)});

  IdTable expected(2, makeAllocator());
  expected.push_back({V(0), V(3)});

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, V(3), 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  assertSameUnorderedContent(expected, resultTable.idTable());
}

TEST_P(TransitivePathTest, idToVar) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(1)});
  sub.push_back({V(1), V(2)});
  sub.push_back({V(1), V(3)});
  sub.push_back({V(2), V(3)});

  IdTable expected(2, makeAllocator());
  expected.push_back({V(0), V(1)});
  expected.push_back({V(0), V(2)});
  expected.push_back({V(0), V(3)});

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  assertSameUnorderedContent(expected, resultTable.idTable());
}

TEST_P(TransitivePathTest, varToId) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(1)});
  sub.push_back({V(1), V(2)});
  sub.push_back({V(1), V(3)});
  sub.push_back({V(2), V(3)});

  IdTable expected(2, makeAllocator());
  expected.push_back({V(2), V(3)});
  expected.push_back({V(1), V(3)});
  expected.push_back({V(0), V(3)});

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, V(3), 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  assertSameUnorderedContent(expected, resultTable.idTable());
}

TEST_P(TransitivePathTest, varTovar) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(1)});
  sub.push_back({V(1), V(2)});
  sub.push_back({V(1), V(3)});
  sub.push_back({V(2), V(3)});

  IdTable expected(2, makeAllocator());
  expected.push_back({V(0), V(1)});
  expected.push_back({V(0), V(2)});
  expected.push_back({V(0), V(3)});
  expected.push_back({V(1), V(2)});
  expected.push_back({V(1), V(3)});
  expected.push_back({V(2), V(3)});

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  assertSameUnorderedContent(expected, resultTable.idTable());
}

TEST_P(TransitivePathTest, unlimitedMaxLength) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(2)});
  sub.push_back({V(2), V(4)});
  sub.push_back({V(4), V(7)});
  sub.push_back({V(0), V(7)});
  sub.push_back({V(3), V(3)});
  sub.push_back({V(7), V(0)});
  // Disconnected component.
  sub.push_back({V(10), V(11)});

  IdTable expected(2, makeAllocator());
  expected.push_back({V(0), V(2)});
  expected.push_back({V(0), V(4)});
  expected.push_back({V(0), V(7)});
  expected.push_back({V(0), V(0)});
  expected.push_back({V(2), V(4)});
  expected.push_back({V(2), V(7)});
  expected.push_back({V(2), V(0)});
  expected.push_back({V(2), V(2)});
  expected.push_back({V(4), V(7)});
  expected.push_back({V(4), V(0)});
  expected.push_back({V(4), V(2)});
  expected.push_back({V(4), V(4)});
  expected.push_back({V(3), V(3)});
  expected.push_back({V(7), V(0)});
  expected.push_back({V(7), V(2)});
  expected.push_back({V(7), V(4)});
  expected.push_back({V(7), V(7)});
  expected.push_back({V(10), V(11)});

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  assertSameUnorderedContent(expected, resultTable.idTable());
}

TEST_P(TransitivePathTest, idToLeftBound) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(1)});
  sub.push_back({V(1), V(2)});
  sub.push_back({V(1), V(3)});
  sub.push_back({V(2), V(3)});
  sub.push_back({V(3), V(4)});

  IdTable leftOpTable(2, makeAllocator());
  leftOpTable.push_back({V(0), V(1)});
  leftOpTable.push_back({V(0), V(2)});
  leftOpTable.push_back({V(0), V(3)});

  IdTable expected(3, makeAllocator());
  expected.push_back({V(1), V(4), V(0)});
  expected.push_back({V(2), V(4), V(0)});
  expected.push_back({V(3), V(4), V(0)});

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, V(4), 1);
  auto T = makePathLeftBound(
      std::move(sub), {Variable{"?start"}, Variable{"?target"}},
      std::move(leftOpTable), 1, {Variable{"?x"}, Variable{"?start"}},
      std::move(left), std::move(right), 0, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  assertSameUnorderedContent(expected, resultTable.idTable());
}

TEST_P(TransitivePathTest, idToRightBound) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(1)});
  sub.push_back({V(1), V(2)});
  sub.push_back({V(1), V(3)});
  sub.push_back({V(2), V(3)});
  sub.push_back({V(3), V(4)});

  IdTable rightOpTable(2, makeAllocator());
  rightOpTable.push_back({V(2), V(5)});
  rightOpTable.push_back({V(3), V(5)});
  rightOpTable.push_back({V(4), V(5)});

  IdTable expected(3, makeAllocator());
  expected.push_back({V(0), V(2), V(5)});
  expected.push_back({V(0), V(3), V(5)});
  expected.push_back({V(0), V(4), V(5)});

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathRightBound(
      std::move(sub), {Variable{"?start"}, Variable{"?target"}},
      std::move(rightOpTable), 0, {Variable{"?target"}, Variable{"?x"}},
      std::move(left), std::move(right), 0, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  assertSameUnorderedContent(expected, resultTable.idTable());
}

TEST_P(TransitivePathTest, leftBoundToVar) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(1), V(2)});
  sub.push_back({V(2), V(3)});
  sub.push_back({V(2), V(4)});
  sub.push_back({V(3), V(4)});

  IdTable leftOpTable(2, makeAllocator());
  leftOpTable.push_back({V(0), V(1)});
  leftOpTable.push_back({V(0), V(2)});
  leftOpTable.push_back({V(0), V(3)});

  IdTable expected(3, makeAllocator());
  expected.push_back({V(1), V(1), V(0)});
  expected.push_back({V(1), V(2), V(0)});
  expected.push_back({V(1), V(3), V(0)});
  expected.push_back({V(1), V(4), V(0)});
  expected.push_back({V(2), V(2), V(0)});
  expected.push_back({V(2), V(3), V(0)});
  expected.push_back({V(2), V(4), V(0)});
  expected.push_back({V(3), V(3), V(0)});
  expected.push_back({V(3), V(4), V(0)});

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathLeftBound(
      std::move(sub), {Variable{"?start"}, Variable{"?target"}},
      std::move(leftOpTable), 1, {Variable{"?x"}, Variable{"?start"}},
      std::move(left), std::move(right), 0, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  assertSameUnorderedContent(expected, resultTable.idTable());
}

TEST_P(TransitivePathTest, rightBoundToVar) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(1), V(2)});
  sub.push_back({V(2), V(3)});
  sub.push_back({V(2), V(4)});
  sub.push_back({V(3), V(4)});

  IdTable rightOpTable(2, makeAllocator());
  rightOpTable.push_back({V(2), V(5)});
  rightOpTable.push_back({V(3), V(5)});
  rightOpTable.push_back({V(4), V(5)});

  IdTable expected(3, makeAllocator());
  expected.push_back({V(1), V(2), V(5)});
  expected.push_back({V(1), V(3), V(5)});
  expected.push_back({V(1), V(4), V(5)});
  expected.push_back({V(2), V(2), V(5)});
  expected.push_back({V(2), V(3), V(5)});
  expected.push_back({V(2), V(4), V(5)});
  expected.push_back({V(3), V(3), V(5)});
  expected.push_back({V(3), V(4), V(5)});
  expected.push_back({V(4), V(4), V(5)});

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T = makePathRightBound(
      std::move(sub), {Variable{"?start"}, Variable{"?target"}},
      std::move(rightOpTable), 0, {Variable{"?target"}, Variable{"?x"}},
      std::move(left), std::move(right), 0, std::numeric_limits<size_t>::max());

  auto resultTable = T->computeResultOnlyForTesting();
  assertSameUnorderedContent(expected, resultTable.idTable());
}

TEST_P(TransitivePathTest, maxLength2FromVariable) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(2)});
  sub.push_back({V(2), V(4)});
  sub.push_back({V(4), V(7)});
  sub.push_back({V(0), V(7)});
  sub.push_back({V(3), V(3)});
  sub.push_back({V(7), V(0)});
  // Disconnected component.
  sub.push_back({V(10), V(11)});

  IdTable expected(2, makeAllocator());

  expected.push_back({V(0), V(2)});
  expected.push_back({V(0), V(4)});
  expected.push_back({V(0), V(7)});
  expected.push_back({V(0), V(0)});
  expected.push_back({V(2), V(4)});
  expected.push_back({V(2), V(7)});
  expected.push_back({V(4), V(7)});
  expected.push_back({V(4), V(0)});
  expected.push_back({V(3), V(3)});
  expected.push_back({V(7), V(0)});
  expected.push_back({V(7), V(2)});
  expected.push_back({V(7), V(7)});
  expected.push_back({V(10), V(11)});

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, 2);
  auto resultTable = T->computeResultOnlyForTesting();
  assertSameUnorderedContent(expected, resultTable.idTable());
}

TEST_P(TransitivePathTest, maxLength2FromId) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(2)});
  sub.push_back({V(2), V(4)});
  sub.push_back({V(4), V(7)});
  sub.push_back({V(0), V(7)});
  sub.push_back({V(3), V(3)});
  sub.push_back({V(7), V(0)});
  // Disconnected component.
  sub.push_back({V(10), V(11)});

  IdTable expected(2, makeAllocator());

  expected.push_back({V(7), V(0)});
  expected.push_back({V(7), V(2)});
  expected.push_back({V(7), V(7)});

  TransitivePathSide left(std::nullopt, 0, V(7), 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, 2);
  auto resultTable = T->computeResultOnlyForTesting();
  assertSameUnorderedContent(expected, resultTable.idTable());
}

TEST_P(TransitivePathTest, maxLength2ToId) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(2)});
  sub.push_back({V(2), V(4)});
  sub.push_back({V(4), V(7)});
  sub.push_back({V(0), V(7)});
  sub.push_back({V(3), V(3)});
  sub.push_back({V(7), V(0)});
  // Disconnected component.
  sub.push_back({V(10), V(11)});

  IdTable expected(2, makeAllocator());

  expected.push_back({V(0), V(2)});
  expected.push_back({V(7), V(2)});

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, V(2), 1);
  auto T =
      makePathUnbound(std::move(sub), {Variable{"?start"}, Variable{"?target"}},
                      left, right, 1, 2);
  auto resultTable = T->computeResultOnlyForTesting();
  assertSameUnorderedContent(expected, resultTable.idTable());
}

INSTANTIATE_TEST_SUITE_P(TransitivePathTestSuite, TransitivePathTest,
                         testing::Bool(),
                         [](const testing::TestParamInfo<bool>& info) {
                           return info.param ? "TransitivePathBinSearch"
                                             : "TransitivePathHashMap";
                         });
