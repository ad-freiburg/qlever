// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gmock/gmock.h>

#include <algorithm>
#include <array>
#include <vector>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/LeapfrogTriangleJoin.h"
#include "engine/Sort.h"

namespace {
using V = Variable;
using Row = std::array<int64_t, 3>;
using Edge = std::vector<std::array<int64_t, 2>>;

// A two-column input with the given `variables`. `sorted` tells the
// `ValuesForTesting` that the rows are sorted on both columns.
std::shared_ptr<QueryExecutionTree> makeInput(const Edge& rows, const V& first,
                                              const V& second,
                                              bool sorted = false) {
  auto* qec = ad_utility::testing::getQec();
  VectorTable table;
  for (const auto& [a, b] : rows) {
    table.push_back({a, b});
  }
  return ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector(table, ad_utility::testing::IntId),
      std::vector<std::optional<V>>{first, second}, false,
      sorted ? std::vector<ColumnIndex>{0, 1} : std::vector<ColumnIndex>{});
}

// The triangle join of `xy`, `yz`, `xz` by three nested loops, sorted.
std::vector<Row> naiveTriangles(const Edge& xy, const Edge& yz,
                                const Edge& xz) {
  std::vector<Row> result;
  for (const auto& [x, y] : xy) {
    for (const auto& [y2, z] : yz) {
      for (const auto& [x2, z2] : xz) {
        if (y == y2 && x == x2 && z == z2) {
          result.push_back({x, y, z});
        }
      }
    }
  }
  ql::ranges::sort(result);
  return result;
}

// The rows of a result with three `IntId` columns.
std::vector<Row> toRows(const IdTableView<0>& table) {
  std::vector<Row> rows;
  for (const auto& row : table) {
    rows.push_back({row[0].getInt(), row[1].getInt(), row[2].getInt()});
  }
  return rows;
}

// Compute the join of the given inputs (in the default column order).
std::vector<Row> computeTriangles(const Edge& xy, const Edge& yz,
                                  const Edge& xz) {
  auto* qec = ad_utility::testing::getQec();
  LeapfrogTriangleJoin join{qec,
                            makeInput(xy, V{"?x"}, V{"?y"}),
                            makeInput(yz, V{"?y"}, V{"?z"}),
                            makeInput(xz, V{"?x"}, V{"?z"}),
                            V{"?x"},
                            V{"?y"},
                            V{"?z"}};
  auto result = join.computeResultOnlyForTesting();
  return toRows(result.idTableView());
}
}  // namespace

// _____________________________________________________________________________
TEST(LeapfrogTriangleJoin, matchesNestedLoopJoin) {
  Edge xy{{1, 2}, {1, 3}, {2, 3}, {4, 5}, {4, 6}, {7, 8}};
  Edge yz{{2, 3}, {3, 1}, {3, 4}, {5, 6}, {6, 9}, {8, 1}};
  Edge xz{{1, 3}, {1, 4}, {2, 1}, {4, 6}, {4, 9}, {7, 2}};
  auto expected = naiveTriangles(xy, yz, xz);
  ASSERT_FALSE(expected.empty());
  EXPECT_EQ(computeTriangles(xy, yz, xz), expected);
}

// Duplicate input rows multiply, as for a chain of binary joins.
TEST(LeapfrogTriangleJoin, bagSemantics) {
  Edge xy{{1, 2}, {1, 2}, {1, 3}};
  Edge yz{{2, 5}, {2, 5}, {2, 5}, {3, 5}};
  Edge xz{{1, 5}, {1, 5}};
  auto expected = naiveTriangles(xy, yz, xz);
  // (1, 2, 5): 2 * 3 * 2 times, (1, 3, 5): 1 * 1 * 2 times.
  ASSERT_EQ(expected.size(), 14u);
  EXPECT_EQ(computeTriangles(xy, yz, xz), expected);
}

// Inputs without a triangle, and empty inputs.
TEST(LeapfrogTriangleJoin, emptyResults) {
  EXPECT_TRUE(computeTriangles({{1, 2}}, {{2, 3}}, {{1, 4}}).empty());
  EXPECT_TRUE(computeTriangles({}, {{2, 3}}, {{1, 3}}).empty());
  EXPECT_TRUE(computeTriangles({{1, 2}}, {}, {{1, 3}}).empty());
  EXPECT_TRUE(computeTriangles({{1, 2}}, {{2, 3}}, {}).empty());
}

// A pseudo-random graph, compared against the nested loop join.
TEST(LeapfrogTriangleJoin, randomGraph) {
  Edge edges;
  uint64_t state = 42;
  auto next = [&state]() {
    state = state * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<int64_t>((state >> 33) % 30);
  };
  for (size_t i = 0; i < 300; ++i) {
    edges.push_back({next(), next()});
  }
  auto expected = naiveTriangles(edges, edges, edges);
  ASSERT_FALSE(expected.empty());
  EXPECT_EQ(computeTriangles(edges, edges, edges), expected);
}

// Inputs whose columns are in the opposite order of the variable order, and
// inputs that are already sorted, need no extra handling by the caller.
TEST(LeapfrogTriangleJoin, columnOrderAndSorting) {
  auto* qec = ad_utility::testing::getQec();
  Edge xy{{1, 2}, {1, 3}, {2, 3}};
  Edge yz{{2, 3}, {3, 1}, {3, 4}};
  Edge xz{{1, 3}, {1, 4}, {2, 1}};
  Edge zy;
  for (const auto& [y, z] : yz) {
    zy.push_back({z, y});
  }
  LeapfrogTriangleJoin join{qec,
                            makeInput(xy, V{"?x"}, V{"?y"}, true),
                            makeInput(zy, V{"?z"}, V{"?y"}),
                            makeInput(xz, V{"?x"}, V{"?z"}, true),
                            V{"?x"},
                            V{"?y"},
                            V{"?z"}};
  // Only the input with swapped columns gets a `Sort`.
  auto children = join.getChildren();
  ASSERT_EQ(children.size(), 3u);
  EXPECT_EQ(dynamic_cast<const Sort*>(children[0]->getRootOperation().get()),
            nullptr);
  EXPECT_NE(dynamic_cast<const Sort*>(children[1]->getRootOperation().get()),
            nullptr);
  EXPECT_EQ(dynamic_cast<const Sort*>(children[2]->getRootOperation().get()),
            nullptr);
  auto result = join.computeResultOnlyForTesting();
  EXPECT_EQ(toRows(result.idTableView()), naiveTriangles(xy, yz, xz));
  EXPECT_THAT(result.sortedBy(), ::testing::ElementsAre(0, 1, 2));
}

// The metadata of the operation.
TEST(LeapfrogTriangleJoin, metadata) {
  auto* qec = ad_utility::testing::getQec();
  auto makeJoin = [qec](const Edge& xz) {
    return LeapfrogTriangleJoin{qec,
                                makeInput({{1, 2}}, V{"?x"}, V{"?y"}),
                                makeInput({{2, 3}}, V{"?y"}, V{"?z"}),
                                makeInput(xz, V{"?x"}, V{"?z"}),
                                V{"?x"},
                                V{"?y"},
                                V{"?z"}};
  };
  auto join = makeJoin({{1, 3}});
  EXPECT_EQ(join.getResultWidth(), 3u);
  EXPECT_EQ(join.getDescriptor(), "LeapfrogTriangleJoin on ?x ?y ?z");
  EXPECT_THAT(join.getCacheKey(),
              ::testing::StartsWith("LEAPFROG_TRIANGLE_JOIN"));
  EXPECT_NE(join.getCacheKey(), makeJoin({{1, 4}}).getCacheKey());
  EXPECT_FALSE(join.knownEmptyResult());
  EXPECT_FLOAT_EQ(join.getMultiplicity(0), 1.0f);
  EXPECT_GT(join.getCostEstimate(), 0u);
  const auto& varCols = join.getExternallyVisibleVariableColumns();
  EXPECT_EQ(varCols.at(V{"?x"}), makeAlwaysDefinedColumn(0));
  EXPECT_EQ(varCols.at(V{"?y"}), makeAlwaysDefinedColumn(1));
  EXPECT_EQ(varCols.at(V{"?z"}), makeAlwaysDefinedColumn(2));

  auto clone = join.clone();
  ASSERT_NE(clone, nullptr);
  EXPECT_EQ(clone->getCacheKey(), join.getCacheKey());
  EXPECT_EQ(clone->getDescriptor(), join.getDescriptor());
}

// Inputs that do not bind the variables of their edge are rejected.
TEST(LeapfrogTriangleJoin, invalidInputs) {
  auto* qec = ad_utility::testing::getQec();
  auto xy = makeInput({{1, 2}}, V{"?x"}, V{"?y"});
  auto yz = makeInput({{2, 3}}, V{"?y"}, V{"?z"});
  auto xz = makeInput({{1, 3}}, V{"?x"}, V{"?z"});
  auto wrong = makeInput({{1, 3}}, V{"?x"}, V{"?w"});
  EXPECT_ANY_THROW(
      (LeapfrogTriangleJoin{qec, xy, yz, wrong, V{"?x"}, V{"?y"}, V{"?z"}}));
  EXPECT_ANY_THROW(
      (LeapfrogTriangleJoin{qec, xy, yz, xz, V{"?x"}, V{"?x"}, V{"?z"}}));
}
