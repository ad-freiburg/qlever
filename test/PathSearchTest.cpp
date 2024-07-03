// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include <gtest/gtest.h>

#include "engine/PathSearch.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/ValuesForTesting.h"
#include "gmock/gmock.h"
#include "util/IdTableHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"

using ad_utility::testing::getQec;
namespace {
auto V = ad_utility::testing::VocabId;
auto I = ad_utility::testing::IntId;
using Var = Variable;
using Vars = std::vector<std::optional<Variable>>;

}  // namespace

Result performPathSearch(PathSearchConfiguration config, IdTable input,
                         Vars vars) {
  auto qec = getQec();
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(input), vars);
  PathSearch p = PathSearch(qec, std::move(subtree), config);

  return p.computeResult(false);
}

TEST(PathSearchTest, constructor) {
  auto qec = getQec();
  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(1)};
  PathSearchConfiguration config{
      ALL_PATHS,   sources,           targets,           Var{"?start"},
      Var{"?end"}, Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};
  PathSearch p = PathSearch(qec, nullptr, config);
}

TEST(PathSearchTest, emptyGraph) {
  auto sub = makeIdTableFromVector({});
  sub.setNumColumns(2);
  auto expected = makeIdTableFromVector({});
  expected.setNumColumns(4);

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(4)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      ALL_PATHS,   sources,           targets,           Var{"?start"},
      Var{"?end"}, Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

/**
 * Graph:
 * 0 -> 1 -> 2 -> 3 -> 4
 */
TEST(PathSearchTest, singlePath) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {2, 3}, {3, 4}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(1), V(2), I(0), I(1)},
      {V(2), V(3), I(0), I(2)},
      {V(3), V(4), I(0), I(3)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(4)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      ALL_PATHS,   sources,           targets,           Var{"?start"},
      Var{"?end"}, Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, singlePathWithProperties) {
  auto sub =
      makeIdTableFromVector({{0, 1, 10}, {1, 2, 20}, {2, 3, 30}, {3, 4, 40}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0), V(10)},
      {V(1), V(2), I(0), I(1), V(20)},
      {V(2), V(3), I(0), I(2), V(30)},
      {V(3), V(4), I(0), I(3), V(40)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(4)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}, Variable{"?edgeProperty"}};
  PathSearchConfiguration config{ALL_PATHS,         sources,
                                 targets,           Var{"?start"},
                                 Var{"?end"},       Var{"?edgeIndex"},
                                 Var{"?pathIndex"}, {Var{"?edgeProperty"}}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, singlePathWithDijkstra) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {2, 3}, {3, 4}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(1), V(2), I(0), I(1)},
      {V(2), V(3), I(0), I(2)},
      {V(3), V(4), I(0), I(3)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(4)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      SHORTEST_PATHS, sources,           targets,           Var{"?start"},
      Var{"?end"},    Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, singlePathWithDijkstraAndProperties) {
  auto sub =
      makeIdTableFromVector({{0, 1, 10}, {1, 2, 20}, {2, 3, 30}, {3, 4, 40}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0), V(10)},
      {V(1), V(2), I(0), I(1), V(20)},
      {V(2), V(3), I(0), I(2), V(30)},
      {V(3), V(4), I(0), I(3), V(40)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(4)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}, Variable{"?edgeProperty"}};
  PathSearchConfiguration config{SHORTEST_PATHS,    sources,
                                 targets,           Var{"?start"},
                                 Var{"?end"},       Var{"?edgeIndex"},
                                 Var{"?pathIndex"}, {Var{"?edgeProperty"}}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

/**
 * Graph:
 *      0
 *     / \
 *  1 <   > 3
 *   \     /
 *    > 2 <
 */
TEST(PathSearchTest, twoPathsOneTarget) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {0, 3}, {3, 2}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(1), V(2), I(0), I(1)},
      {V(0), V(3), I(1), I(0)},
      {V(3), V(2), I(1), I(1)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(2)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      ALL_PATHS,   sources,           targets,           Var{"?start"},
      Var{"?end"}, Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

/**
 * Graph:
 *          0
 *         / \
 *      1 <   > 3
 *     /        \
 *  2 <          > 4
 */
TEST(PathSearchTest, twoPathsTwoTargets) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {0, 3}, {3, 4}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(1), V(2), I(0), I(1)},
      {V(0), V(3), I(1), I(0)},
      {V(3), V(4), I(1), I(1)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(2), V(4)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      ALL_PATHS,   sources,           targets,           Var{"?start"},
      Var{"?end"}, Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

/**
 * Graph:
 *
 *    2<---1
 *     \   ^
 *      \  |
 *       > 0
 */
TEST(PathSearchTest, cycle) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {2, 0}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(1), V(2), I(0), I(1)},
      {V(2), V(0), I(0), I(2)},
  });

  std::vector<Id> sources{V(0)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      ALL_PATHS,   sources,           sources,           Var{"?start"},
      Var{"?end"}, Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

/**
 * Graph:
 *
 *    2<---1--->3
 *     \   ^   /
 *      \  |  /
 *       > 0 <
 */
TEST(PathSearchTest, twoCycle) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {2, 0}, {1, 3}, {3, 0}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(1), V(2), I(0), I(1)},
      {V(2), V(0), I(0), I(2)},
      {V(0), V(1), I(1), I(0)},
      {V(1), V(3), I(1), I(1)},
      {V(3), V(0), I(1), I(2)},
  });

  std::vector<Id> sources{V(0)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      ALL_PATHS,   sources,           sources,           Var{"?start"},
      Var{"?end"}, Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

/**
 * Graph:
 *
 *     0
 *    / \
 *   1   2
 *    \ / \
 *     3   4
 */
TEST(PathSearchTest, allPaths) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 3}, {0, 2}, {2, 3}, {2, 4}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(0), V(1), I(1), I(0)},
      {V(1), V(3), I(1), I(1)},
      {V(0), V(2), I(2), I(0)},
      {V(2), V(3), I(2), I(1)},
      {V(0), V(2), I(3), I(0)},
      {V(0), V(2), I(4), I(0)},
      {V(2), V(4), I(4), I(1)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      ALL_PATHS,   sources,           targets,           Var{"?start"},
      Var{"?end"}, Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, allPathsWithPropertiesSwitched) {
  auto sub = makeIdTableFromVector({{0, 1, 10, 11},
                                    {1, 3, 20, 21},
                                    {0, 2, 30, 31},
                                    {2, 3, 40, 41},
                                    {2, 4, 50, 51}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0), V(11), V(10)},
      {V(0), V(1), I(1), I(0), V(11), V(10)},
      {V(1), V(3), I(1), I(1), V(21), V(20)},
      {V(0), V(2), I(2), I(0), V(31), V(30)},
      {V(2), V(3), I(2), I(1), V(41), V(40)},
      {V(0), V(2), I(3), I(0), V(31), V(30)},
      {V(0), V(2), I(4), I(0), V(31), V(30)},
      {V(2), V(4), I(4), I(1), V(51), V(50)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{};
  Vars vars = {Variable{"?start"}, Variable{"?end"}, Variable{"?edgeProperty1"},
               Variable{"?edgeProperty2"}};
  PathSearchConfiguration config{
      ALL_PATHS,         sources,
      targets,           Var{"?start"},
      Var{"?end"},       Var{"?edgeIndex"},
      Var{"?pathIndex"}, {Var{"?edgeProperty2"}, Var{"?edgeProperty1"}}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

/**
 * Graph:
 *
 *     0
 *    / \
 *   1   2
 *   |   |
 *   |   3
 *    \ /
 *     4
 */
TEST(PathSearchTest, singleShortestPath) {
  auto sub = makeIdTableFromVector({{0, 1}, {0, 2}, {1, 4}, {2, 3}, {3, 4}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(1), V(4), I(0), I(1)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(4)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      SHORTEST_PATHS, sources,           targets,           Var{"?start"},
      Var{"?end"},    Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

/**
 * Graph:
 *
 *     0
 *    /|\
 *   1 2 4
 *   | | |
 *   | 3 |
 *    \|/
 *     5
 */
TEST(PathSearchTest, twoShortestPaths) {
  auto sub = makeIdTableFromVector(
      {{0, 1}, {0, 2}, {0, 4}, {1, 5}, {2, 3}, {3, 5}, {4, 5}});
  auto expected = makeIdTableFromVector({
      {V(0), V(4), I(0), I(0)},
      {V(4), V(5), I(0), I(1)},
      {V(0), V(1), I(1), I(0)},
      {V(1), V(5), I(1), I(1)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(5)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      SHORTEST_PATHS, sources,           targets,           Var{"?start"},
      Var{"?end"},    Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

/**
 * Graph:
 * 0 -> 1 -> 2 -> 3 -> 4
 *                     ^
 *                    /
 *                   5
 */
TEST(PathSearchTest, singlePathWithIrrelevantNode) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {2, 3}, {3, 4}, {5, 4}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(1), V(2), I(0), I(1)},
      {V(2), V(3), I(0), I(2)},
      {V(3), V(4), I(0), I(3)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(4)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      ALL_PATHS,   sources,           targets,           Var{"?start"},
      Var{"?end"}, Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, shortestPathWithIrrelevantNode) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {2, 3}, {3, 4}, {5, 4}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(1), V(2), I(0), I(1)},
      {V(2), V(3), I(0), I(2)},
      {V(3), V(4), I(0), I(3)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(4)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      SHORTEST_PATHS, sources,           targets,           Var{"?start"},
      Var{"?end"},    Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

/**
 * Graph:
 *       0
 *       |
 *       1
 *      / \
 *     2   3
 *      \ /
 *       4
 *       |
 *       5
 */
TEST(PathSearchTest, allPathsElongatedDiamond) {
  auto sub =
      makeIdTableFromVector({{0, 1}, {1, 2}, {1, 3}, {2, 4}, {3, 4}, {4, 5}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(1), V(2), I(0), I(1)},
      {V(2), V(4), I(0), I(2)},
      {V(4), V(5), I(0), I(3)},
      {V(0), V(1), I(1), I(0)},
      {V(1), V(3), I(1), I(1)},
      {V(3), V(4), I(1), I(2)},
      {V(4), V(5), I(1), I(3)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(5)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      ALL_PATHS,   sources,           targets,           Var{"?start"},
      Var{"?end"}, Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, shortestPathsElongatedDiamond) {
  auto sub =
      makeIdTableFromVector({{0, 1}, {1, 2}, {1, 3}, {2, 4}, {3, 4}, {4, 5}});
  auto expected = makeIdTableFromVector({{V(0), V(1), I(0), I(0)},
                                         {V(1), V(2), I(0), I(1)},
                                         {V(2), V(4), I(0), I(2)},
                                         {V(4), V(5), I(0), I(3)}});

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(5)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      SHORTEST_PATHS, sources,           targets,           Var{"?start"},
      Var{"?end"},    Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

/**
 * Graph:
 *  0       4
 *   \     /
 *    2-->3
 *   /     \
 *  1       5
 */
TEST(PathSearchTest, multiSourceMultiTargetallPaths) {
  auto sub = makeIdTableFromVector({{0, 2}, {1, 2}, {2, 3}, {3, 4}, {3, 5}});
  auto expected = makeIdTableFromVector({
      {V(0), V(2), I(0), I(0)},
      {V(2), V(3), I(0), I(1)},
      {V(3), V(4), I(0), I(2)},
      {V(0), V(2), I(1), I(0)},
      {V(2), V(3), I(1), I(1)},
      {V(3), V(5), I(1), I(2)},
      {V(1), V(2), I(2), I(0)},
      {V(2), V(3), I(2), I(1)},
      {V(3), V(4), I(2), I(2)},
      {V(1), V(2), I(3), I(0)},
      {V(2), V(3), I(3), I(1)},
      {V(3), V(5), I(3), I(2)},
  });

  std::vector<Id> sources{V(0), V(1)};
  std::vector<Id> targets{V(4), V(5)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{
      ALL_PATHS,   sources,           targets,           Var{"?start"},
      Var{"?end"}, Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, multiSourceMultiTargetshortestPaths) {
  auto sub = makeIdTableFromVector({{0, 2}, {1, 2}, {2, 3}, {3, 4}, {3, 5}});
  auto expected = makeIdTableFromVector({
      {V(0), V(2), I(0), I(0)},
      {V(2), V(3), I(0), I(1)},
      {V(3), V(4), I(0), I(2)},
      {V(0), V(2), I(1), I(0)},
      {V(2), V(3), I(1), I(1)},
      {V(3), V(5), I(1), I(2)},
      {V(1), V(2), I(2), I(0)},
      {V(2), V(3), I(2), I(1)},
      {V(3), V(4), I(2), I(2)},
      {V(1), V(2), I(3), I(0)},
      {V(2), V(3), I(3), I(1)},
      {V(3), V(5), I(3), I(2)},
  });

  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  std::vector<Id> sources{V(0), V(1)};
  std::vector<Id> targets{V(4), V(5)};
  PathSearchConfiguration config{
      SHORTEST_PATHS, sources,           targets,           Var{"?start"},
      Var{"?end"},    Var{"?edgeIndex"}, Var{"?pathIndex"}, {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}
