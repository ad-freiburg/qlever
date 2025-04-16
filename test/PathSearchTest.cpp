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
#include "util/OperationTestHelpers.h"

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
  PathSearch p = PathSearch(qec, std::move(subtree), std::move(config));

  return p.computeResult(false);
}

TEST(PathSearchTest, constructor) {
  auto qec = getQec();
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  auto sub = makeIdTableFromVector({});
  sub.setNumColumns(2);
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(sub), vars);

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(1)};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};
  PathSearch p = PathSearch(qec, std::move(subtree), config);
}

TEST(PathSearchTest, emptyGraph) {
  auto sub = makeIdTableFromVector({});
  sub.setNumColumns(2);
  auto expected = makeIdTableFromVector({});
  expected.setNumColumns(4);

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(4)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

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
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

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
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {Var{"?edgeProperty"}}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, singlePathAllSources) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {2, 3}, {3, 4}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0), V(0)},
      {V(1), V(2), I(0), I(1), V(0)},
      {V(2), V(3), I(0), I(2), V(0)},
      {V(3), V(4), I(0), I(3), V(0)},
      {V(1), V(2), I(1), I(0), V(1)},
      {V(2), V(3), I(1), I(1), V(1)},
      {V(3), V(4), I(1), I(2), V(1)},
      {V(2), V(3), I(2), I(0), V(2)},
      {V(3), V(4), I(2), I(1), V(2)},
      {V(3), V(4), I(3), I(0), V(3)},
  });

  std::vector<Id> targets{V(4)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 Var{"?sources"},
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, singlePathAllTargets) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {2, 3}, {3, 4}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0), V(1)},
      {V(0), V(1), I(1), I(0), V(2)},
      {V(1), V(2), I(1), I(1), V(2)},
      {V(0), V(1), I(2), I(0), V(3)},
      {V(1), V(2), I(2), I(1), V(3)},
      {V(2), V(3), I(2), I(2), V(3)},
      {V(0), V(1), I(3), I(0), V(4)},
      {V(1), V(2), I(3), I(1), V(4)},
      {V(2), V(3), I(3), I(2), V(4)},
      {V(3), V(4), I(3), I(3), V(4)},
  });

  std::vector<Id> sources{V(0)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 Var{"?targets"},
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

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
      {V(0), V(3), I(0), I(0)},
      {V(3), V(2), I(0), I(1)},
      {V(0), V(1), I(1), I(0)},
      {V(1), V(2), I(1), I(1)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(2)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

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
      {V(0), V(3), I(0), I(0)},
      {V(3), V(4), I(0), I(1)},
      {V(0), V(1), I(1), I(0)},
      {V(1), V(2), I(1), I(1)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(2), V(4)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

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
      {V(0), V(1), I(0), I(0), V(1)},
      {V(0), V(1), I(1), I(0), V(2)},
      {V(1), V(2), I(1), I(1), V(2)},
  });

  std::vector<Id> sources{V(0)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 Var{"?targets"},
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

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
  auto expected = makeIdTableFromVector({{V(0), V(1), I(0), I(0), V(1)},
                                         {V(0), V(1), I(1), I(0), V(3)},
                                         {V(1), V(3), I(1), I(1), V(3)},
                                         {V(0), V(1), I(2), I(0), V(2)},
                                         {V(1), V(2), I(2), I(1), V(2)}});

  std::vector<Id> sources{V(0)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 Var{"?targets"},
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

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
  auto sub = makeIdTableFromVector({{0, 1}, {0, 2}, {1, 3}, {2, 3}, {2, 4}});
  auto expected = makeIdTableFromVector({
      {V(0), V(2), I(0), I(0), V(2)},
      {V(0), V(2), I(1), I(0), V(4)},
      {V(2), V(4), I(1), I(1), V(4)},
      {V(0), V(2), I(2), I(0), V(3)},
      {V(2), V(3), I(2), I(1), V(3)},
      {V(0), V(1), I(3), I(0), V(1)},
      {V(0), V(1), I(4), I(0), V(3)},
      {V(1), V(3), I(4), I(1), V(3)},
  });

  std::vector<Id> sources{V(0)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 Var{"?targets"},
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

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
      {V(0), V(2), I(0), I(0), V(2), V(31), V(30)},
      {V(0), V(2), I(1), I(0), V(4), V(31), V(30)},
      {V(2), V(4), I(1), I(1), V(4), V(51), V(50)},
      {V(0), V(2), I(2), I(0), V(3), V(31), V(30)},
      {V(2), V(3), I(2), I(1), V(3), V(41), V(40)},
      {V(0), V(1), I(3), I(0), V(1), V(11), V(10)},
      {V(0), V(1), I(4), I(0), V(3), V(11), V(10)},
      {V(1), V(3), I(4), I(1), V(3), V(21), V(20)},
  });

  std::vector<Id> sources{V(0)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}, Variable{"?edgeProperty1"},
               Variable{"?edgeProperty2"}};
  PathSearchConfiguration config{
      PathSearchAlgorithm::ALL_PATHS,
      sources,
      Var{"?targets"},
      Var{"?start"},
      Var{"?end"},
      Var{"?edgeIndex"},
      Var{"?pathIndex"},
      {Var{"?edgeProperty2"}, Var{"?edgeProperty1"}}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

/**
 * Graph:
 *
 *     0
 *     |\
 *     | \
 *     1->2->3
 */
TEST(PathSearchTest, allPathsPartialAllTargets) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {0, 2}, {2, 3}});
  auto expected = makeIdTableFromVector({
      {V(0), V(2), I(0), I(0), V(2)},
      {V(0), V(2), I(1), I(0), V(3)},
      {V(2), V(3), I(1), I(1), V(3)},
      {V(0), V(1), I(2), I(0), V(1)},
      {V(0), V(1), I(3), I(0), V(2)},
      {V(1), V(2), I(3), I(1), V(2)},
      {V(0), V(1), I(4), I(0), V(3)},
      {V(1), V(2), I(4), I(1), V(3)},
      {V(2), V(3), I(4), I(2), V(3)},
  });

  std::vector<Id> sources{V(0)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 Var{"?targets"},
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, allPathsPartialAllSources) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {0, 2}, {2, 3}});
  auto expected = makeIdTableFromVector({
      {V(0), V(2), I(0), I(0), V(0)},
      {V(2), V(3), I(0), I(1), V(0)},
      {V(0), V(1), I(1), I(0), V(0)},
      {V(1), V(2), I(1), I(1), V(0)},
      {V(2), V(3), I(1), I(2), V(0)},
      {V(1), V(2), I(2), I(0), V(1)},
      {V(2), V(3), I(2), I(1), V(1)},
      {V(2), V(3), I(3), I(0), V(2)},
  });

  std::vector<Id> targets{V(3)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 Var{"?sources"},
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

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
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

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
TEST(PathSearchTest, elongatedDiamond) {
  auto sub =
      makeIdTableFromVector({{0, 1}, {1, 2}, {1, 3}, {2, 4}, {3, 4}, {4, 5}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(1), V(3), I(0), I(1)},
      {V(3), V(4), I(0), I(2)},
      {V(4), V(5), I(0), I(3)},
      {V(0), V(1), I(1), I(0)},
      {V(1), V(2), I(1), I(1)},
      {V(2), V(4), I(1), I(2)},
      {V(4), V(5), I(1), I(3)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(5)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(PathSearchTest, numPathsPerTarget) {
  auto sub =
      makeIdTableFromVector({{0, 1}, {1, 2}, {1, 3}, {2, 4}, {3, 4}, {4, 5}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(1), V(3), I(0), I(1)},
      {V(3), V(4), I(0), I(2)},
      {V(0), V(1), I(1), I(0)},
      {V(1), V(3), I(1), I(1)},
      {V(3), V(4), I(1), I(2)},
      {V(4), V(5), I(1), I(3)},
  });

  std::vector<Id> sources{V(0)};
  std::vector<Id> targets{V(4), V(5)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {},
                                 true,
                                 1};

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
      {V(3), V(5), I(0), I(2)},
      {V(0), V(2), I(1), I(0)},
      {V(2), V(3), I(1), I(1)},
      {V(3), V(4), I(1), I(2)},
      {V(1), V(2), I(2), I(0)},
      {V(2), V(3), I(2), I(1)},
      {V(3), V(5), I(2), I(2)},
      {V(1), V(2), I(3), I(0)},
      {V(2), V(3), I(3), I(1)},
      {V(3), V(4), I(3), I(2)},
  });

  std::vector<Id> sources{V(0), V(1)};
  std::vector<Id> targets{V(4), V(5)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, multiSourceMultiTargetallPathsNotCartesian) {
  auto sub = makeIdTableFromVector({{0, 2}, {1, 2}, {2, 3}, {3, 4}, {3, 5}});
  auto expected = makeIdTableFromVector({
      {V(0), V(2), I(0), I(0)},
      {V(2), V(3), I(0), I(1)},
      {V(3), V(4), I(0), I(2)},
      {V(1), V(2), I(1), I(0)},
      {V(2), V(3), I(1), I(1)},
      {V(3), V(5), I(1), I(2)},
  });

  std::vector<Id> sources{V(0), V(1)};
  std::vector<Id> targets{V(4), V(5)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {},
                                 false};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, sourceBound) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {2, 3}, {3, 4}});
  auto sourceTable = makeIdTableFromVector({{0}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0), V(0)},
      {V(1), V(2), I(0), I(1), V(0)},
      {V(2), V(3), I(0), I(2), V(0)},
      {V(3), V(4), I(0), I(3), V(0)},
  });

  std::vector<Id> targets{V(4)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 Var{"?source"},
                                 targets,
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

  auto qec = getQec();
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(sub), vars);
  auto pathSearch = PathSearch(qec, std::move(subtree), std::move(config));

  Vars sourceTreeVars = {Var{"?source"}};
  auto sourceTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(sourceTable), sourceTreeVars);
  pathSearch.bindSourceSide(sourceTree, 0);

  auto resultTable = pathSearch.computeResult(false);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, targetBound) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {2, 3}, {3, 4}});
  auto targetTable = makeIdTableFromVector({{4}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0), V(4)},
      {V(1), V(2), I(0), I(1), V(4)},
      {V(2), V(3), I(0), I(2), V(4)},
      {V(3), V(4), I(0), I(3), V(4)},
  });

  std::vector<Id> sources{V(0)};
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 sources,
                                 Var{"?target"},
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

  auto qec = getQec();
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(sub), vars);
  auto pathSearch = PathSearch(qec, std::move(subtree), std::move(config));

  Vars targetTreeVars = {Var{"?target"}};
  auto targetTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(targetTable), targetTreeVars);
  pathSearch.bindTargetSide(targetTree, 0);

  auto resultTable = pathSearch.computeResult(false);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

TEST(PathSearchTest, sourceAndTargetBound) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {2, 3}, {3, 4}});
  auto sideTable = makeIdTableFromVector({{0, 4}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0), V(0), V(4)},
      {V(1), V(2), I(0), I(1), V(0), V(4)},
      {V(2), V(3), I(0), I(2), V(0), V(4)},
      {V(3), V(4), I(0), I(3), V(0), V(4)},
  });

  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 Var{"?source"},
                                 Var{"?target"},
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

  auto qec = getQec();
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(sub), vars);
  auto pathSearch = PathSearch(qec, std::move(subtree), std::move(config));

  Vars sideTreeVars = {Var{"?source"}, Var{"?target"}};
  auto sideTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(sideTable), sideTreeVars);
  pathSearch.bindSourceAndTargetSide(sideTree, 0, 1);

  auto resultTable = pathSearch.computeResult(false);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(PathSearchTest, clone) {
  auto sub = makeIdTableFromVector({{0, 1}});

  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{PathSearchAlgorithm::ALL_PATHS,
                                 Var{"?source"},
                                 Var{"?target"},
                                 Var{"?start"},
                                 Var{"?end"},
                                 Var{"?edgeIndex"},
                                 Var{"?pathIndex"},
                                 {}};

  auto qec = getQec();
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(sub), vars);
  PathSearch pathSearch{qec, subtree, std::move(config)};

  auto clone = pathSearch.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(pathSearch, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), pathSearch.getDescriptor());

  pathSearch.bindSourceSide(subtree, 0);
  pathSearch.bindTargetSide(subtree, 0);
  pathSearch.bindSourceAndTargetSide(subtree, 0, 0);

  clone = pathSearch.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(pathSearch, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), pathSearch.getDescriptor());
}
