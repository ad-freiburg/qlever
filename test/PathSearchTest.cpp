// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include <gtest/gtest.h>

#include "engine/PathSearch.h"
#include "engine/QueryExecutionTree.h"
#include "engine/ResultTable.h"
#include "engine/ValuesForTesting.h"
#include "gmock/gmock.h"
#include "util/IdTableHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"

using ad_utility::testing::getQec;
namespace {
auto V = ad_utility::testing::VocabId;
auto I = ad_utility::testing::IntId;
using Vars = std::vector<std::optional<Variable>>;

}  // namespace

ResultTable performPathSearch(PathSearchConfiguration config, IdTable input,
                              Vars vars) {
  auto qec = getQec();
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(input), vars);
  PathSearch p = PathSearch(qec, std::move(subtree), config);

  return p.computeResult();
}

TEST(PathSearchTest, constructor) {
  auto qec = getQec();
  PathSearchConfiguration config{ALL_PATHS, V(0), {V(1)}, 0, 1, 2, 3};
  PathSearch p = PathSearch(qec, nullptr, config);
}

TEST(PathSearchTest, singlePath) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {2, 3}, {3, 4}});
  auto expected = makeIdTableFromVector({
      {V(0), V(1), I(0), I(0)},
      {V(1), V(2), I(0), I(1)},
      {V(2), V(3), I(0), I(2)},
      {V(3), V(4), I(0), I(3)},
  });

  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{ALL_PATHS, V(0), {V(4)}, 0, 1, 2, 3};

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

  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{ALL_PATHS, V(0), {V(2)}, 0, 1, 2, 3};

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

  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{ALL_PATHS, V(0), {V(2), V(4)}, 0, 1, 2, 3};

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

  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{ALL_PATHS, V(0), {V(0)}, 0, 1, 2, 3};

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

  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{ALL_PATHS, V(0), {V(0)}, 0, 1, 2, 3};

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
      {V(0), V(2), I(3), I(0)},
      {V(2), V(3), I(3), I(1)},
      {V(0), V(2), I(4), I(0)},
      {V(2), V(4), I(4), I(1)},
  });

  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  PathSearchConfiguration config{ALL_PATHS, V(0), {}, 0, 1, 2, 3};

  auto resultTable = performPathSearch(config, std::move(sub), vars);
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}
