// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include <gtest/gtest.h>

#include "engine/PathSearch.h"
#include "engine/QueryExecutionTree.h"
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

TEST(PathSearchTest, constructor) {
  auto qec = getQec();
  PathSearchConfiguration config{ALL_PATHS, V(0), V(1), 0, 1, 2, 3};
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

  auto qec = getQec();
  Vars vars = {Variable{"?start"}, Variable{"?end"}};
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(sub), vars);
  PathSearchConfiguration config{ALL_PATHS, V(0), V(4), 0, 1, 2, 3};
  PathSearch p = PathSearch(qec, std::move(subtree), config);

  auto resultTable = p.computeResult();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}
