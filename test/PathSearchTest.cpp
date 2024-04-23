// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include <gtest/gtest.h>

#include "engine/PathSearch.h"
#include "gmock/gmock.h"
#include "util/IdTableHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"

using ad_utility::testing::getQec;
namespace {
auto V = ad_utility::testing::VocabId;
using Vars = std::vector<std::optional<Variable>>;

}  // namespace

TEST(PathSearchTest, constructor) {
  auto qec = getQec();
  PathSearchConfiguration config{ALL_PATHS, V(0), V(1), 0, 1, 2, 3};
  PathSearch p = PathSearch(qec, nullptr, config);
}

TEST(PathSearchTest, singlePath) {
  auto sub = makeIdTableFromVector({{0, 1}, {1, 2}, {2, 3}, {3, 4}});
  auto expected = makeIdTableFromVector({});

  auto qec = getQec();
  PathSearchConfiguration config{ALL_PATHS, V(0), V(4), 0, 1, 2, 3};
  PathSearch p = PathSearch(qec, nullptr, config);

  auto resultTable = p.computeResult();
  ASSERT_THAT(resultTable.idTable(),
              ::testing::UnorderedElementsAreArray(expected));
}
