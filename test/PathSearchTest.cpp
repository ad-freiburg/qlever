// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Herrmann (johannes.r.herrmann(at)gmail.com)

#include <gtest/gtest.h>

#include "engine/PathSearch.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"

using ad_utility::testing::getQec;
namespace {
auto V = ad_utility::testing::VocabId;
using Vars = std::vector<std::optional<Variable>>;

}  // namespace

TEST(PathSearchTest, constructor) {
  auto qec = getQec();
  PathSearch p = PathSearch(qec, nullptr);
}

TEST(PathSearchTest, findPaths) {}

TEST(PathSearchTest, buildGraph) {}
