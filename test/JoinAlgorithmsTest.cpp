//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/JoinAlgorithms/JoinAlgorithms.h"

using namespace ad_utility;
namespace {
using NestedBlock = std::vector<std::vector<size_t>>;

auto makeRowAdder(auto& target) {
  return [&target](auto it1, auto it2) {
    AD_CONTRACT_CHECK(*it1 == *it2);
    target.push_back(*it1);
  };
}
}  // namespace

TEST(JoinAlgorithms, JoinWithBlocksEmptyInput) {
  NestedBlock a;
  NestedBlock b;
  std::vector<size_t> result;
  zipperJoinForBlocksWithoutUndef(a, b, std::less<>{}, makeRowAdder(result));
  EXPECT_TRUE(result.empty());

  a.push_back({13});
  zipperJoinForBlocksWithoutUndef(a, b, std::less<>{}, makeRowAdder(result));
  EXPECT_TRUE(result.empty());

  b.emplace_back();
  zipperJoinForBlocksWithoutUndef(a, b, std::less<>{}, makeRowAdder(result));
  EXPECT_TRUE(result.empty());

  a.clear();
  b.push_back({23, 35});
  zipperJoinForBlocksWithoutUndef(a, b, std::less<>{}, makeRowAdder(result));
  EXPECT_TRUE(result.empty());
}

TEST(JoinAlgorithms, JoinWithBlocksSingleBlock) {
  NestedBlock a{{1, 4, 18, 42}};
  NestedBlock b{{0, 4, 5, 19, 42}};
  std::vector<size_t> result;
  zipperJoinForBlocksWithoutUndef(a, b, std::less<>{}, makeRowAdder(result));
  EXPECT_THAT(result, ::testing::ElementsAre(4, 42));
}

TEST(JoinAlgorithms, JoinWithBlocksMultipleBlocksOverlap) {
  NestedBlock a{{1, 4, 18, 42}, {54, 57, 59}};
  NestedBlock b{{0, 4, 5, 19, 42, 54}, {56, 57, 58}};
  std::vector<size_t> result;
  zipperJoinForBlocksWithoutUndef(a, b, std::less<>{}, makeRowAdder(result));
  EXPECT_THAT(result, ::testing::ElementsAre(4, 42, 54, 57));
}
