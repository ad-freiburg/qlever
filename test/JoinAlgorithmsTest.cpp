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
  NestedBlock a{{1, 4, 18, 42}, {54, 57, 59}, {60, 67}};
  NestedBlock b{{0, 4, 5, 19, 42, 54}, {56, 57, 58, 59}, {61, 67}};
  std::vector<size_t> result;
  zipperJoinForBlocksWithoutUndef(NestedBlock(a), NestedBlock(b), std::less<>{},
                                  makeRowAdder(result));
  EXPECT_THAT(result, ::testing::ElementsAre(4, 42, 54, 57, 59, 67));
  result.clear();
  zipperJoinForBlocksWithoutUndef(NestedBlock(b), NestedBlock(a), std::less<>{},
                                  makeRowAdder(result));
  EXPECT_THAT(result, ::testing::ElementsAre(4, 42, 54, 57, 59, 67));
}

TEST(JoinAlgorithms, JoinWithBlocksMultipleBlocksDuplicates) {
  using NB = std::vector<std::vector<std::array<size_t, 2>>>;
  NB a{{{1, 0}, {42, 0}}, {{42, 1}, {42, 2}}, {{42, 3}, {43, 5}, {67, 0}}};
  NB b{{{2, 0}, {42, 12}, {43, 1}}, {{67, 13}, {69, 14}}};
  std::vector<std::array<size_t, 3>> result;
  std::vector<std::array<size_t, 3>> expectedResult{
      {42, 0, 12}, {42, 1, 12}, {42, 2, 12}, {42, 3, 12}, {67, 0, 13}};
  auto compare = [](auto l, auto r) { return l[0] < r[0]; };
  auto add = [&result](auto it1, auto it2) {
    AD_CORRECTNESS_CHECK((*it1)[0] == (*it1)[0]);
    result.push_back(std::array{(*it1)[0], (*it1)[1], (*it2)[1]});
  };
  zipperJoinForBlocksWithoutUndef(NB{a}, NB{b}, compare, add);
  EXPECT_THAT(result, ::testing::ElementsAreArray(expectedResult));
  result.clear();
  for (auto& [x, y, z] : expectedResult) {
    std::swap(y, z);
  }
  zipperJoinForBlocksWithoutUndef(NB{b}, NB{a}, compare, add);
  EXPECT_THAT(result, ::testing::ElementsAreArray(expectedResult));
}
