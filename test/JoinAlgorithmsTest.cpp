//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/GTestHelpers.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"
#include "util/TransparentFunctors.h"

using namespace ad_utility;
namespace {

// Some helpers for testing the joining of blocks of Integers.
using Block = std::vector<std::array<size_t, 2>>;
using NestedBlock = std::vector<Block>;
using JoinResult = std::vector<std::array<size_t, 3>>;

auto makeRowAdder(JoinResult& target) {
  // `it1, it2` must be (const) iterators to a `Block`.
  return [&target](auto it1, auto it2) {
    auto [x1, x2] = *it1;
    auto [y1, y2] = *it2;
    AD_CONTRACT_CHECK(x1 == y1);
    target.push_back(std::array{x1, x2, y2});
  };
}

using ad_utility::source_location;
// Test that when joining `a` and `b` on the first column then the result is
// equal to the `expected` result.
// TODO<joka921> We could also resplit inputs into blocks randomly and thus add
// more test cases automatically.
void testJoin(const NestedBlock& a, const NestedBlock& b, JoinResult expected,
              source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l);
  JoinResult result;
  auto compare = [](auto l, auto r) { return l[0] < r[0]; };
  zipperJoinForBlocksWithoutUndef(a, b, compare, makeRowAdder(result));
  // The result must be sorted on the first column
  EXPECT_TRUE(std::ranges::is_sorted(result, std::less<>{}, ad_utility::first));
  // The exact order of the elements with the same first column is not important
  // and depends on implementation details. We therefore do not enforce it here.
  EXPECT_THAT(result, ::testing::UnorderedElementsAreArray(expected));
  result.clear();
  for (auto& [x, y, z] : expected) {
    std::swap(y, z);
  }
  zipperJoinForBlocksWithoutUndef(b, a, compare, makeRowAdder(result));
  EXPECT_TRUE(std::ranges::is_sorted(result, std::less<>{}, ad_utility::first));
  EXPECT_THAT(result, ::testing::UnorderedElementsAreArray(expected));
}
}  // namespace

// ________________________________________________________________________________________
TEST(JoinAlgorithms, JoinWithBlocksEmptyInput) {
  testJoin({}, {}, {});

  testJoin({{{13, 0}}}, {}, {});
  testJoin({{}, {{13, 0}}, {}}, {{}}, {});
}

TEST(JoinAlgorithms, JoinWithBlocksSingleBlock) {
  NestedBlock a{{{1, 11}, {4, 12}, {18, 13}, {42, 14}}};
  NestedBlock b{{{0, 24}, {4, 25}, {5, 25}, {19, 26}, {42, 27}}};
  JoinResult expectedResult{{4, 12, 25}, {42, 14, 27}};
  testJoin(a, b, expectedResult);
}

TEST(JoinAlgorithms, JoinWithBlocksMultipleBlocksOverlap) {
  NestedBlock a{{{1, 10}, {4, 11}, {18, 12}, {42, 13}},
                {{54, 14}, {57, 15}, {59, 16}},
                {{60, 17}, {67, 18}}};
  NestedBlock b{{{0, 20}, {4, 21}, {5, 22}, {19, 23}, {42, 24}, {54, 25}},
                {{56, 26}, {57, 27}, {58, 28}, {59, 29}},
                {{61, 30}, {67, 30}}};
  JoinResult expectedResult{{4, 11, 21},  {42, 13, 24}, {54, 14, 25},
                            {57, 15, 27}, {59, 16, 29}, {67, 18, 30}};
  testJoin(a, b, expectedResult);
}

TEST(JoinAlgorithms, JoinWithBlocksMultipleBlocksPerElement) {
  NestedBlock a{
      {{1, 0}, {42, 0}}, {{42, 1}, {42, 2}}, {{42, 3}, {48, 5}, {67, 0}}};
  NestedBlock b{{{2, 0}, {42, 12}, {43, 1}}, {{67, 13}, {69, 14}}};
  JoinResult expectedResult{
      {42, 0, 12}, {42, 1, 12}, {42, 2, 12}, {42, 3, 12}, {67, 0, 13}};
  testJoin(a, b, expectedResult);
}

TEST(JoinAlgorithms, JoinWithBlocksMultipleBlocksPerElementBothSides) {
  NestedBlock a{{{42, 0}}, {{42, 1}, {42, 2}}, {{42, 3}, {67, 0}}};
  NestedBlock b{{{2, 0}, {42, 12}}, {{42, 13}, {67, 14}}};
  std::vector<std::array<size_t, 3>> expectedResult{
      {42, 0, 12}, {42, 0, 13}, {42, 1, 12}, {42, 2, 12}, {42, 1, 13},
      {42, 2, 13}, {42, 3, 12}, {42, 3, 13}, {67, 0, 14}};
  testJoin(a, b, expectedResult);
}
