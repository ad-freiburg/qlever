//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <random>
#include <set>

#include "./util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"
#include "util/TransparentFunctors.h"

using namespace ad_utility;
namespace {

// A magic constant to test the optional joins.
static constexpr size_t U = std::numeric_limits<size_t>::max() - 42;

// Some helpers for testing the joining of blocks of Integers.
using Block = std::vector<std::array<size_t, 2>>;
using NestedBlock = std::vector<Block>;
using JoinResult = std::vector<std::array<size_t, 3>>;

struct RowAdder {
  const Block* left_{};
  const Block* right_{};
  JoinResult* target_{};

  void setInput(const Block& left, const Block& right) {
    left_ = &left;
    right_ = &right;
  }

  void setOnlyLeftInputForOptionalJoin(const Block& left) { left_ = &left; }

  void addRow(size_t leftIndex, size_t rightIndex) {
    auto [x1, x2] = (*left_)[leftIndex];
    auto [y1, y2] = (*right_)[rightIndex];
    AD_CONTRACT_CHECK(x1 == y1);
    target_->push_back(std::array{x1, x2, y2});
  }
  template <typename R1, typename R2>
  void addRows(const R1& left, const R2& right) {
    for (auto a : left) {
      for (auto b : right) {
        addRow(a, b);
      }
    }
  }

  void addOptionalRow(size_t leftIndex) {
    auto [x1, x2] = (*left_)[leftIndex];
    target_->emplace_back(std::array{x1, x2, U});
  }

  void flush() const {
    // Does nothing, but is required for the interface.
  }
};

auto makeRowAdder(JoinResult& target) {
  return RowAdder{nullptr, nullptr, &target};
}

using ad_utility::source_location;
// Test that when joining `a` and `b` on the first column then the result is
// equal to the `expected` result.
// TODO<joka921> We could also resplit inputs into blocks randomly and thus add
// more test cases automatically.
template <bool DoOptionalJoin = false>
void testJoin(const NestedBlock& a, const NestedBlock& b, JoinResult expected,
              source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  JoinResult result;
  auto compare = [](auto l, auto r) { return l[0] < r[0]; };
  auto adder = makeRowAdder(result);
  if constexpr (DoOptionalJoin) {
    zipperJoinForBlocksWithoutUndef(a, b, compare, adder, ql::identity{},
                                    ql::identity{},
                                    ad_utility::OptionalJoinTag{});
  } else {
    zipperJoinForBlocksWithoutUndef(a, b, compare, adder);
  }
  // The result must be sorted on the first column
  EXPECT_TRUE(ql::ranges::is_sorted(result, std::less<>{}, ad_utility::first));
  // The exact order of the elements with the same first column is not important
  // and depends on implementation details. We therefore do not enforce it here.
  EXPECT_THAT(result, ::testing::UnorderedElementsAreArray(expected));

  if constexpr (DoOptionalJoin) {
    return;
  }
  result.clear();
  for (auto& [x, y, z] : expected) {
    std::swap(y, z);
  }

  {
    auto adder = makeRowAdder(result);
    zipperJoinForBlocksWithoutUndef(b, a, compare, adder);
    EXPECT_TRUE(
        ql::ranges::is_sorted(result, std::less<>{}, ad_utility::first));
    EXPECT_THAT(result, ::testing::UnorderedElementsAreArray(expected));
  }
}
void testOptionalJoin(const NestedBlock& a, const NestedBlock& b,
                      JoinResult expected,
                      source_location l = AD_CURRENT_SOURCE_LOC()) {
  testJoin<true>(a, b, std::move(expected), l);
}
}  // namespace

// ________________________________________________________________________________________
TEST(JoinAlgorithms, JoinWithBlocksEmptyInput) {
  testJoin({}, {}, {});
  testOptionalJoin({}, {}, {});

  testJoin({{{13, 0}}}, {}, {});
  testOptionalJoin({{{13, 0}}}, {}, {{13, 0, U}});
  // Optional joins are not symmetric.
  testOptionalJoin({}, {{{13, 0}}}, {});

  testJoin({{}, {{13, 0}}, {}}, {{}}, {});
  testOptionalJoin({{}, {{13, 0}}, {}}, {{}}, {{13, 0, U}});
}

// ________________________________________________________________________________________
TEST(JoinAlgorithms, JoinWithBlocksSingleBlock) {
  NestedBlock a{{{1, 11}, {4, 12}, {18, 13}, {42, 14}}};
  NestedBlock b{{{0, 24}, {4, 25}, {5, 25}, {19, 26}, {42, 27}}};
  JoinResult expectedResult{{4, 12, 25}, {42, 14, 27}};
  testJoin(a, b, expectedResult);

  JoinResult expectedResultOptional{
      {1, 11, U}, {4, 12, 25}, {18, 13, U}, {42, 14, 27}};
  testOptionalJoin(a, b, expectedResultOptional);
}

// ________________________________________________________________________________________
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

  JoinResult expectedResultOptional{{1, 10, U},   {4, 11, 21},  {18, 12, U},
                                    {42, 13, 24}, {54, 14, 25}, {57, 15, 27},
                                    {59, 16, 29}, {60, 17, U},  {67, 18, 30}};
  testOptionalJoin(a, b, expectedResultOptional);
}

// ________________________________________________________________________________________
TEST(JoinAlgorithms, JoinWithBlocksMultipleBlocksPerElement) {
  NestedBlock a{{{1, 0}, {42, 0}},
                {{42, 1}, {42, 2}},
                {{42, 3}, {48, 5}, {67, 0}},
                {{96, 32}},
                {{96, 33}}};
  NestedBlock b{{{2, 0}, {42, 12}, {43, 1}}, {{67, 13}, {69, 14}}};
  JoinResult expectedResult{
      {42, 0, 12}, {42, 1, 12}, {42, 2, 12}, {42, 3, 12}, {67, 0, 13}};
  testJoin(a, b, expectedResult);

  JoinResult expectedResultOptional{{1, 0, U},   {42, 0, 12}, {42, 1, 12},
                                    {42, 2, 12}, {42, 3, 12}, {48, 5, U},
                                    {67, 0, 13}, {96, 32, U}, {96, 33, U}};
  testOptionalJoin(a, b, expectedResultOptional);
}

// ________________________________________________________________________________________
TEST(JoinAlgorithms, JoinWithBlocksMoreThanThreeBlocksPerElement) {
  NestedBlock a{{{42, 0}},  {{42, 1}}, {{42, 2}}, {{42, 3}, {48, 5}, {67, 0}},
                {{96, 32}}, {{96, 33}}};
  NestedBlock b{{{42, 12}, {67, 13}}};
  JoinResult expectedResult{
      {42, 0, 12}, {42, 1, 12}, {42, 2, 12}, {42, 3, 12}, {67, 0, 13}};
  testJoin(a, b, expectedResult);
}

// Test the coverage of a corner case.
TEST(JoinAlgorithms, JoinWithBlocksExactlyFourBlocksPerElement) {
  NestedBlock a{{{42, 0}}, {{42, 1}},          {{42, 2}},  {{42, 3}},
                {},        {{48, 5}, {67, 0}}, {{96, 32}}, {{96, 33}}};
  NestedBlock b{{{42, 12}, {67, 13}}};
  JoinResult expectedResult{
      {42, 0, 12}, {42, 1, 12}, {42, 2, 12}, {42, 3, 12}, {67, 0, 13}};
  testJoin(a, b, expectedResult);
}

// Test the handling of many empty blocks.
TEST(JoinAlgorithms, JoinWithEmptyBlocks) {
  NestedBlock a{{{42, 1}, {42, 2}}, {{42, 3}}};
  // The join has to handle all the entries with `42` in the first column at
  // once. In `b` there are more than 3 empty blocks between blocks with this
  // entry. There was previously a bug in this case.
  NestedBlock b{{{42, 16}}, {}, {}, {}, {}, {}, {}, {}, {}, {{42, 23}}};
  JoinResult expectedResult{{42, 1, 16}, {42, 1, 23}, {42, 2, 16},
                            {42, 2, 23}, {42, 3, 16}, {42, 3, 23}};
  testJoin(a, b, expectedResult);
}

// ________________________________________________________________________________________
TEST(JoinAlgorithms, JoinWithBlocksMultipleBlocksPerElementBothSides) {
  NestedBlock a{{{42, 0}}, {{42, 1}, {42, 2}}, {{42, 3}, {67, 0}}};
  NestedBlock b{{{2, 0}, {42, 12}}, {{42, 13}, {67, 14}}};
  std::vector<std::array<size_t, 3>> expectedResult{
      {42, 0, 12}, {42, 0, 13}, {42, 1, 12}, {42, 2, 12}, {42, 1, 13},
      {42, 2, 13}, {42, 3, 12}, {42, 3, 13}, {67, 0, 14}};
  testJoin(a, b, expectedResult);

  // All elements of `a` have a matching counterpart in `b` so the result for
  // the optional join stays the same.
  testOptionalJoin(a, b, expectedResult);
}

// Regression tests for https://github.com/ad-freiburg/qlever/issues/2051
TEST(JoinAlgorithms, JoinWithEquivalentValuesOverMultipleEnclosedBlocks) {
  // The number of equivalent blocks is based on this constant in the code.
  static_assert(ad_utility::detail::FETCH_BLOCKS == 3);
  {
    NestedBlock a{{{1, 10}}, {{1, 11}}, {{1, 12}}, {{1, 13}}, {{2, 20}}};
    NestedBlock b{{{1, 100}}, {{1, 101}}, {{1, 102}}};
    std::vector<std::array<size_t, 3>> expectedResult{
        {1, 10, 100}, {1, 10, 101}, {1, 10, 102}, {1, 11, 100},
        {1, 11, 101}, {1, 11, 102}, {1, 12, 100}, {1, 12, 101},
        {1, 12, 102}, {1, 13, 100}, {1, 13, 101}, {1, 13, 102},
    };
    testJoin(a, b, expectedResult);
  }
  {
    NestedBlock a{{{1, 10}}, {{1, 11}}, {{1, 12}}, {{1, 13}}, {{2, 20}}};
    NestedBlock b{{{1, 100}}, {{1, 101}}};
    std::vector<std::array<size_t, 3>> expectedResult{
        {1, 10, 100}, {1, 10, 101}, {1, 11, 100}, {1, 11, 101},
        {1, 12, 100}, {1, 12, 101}, {1, 13, 100}, {1, 13, 101}};
    testJoin(a, b, expectedResult);
  }
  {
    NestedBlock a{{{1, 10}}, {{1, 11}}, {{1, 12}}, {{1, 13}}, {{2, 20}}};
    NestedBlock b{{{1, 100}}, {{1, 101}}, {{1, 102}, {3, 300}}};
    std::vector<std::array<size_t, 3>> expectedResult{
        {1, 10, 100}, {1, 10, 101}, {1, 10, 102}, {1, 11, 100},
        {1, 11, 101}, {1, 11, 102}, {1, 12, 100}, {1, 12, 101},
        {1, 12, 102}, {1, 13, 100}, {1, 13, 101}, {1, 13, 102}};
    testJoin(a, b, expectedResult);
  }
  {
    NestedBlock a{{{1, 10}}, {{1, 11}}, {{1, 12}}, {{1, 13}}, {{2, 20}}};
    NestedBlock b{{{1, 100}}, {{1, 101}}, {{3, 300}}};
    std::vector<std::array<size_t, 3>> expectedResult{
        {1, 10, 100}, {1, 10, 101}, {1, 11, 100}, {1, 11, 101},
        {1, 12, 100}, {1, 12, 101}, {1, 13, 100}, {1, 13, 101}};
    testJoin(a, b, expectedResult);
  }
  {
    NestedBlock a{{{1, 10}}, {{1, 11}}, {{1, 12}}, {{1, 13}}, {{2, 20}}};
    NestedBlock b{{{1, 100}}, {{1, 101}}, {{1, 102}}, {{1, 103}}};
    std::vector<std::array<size_t, 3>> expectedResult{
        {1, 10, 100}, {1, 10, 101}, {1, 10, 102}, {1, 10, 103},
        {1, 11, 100}, {1, 11, 101}, {1, 11, 102}, {1, 11, 103},
        {1, 12, 100}, {1, 12, 101}, {1, 12, 102}, {1, 12, 103},
        {1, 13, 100}, {1, 13, 101}, {1, 13, 102}, {1, 13, 103}};
    testJoin(a, b, expectedResult);
  }
  {
    NestedBlock a{{{1, 10}}, {{1, 11}}, {{1, 12}}, {{1, 13}}, {{2, 20}}};
    NestedBlock b{{{1, 100}}, {{1, 101}}, {{1, 102}}, {{1, 103}}, {{1, 104}}};
    std::vector<std::array<size_t, 3>> expectedResult{
        {1, 10, 100}, {1, 10, 101}, {1, 10, 102}, {1, 10, 103}, {1, 10, 104},
        {1, 11, 100}, {1, 11, 101}, {1, 11, 102}, {1, 11, 103}, {1, 11, 104},
        {1, 12, 100}, {1, 12, 101}, {1, 12, 102}, {1, 12, 103}, {1, 12, 104},
        {1, 13, 100}, {1, 13, 101}, {1, 13, 102}, {1, 13, 103}, {1, 13, 104}};
    testJoin(a, b, expectedResult);
  }
}

namespace {

// Replacement for `Id`, but with an additional tag to distinguish between ids
// with the same value for testing.
struct FakeId {
  Id value_;
  std::string_view tag_;

  operator Id() const { return value_; }
  auto operator==(const FakeId& other) const {
    return value_.getBits() == other.value_.getBits() && tag_ == other.tag_;
  }

  friend std::ostream& operator<<(std::ostream& os, const FakeId& id) {
    return os << "FakeId{" << id.value_ << ", " << id.tag_ << "}";
  }
};

// RowAdder implementation that works with FakeIds and allows to tell undefined
// ids apart from each other.
struct RowAdderWithUndef {
  const std::vector<FakeId>* left_ = nullptr;
  const std::vector<FakeId>* right_ = nullptr;
  std::vector<std::array<FakeId, 2>> output_{};

  void setInput(const std::vector<FakeId>& left,
                const std::vector<FakeId>& right) {
    left_ = &left;
    right_ = &right;
  }

  void setOnlyLeftInputForOptionalJoin(const std::vector<FakeId>& left) {
    left_ = &left;
  }

  void addRow(size_t leftIndex, size_t rightIndex) {
    auto id1 = (*left_)[leftIndex];
    auto id2 = (*right_)[rightIndex];
    output_.push_back({id1, id2});
  }

  template <typename R1, typename R2>
  void addRows(const R1& left, const R2& right) {
    for (auto a : left) {
      for (auto b : right) {
        addRow(a, b);
      }
    }
  }

  void addOptionalRow(size_t leftIndex) {
    auto id = (*left_)[leftIndex];
    output_.push_back({id, FakeId{Id::makeUndefined(), "OPTIONAL"}});
  }

  void flush() const {
    // Does nothing, but is required for the interface.
  }

  const auto& getOutput() const { return output_; }
};

// Join both vectors `a` and `b` and assert that the result is equal to the
// given `expected` result. Joins are performed 2 times, the second time with
// `a` and `b` swapped.
void testDynamicJoinWithUndef(const std::vector<std::vector<FakeId>>& a,
                              const std::vector<std::vector<FakeId>>& b,
                              std::vector<std::array<FakeId, 2>> expected,
                              source_location l = AD_CURRENT_SOURCE_LOC()) {
  using namespace std::placeholders;
  using namespace std::ranges;
  auto trace = generateLocationTrace(l);
  auto compare = [](FakeId l, FakeId r) {
    return static_cast<Id>(l) < static_cast<Id>(r);
  };
  AD_CONTRACT_CHECK(is_sorted(a | views::join, {}, ad_utility::staticCast<Id>));
  AD_CONTRACT_CHECK(is_sorted(b | views::join, {}, ad_utility::staticCast<Id>));
  auto validationProjection = [](const std::array<FakeId, 2>& fakeIds) -> Id {
    const auto& [x, y] = fakeIds;
    return x == Id::makeUndefined() ? y : x;
  };
  {
    RowAdderWithUndef adder{};
    zipperJoinForBlocksWithPotentialUndef(a, b, compare, adder);
    const auto& result = adder.getOutput();
    // The result must be sorted on the first column
    EXPECT_TRUE(is_sorted(result, std::less<>{}, validationProjection));
    // The exact order of the elements with the same first column is not
    // important and depends on implementation details. We therefore do not
    // enforce it here.
    EXPECT_THAT(result, ::testing::UnorderedElementsAreArray(expected));
  }

  for (auto& [x, y] : expected) {
    std::swap(x, y);
  }

  {
    RowAdderWithUndef adder{};
    zipperJoinForBlocksWithPotentialUndef(b, a, compare, adder);
    const auto& result = adder.getOutput();
    EXPECT_TRUE(is_sorted(result, std::less<>{}, validationProjection));
    EXPECT_THAT(result, ::testing::UnorderedElementsAreArray(expected));
  }
}
using F = FakeId;
auto I = Id::makeFromInt;
}  // namespace

// ________________________________________________________________________________________
TEST(JoinAlgorithms, JoinWithBlocksWithUndefOnOneSide) {
  auto U = Id::makeUndefined();
  std::vector<std::vector<FakeId>> a{{{U, "a0"}},
                                     {{I(42), "a1"}, {I(42), "a2"}},
                                     {{I(42), "a3"}, {I(67), "a4"}},
                                     {{I(67), "a5"}},
                                     {{I(67), "a6"}},
                                     {{I(67), "a7"}},
                                     {{I(67), "a8"}},
                                     {{I(67), "a9"}},
                                     {{I(67), "a10"}},
                                     {{I(68), "a11"}},
                                     {{I(68), "a12"}},
                                     {{I(68), "a13"}},
                                     {{I(68), "a14"}},
                                     {{I(68), "a15"}},
                                     {{I(68), "a16"}},
                                     {{I(68), "a17"}}};
  std::vector<std::vector<FakeId>> b{{{I(2), "b0"}, {I(42), "b1"}},
                                     {{I(42), "b2"}, {I(67), "b3"}}};
  std::vector<std::array<FakeId, 2>> expectedResult{
      {F{U, "a0"}, F{I(2), "b0"}},       {F{U, "a0"}, F{I(42), "b1"}},
      {F{U, "a0"}, F{I(42), "b2"}},      {F{I(42), "a1"}, F{I(42), "b1"}},
      {F{I(42), "a1"}, F{I(42), "b2"}},  {F{I(42), "a2"}, F{I(42), "b1"}},
      {F{I(42), "a2"}, F{I(42), "b2"}},  {F{I(42), "a3"}, F{I(42), "b1"}},
      {F{I(42), "a3"}, F{I(42), "b2"}},  {F{U, "a0"}, F{I(67), "b3"}},
      {F{I(67), "a4"}, F{I(67), "b3"}},  {F{I(67), "a5"}, F{I(67), "b3"}},
      {F{I(67), "a6"}, F{I(67), "b3"}},  {F{I(67), "a7"}, F{I(67), "b3"}},
      {F{I(67), "a8"}, F{I(67), "b3"}},  {F{I(67), "a9"}, F{I(67), "b3"}},
      {F{I(67), "a10"}, F{I(67), "b3"}},
  };
  testDynamicJoinWithUndef(a, b, expectedResult);
}

// ________________________________________________________________________________________
TEST(JoinAlgorithms, JoinWithBlocksWithUndefOnBothSides) {
  auto U = Id::makeUndefined();
  std::vector<std::vector<FakeId>> a{{{U, "a0"}},
                                     {{I(42), "a1"}, {I(42), "a2"}},
                                     {{I(42), "a3"}, {I(67), "a4"}},
                                     {{I(67), "a5"}},
                                     {{I(67), "a6"}},
                                     {{I(67), "a7"}},
                                     {{I(67), "a8"}},
                                     {{I(68), "a9"}},
                                     {{I(68), "a10"}},
                                     {{I(68), "a11"}},
                                     {{I(68), "a12"}}};
  std::vector<std::vector<FakeId>> b{{{U, "b0"}},
                                     {{U, "b1"}, {I(2), "b2"}, {I(42), "b3"}},
                                     {{I(42), "b4"}, {I(67), "b5"}}};
  std::vector<std::array<FakeId, 2>> expectedResult{
      {F{U, "a0"}, F{U, "b0"}},         {F{U, "a0"}, F{U, "b1"}},
      {F{U, "a0"}, F{I(2), "b2"}},      {F{U, "a0"}, F{I(42), "b3"}},
      {F{U, "a0"}, F{I(42), "b4"}},     {F{I(42), "a1"}, F{U, "b0"}},
      {F{I(42), "a1"}, F{U, "b1"}},     {F{I(42), "a2"}, F{U, "b0"}},
      {F{I(42), "a2"}, F{U, "b1"}},     {F{I(42), "a3"}, F{U, "b0"}},
      {F{I(42), "a3"}, F{U, "b1"}},     {F{I(42), "a1"}, F{I(42), "b3"}},
      {F{I(42), "a2"}, F{I(42), "b3"}}, {F{I(42), "a3"}, F{I(42), "b3"}},
      {F{I(42), "a1"}, F{I(42), "b4"}}, {F{I(42), "a2"}, F{I(42), "b4"}},
      {F{I(42), "a3"}, F{I(42), "b4"}}, {F{U, "a0"}, F{I(67), "b5"}},
      {F{I(67), "a4"}, F{U, "b0"}},     {F{I(67), "a4"}, F{U, "b1"}},
      {F{I(67), "a5"}, F{U, "b0"}},     {F{I(67), "a5"}, F{U, "b1"}},
      {F{I(67), "a6"}, F{U, "b0"}},     {F{I(67), "a6"}, F{U, "b1"}},
      {F{I(67), "a7"}, F{U, "b0"}},     {F{I(67), "a7"}, F{U, "b1"}},
      {F{I(67), "a8"}, F{U, "b0"}},     {F{I(67), "a8"}, F{U, "b1"}},
      {F{I(67), "a4"}, F{I(67), "b5"}}, {F{I(67), "a5"}, F{I(67), "b5"}},
      {F{I(67), "a6"}, F{I(67), "b5"}}, {F{I(67), "a7"}, F{I(67), "b5"}},
      {F{I(67), "a8"}, F{I(67), "b5"}}, {F{I(68), "a9"}, F{U, "b0"}},
      {F{I(68), "a9"}, F{U, "b1"}},     {F{I(68), "a10"}, F{U, "b0"}},
      {F{I(68), "a10"}, F{U, "b1"}},    {F{I(68), "a11"}, F{U, "b0"}},
      {F{I(68), "a11"}, F{U, "b1"}},    {F{I(68), "a12"}, F{U, "b0"}},
      {F{I(68), "a12"}, F{U, "b1"}},
  };
  testDynamicJoinWithUndef(a, b, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, JoinWithBlocksOneSideSingleUndef) {
  auto U = Id::makeUndefined();
  std::vector<std::vector<FakeId>> a{{{U, "a0"}}};
  std::vector<std::vector<FakeId>> b{{{I(1), "b0"}, {I(2), "b1"}}};

  std::vector<std::array<FakeId, 2>> expectedResult{
      {F{U, "a0"}, F{I(1), "b0"}},
      {F{U, "a0"}, F{I(2), "b1"}},
  };
  testDynamicJoinWithUndef(a, b, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, JoinWithBlocksOneUndefinedValueMixedWithOtherValues) {
  auto U = Id::makeUndefined();
  std::vector<std::vector<FakeId>> a{{{U, "a0"}, {I(1), "a1"}, {I(2), "a2"}}};
  std::vector<std::vector<FakeId>> b{{{U, "b0"}, {I(2), "b1"}, {I(3), "b2"}}};

  std::vector<std::array<FakeId, 2>> expectedResult{
      {F{U, "a0"}, F{U, "b0"}},       {F{I(1), "a1"}, F{U, "b0"}},
      {F{U, "a0"}, F{I(2), "b1"}},    {F{I(2), "a2"}, F{U, "b0"}},
      {F{I(2), "a2"}, F{I(2), "b1"}}, {F{U, "a0"}, F{I(3), "b2"}},
  };
  testDynamicJoinWithUndef(a, b, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, UndefinedJoinWorksWithoutUndefinedValues) {
  std::vector<std::vector<FakeId>> a{{{I(1), "a1"}, {I(2), "a2"}}};
  std::vector<std::vector<FakeId>> b{{{I(2), "b1"}, {I(3), "b2"}}};

  std::vector<std::array<FakeId, 2>> expectedResult{
      {F{I(2), "a2"}, F{I(2), "b1"}}};
  testDynamicJoinWithUndef(a, b, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, JoinWithBlocksMultipleGroupsAfterUndefined) {
  auto U = Id::makeUndefined();
  std::vector<std::vector<FakeId>> a{
      {{U, "a0"}, {I(1), "a1"}, {I(2), "a2"}, {I(3), "a3"}}};
  std::vector<std::vector<FakeId>> b{
      {{U, "b0"}, {I(1), "b1"}, {I(2), "b2"}, {I(3), "b3"}}};

  std::vector<std::array<FakeId, 2>> expectedResult{
      {F{U, "a0"}, F{U, "b0"}},       {F{I(1), "a1"}, F{U, "b0"}},
      {F{U, "a0"}, F{I(1), "b1"}},    {F{I(1), "a1"}, F{I(1), "b1"}},
      {F{U, "a0"}, F{I(2), "b2"}},    {F{I(2), "a2"}, F{U, "b0"}},
      {F{I(2), "a2"}, F{I(2), "b2"}}, {F{U, "a0"}, F{I(3), "b3"}},
      {F{I(3), "a3"}, F{U, "b0"}},    {F{I(3), "a3"}, F{I(3), "b3"}},
  };
  testDynamicJoinWithUndef(a, b, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, TrailingEmptyBlocksAreHandledWell) {
  auto U = Id::makeUndefined();
  std::vector<std::vector<FakeId>> a{
      {{U, "a0"}}, {{I(1), "a1"}}, {{I(2), "a2"}}, {{I(3), "a3"}}};
  std::vector<std::vector<FakeId>> b{{{I(3), "b0"}}, {}, {}, {}, {}};

  std::vector<std::array<FakeId, 2>> expectedResult{
      {F{U, "a0"}, F{I(3), "b0"}}, {F{I(3), "a3"}, F{I(3), "b0"}}};
  testDynamicJoinWithUndef(a, b, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, EmptyBlocksInTheMiddleAreHandledWell) {
  auto U = Id::makeUndefined();
  std::vector<std::vector<FakeId>> a{
      {{U, "a0"}}, {{I(1), "a1"}}, {{I(2), "a2"}}, {{I(3), "a3"}}};
  std::vector<std::vector<FakeId>> b{{{I(1), "b0"}}, {}, {{I(1), "b1"}}, {}, {},
                                     {{I(3), "b2"}}};

  std::vector<std::array<FakeId, 2>> expectedResult{
      {F{U, "a0"}, F{I(1), "b0"}},    {F{U, "a0"}, F{I(1), "b1"}},
      {F{I(1), "a1"}, F{I(1), "b0"}}, {F{I(1), "a1"}, F{I(1), "b1"}},
      {F{U, "a0"}, F{I(3), "b2"}},    {F{I(3), "a3"}, F{I(3), "b2"}}};
  testDynamicJoinWithUndef(a, b, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithm, DefaultIsUndefinedFunctionAlwaysReturnsFalse) {
  // This test is mostly for coverage purposes.
  RowAdderWithUndef adder{};
  std::vector<std::vector<FakeId>> dummyBlocks{};
  auto compare = [](auto l, auto r) { return static_cast<Id>(l) < r; };
  auto joinSide = ad_utility::detail::makeJoinSide(dummyBlocks, ql::identity{});
  ad_utility::detail::BlockZipperJoinImpl impl{joinSide, joinSide, compare,
                                               adder};
  EXPECT_FALSE(impl.isUndefined_("Something"));
  EXPECT_FALSE(impl.isUndefined_(1));
  EXPECT_FALSE(impl.isUndefined_(I(1)));
  EXPECT_FALSE(impl.isUndefined_(Id::makeUndefined()));
}

// _____________________________________________________________________________
// Tests for specialOptionalJoinForBlocks
// _____________________________________________________________________________

namespace {
// Helper types for testing special optional join with Id values.
using IdBlock = IdTable;
using IdNestedBlock = std::vector<IdBlock>;
using IdJoinResult = std::vector<std::array<Id, 4>>;

auto makeVec = [](const auto&... tables) {
  std::vector<IdTable> result;
  (..., result.push_back(tables.clone()));
  return result;
};

auto makeTable = [](const VectorTable& table) {
  return makeIdTableFromVector(table);
};

// RowAdder for Id-based blocks with payload columns.
struct IdRowAdder {
  using TableWithJoinCols = ad_utility::IdTableAndFirstCols<2, IdTable>;
  IdJoinResult* target_{};
  IdTableView<0> leftTable_{2, ad_utility::testing::makeAllocator()};
  IdTableView<0> rightTable_{2, ad_utility::testing::makeAllocator()};

  // Called by cartesian product path with materialized tables.
  void setInput(const auto& left, const auto& right) {
    leftTable_ = left.template asStaticView<0>();
    rightTable_ = right.template asStaticView<0>();
  }

  void setOnlyLeftInputForOptionalJoin(const auto& left) {
    leftTable_ = left.template asStaticView<0>();
  }

  void addRow(size_t leftIndex, size_t rightIndex) {
    // Cartesian path: use materialized tables.
    auto x1 = leftTable_(leftIndex, 0);
    auto x2 = leftTable_(leftIndex, 1);
    auto y1 = rightTable_(rightIndex, 0);
    auto y2 = rightTable_(rightIndex, 1);
    auto leftPayload = leftTable_(leftIndex, 2);
    auto rightPayload = rightTable_(rightIndex, 2);
    AD_CONTRACT_CHECK(x1 == y1);
    AD_CONTRACT_CHECK(x2.isUndefined() || x2 == y2);
    target_->push_back(
        std::array{x1, x2.isUndefined() ? y2 : x2, leftPayload, rightPayload});
  }

  void addOptionalRow(size_t leftIndex) {
    auto x1 = leftTable_(leftIndex, 0);
    auto x2 = leftTable_(leftIndex, 1);
    auto leftPayload = leftTable_(leftIndex, 2);
    target_->emplace_back(std::array{x1, x2, leftPayload, Id::makeUndefined()});
  }

  template <typename R1, typename R2>
  void addRows(const R1& leftIndices, const R2& rightIndices) {
    for (auto leftIdx : leftIndices) {
      for (auto rightIdx : rightIndices) {
        addRow(leftIdx, rightIdx);
      }
    }
  }

  void flush() const {
    // Does nothing, but is required for the interface.
  }
};

auto makeIdRowAdder(IdJoinResult& target) { return IdRowAdder{&target}; }

// Helper function for creating undefined Ids.
auto U2() { return Id::makeUndefined(); }

// Helper function to test the special optional join with blocks.
void testSpecialOptionalJoin(IdNestedBlock a, IdNestedBlock b,
                             IdJoinResult expected, size_t numJoinColumns = 2,
                             source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  IdJoinResult result;
  auto adder = makeIdRowAdder(result);

  // Wrap IdTables in IdTableAndFirstCols to expose only the join columns.
  using TableWithJoinCols = ad_utility::IdTableAndFirstCols<2, IdTable>;
  std::vector<TableWithJoinCols> wrappedA;
  std::vector<TableWithJoinCols> wrappedB;
  for (auto& table : a) {
    wrappedA.emplace_back(std::move(table), LocalVocab{});
  }
  for (auto& table : b) {
    wrappedB.emplace_back(std::move(table), LocalVocab{});
  }

  ad_utility::specialOptionalJoinForBlocks(
      std::move(wrappedA), std::move(wrappedB), numJoinColumns, adder);

  // The result must be sorted on the first two columns (the join columns).
  EXPECT_TRUE(ql::ranges::is_sorted(
      result, std::less<>{},
      [](const auto& array) { return std::tie(array[0], array[1]); }));
  // The exact order of the elements with the same first column is not important
  // and depends on implementation details. We therefore do not enforce it here.
  EXPECT_THAT(result, ::testing::UnorderedElementsAreArray(expected));
}

// Split an IdTable into multiple blocks based on split points.
// splitPoints are indices where to split (exclusive start of next block).
// Empty blocks are created when split points are consecutive.
IdNestedBlock splitIdTable(const IdTable& table,
                           const std::vector<size_t>& splitPoints) {
  IdNestedBlock result;
  size_t start = 0;

  for (size_t splitPoint : splitPoints) {
    AD_CONTRACT_CHECK(splitPoint <= table.numRows());
    IdTable block(table.numColumns(), table.getAllocator());
    block.resize(splitPoint - start);
    for (size_t i = 0; i < splitPoint - start; ++i) {
      for (size_t col = 0; col < table.numColumns(); ++col) {
        block(i, col) = table(start + i, col);
      }
    }
    result.push_back(std::move(block));
    start = splitPoint;
  }

  // Add final block from last split point to end.
  IdTable block(table.numColumns(), table.getAllocator());
  block.resize(table.numRows() - start);
  for (size_t i = 0; i < table.numRows() - start; ++i) {
    for (size_t col = 0; col < table.numColumns(); ++col) {
      block(i, col) = table(start + i, col);
    }
  }
  result.push_back(std::move(block));

  return result;
}

// Generate multiple split configurations for testing.
// Returns a vector of split point vectors.
std::vector<std::vector<size_t>> generateSplitConfigurations(
    size_t tableSize, int numRandomSplits, int seed) {
  std::vector<std::vector<size_t>> configs;

  // Pathological case 1: All in one block (no splits).
  configs.push_back({});

  // Pathological case 2: One element per block (split after each element).
  if (tableSize > 0) {
    std::vector<size_t> onePerElement;
    for (size_t i = 1; i < tableSize; ++i) {
      onePerElement.push_back(i);
    }
    configs.push_back(onePerElement);
  }

  // Generate random split configurations.
  std::mt19937 rng(seed);
  for (int i = 0; i < numRandomSplits; ++i) {
    if (tableSize <= 1) {
      // Can't split tables with 0 or 1 rows in interesting ways.
      continue;
    }

    std::vector<size_t> splits;
    // Decide how many splits to make (between 1 and tableSize).
    std::uniform_int_distribution<size_t> numSplitsDist(
        1, std::min(tableSize, size_t{10}));
    size_t numSplits = numSplitsDist(rng);

    // Generate random split points.
    std::uniform_int_distribution<size_t> splitPointDist(1, tableSize - 1);
    std::set<size_t> splitSet;
    for (size_t j = 0; j < numSplits; ++j) {
      splitSet.insert(splitPointDist(rng));
    }

    // Randomly add empty blocks by duplicating some split points.
    std::uniform_real_distribution<double> emptyBlockChance(0.0, 1.0);
    std::vector<size_t> splitVec(splitSet.begin(), splitSet.end());
    for (size_t splitPoint : splitVec) {
      if (emptyBlockChance(rng) < 0.2) {  // 20% chance of empty block.
        splitSet.insert(splitPoint);
      }
    }

    splits.assign(splitSet.begin(), splitSet.end());
    configs.push_back(splits);
  }

  return configs;
}

// Test the special optional join with automatic block splitting.
// Takes single blocks as input and tests with various split configurations.
void testSpecialOptionalJoinWithSplits(
    const IdTable& leftTable, const IdTable& rightTable,
    const IdJoinResult& expected, int numRandomSplits = 10,
    size_t numJoinColumns = 2, source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);

  // Generate split configurations for both sides.
  auto leftConfigs =
      generateSplitConfigurations(leftTable.numRows(), numRandomSplits, 42);
  auto rightConfigs =
      generateSplitConfigurations(rightTable.numRows(), numRandomSplits, 123);

  // Test all combinations of left and right split configurations.
  for (size_t leftIdx = 0; leftIdx < leftConfigs.size(); ++leftIdx) {
    for (size_t rightIdx = 0; rightIdx < rightConfigs.size(); ++rightIdx) {
      auto leftBlocks = splitIdTable(leftTable, leftConfigs[leftIdx]);
      auto rightBlocks = splitIdTable(rightTable, rightConfigs[rightIdx]);

      testSpecialOptionalJoin(std::move(leftBlocks), std::move(rightBlocks),
                              expected, numJoinColumns, l);
    }
  }
}
}  // namespace

// _____________________________________________________________________________
TEST(JoinAlgorithms, SpecialOptionalJoinEmptyInputs) {
  testSpecialOptionalJoin({}, {}, {});

  auto emptyTable = IdTable(3, makeUnlimitedAllocator<Id>());
  auto nonEmpty = makeIdTableFromVector({{I(13), I(0), I(100)}});
  testSpecialOptionalJoin(makeVec(nonEmpty), makeVec(emptyTable),
                          {{I(13), I(0), I(100), Id::makeUndefined()}});

  testSpecialOptionalJoin(makeVec(emptyTable), makeVec(nonEmpty), {});
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, SpecialOptionalJoinSingleBlockWithSplits) {
  auto leftTable = makeTable({{I(1), I(11), I(101)},
                              {I(4), I(12), I(102)},
                              {I(4), I(12), I(103)},
                              {I(42), I(14), I(104)}});
  auto rightTable = makeTable({{I(0), I(24), I(200)},
                               {I(4), I(12), I(201)},
                               {I(4), I(12), I(202)},
                               {I(5), I(25), I(203)},
                               {I(19), I(26), I(204)},
                               {I(42), I(27), I(205)}});
  IdJoinResult expectedResult{{I(1), I(11), I(101), Id::makeUndefined()},
                              {I(4), I(12), I(102), I(201)},
                              {I(4), I(12), I(102), I(202)},
                              {I(4), I(12), I(103), I(201)},
                              {I(4), I(12), I(103), I(202)},
                              {I(42), I(14), I(104), Id::makeUndefined()}};
  testSpecialOptionalJoinWithSplits(leftTable, rightTable, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, SpecialOptionalJoinWithUndefsOnLeft) {
  // Test that left entries with undefined in second column match right entries
  // on first column only, and the result contains the right's second column.
  auto leftTable = makeTable(
      {{I(1), U2(), I(101)}, {I(4), U2(), I(102)}, {I(5), I(50), I(103)}});
  auto rightTable = makeTable(
      {{I(1), I(10), I(201)}, {I(4), I(40), I(202)}, {I(5), I(50), I(203)}});
  IdJoinResult expectedResult{{I(1), I(10), I(101), I(201)},
                              {I(4), I(40), I(102), I(202)},
                              {I(5), I(50), I(103), I(203)}};
  testSpecialOptionalJoinWithSplits(leftTable, rightTable, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, SpecialOptionalJoinMultipleUndefsForSameFirstColumn) {
  // Test multiple left entries with same first column and undefined second
  // column. Each should match all right entries with that first column
  // (cartesian product).
  auto leftTable = makeTable({{I(5), U2(), I(101)},
                              {I(5), U2(), I(102)},
                              {I(5), U2(), I(103)},
                              {I(10), I(100), I(104)}});
  auto rightTable = makeTable(
      {{I(5), I(50), I(201)}, {I(5), I(51), I(202)}, {I(10), I(100), I(203)}});
  IdJoinResult expectedResult{
      {I(5), I(50), I(101), I(201)},  {I(5), I(50), I(102), I(201)},
      {I(5), I(50), I(103), I(201)},  // 3 left undefs match I(5), I(50)
      {I(5), I(51), I(101), I(202)},  {I(5), I(51), I(102), I(202)},
      {I(5), I(51), I(103), I(202)},  // 3 left undefs match I(5), I(51)
      {I(10), I(100), I(104), I(203)}};
  testSpecialOptionalJoinWithSplits(leftTable, rightTable, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, SpecialOptionalJoinMultipleEntriesSameFirstColumn) {
  // Test multiple entries with same first and second columns but different
  // payloads. Tests the cartesian product behavior.
  auto leftTable = makeTable({{I(3), I(30), I(101)},
                              {I(3), I(30), I(102)},
                              {I(3), I(30), I(103)},
                              {I(7), I(70), I(104)}});
  auto rightTable = makeTable(
      {{I(3), I(30), I(201)}, {I(3), I(30), I(202)}, {I(7), I(70), I(203)}});
  IdJoinResult expectedResult{
      {I(3), I(30), I(101), I(201)},
      {I(3), I(30), I(101), I(202)},
      {I(3), I(30), I(102), I(201)},
      {I(3), I(30), I(102), I(202)},
      {I(3), I(30), I(103), I(201)},
      {I(3), I(30), I(103), I(202)},  // 3x2 = 6 cartesian results.
      {I(7), I(70), I(104), I(203)}};
  testSpecialOptionalJoinWithSplits(leftTable, rightTable, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, SpecialOptionalJoinMultipleEntriesCartesian) {
  // Test cartesian product via UNDEF matching: multiple left entries with same
  // first column and UNDEF in second column should match all right entries
  // with same first column.
  auto leftTable = makeTable({{I(3), U2(), I(101)},
                              {I(3), U2(), I(102)},
                              {I(3), U2(), I(103)},
                              {I(7), I(70), I(104)}});
  auto rightTable = makeTable(
      {{I(3), I(30), I(201)}, {I(3), I(31), I(202)}, {I(7), I(70), I(203)}});
  IdJoinResult expectedResult{
      {I(3), I(30), I(101), I(201)}, {I(3), I(30), I(102), I(201)},
      {I(3), I(30), I(103), I(201)},  // 3 left undefs match I(3), I(30)
      {I(3), I(31), I(101), I(202)}, {I(3), I(31), I(102), I(202)},
      {I(3), I(31), I(103), I(202)},  // 3 left undefs match I(3), I(31)
      {I(7), I(70), I(104), I(203)}};
  testSpecialOptionalJoinWithSplits(leftTable, rightTable, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, SpecialOptionalJoinNoMatches) {
  // Test when left entries have no matching right entries.
  // All left entries should appear in result with their payloads and undefined
  // right payloads.
  auto leftTable = makeTable(
      {{I(1), I(10), I(101)}, {I(2), I(20), I(102)}, {I(3), I(30), I(103)}});
  auto rightTable = makeTable(
      {{I(1), I(50), I(201)}, {I(6), I(60), I(202)}, {I(7), I(70), I(203)}});
  IdJoinResult expectedResult{{I(1), I(10), I(101), U2()},
                              {I(2), I(20), I(102), U2()},
                              {I(3), I(30), I(103), U2()}};
  testSpecialOptionalJoinWithSplits(leftTable, rightTable, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, SpecialOptionalJoinPartialMatches) {
  // Test mix of matching and non-matching left entries.
  auto leftTable = makeTable({{I(1), I(10), I(101)},
                              {I(2), I(20), I(102)},
                              {I(3), U2(), I(103)},
                              {I(4), I(40), I(104)}});
  auto rightTable = makeTable(
      {{I(2), I(20), I(201)}, {I(3), I(30), I(202)}, {I(5), I(50), I(203)}});
  IdJoinResult expectedResult{
      {I(1), I(10), I(101), U2()},    // No match, keep original.
      {I(2), I(20), I(102), I(201)},  // Exact match.
      {I(3), I(30), I(103),
       I(202)},  // Left has U2(), matches right on first column.
      {I(4), I(40), I(104), U2()}  // No match, keep original.
  };
  testSpecialOptionalJoinWithSplits(leftTable, rightTable, expectedResult);
}

// _____________________________________________________________________________
TEST(JoinAlgorithms, SpecialOptionalJoinComplexCombination) {
  // Comprehensive test combining all scenarios: undefs, multiples,
  // matches/non-matches, and cartesian products.
  auto leftTable = makeTable({{I(1), U2(), I(101)},
                              {I(1), U2(), I(102)},
                              {I(2), I(20), I(103)},
                              {I(3), I(30), I(104)},
                              {I(3), I(31), I(105)},
                              {I(4), U2(), I(106)},
                              {I(5), I(50), I(107)},
                              {I(6), I(60), I(108)}});
  auto rightTable = makeTable({{I(1), I(10), I(201)},
                               {I(1), I(11), I(202)},
                               {I(2), I(20), I(203)},
                               {I(3), I(30), I(204)},
                               {I(4), I(40), I(205)},
                               {I(7), I(70), I(206)}});
  IdJoinResult expectedResult{
      {I(1), I(10), I(101),
       I(201)},  // Left I(1), U2() matches right I(1), I(10).
      {I(1), I(10), I(102), I(201)},  // Second left I(1), U2() also matches.
      {I(1), I(11), I(101),
       I(202)},  // Left I(1), U2() matches right I(1), I(11).
      {I(1), I(11), I(102),
       I(202)},  // Second left I(1), U2() also matches (cartesian: 2x2=4).
      {I(2), I(20), I(103), I(203)},  // Exact match.
      {I(3), I(30), I(104), I(204)},  // Exact match on both columns.
      {I(3), I(31), I(105),
       U2()},  // Left I(3), I(31) doesn't match, keep original.
      {I(4), I(40), I(106),
       I(205)},  // Left I(4), U2() matches right I(4), I(40).
      {I(5), I(50), I(107), U2()},  // No match on right, keep original.
      {I(6), I(60), I(108), U2()}   // No match on right, keep original.
  };
  testSpecialOptionalJoinWithSplits(leftTable, rightTable, expectedResult);
}
