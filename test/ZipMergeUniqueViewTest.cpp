// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <forward_list>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "util/GTestHelpers.h"
#include "util/StringUtils.h"
#include "util/views/ZipMergeUniqueView.h"

// We need to explicitly tell GTest how to print a `ZipMergeUniqueView`,
// because the latter inherits an implicit `operator<<` from `range-v3`s
// `view_interface`. That operator leads to a hard compile error if the value
// type of the view is not printable. Note: `::testing::PrintToString`
// already falls back to a hex byte representation for non-streamable types, so
// no SFINAE machinery is needed here.
namespace ad_utility {
template <typename V1, typename V2, typename Compare, typename Projection>
void PrintTo(const ZipMergeUniqueView<V1, V2, Compare, Projection>& view,
             std::ostream* os) {
  std::vector<std::string> strs;
  for (const auto& elem : view) {
    strs.push_back(::testing::PrintToString(elem));
  }
  *os << '[';
  lazyStrJoin(os, strs, ",");
  *os << ']';
}
}  // namespace ad_utility

// Pair of the two container types passed to `ZipMergeUniqueView` for testing
// different input types. The difference to a normal `std::pair` is that this
// *only* carries the types.
template <typename C1, typename C2>
struct TypePair {
  using First = C1;
  using Second = C2;
};

template <typename TypePair>
class ZipMergeUniqueViewTypedTest : public testing::Test {};

using IntContainerTypes =
    testing::Types<TypePair<std::vector<int>, std::vector<int>>,
                   TypePair<std::forward_list<int>, std::forward_list<int>>,
                   TypePair<std::vector<int>, std::forward_list<int>>,
                   TypePair<std::forward_list<int>, std::vector<int>>>;
TYPED_TEST_SUITE(ZipMergeUniqueViewTypedTest, IntContainerTypes);

TYPED_TEST(ZipMergeUniqueViewTypedTest, integerZip) {
  using First = typename TypeParam::First;
  using Second = typename TypeParam::Second;
  EXPECT_THAT(ad_utility::ZipMergeUniqueView(First{}, Second{}),
              testing::ElementsAre());
  EXPECT_THAT(ad_utility::ZipMergeUniqueView(First{1, 2, 3, 4}, Second{}),
              testing::ElementsAre(1, 2, 3, 4));
  EXPECT_THAT(ad_utility::ZipMergeUniqueView(First{}, Second{1, 2, 3, 4}),
              testing::ElementsAre(1, 2, 3, 4));
  // Duplicated elements: the element from the second range wins.
  EXPECT_THAT(
      ad_utility::ZipMergeUniqueView(First{1, 2, 3, 4}, Second{1, 2, 3, 4}),
      testing::ElementsAre(1, 2, 3, 4));
  EXPECT_THAT(ad_utility::ZipMergeUniqueView(First{1, 3, 5}, Second{0, 2, 4}),
              testing::ElementsAre(0, 1, 2, 3, 4, 5));
  EXPECT_THAT(ad_utility::ZipMergeUniqueView(First{1, 2, 3}, Second{4, 5, 6}),
              testing::ElementsAre(1, 2, 3, 4, 5, 6));
  // A custom comparator for differently sorted parts.
  EXPECT_THAT(ad_utility::ZipMergeUniqueView(First{5, 3, 1}, Second{4, 2, 0},
                                             std::greater{}),
              testing::ElementsAre(5, 4, 3, 2, 1, 0));
}

TYPED_TEST(ZipMergeUniqueViewTypedTest, MultiPass) {
  using First = typename TypeParam::First;
  using Second = typename TypeParam::Second;
  auto a = First{1, 3, 5};
  auto b = Second{0, 2, 4};
  auto view = ad_utility::ZipMergeUniqueView(a, b);
  auto it = view.begin();
  auto itCopy = it;
  std::vector<int> r1, r2;
  while (it != view.end()) r1.push_back(*it++);
  while (itCopy != view.end()) r2.push_back(*itCopy++);
  EXPECT_THAT(r1, testing::ElementsAre(0, 1, 2, 3, 4, 5));
  EXPECT_THAT(r2, testing::ElementsAre(0, 1, 2, 3, 4, 5));
}

TYPED_TEST(ZipMergeUniqueViewTypedTest, Equality) {
  using First = typename TypeParam::First;
  using Second = typename TypeParam::Second;
  {
    auto a = First{};
    auto b = Second{};
    auto view = ad_utility::ZipMergeUniqueView(a, b);
    EXPECT_EQ(view.begin(), view.end());
  }
  {
    auto a = First{1, 3, 5};
    auto b = Second{0, 2, 4};
    auto view = ad_utility::ZipMergeUniqueView(a, b);
    auto begin = view.begin();
    EXPECT_NE(begin, view.end());
    auto beginCopy = begin;
    EXPECT_EQ(begin, beginCopy);
    ++begin;
    EXPECT_NE(begin, beginCopy);
    ++beginCopy;
    EXPECT_EQ(begin, beginCopy);
  }
}

namespace {
template <bool IsForward, typename V1, typename V2>
void checkIteratorCategory() {
  using View = decltype(ad_utility::ZipMergeUniqueView(std::declval<V1>(),
                                                       std::declval<V2>()));
  using It = ql::ranges::iterator_t<View>;
  static_assert(ql::ranges::input_range<View>);
  static_assert(ql::concepts::input_iterator<It>);
  static_assert(ql::ranges::forward_range<View> == IsForward);
  static_assert(ql::concepts::forward_iterator<It> == IsForward);
}
}  // namespace

TEST(ZipMergeUniqueViewTest, IteratorCategory) {
  // If both inputs have `forward_iterator`s then the resulting
  // `ZipMergeUniqueView`
  // also has `forward_iterator`s, otherwise it's only `input_iterator`.
  using ForwardContainer = std::vector<int>;
  using InputContainer =
      decltype(ad_utility::ForceInputView{std::declval<ForwardContainer&>()});
  checkIteratorCategory<true, ForwardContainer&, ForwardContainer&>();
  checkIteratorCategory<false, ForwardContainer&, InputContainer>();
  checkIteratorCategory<false, InputContainer, InputContainer>();
}

TEST(ZipMergeUniqueViewTest, intPairZip) {
  using Pair = std::pair<int, int>;
  using Pairs = std::vector<Pair>;
  EXPECT_THAT(ad_utility::ZipMergeUniqueView(Pairs{{1, 1}, {2, 1}, {3, 1}},
                                             Pairs{{1, 2}, {2, 2}, {3, 2}}),
              testing::ElementsAre(Pair(1, 1), Pair(1, 2), Pair(2, 1),
                                   Pair(2, 2), Pair(3, 1), Pair(3, 2)));
  // Under the projection both parts have elements 1, 2 and 3. The element from
  // the second range wins on equal keys.
  EXPECT_THAT(ad_utility::ZipMergeUniqueView(Pairs{{1, 1}, {2, 1}, {3, 1}},
                                             Pairs{{1, 2}, {3, 2}},
                                             std::less<>{}, &Pair::first),
              testing::ElementsAre(Pair(1, 2), Pair(2, 1), Pair(3, 2)));
}
