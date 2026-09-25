// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "util/views/ChunkedIotaView.h"

using ad_utility::ChunkedIotaView;

namespace {
// Collect all chunks of the `view` into a vector.
template <typename T>
std::vector<std::pair<T, T>> toVector(const ChunkedIotaView<T>& view) {
  std::vector<std::pair<T, T>> result;
  for (auto chunk : view) {
    result.push_back(chunk);
  }
  return result;
}
}  // namespace

// The view is a (sized) forward range.
static_assert(ql::ranges::forward_range<ChunkedIotaView<size_t>>);
static_assert(ql::ranges::view<ChunkedIotaView<size_t>>);
static_assert(ql::ranges::sized_range<ChunkedIotaView<size_t>>);

// The `first` and the `end` have to be of exactly the same type.
static_assert(
    std::is_constructible_v<ChunkedIotaView<size_t>, size_t, size_t, size_t>);
static_assert(
    !std::is_constructible_v<ChunkedIotaView<size_t>, int, size_t, size_t>);
static_assert(
    !std::is_constructible_v<ChunkedIotaView<size_t>, size_t, int, size_t>);

// _____________________________________________________________________________
TEST(ChunkedIotaView, basicChunks) {
  using P = std::pair<size_t, size_t>;
  ChunkedIotaView view{size_t{0}, size_t{7}, size_t{3}};
  EXPECT_THAT(toVector(view),
              ::testing::ElementsAre(P{0, 3}, P{3, 6}, P{6, 7}));
  EXPECT_EQ(view.size(), 3u);

  // The view can be iterated multiple times.
  EXPECT_EQ(toVector(view), toVector(view));

  // The chunks exactly divide the range.
  ChunkedIotaView exact{size_t{2}, size_t{8}, size_t{2}};
  EXPECT_THAT(toVector(exact),
              ::testing::ElementsAre(P{2, 4}, P{4, 6}, P{6, 8}));
  EXPECT_EQ(exact.size(), 3u);

  // A single chunk that is smaller than the `blockSize`.
  ChunkedIotaView single{size_t{5}, size_t{6}, size_t{100}};
  EXPECT_THAT(toVector(single), ::testing::ElementsAre(P{5, 6}));
  EXPECT_EQ(single.size(), 1u);
}

// _____________________________________________________________________________
TEST(ChunkedIotaView, emptyRange) {
  ChunkedIotaView view{size_t{4}, size_t{4}, size_t{3}};
  EXPECT_TRUE(toVector(view).empty());
  EXPECT_EQ(view.size(), 0u);
  EXPECT_TRUE(ChunkedIotaView<size_t>{}.empty());
}

// _____________________________________________________________________________
TEST(ChunkedIotaView, signedAndNoOverflow) {
  using P = std::pair<int, int>;
  ChunkedIotaView view{-3, 2, 2};
  EXPECT_THAT(toVector(view),
              ::testing::ElementsAre(P{-3, -1}, P{-1, 1}, P{1, 2}));

  // Computing the end of the last chunk must not overflow.
  constexpr auto max = std::numeric_limits<size_t>::max();
  using PS = std::pair<size_t, size_t>;
  ChunkedIotaView large{max - 5, max, size_t{4}};
  EXPECT_THAT(toVector(large),
              ::testing::ElementsAre(PS{max - 5, max - 1}, PS{max - 1, max}));
}

// _____________________________________________________________________________
TEST(ChunkedIotaView, invalidArguments) {
  EXPECT_ANY_THROW((ChunkedIotaView{size_t{3}, size_t{2}, size_t{1}}));
  EXPECT_ANY_THROW((ChunkedIotaView{size_t{0}, size_t{2}, size_t{0}}));
}
