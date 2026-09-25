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

using ad_utility::chunkedIotaView;

namespace {
// Collect all chunks of the `view` into a vector.
template <typename View>
auto toVector(const View& view) {
  std::vector<ql::ranges::range_value_t<View>> result;
  for (auto chunk : view) {
    result.push_back(chunk);
  }
  return result;
}

// A function object that calls `chunkedIotaView`, s.t. we can check which
// argument types are accepted via `std::is_invocable_v`.
struct CallChunkedIotaView {
  template <typename A, typename B, typename C>
  auto operator()(A a, B b, C c) const -> decltype(chunkedIotaView(a, b, c)) {
    return chunkedIotaView(a, b, c);
  }
};

// Return `true` iff `chunkedIotaView` can be called with arguments of the
// types `A`, `B` and `C`.
template <typename A, typename B, typename C>
constexpr bool canBeCalled = std::is_invocable_v<CallChunkedIotaView, A, B, C>;
}  // namespace

// The `first` and the `end` have to be of exactly the same type.
static_assert(canBeCalled<size_t, size_t, size_t>);
static_assert(!canBeCalled<int, size_t, size_t>);
static_assert(!canBeCalled<size_t, int, size_t>);

// _____________________________________________________________________________
TEST(ChunkedIotaView, basicChunks) {
  using P = std::pair<size_t, size_t>;
  auto view = chunkedIotaView(size_t{0}, size_t{7}, size_t{3});
  EXPECT_THAT(toVector(view),
              ::testing::ElementsAre(P{0, 3}, P{3, 6}, P{6, 7}));

  // The view can be iterated multiple times.
  EXPECT_EQ(toVector(view), toVector(view));

  // The chunks exactly divide the range.
  EXPECT_THAT(toVector(chunkedIotaView(size_t{2}, size_t{8}, size_t{2})),
              ::testing::ElementsAre(P{2, 4}, P{4, 6}, P{6, 8}));

  // A single chunk that is smaller than the `blockSize`.
  EXPECT_THAT(toVector(chunkedIotaView(size_t{5}, size_t{6}, size_t{100})),
              ::testing::ElementsAre(P{5, 6}));
}

// _____________________________________________________________________________
TEST(ChunkedIotaView, emptyRange) {
  EXPECT_TRUE(
      toVector(chunkedIotaView(size_t{4}, size_t{4}, size_t{3})).empty());
}

// _____________________________________________________________________________
TEST(ChunkedIotaView, signedAndLargeValues) {
  using P = std::pair<int, int>;
  EXPECT_THAT(toVector(chunkedIotaView(-3, 2, 2)),
              ::testing::ElementsAre(P{-3, -1}, P{-1, 1}, P{1, 2}));

  // Chunks that end at the maximal value of the type.
  constexpr auto max = std::numeric_limits<size_t>::max();
  using PS = std::pair<size_t, size_t>;
  EXPECT_THAT(toVector(chunkedIotaView(max - 5, max, size_t{4})),
              ::testing::ElementsAre(PS{max - 5, max - 1}, PS{max - 1, max}));
}

// _____________________________________________________________________________
TEST(ChunkedIotaView, invalidArguments) {
  EXPECT_ANY_THROW(chunkedIotaView(size_t{3}, size_t{2}, size_t{1}));
  EXPECT_ANY_THROW(chunkedIotaView(size_t{0}, size_t{2}, size_t{0}));
}
