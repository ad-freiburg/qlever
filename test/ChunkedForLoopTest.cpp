//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include <atomic>
#include <list>
#include <range/v3/view/zip.hpp>
#include <vector>

#include "util/ChunkedForLoop.h"
#include "util/Forward.h"

using ad_utility::chunkedCopy;
using ad_utility::chunkedFill;
using ad_utility::chunkedForLoop;

TEST(ChunkedForLoop, testEmptyRange) {
  bool flag = false;
  chunkedForLoop<1>(0, 0, [&](size_t) { flag = true; }, [&]() { flag = true; });
  chunkedForLoop<10>(
      0, 0, [&](size_t) { flag = true; }, [&]() { flag = true; });
  chunkedForLoop<100>(
      0, 0, [&](size_t) { flag = true; }, [&]() { flag = true; });
  chunkedForLoop<std::numeric_limits<size_t>::max()>(
      0, 0, [&](size_t) { flag = true; }, [&]() { flag = true; });

  EXPECT_FALSE(flag);
}

// _____________________________________________________________________________________________________________________
TEST(ChunkedForLoop, testReverseRange) {
  bool flag = false;
  chunkedForLoop<1>(1, 0, [&](size_t) { flag = true; }, [&]() { flag = true; });
  chunkedForLoop<10>(
      2, 0, [&](size_t) { flag = true; }, [&]() { flag = true; });
  chunkedForLoop<100>(
      3, 0, [&](size_t) { flag = true; }, [&]() { flag = true; });
  chunkedForLoop<std::numeric_limits<size_t>::max()>(
      4, 0, [&](size_t) { flag = true; }, [&]() { flag = true; });

  EXPECT_FALSE(flag);
}

// _____________________________________________________________________________________________________________________
TEST(ChunkedForLoop, verifyBiggerChunkSizeWorks) {
  size_t counter = 0;
  size_t chunkCounter = 0;
  chunkedForLoop<10>(
      0, 3, [&](size_t) { counter++; }, [&]() { chunkCounter++; });

  EXPECT_EQ(counter, 3);
  EXPECT_EQ(chunkCounter, 1);
}

// _____________________________________________________________________________________________________________________
TEST(ChunkedForLoop, verifyEqualChunkSizeWorks) {
  size_t counter = 0;
  size_t chunkCounter = 0;
  chunkedForLoop<5>(
      3, 8, [&](size_t) { counter++; }, [&]() { chunkCounter++; });

  EXPECT_EQ(counter, 5);
  EXPECT_EQ(chunkCounter, 1);
}

// _____________________________________________________________________________________________________________________
TEST(ChunkedForLoop, verifySmallerChunkSizeWorks) {
  size_t counter = 0;
  size_t chunkCounter = 0;
  chunkedForLoop<7>(
      1, 98, [&](size_t) { counter++; }, [&]() { chunkCounter++; });

  EXPECT_EQ(counter, 97);
  EXPECT_EQ(chunkCounter, 14);
}

// _____________________________________________________________________________________________________________________
TEST(ChunkedForLoop, verifyIndexIsCorrectlyCounting) {
  size_t counter = 7;
  chunkedForLoop<7>(
      7, 19,
      [&](size_t index) {
        EXPECT_EQ(index, counter);
        counter++;
      },
      []() {});

  EXPECT_EQ(counter, 19);
}

// _____________________________________________________________________________________________________________________
TEST(ChunkedForLoop, verifyBreakWorksAsExpected) {
  size_t counter = 0;
  size_t chunkCounter = 0;
  chunkedForLoop<7>(
      3, 19,
      [&](size_t index, const auto& breakLoop) {
        counter++;
        if (index >= 6) {
          breakLoop();
        }
      },
      [&]() { chunkCounter++; });

  EXPECT_EQ(counter, 4);
  EXPECT_EQ(chunkCounter, 1);
}

// _____________________________________________________________________________________________________________________
TEST(ChunkedForLoop, chunkedFillHandlesEmptyRange) {
  size_t chunkCounter = 0;
  chunkedFill(std::array<int, 0>{}, 0, 10, [&]() { chunkCounter++; });

  EXPECT_EQ(chunkCounter, 1);
}

// _____________________________________________________________________________________________________________________
TEST(ChunkedForLoop, chunkedFillFillsCorrectly) {
  size_t chunkCounter = 0;
  std::array<int, 21> elements{};
  chunkedFill(elements, 42, 10, [&]() { chunkCounter++; });

  EXPECT_EQ(chunkCounter, 3);
  EXPECT_THAT(elements, ::testing::Each(::testing::Eq(42)));
}

// _____________________________________________________________________________________________________________________
TEST(ChunkedForLoop, chunkedCopyHandlesEmptyRange) {
  size_t chunkCounter = 0;
  std::array<int, 0> output{};
  chunkedCopy(std::array<int, 0>{}, output.begin(), 2,
              [&]() { chunkCounter++; });

  EXPECT_EQ(chunkCounter, 1);
}

// _____________________________________________________________________________________________________________________
TEST(ChunkedForLoop, chunkedCopyCopiesCorrectly) {
  size_t chunkCounter = 0;
  std::array<int, 5> input{5, 4, 3, 2, 1};
  std::array<int, 5> output{};
  chunkedCopy(input, output.begin(), 2, [&]() { chunkCounter++; });

  EXPECT_EQ(chunkCounter, 3);
  EXPECT_EQ(input, output);
}

// Test that `chunkedForEach` visits every element in order and calls the
// `chunkOperation` after every `chunkSize` elements and at the end, for random
// access ranges, non-random-access ranges, move-only views and zipped ranges.
TEST(ChunkedForLoop, chunkedForEach) {
  // Run `chunkedForEach` on the `range` and return the visited elements, with
  // a `-1` for each call of the `chunkOperation`.
  auto visit = [](auto&& range, size_t chunkSize) {
    std::vector<int> visited;
    ad_utility::chunkedForEach(
        AD_FWD(range), [&visited](int element) { visited.push_back(element); },
        [&visited]() { visited.push_back(-1); }, chunkSize);
    return visited;
  };
  using ::testing::ElementsAre;
  std::vector<int> input{1, 2, 3, 4, 5};
  // A random access range.
  EXPECT_THAT(visit(input, 2), ElementsAre(1, 2, -1, 3, 4, -1, 5, -1));
  EXPECT_THAT(visit(input, 5), ElementsAre(1, 2, 3, 4, 5, -1));
  EXPECT_THAT(visit(input, 7), ElementsAre(1, 2, 3, 4, 5, -1));
  EXPECT_THAT(visit(std::vector<int>{}, 2), ElementsAre());
  // A range that is not random access.
  std::list<int> list{input.begin(), input.end()};
  EXPECT_THAT(visit(list, 2), ElementsAre(1, 2, -1, 3, 4, -1, 5, -1));
  EXPECT_THAT(visit(std::list<int>{}, 2), ElementsAre());
  // A move-only view.
  auto moveOnlyView = ql::views::transform(std::vector<int>{1, 2, 3},
                                           [](int i) { return 2 * i; });
  static_assert(!std::is_copy_constructible_v<decltype(moveOnlyView)>);
  EXPECT_THAT(visit(moveOnlyView, 2), ElementsAre(2, 4, -1, 6, -1));
  // A zipped range.
  std::vector<int> visited;
  ad_utility::chunkedForEach(
      ::ranges::views::zip(input, list),
      [&visited](const auto& pair) {
        const auto& [a, b] = pair;
        visited.push_back(a * b);
      },
      [&visited]() { visited.push_back(-1); }, 3);
  EXPECT_THAT(visited, ElementsAre(1, 4, 9, -1, 16, 25, -1));
  // The chunk size must not be zero.
  EXPECT_ANY_THROW(visit(input, 0));
}
