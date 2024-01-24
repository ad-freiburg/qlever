//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <atomic>

#include "util/ChunkedForLoop.h"

using ad_utility::chunkedForLoop;

TEST(ChunkedForLoop, testEmptyRange) {
  std::atomic_bool flag = false;
  chunkedForLoop<1>(
      0, 0, [&](size_t) { flag = true; }, [&]() { flag = true; });
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
  std::atomic_bool flag = false;
  chunkedForLoop<1>(
      1, 0, [&](size_t) { flag = true; }, [&]() { flag = true; });
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
  std::atomic_size_t counter = 0;
  std::atomic_size_t chunkCounter = 0;
  chunkedForLoop<10>(
      0, 3, [&](size_t) { counter++; }, [&]() { chunkCounter++; });

  EXPECT_EQ(counter, 3);
  EXPECT_EQ(chunkCounter, 1);
}

// _____________________________________________________________________________________________________________________
TEST(ChunkedForLoop, verifyEqualChunkSizeWorks) {
  std::atomic_size_t counter = 0;
  std::atomic_size_t chunkCounter = 0;
  chunkedForLoop<5>(
      3, 8, [&](size_t) { counter++; }, [&]() { chunkCounter++; });

  EXPECT_EQ(counter, 5);
  EXPECT_EQ(chunkCounter, 1);
}

// _____________________________________________________________________________________________________________________
TEST(ChunkedForLoop, verifySmallerChunkSizeWorks) {
  std::atomic_size_t counter = 0;
  std::atomic_size_t chunkCounter = 0;
  chunkedForLoop<7>(
      1, 98, [&](size_t) { counter++; }, [&]() { chunkCounter++; });

  EXPECT_EQ(counter, 97);
  EXPECT_EQ(chunkCounter, 14);
}

// _____________________________________________________________________________________________________________________
TEST(ChunkedForLoop, verifyIndexIsCorrectlyCounting) {
  std::atomic_size_t counter = 7;
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
  std::atomic_size_t counter = 0;
  std::atomic_size_t chunkCounter = 0;
  chunkedForLoop<7>(
      7, 19,
      [&](size_t index, const auto& breakLoop) {
        counter++;
        if (index >= 10) {
          breakLoop();
        }
      },
      [&]() { chunkCounter++; });

  EXPECT_EQ(counter, 4);
  EXPECT_EQ(chunkCounter, 1);
}
