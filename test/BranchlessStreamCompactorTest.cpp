// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "engine/BranchlessStreamCompactor.h"
#include "global/Id.h"

using namespace ql::engine::vector;

TEST(BranchlessStreamCompactorTest, CompactEvenNumbers) {
  std::vector<Id> input;
  input.reserve(100);
  for (int i = 0; i < 100; ++i) {
    input.push_back(Id::makeFromInt(i));
  }

  std::vector<Id> output(100);
  size_t count = BranchlessStreamCompactor::compact(
      input, output, [](Id id) { return id.getInt() % 2 == 0; });

  EXPECT_EQ(count, 50u);
  for (size_t i = 0; i < count; ++i) {
    EXPECT_EQ(output[i], Id::makeFromInt(static_cast<int>(i * 2)));
  }
}

TEST(BranchlessStreamCompactorTest, EmptyInputReturnsZero) {
  std::vector<Id> input;
  std::vector<Id> output(4);
  size_t count = BranchlessStreamCompactor::compact(
      input, output, [](Id id) { return id.getInt() % 2 == 0; });
  EXPECT_EQ(count, 0u);
}

TEST(BranchlessStreamCompactorTest, AllElementsMatch) {
  std::vector<Id> input;
  for (int i = 0; i < 10; ++i) {
    input.push_back(Id::makeFromInt(2 * i));
  }
  std::vector<Id> output(10);
  size_t count = BranchlessStreamCompactor::compact(
      input, output, [](Id id) { return id.getInt() % 2 == 0; });
  EXPECT_EQ(count, 10u);
  for (size_t i = 0; i < count; ++i) {
    EXPECT_EQ(output[i], Id::makeFromInt(static_cast<int>(2 * i)));
  }
}

TEST(BranchlessStreamCompactorTest, NoElementsMatch) {
  std::vector<Id> input;
  for (int i = 0; i < 10; ++i) {
    input.push_back(Id::makeFromInt(2 * i + 1));
  }
  std::vector<Id> output(10);
  size_t count = BranchlessStreamCompactor::compact(
      input, output, [](Id id) { return id.getInt() % 2 == 0; });
  EXPECT_EQ(count, 0u);
}

TEST(BranchlessStreamCompactorTest, SizeNotDivisibleByFourUsesEpilogue) {
  // 7 elements: 4 via the unrolled loop, 3 via the scalar epilogue.
  std::vector<Id> input;
  for (int i = 0; i < 7; ++i) {
    input.push_back(Id::makeFromInt(i));
  }
  std::vector<Id> output(7);
  size_t count = BranchlessStreamCompactor::compact(
      input, output, [](Id id) { return id.getInt() % 2 == 0; });
  ASSERT_EQ(count, 4u);
  for (size_t i = 0; i < count; ++i) {
    EXPECT_EQ(output[i], Id::makeFromInt(static_cast<int>(i * 2)));
  }
}

TEST(BranchlessStreamCompactorTest, SmallerThanUnrollWidthSkipsLoop) {
  // 3 elements: the unrolled loop is skipped entirely.
  std::vector<Id> input;
  for (int i = 0; i < 3; ++i) {
    input.push_back(Id::makeFromInt(i));
  }
  std::vector<Id> output(3);
  size_t count = BranchlessStreamCompactor::compact(
      input, output, [](Id id) { return id.getInt() % 2 == 0; });
  ASSERT_EQ(count, 2u);
  EXPECT_EQ(output[0], Id::makeFromInt(0));
  EXPECT_EQ(output[1], Id::makeFromInt(2));
}

namespace {
// The elements of `input` with `keep[i] == 1`, by a plain loop.
std::vector<Id> naiveCompact(const std::vector<Id>& input,
                             const std::vector<uint8_t>& keep) {
  std::vector<Id> result;
  for (size_t i = 0; i < input.size(); ++i) {
    if (keep[i]) {
      result.push_back(input[i]);
    }
  }
  return result;
}
}  // namespace

// `compactByMask` keeps exactly the masked elements in order, for all input
// sizes around the unroll width and different densities of the mask, with an
// output that has exactly the size of the result.
TEST(BranchlessStreamCompactorTest, CompactByMaskMatchesPlainLoop) {
  uint64_t state = 7;
  auto nextBit = [&state](uint64_t oneIn) {
    state = state * 6364136223846793005ULL + 1442695040888963407ULL;
    return static_cast<uint8_t>((state >> 33) % oneIn == 0);
  };
  for (size_t size = 0; size < 40; ++size) {
    for (uint64_t oneIn : {1, 2, 3, 10}) {
      std::vector<Id> input;
      std::vector<uint8_t> keep;
      for (size_t i = 0; i < size; ++i) {
        input.push_back(Id::makeFromInt(static_cast<int64_t>(i)));
        keep.push_back(nextBit(oneIn));
      }
      auto expected = naiveCompact(input, keep);
      std::vector<Id> output(expected.size());
      size_t count =
          BranchlessStreamCompactor::compactByMask(input, keep, output);
      ASSERT_EQ(count, expected.size());
      EXPECT_EQ(output, expected);

      // With more room in the output, the result is the same.
      std::vector<Id> largeOutput(size + 4);
      count =
          BranchlessStreamCompactor::compactByMask(input, keep, largeOutput);
      ASSERT_EQ(count, expected.size());
      largeOutput.resize(count);
      EXPECT_EQ(largeOutput, expected);
    }
  }
}

// The mask must have the size of the input, and the output must have room
// for all kept elements.
TEST(BranchlessStreamCompactorTest, CompactByMaskChecksSizes) {
  std::vector<Id> input{Id::makeFromInt(1), Id::makeFromInt(2)};
  std::vector<uint8_t> keep{1, 1};
  std::vector<uint8_t> shortKeep{1};
  std::vector<Id> output(2);
  std::vector<Id> smallOutput(1);
  EXPECT_ANY_THROW(
      BranchlessStreamCompactor::compactByMask(input, shortKeep, output));
  EXPECT_ANY_THROW(
      BranchlessStreamCompactor::compactByMask(input, keep, smallOutput));
}
