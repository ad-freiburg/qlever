// Copyright 2025 The QLever Authors, in particular:
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "parser/ParallelBuffer.h"

using namespace testing;

// Collect all blocks from `buf` into a single string for easy comparison.
static std::string drainToString(ParallelBuffer& buf) {
  std::string result;
  while (auto block = buf.getNextBlock()) {
    result.append(block->data(), block->size());
  }
  return result;
}

// ____________________________________________________________________________
TEST(StringParallelBuffer, EmptyContent) {
  StringParallelBuffer buf{"", 16};
  EXPECT_EQ(buf.getNextBlock(), std::nullopt);
}

// ____________________________________________________________________________
TEST(StringParallelBuffer, ContentSmallerThanBlocksize) {
  std::string content = "hello";
  StringParallelBuffer buf{content, 1024};
  auto block = buf.getNextBlock();
  ASSERT_TRUE(block.has_value());
  EXPECT_EQ(std::string(block->data(), block->size()), content);
  EXPECT_EQ(buf.getNextBlock(), std::nullopt);
}

// ____________________________________________________________________________
TEST(StringParallelBuffer, ContentLargerThanBlocksize) {
  std::string content = "abcdefghij";
  StringParallelBuffer buf{content, 3};
  EXPECT_EQ(drainToString(buf), content);
}

// ____________________________________________________________________________
TEST(StringParallelBuffer, ContentExactMultipleOfBlocksize) {
  std::string content = "abcdef";
  StringParallelBuffer buf{content, 2};
  EXPECT_EQ(drainToString(buf), content);
  // After draining, a fresh call must still return nullopt.
  EXPECT_EQ(buf.getNextBlock(), std::nullopt);
}
