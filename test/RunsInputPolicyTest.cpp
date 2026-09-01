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

#include <cstddef>
#include <vector>

#include "util/MemorySize/MemorySize.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

using namespace ad_utility::parallelBlockMerge;

namespace {
// A minimal type that fulfills the `BlockedRunsInput` concept. It is only used
// to check that the concept is satisfiable, the actual data are irrelevant.
struct DummyInput {
  using Key = int;
  using Block = std::vector<int>;

  size_t numRuns() const { return 0; }
  size_t numBlocks([[maybe_unused]] size_t run) const { return 0; }
  size_t numElementsInBlock([[maybe_unused]] size_t run,
                            [[maybe_unused]] size_t block) const {
    return 0;
  }
  const Key& firstKey([[maybe_unused]] size_t run,
                      [[maybe_unused]] size_t block) const {
    return key_;
  }
  const Key& lastKey([[maybe_unused]] size_t run,
                     [[maybe_unused]] size_t block) const {
    return key_;
  }
  Block readBlock([[maybe_unused]] size_t run,
                  [[maybe_unused]] size_t block) const {
    return {};
  }
  Block makeEmptyBlock() const { return {}; }
  void appendToBlock(Block& block, int el) const { block.push_back(el); }
  ad_utility::MemorySize memorySizeOfElement([[maybe_unused]] int el) const {
    return ad_utility::MemorySize::bytes(sizeof(int));
  }

 private:
  Key key_ = 0;
};

// A type that is missing several of the required member functions.
struct NotAnInput {};

// A type that has the required nested types, but not the member functions.
struct AlmostAnInput {
  using Key = int;
  using Block = std::vector<int>;
};

static_assert(BlockedRunsInput<DummyInput>);
static_assert(!BlockedRunsInput<NotAnInput>);
static_assert(!BlockedRunsInput<AlmostAnInput>);
}  // namespace

// _____________________________________________________________________________
TEST(RunsInputPolicy, VectorRunsInputMetadata) {
  std::vector<std::vector<int>> runs{{1, 3, 5, 7, 9}, {2, 4}};
  VectorRunsInput<std::vector<int>> input{runs, 2};
  EXPECT_EQ(input.numRuns(), 2u);
  // The last virtual block of a run may be shorter than the others.
  EXPECT_EQ(input.numBlocks(0), 3u);
  EXPECT_EQ(input.numBlocks(1), 1u);
  EXPECT_EQ(input.numElementsInBlock(0, 0), 2u);
  EXPECT_EQ(input.numElementsInBlock(0, 2), 1u);
  EXPECT_EQ(input.firstKey(0, 1), 5);
  EXPECT_EQ(input.lastKey(0, 1), 7);
  EXPECT_EQ(input.lastKey(0, 2), 9);
  EXPECT_THAT(input.readBlock(0, 0), ::testing::ElementsAre(1, 3));
  EXPECT_THAT(input.readBlock(0, 2), ::testing::ElementsAre(9));
  EXPECT_THAT(input.readBlock(1, 0), ::testing::ElementsAre(2, 4));
  auto block = input.makeEmptyBlock();
  EXPECT_THAT(block, ::testing::IsEmpty());
  input.appendToBlock(block, 42);
  EXPECT_THAT(block, ::testing::ElementsAre(42));
  EXPECT_EQ(input.memorySizeOfElement(42),
            ad_utility::MemorySize::bytes(sizeof(int)));
  // The size of a virtual block has to be positive.
  EXPECT_ANY_THROW((VectorRunsInput<std::vector<int>>{runs, 0}));
}
