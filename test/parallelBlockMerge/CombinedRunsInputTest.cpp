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

#include <vector>

#include "../util/ParallelBlockMergeTestHelpers.h"
#include "util/memorySize/MemorySize.h"
#include "util/parallelBlockMerge/CombinedRunsInput.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

using namespace ad_utility::parallelBlockMerge;
using namespace parallelBlockMergeTestHelpers;
using ::testing::ElementsAre;

static_assert(InputConcept<CombinedRunsInput<VectorInput<int>>>);

// Test that the runs of the combined inputs are numbered consecutively, in the
// order of the inputs, and that all the operations are forwarded to the input
// that a run belongs to.
TEST(CombinedRunsInput, runsAreNumberedConsecutively) {
  std::vector<VectorInput<int>> inputs;
  inputs.push_back(makeVectorInput<int>({{1, 3, 5}, {2}}, 2));
  // An input without any runs is skipped.
  inputs.push_back(makeVectorInput<int>({}, 2));
  inputs.push_back(makeVectorInput<int>({{4, 6, 8, 10, 12}}, 2));
  CombinedRunsInput input{std::move(inputs)};
  ASSERT_EQ(input.numRuns(), 3u);
  EXPECT_EQ(input.numBlocks(0), 2u);
  EXPECT_EQ(input.numBlocks(1), 1u);
  EXPECT_EQ(input.numBlocks(2), 3u);
  EXPECT_EQ(input.numElementsInBlock(0, 1), 1u);
  EXPECT_EQ(input.numElementsInBlock(2, 0), 2u);
  EXPECT_EQ(input.firstElement(2, 1), 8);
  EXPECT_EQ(input.lastElement(2, 1), 10);
  EXPECT_EQ(input.firstElement(1, 0), 2);
  EXPECT_THAT(input.getBlock(0, 0), ElementsAre(1, 3));
  EXPECT_THAT(input.getBlock(2, 2), ElementsAre(12));
  // Runs beyond the last one are rejected.
  EXPECT_ANY_THROW(input.numBlocks(3));

  // The output blocks are those of the inputs.
  auto block = input.makeEmptyBlock();
  input.appendToBlock(block, 7);
  EXPECT_THAT(block, ElementsAre(7));
  EXPECT_EQ(input.memorySizeOfElement(7),
            ad_utility::MemorySize::bytes(sizeof(int)));
}

// Test that at least one input is required.
TEST(CombinedRunsInput, requiresAnInput) {
  EXPECT_ANY_THROW(
      CombinedRunsInput<VectorInput<int>>{std::vector<VectorInput<int>>{}});
}
