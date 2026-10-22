// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include "./util/GTestHelpers.h"
#include "util/Random.h"
#include "util/views/UniqueBlockView.h"

TEST(UniqueBlockView, randomInput) {
  const uint64_t numInts = 50'000;
  std::vector<int> ints;

  ad_utility::SlowRandomIntGenerator<int> r(0, 1000);
  for (size_t i = 0; i < numInts; ++i) {
    ints.push_back(r());
  }

  std::vector<int> intsWithDuplicates;
  intsWithDuplicates.reserve(3 * numInts);
  for (size_t i = 0; i < 3; ++i) {
    for (auto num : ints) {
      intsWithDuplicates.push_back(num);
    }
  }

  std::sort(intsWithDuplicates.begin(), intsWithDuplicates.end());

  ad_utility::SlowRandomIntGenerator<int> vecSizeGenerator(2, 200);

  size_t i = 0;
  std::vector<std::vector<int>> inputs;
  while (i < intsWithDuplicates.size()) {
    auto vecSize = vecSizeGenerator();
    size_t nextI = std::min(i + vecSize, intsWithDuplicates.size());
    inputs.emplace_back(intsWithDuplicates.begin() + i,
                        intsWithDuplicates.begin() + nextI);
    i = nextI;
  }

  auto unique = ql::views::join(ad_utility::uniqueBlockView(inputs));
  std::vector<int> result;
  for (const auto& element : unique) {
    result.push_back(element);
  }
  std::sort(ints.begin(), ints.end());
  // Erase "accidentally" unique duplicates from the random initialization.
  auto it = std::unique(ints.begin(), ints.end());
  ints.erase(it, ints.end());
  ASSERT_EQ(ints.size(), result.size());
  ASSERT_EQ(ints, result);
}

namespace {
using IntBlocks = std::vector<std::vector<int>>;

// Run `uniqueBlockView` on the `blocks` with the given `numBlocksInFlight` and
// collect the resulting blocks (not only their elements), so that the tests can
// also check the blocking of the output.
IntBlocks collectUniqueBlocks(IntBlocks blocks, size_t numBlocksInFlight) {
  IntBlocks result;
  for (auto& block :
       ad_utility::uniqueBlockView(std::move(blocks), numBlocksInFlight)) {
    result.push_back(std::move(block));
  }
  return result;
}

// Check that `uniqueBlockView` turns the `input` into the `expected` blocks,
// for every number of blocks in flight that is interesting for the `input`: a
// single block (which makes the pipeline effectively serial), two and three
// blocks, the default, and enough blocks to hold the complete input at once.
void expectUniqueBlocks(
    const IntBlocks& input, const IntBlocks& expected,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  for (size_t numBlocksInFlight :
       {size_t{1}, size_t{2}, size_t{3},
        ad_utility::DEFAULT_UNIQUE_BLOCK_VIEW_NUM_BLOCKS_IN_FLIGHT,
        input.size() + 1}) {
    EXPECT_THAT(collectUniqueBlocks(input, numBlocksInFlight),
                ::testing::ContainerEq(expected))
        << "numBlocksInFlight was " << numBlocksInFlight;
  }
}
}  // namespace

// _____________________________________________________________________________
TEST(UniqueBlockView, emptyInput) {
  expectUniqueBlocks({}, {});
  // Blocks that are empty to begin with are filtered out.
  expectUniqueBlocks({{}, {}, {}}, {});
}

// _____________________________________________________________________________
TEST(UniqueBlockView, singleBlock) {
  expectUniqueBlocks({{1, 1, 2, 2, 2, 3}}, {{1, 2, 3}});
  // A single block without any duplicates is passed through unchanged.
  expectUniqueBlocks({{1, 2, 3}}, {{1, 2, 3}});
}

// _____________________________________________________________________________
TEST(UniqueBlockView, duplicatesAcrossBlockBoundaries) {
  // The duplicates of `2` and `4` straddle a block boundary, so they can only
  // be removed by taking the last element of the previous block into account.
  expectUniqueBlocks({{1, 2, 2}, {2, 3, 4}, {4, 4, 5}}, {{1, 2}, {3, 4}, {5}});
}

// _____________________________________________________________________________
TEST(UniqueBlockView, blocksThatBecomeEmpty) {
  // The second and third block consist only of duplicates of the last element
  // of the first block, so they become empty and are not yielded at all.
  expectUniqueBlocks({{1, 1, 2}, {2, 2}, {2}, {2, 3}}, {{1, 2}, {3}});
  // The same, but the blocks that become empty are the last ones.
  expectUniqueBlocks({{1, 2}, {2}, {2, 2}}, {{1, 2}});
}

// _____________________________________________________________________________
TEST(UniqueBlockView, manyBlocksInFlight) {
  // A single value that spans many blocks: every block but the first one
  // becomes empty, so the pipeline has to skip many more blocks than it has in
  // flight at any time.
  IntBlocks input;
  for (size_t i = 0; i < 100; ++i) {
    input.push_back({7, 7, 7, 7});
  }
  input.push_back({7, 8});
  expectUniqueBlocks(input, {{7}, {8}});

  // Many blocks without any duplicates, to check that the order of the blocks
  // is preserved although they are deduplicated concurrently.
  IntBlocks distinctInput;
  IntBlocks distinctExpected;
  for (int i = 0; i < 100; ++i) {
    distinctInput.push_back({3 * i, 3 * i + 1, 3 * i + 1, 3 * i + 2});
    distinctExpected.push_back({3 * i, 3 * i + 1, 3 * i + 2});
  }
  expectUniqueBlocks(distinctInput, distinctExpected);
}
