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
#include <functional>
#include <optional>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "util/Forward.h"
#include "util/GTestHelpers.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ParallelBlockMergeTestHelpers.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeHelpersImpl.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

using namespace ad_utility::parallelBlockMerge;
using namespace parallelBlockMergeTestHelpers;

namespace {
// An entry of the weighted quantile over the block metadata, see
// `detail::ElementAndWeight`.
using SizeAndWeight = detail::ElementAndWeight<size_t>;
using SizeAndWeightVec = std::vector<SizeAndWeight>;

// An input policy that wraps a `VectorInput<size_t>` but reports every block as
// empty. `VectorInput` rejects empty blocks in its constructor, so this is the
// only way to violate the precondition of `detail::collectElementsAndWeights`.
struct ZeroSizeBlockInput {
  using value_type = size_t;
  using Element = size_t;
  using Block = SizeVec;

  SizeInput wrapped_;

  size_t numRuns() const { return wrapped_.numRuns(); }
  size_t numBlocks(size_t runIdx) const { return wrapped_.numBlocks(runIdx); }
  size_t numElementsInBlock([[maybe_unused]] size_t runIdx,
                            [[maybe_unused]] size_t blockIdx) const {
    return 0;
  }
  const Element& firstElement(size_t runIdx, size_t blockIdx) const {
    return wrapped_.firstElement(runIdx, blockIdx);
  }
  const Element& lastElement(size_t runIdx, size_t blockIdx) const {
    return wrapped_.lastElement(runIdx, blockIdx);
  }
  Block readBlock(size_t runIdx, size_t blockIdx) const {
    return wrapped_.readBlock(runIdx, blockIdx);
  }
  Block makeEmptyBlock() const { return {}; }
  template <typename U>
  void appendToBlock(Block& block, U&& element) const {
    block.push_back(AD_FWD(element));
  }
  ad_utility::MemorySize memorySizeOfElement(
      [[maybe_unused]] const value_type& element) const {
    return ad_utility::MemorySize::bytes(sizeof(value_type));
  }
};

static_assert(InputConcept<ZeroSizeBlockInput>);
}  // namespace

// ___________________________________________________________________________
// The individual steps of the computation of the chunk split points, see
// `MergeHelpersImpl.h`.
// ___________________________________________________________________________

// _____________________________________________________________________________
TEST(MergeHelpers, allBlocksInAllRuns) {
  // The run `[1, 2, 3, 4, 5]` consists of the three blocks `[1, 2]`, `[3, 4]`
  // and `[5]`, the run `[10, 20]` of the single block `[10, 20]`.
  std::vector<SizeVec> runs{SizeVec{1, 2, 3, 4, 5}, SizeVec{10, 20}};
  EXPECT_THAT(
      ::ranges::to_vector(detail::allBlocksInAllRuns(makeVectorInput(runs, 2))),
      ::testing::ElementsAre(Pair(0u, 0u), Pair(0u, 1u), Pair(0u, 2u),
                             Pair(1u, 0u)));
  // An input without any run, and one with only empty runs, has no block at
  // all.
  EXPECT_THAT(::ranges::to_vector(detail::allBlocksInAllRuns(
                  makeVectorInput(std::vector<SizeVec>{}, 2))),
              ::testing::IsEmpty());
  std::vector<SizeVec> emptyRuns{SizeVec{}, SizeVec{}};
  EXPECT_THAT(::ranges::to_vector(
                  detail::allBlocksInAllRuns(makeVectorInput(emptyRuns, 2))),
              ::testing::IsEmpty());
  // An empty run in the middle is simply skipped, and the indices of the
  // following runs are unaffected.
  std::vector<SizeVec> runsWithGap{SizeVec{1, 2}, SizeVec{}, SizeVec{3}};
  EXPECT_THAT(::ranges::to_vector(
                  detail::allBlocksInAllRuns(makeVectorInput(runsWithGap, 2))),
              ::testing::ElementsAre(Pair(0u, 0u), Pair(2u, 0u)));
}

// _____________________________________________________________________________
TEST(MergeHelpers, collectElementsAndWeights) {
  // The blocks are `[1, 2]`, `[3, 4]`, `[5]` and `[10, 20]`, so the last
  // elements are `2`, `4`, `5` and `20` with the weights `2`, `2`, `1` and `2`.
  // The order is that of `allBlocksInAllRuns`, that is by run and then by
  // block.
  std::vector<SizeVec> runs{SizeVec{1, 2, 3, 4, 5}, SizeVec{10, 20}};
  EXPECT_THAT(
      detail::collectElementsAndWeights(makeVectorInput(runs, 2)),
      ::testing::ElementsAre(SizeAndWeight(2u, 2u), SizeAndWeight(4u, 2u),
                             SizeAndWeight(5u, 1u), SizeAndWeight(20u, 2u)));
  // The elements are deliberately *not* sorted, that is what
  // `sortAndAccumulateWeights` is for.
  std::vector<SizeVec> unsortedRuns{SizeVec{10, 20}, SizeVec{1, 2}};
  EXPECT_THAT(
      detail::collectElementsAndWeights(makeVectorInput(unsortedRuns, 2)),
      ::testing::ElementsAre(SizeAndWeight(20u, 2u), SizeAndWeight(2u, 2u)));
  // An input without any block yields no entry at all.
  EXPECT_THAT(detail::collectElementsAndWeights(
                  makeVectorInput(std::vector<SizeVec>{}, 2)),
              ::testing::IsEmpty());
  std::vector<SizeVec> emptyRuns{SizeVec{}, SizeVec{}};
  EXPECT_THAT(detail::collectElementsAndWeights(makeVectorInput(emptyRuns, 2)),
              ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(MergeHelpers, collectElementsAndWeightsRejectsEmptyBlocks) {
  // A block without any element violates the precondition, because it has no
  // last element that could be weighted.
  std::vector<SizeVec> runs{SizeVec{1, 2}};
  ZeroSizeBlockInput input{makeVectorInput(runs, 2)};
  AD_EXPECT_THROW_WITH_MESSAGE(detail::collectElementsAndWeights(input),
                               ::testing::HasSubstr("numElements > 0"));
}

// _____________________________________________________________________________
TEST(MergeHelpers, sortAndAccumulateWeights) {
  // The entries are sorted by their element, the weights of equal elements are
  // added up, and the weights are then replaced by their prefix sums.
  SizeAndWeightVec entries{
      {30u, 1u}, {10u, 2u}, {30u, 4u}, {20u, 3u}, {10u, 5u}};
  EXPECT_THAT(
      detail::sortAndAccumulateWeights(entries, std::less<>{}),
      ::testing::ElementsAre(SizeAndWeight(10u, 7u), SizeAndWeight(20u, 10u),
                             SizeAndWeight(30u, 15u)));
  // An input without any entry, and a single entry, whose weight is its own
  // prefix sum.
  EXPECT_THAT(
      detail::sortAndAccumulateWeights(SizeAndWeightVec{}, std::less<>{}),
      ::testing::IsEmpty());
  EXPECT_THAT(detail::sortAndAccumulateWeights(SizeAndWeightVec{{5u, 4u}},
                                               std::less<>{}),
              ::testing::ElementsAre(SizeAndWeight(5u, 4u)));
  // All elements are equal, so they are merged into a single entry that holds
  // the total weight. This is the case that makes a split impossible.
  EXPECT_THAT(
      detail::sortAndAccumulateWeights(
          SizeAndWeightVec{{7u, 1u}, {7u, 2u}, {7u, 3u}}, std::less<>{}),
      ::testing::ElementsAre(SizeAndWeight(7u, 6u)));
  // An input that is already sorted and has no duplicates is only accumulated.
  EXPECT_THAT(
      detail::sortAndAccumulateWeights(
          SizeAndWeightVec{{1u, 1u}, {2u, 2u}, {3u, 3u}}, std::less<>{}),
      ::testing::ElementsAre(SizeAndWeight(1u, 1u), SizeAndWeight(2u, 3u),
                             SizeAndWeight(3u, 6u)));
  // The order is that of the `comparator` and not the natural one.
  EXPECT_THAT(
      detail::sortAndAccumulateWeights(
          SizeAndWeightVec{{1u, 1u}, {3u, 3u}, {2u, 2u}}, std::greater<>{}),
      ::testing::ElementsAre(SizeAndWeight(3u, 3u), SizeAndWeight(2u, 5u),
                             SizeAndWeight(1u, 6u)));

  // Entries are merged if the `comparator` considers their elements
  // equivalent, even if the elements are not identical. `ComparePairs` only
  // looks at the first component, so the two entries with the key `1` are
  // merged; which of their elements survives is deliberately unspecified.
  std::vector<detail::ElementAndWeight<Pair>> pairs{
      {Pair{1u, 100u}, 2u}, {Pair{2u, 50u}, 4u}, {Pair{1u, 200u}, 3u}};
  auto merged = detail::sortAndAccumulateWeights(pairs, ComparePairs{});
  ASSERT_EQ(merged.size(), 2u);
  EXPECT_EQ(merged.at(0).first.first, 1u);
  EXPECT_EQ(merged.at(0).second, 5u);
  EXPECT_EQ(merged.at(1).first, Pair(2u, 50u));
  EXPECT_EQ(merged.at(1).second, 9u);
}

// _____________________________________________________________________________
TEST(MergeHelpers, uniformTargets) {
  // The targets are the total number of elements times `i / numChunks`, for
  // `i` from `1` to `numChunks - 1`, so there is one target per split point.
  EXPECT_THAT(detail::uniformTargets(100, 4),
              ::testing::ElementsAre(25u, 50u, 75u));
  EXPECT_THAT(detail::uniformTargets(100, 2), ::testing::ElementsAre(50u));
  // The division is truncating.
  EXPECT_THAT(detail::uniformTargets(10, 4),
              ::testing::ElementsAre(2u, 5u, 7u));
  // A single chunk (or none at all) needs no target.
  EXPECT_THAT(detail::uniformTargets(100, 1), ::testing::IsEmpty());
  EXPECT_THAT(detail::uniformTargets(100, 0), ::testing::IsEmpty());
  // NOTE: A target is always at least `1`, so far more chunks than elements
  // yield the target `1` several times. That is deliberate and harmless, see
  // `uniformTargets` and the test of `pickChunkSplitPoints` below.
  EXPECT_THAT(detail::uniformTargets(3, 8),
              ::testing::ElementsAre(1u, 1u, 1u, 1u, 1u, 2u, 2u));
  EXPECT_THAT(detail::uniformTargets(0, 4), ::testing::ElementsAre(1u, 1u, 1u));
}

// _____________________________________________________________________________
TEST(MergeHelpers, targetsFromChunkSizes) {
  auto targets = [](size_t totalNumElements, const SizeVec& firstChunkSizes,
                    size_t remainingChunkSize) {
    return detail::targetsFromChunkSizes(totalNumElements, firstChunkSizes,
                                         remainingChunkSize);
  };
  // The `i`-th target is the total size of the first `i` chunks.
  EXPECT_THAT(targets(100, {}, 25), ::testing::ElementsAre(25u, 50u, 75u));
  EXPECT_THAT(targets(100, {5, 5, 10}, 40),
              ::testing::ElementsAre(5u, 10u, 20u, 60u));
  // The leading sizes cover the whole input exactly, so no chunk is left for
  // the remaining size.
  EXPECT_THAT(targets(100, {50, 50}, 10), ::testing::ElementsAre(50u));
  // A leading size that already exceeds the input drops all the sizes after it.
  EXPECT_THAT(targets(100, {30, 500, 7}, 10), ::testing::ElementsAre(30u));
  // A single chunk that is at least as large as the whole input needs no target
  // at all, and neither does an input without any element.
  EXPECT_THAT(targets(100, {}, 1000), ::testing::IsEmpty());
  EXPECT_THAT(targets(100, {100}, 10), ::testing::IsEmpty());
  EXPECT_THAT(targets(0, {}, 4), ::testing::IsEmpty());
  EXPECT_THAT(targets(0, {1, 2}, 4), ::testing::IsEmpty());
  EXPECT_THAT(targets(1, {}, 1), ::testing::IsEmpty());
  // The smallest possible chunks: one element each.
  EXPECT_THAT(targets(5, {}, 1), ::testing::ElementsAre(1u, 2u, 3u, 4u));
  // In contrast to `uniformTargets`, these targets are strictly increasing,
  // because all the chunk sizes are strictly positive (which the public
  // `computeChunkBoundaries` checks).
  auto manyTargets = targets(1000, {1, 2, 3}, 7);
  ASSERT_FALSE(manyTargets.empty());
  for (size_t i = 1; i < manyTargets.size(); ++i) {
    EXPECT_LT(manyTargets[i - 1], manyTargets[i]);
  }
}

// _____________________________________________________________________________
TEST(MergeHelpers, pickChunkSplitPoints) {
  // Four blocks whose last elements are `10`, `20`, `30` and `40`, with the
  // accumulated weights `3`, `7`, `9` and `12`.
  const SizeAndWeightVec entries{{10u, 3u}, {20u, 7u}, {30u, 9u}, {40u, 12u}};
  auto pick = [&entries](const SizeVec& targets) {
    return detail::pickChunkSplitPoints(entries, targets);
  };
  // Without any target there is no split point, and hence a single chunk.
  EXPECT_THAT(pick({}), ::testing::IsEmpty());
  // A target picks the smallest element whose accumulated weight reaches it.
  EXPECT_THAT(pick({3u}), ::testing::ElementsAre(10u));
  EXPECT_THAT(pick({1u}), ::testing::ElementsAre(10u));
  EXPECT_THAT(pick({4u}), ::testing::ElementsAre(20u));
  EXPECT_THAT(pick({12u}), ::testing::ElementsAre(40u));
  EXPECT_THAT(pick({3u, 7u, 9u}), ::testing::ElementsAre(10u, 20u, 30u));
  // Equal targets do not yield the same split point twice, because an entry
  // that was picked is never looked at again. This is what makes the targets of
  // `uniformTargets` harmless.
  EXPECT_THAT(pick({3u, 3u, 3u}), ::testing::ElementsAre(10u, 20u, 30u));
  EXPECT_THAT(pick({1u, 1u, 1u, 1u, 1u}),
              ::testing::ElementsAre(10u, 20u, 30u, 40u));
  // A target that no accumulated weight reaches ends the scan, so the targets
  // after it are ignored as well.
  EXPECT_THAT(pick({13u}), ::testing::IsEmpty());
  EXPECT_THAT(pick({9u, 13u, 4u}), ::testing::ElementsAre(30u));
  // Without any entry there is nothing that could be picked.
  EXPECT_THAT(detail::pickChunkSplitPoints(SizeAndWeightVec{}, SizeVec{1u, 2u}),
              ::testing::IsEmpty());
  // The result is strictly increasing, no matter which targets are asked for.
  auto splitPoints = pick({1u, 1u, 5u, 7u, 8u, 8u, 12u, 12u});
  for (size_t i = 1; i < splitPoints.size(); ++i) {
    EXPECT_LT(splitPoints[i - 1], splitPoints[i]);
  }
}

// ___________________________________________________________________________
// The helpers of `MergeHelpers.h` itself.
// ___________________________________________________________________________

// _____________________________________________________________________________
TEST(MergeHelpers, totalNumElements) {
  EXPECT_EQ(
      detail::totalNumElements(makeVectorInput(std::vector<SizeVec>{}, 7)), 0u);
  std::vector<SizeVec> emptyRuns{SizeVec{}, SizeVec{}};
  EXPECT_EQ(detail::totalNumElements(makeVectorInput(emptyRuns, 7)), 0u);
  // Blocks that are not completely filled are counted correctly, so the block
  // size must not make a difference.
  auto runs = makeRandomRuns(5, 30, 70);
  size_t expected = sortedConcatenation(runs).size();
  for (size_t blockSize : {1u, 3u, 64u, 1000u}) {
    EXPECT_EQ(detail::totalNumElements(makeVectorInput(runs, blockSize)),
              expected);
  }
}

// _____________________________________________________________________________
TEST(MergeHelpers, chunkSplitPointsAreStrictlyIncreasing) {
  auto runs = makeRandomRuns(20, 100, 200);
  auto input = makeVectorInput(runs, 7);
  for (size_t numChunks : {2u, 3u, 8u, 64u}) {
    auto boundaries = computeChunkBoundaries(input, std::less<>{}, numChunks);
    EXPECT_LE(boundaries.size(), numChunks);
    auto splitPoints = chunkSplitPoints(boundaries);
    for (size_t i = 1; i < splitPoints.size(); ++i) {
      EXPECT_LT(splitPoints[i - 1], splitPoints[i]);
    }
  }
}

// _____________________________________________________________________________
TEST(MergeHelpers, blocksWithEqualLastElements) {
  // Four identical runs, each of which consists of the four blocks
  // `[0 ... 0]`, `[1 ... 1]`, `[2 ... 2]` and `[3 ... 3]`. Every one of the
  // four distinct elements is therefore the last element of four different
  // blocks, so the entries of the weighted quantile have to be merged; without
  // that, a single element could start more than one chunk, and
  // the quantiles would be off by up to a factor of the number of runs.
  auto input = makeVectorInput(runsWithEqualLastElements(), 10);
  auto boundaries = computeChunkBoundaries(input, std::less<>{}, 4);
  // The accumulated weights are `40`, `80`, `120` and `160`, and the targets
  // for four chunks are `40`, `80` and `120`, so every distinct element but the
  // largest one starts a chunk.
  EXPECT_THAT(chunkSplitPoints(boundaries), ::testing::ElementsAre(0u, 1u, 2u));
  // NOTE: The first chunk `[-infinity, 0)` is empty, which is perfectly legal,
  // see `pickChunkSplitPoints`.
  EXPECT_FALSE(boundaries.at(0).lo_.has_value());
  EXPECT_EQ(boundaries.at(0).hi_, 0u);
}

// _____________________________________________________________________________
TEST(MergeHelpers, chunkBoundaryEdgeCases) {
  // All elements are equal, so there is no way to actually split the input.
  // NOTE: The single split point that is picked yields an empty first chunk,
  // see `pickChunkSplitPoints`; the second chunk then holds all the elements.
  {
    EXPECT_THAT(
        chunkSplitPoints(computeChunkBoundaries(
            makeVectorInput(runsWithEqualElements(), 7), std::less<>{}, 8)),
        ::testing::ElementsAre(42u));
  }
  // Zero runs, and only empty runs.
  {
    EXPECT_THAT(
        chunkSplitPoints(computeChunkBoundaries(
            makeVectorInput(std::vector<SizeVec>{}, 7), std::less<>{}, 8)),
        ::testing::IsEmpty());
    std::vector<SizeVec> runs{SizeVec{}, SizeVec{}};
    EXPECT_THAT(chunkSplitPoints(computeChunkBoundaries(
                    makeVectorInput(runs, 7), std::less<>{}, 8)),
                ::testing::IsEmpty());
  }
  // A single chunk requires no split point at all.
  {
    auto runs = makeRandomRuns(4, 50, 50);
    for (size_t numChunks : {0u, 1u}) {
      auto boundaries = computeChunkBoundaries(makeVectorInput(runs, 7),
                                               std::less<>{}, numChunks);
      EXPECT_EQ(boundaries.size(), 1u);
      EXPECT_THAT(chunkSplitPoints(boundaries), ::testing::IsEmpty());
    }
  }
  // More chunks than blocks: the number of split points is bounded by the
  // number of distinct last elements of the blocks.
  {
    std::vector<SizeVec> runs{SizeVec{1, 2, 3, 4, 5, 6}};
    auto splitPoints = chunkSplitPoints(
        computeChunkBoundaries(makeVectorInput(runs, 2), std::less<>{}, 100));
    EXPECT_LE(splitPoints.size(), 3u);
    for (size_t i = 1; i < splitPoints.size(); ++i) {
      EXPECT_LT(splitPoints[i - 1], splitPoints[i]);
    }
  }
  // One huge run and many tiny ones. The split points have to follow the huge
  // run.
  {
    auto splitPoints = chunkSplitPoints(computeChunkBoundaries(
        makeVectorInput(oneHugeAndManyTinyRuns(), 64), std::less<>{}, 8));
    EXPECT_FALSE(splitPoints.empty());
    EXPECT_LE(splitPoints.size(), 7u);
    // The split points are spread over the whole range of the huge run and are
    // not all crammed into the range of the tiny ones.
    EXPECT_GT(splitPoints.back(), 1000u);
  }
}

// _____________________________________________________________________________
TEST(MergeHelpers, chunkBoundariesFromExplicitChunkSizes) {
  // A single run with the elements `0 ... 99`, one element per block, so that
  // the split points can be predicted exactly.
  SizeVec run(100);
  ql::ranges::generate(run, [i = size_t{0}]() mutable { return i++; });
  std::vector<SizeVec> runs{run};
  auto input = makeVectorInput(runs, 1);
  auto splitPointsFor = [&input](ChunkSizes chunkSizes) {
    return chunkSplitPoints(
        computeChunkBoundaries(input, std::less<>{}, std::move(chunkSizes)));
  };

  // NOTE: A chunk starts at the largest element that is still needed to reach a
  // target, so a chunk that is supposed to start after `n` elements starts at
  // the element `n - 1`. That is the same convention as for a uniform number of
  // chunks, see `computeChunkBoundaries`, and it is why the chunk sizes below
  // are only exact up to that single element.

  // Uniform chunks of 25 elements each, so the chunks start after `25`, `50`
  // and `75` elements.
  EXPECT_THAT(splitPointsFor(ChunkSizes{{}, 25}),
              ::testing::ElementsAre(24u, 49u, 74u));
  // Three small leading chunks, then chunks of 40, so the chunks start after
  // `5`, `10`, `20` and `60` elements.
  EXPECT_THAT(splitPointsFor(ChunkSizes{{5, 5, 10}, 40}),
              ::testing::ElementsAre(4u, 9u, 19u, 59u));
  // The leading sizes cover the whole input exactly, so no chunk is left for
  // `remainingChunkSize_`.
  EXPECT_THAT(splitPointsFor(ChunkSizes{{50, 50}, 10}),
              ::testing::ElementsAre(49u));
  // The leading sizes exceed the input, so the surplus ones are dropped.
  EXPECT_THAT(splitPointsFor(ChunkSizes{{30, 500, 7}, 10}),
              ::testing::ElementsAre(29u));
}

// _____________________________________________________________________________
TEST(MergeHelpers, chunkBoundariesFromChunkSizesEdgeCases) {
  auto runs = makeRandomRuns(4, 50, 50);
  auto input = makeVectorInput(runs, 7);
  auto splitPointsFor = [](const auto& theInput, ChunkSizes chunkSizes) {
    return chunkSplitPoints(
        computeChunkBoundaries(theInput, std::less<>{}, std::move(chunkSizes)));
  };

  // A chunk that is at least as large as the whole input needs no split point.
  EXPECT_THAT(splitPointsFor(input, ChunkSizes{{}, 1000}),
              ::testing::IsEmpty());
  EXPECT_THAT(splitPointsFor(input, ChunkSizes{{1000}, 10}),
              ::testing::IsEmpty());
  // Zero runs, and only empty runs.
  EXPECT_THAT(splitPointsFor(makeVectorInput(std::vector<SizeVec>{}, 7),
                             ChunkSizes{{}, 4}),
              ::testing::IsEmpty());
  std::vector<SizeVec> emptyRuns{SizeVec{}, SizeVec{}};
  EXPECT_THAT(splitPointsFor(makeVectorInput(emptyRuns, 7), ChunkSizes{{}, 4}),
              ::testing::IsEmpty());
  // All elements are equal, so there is no way to actually split the input, no
  // matter which chunk sizes are requested. The single split point that is
  // picked yields an empty first chunk, see `pickChunkSplitPoints`.
  EXPECT_THAT(splitPointsFor(makeVectorInput(runsWithEqualElements(), 7),
                             ChunkSizes{{2, 2}, 2}),
              ::testing::ElementsAre(42u));
  // The split points are strictly increasing, also for very small chunks.
  {
    auto splitPoints = splitPointsFor(input, ChunkSizes{{1, 2, 3}, 1});
    for (size_t i = 1; i < splitPoints.size(); ++i) {
      EXPECT_LT(splitPoints[i - 1], splitPoints[i]);
    }
  }
  // A size of zero is illegal, because it would describe an empty chunk.
  AD_EXPECT_THROW_WITH_MESSAGE(
      computeChunkBoundaries(input, std::less<>{}, ChunkSizes{{}, 0}),
      ::testing::HasSubstr("remainingChunkSize_ > 0"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      computeChunkBoundaries(input, std::less<>{}, ChunkSizes{{4, 0, 4}, 4}),
      ::testing::HasSubstr("size > 0"));
}

// _____________________________________________________________________________
TEST(MergeHelpers, chunkBoundariesDescribeTheRangeOfEveryChunk) {
  auto boundaries = detail::chunkBoundariesFromSplitPoints(SizeVec{10, 20});
  ASSERT_EQ(boundaries.size(), 3u);
  // The lower bound of the first and the upper bound of the last chunk are
  // empty, that is minus and plus infinity.
  EXPECT_FALSE(boundaries.at(0).lo_.has_value());
  EXPECT_EQ(boundaries.at(0).hi_, 10u);
  EXPECT_EQ(boundaries.at(1).lo_, 10u);
  EXPECT_EQ(boundaries.at(1).hi_, 20u);
  EXPECT_EQ(boundaries.at(2).lo_, 20u);
  EXPECT_FALSE(boundaries.at(2).hi_.has_value());

  // Without any split point there is a single chunk that covers everything,
  // which is also what `singleChunk` yields.
  for (const auto& single : {detail::chunkBoundariesFromSplitPoints(SizeVec{}),
                             singleChunk<size_t>()}) {
    ASSERT_EQ(single.size(), 1u);
    EXPECT_FALSE(single.at(0).lo_.has_value());
    EXPECT_FALSE(single.at(0).hi_.has_value());
  }
}

// _____________________________________________________________________________
TEST(MergeHelpers, blockRangeForRun) {
  // Two runs with the blocks `[0, 1, 2]`, `[3, 4, 5]`, `[6, 7, 8]` and
  // `[10, 11, 12]`, plus an empty run.
  std::vector<SizeVec> runs{SizeVec{0, 1, 2, 3, 4, 5, 6, 7, 8},
                            SizeVec{10, 11, 12}, SizeVec{}};
  auto input = makeVectorInput(runs, 3);
  const std::less<> comparator;
  auto range = [&input, &comparator](std::optional<size_t> lo,
                                     std::optional<size_t> hi, size_t runIdx) {
    auto result = detail::blockRangeForRun(
        input, comparator, ChunkBoundary<size_t>{std::move(lo), std::move(hi)},
        runIdx);
    return std::pair<size_t, size_t>{result.firstBlockIdx_,
                                     result.endBlockIdx_};
  };
  // Without any bounds, all blocks of the run are in the range.
  EXPECT_EQ(range(std::nullopt, std::nullopt, 0), Pair(0u, 3u));
  EXPECT_EQ(range(std::nullopt, std::nullopt, 1), Pair(0u, 1u));
  // An empty run contributes no block at all.
  EXPECT_EQ(range(std::nullopt, std::nullopt, 2), Pair(0u, 0u));
  EXPECT_TRUE(
      detail::blockRangeForRun(input, comparator, ChunkBoundary<size_t>{}, 2)
          .empty());

  // The upper bound `3` is exactly the first element of the second block, so
  // that block is already outside of the range. This pins the exact form of the
  // predicate for the end of the range.
  EXPECT_EQ(range(std::nullopt, 3, 0), Pair(0u, 1u));
  // Symmetrically, the lower bound `3` is greater than the last element of the
  // first block, so that block is outside of the range.
  EXPECT_EQ(range(3, std::nullopt, 0), Pair(1u, 3u));
  // Bounds that lie inside a block keep exactly that block.
  EXPECT_EQ(range(4, 5, 0), Pair(1u, 2u));
  // A bound that lies between two blocks.
  EXPECT_EQ(range(2, 6, 0), Pair(0u, 2u));

  // Bounds that exclude the whole run yield an empty range.
  EXPECT_TRUE(detail::blockRangeForRun(input, comparator,
                                       ChunkBoundary<size_t>{100, std::nullopt},
                                       0)
                  .empty());
  EXPECT_TRUE(detail::blockRangeForRun(
                  input, comparator, ChunkBoundary<size_t>{std::nullopt, 0}, 0)
                  .empty());
  // The second run lies completely above the range `[0, 9)`.
  EXPECT_TRUE(detail::blockRangeForRun(input, comparator,
                                       ChunkBoundary<size_t>{0, 9}, 1)
                  .empty());
}
