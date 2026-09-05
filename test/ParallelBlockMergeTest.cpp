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
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Random.h"
#include "util/parallelBlockMerge/ParallelBlockMerge.h"

using namespace ad_utility::parallelBlockMerge;

namespace {
using SizeVec = std::vector<size_t>;
using SizeRuns = VectorRunsInput<SizeVec>;
using StringRun = ql::ranges::subrange<std::vector<std::string>::iterator>;
using StringRuns = VectorRunsInput<StringRun>;

static_assert(BlockedRunsInput<SizeRuns>);
static_assert(BlockedRunsInput<StringRuns>);

using Pair = std::pair<size_t, size_t>;
using PairRuns = VectorRunsInput<std::vector<Pair>>;

// An element with a cheap key and a payload that is emptied when the element is
// moved from. In contrast to a plain `std::string`, moving such an element does
// not change its key, so the (I/O-free) block metadata of a `VectorRunsInput`
// (which aliases the underlying vectors) stays valid while the elements are
// already being moved out of those vectors.
using KeyAndPayload = std::pair<size_t, std::string>;
using KeyAndPayloadRun =
    ql::ranges::subrange<std::vector<KeyAndPayload>::iterator>;
using KeyAndPayloadRuns = VectorRunsInput<KeyAndPayloadRun>;

// Compare only the keys of two `KeyAndPayload`s.
struct CompareKeys {
  bool operator()(const KeyAndPayload& a, const KeyAndPayload& b) const {
    return a.first < b.first;
  }
};

// A comparator that only looks at the first component of a pair, so that ties
// are visible in the second component.
struct ComparePairs {
  bool operator()(const Pair& a, const Pair& b) const {
    return a.first < b.first;
  }
};

// An input policy that wraps a `VectorRunsInput<SizeVec>` and additionally
// records every call to `readBlock` and (optionally) throws from the
// `throwAtRead_`-th of them. The recorded state is shared between all copies,
// because the merge takes the input by value.
struct InstrumentedInput {
  using value_type = size_t;
  using Key = size_t;
  using Block = SizeVec;

  // The shared state of all copies of an `InstrumentedInput`.
  struct State {
    // NOTE: The `mutex_` is only there because `BlockedRunsInput` requires
    // `readBlock` to be thread-safe. A serial merge only ever reads from the
    // consuming thread.
    std::mutex mutex_{};
    std::vector<std::pair<size_t, size_t>> readBlocks_{};
    // The number of the call to `readBlock` that throws. The value `0` means
    // "never throw".
    size_t throwAtRead_ = 0;
  };

  SizeRuns wrapped_;
  std::shared_ptr<State> state_ = std::make_shared<State>();

  size_t numRuns() const { return wrapped_.numRuns(); }
  size_t numBlocks(size_t run) const { return wrapped_.numBlocks(run); }
  size_t numElementsInBlock(size_t run, size_t block) const {
    return wrapped_.numElementsInBlock(run, block);
  }
  const Key& firstKey(size_t run, size_t block) const {
    return wrapped_.firstKey(run, block);
  }
  const Key& lastKey(size_t run, size_t block) const {
    return wrapped_.lastKey(run, block);
  }
  Block readBlock(size_t run, size_t block) const {
    size_t numReads = 0;
    {
      std::lock_guard<std::mutex> lock{state_->mutex_};
      state_->readBlocks_.emplace_back(run, block);
      numReads = state_->readBlocks_.size();
    }
    if (state_->throwAtRead_ != 0 && numReads >= state_->throwAtRead_) {
      throw std::runtime_error{"readBlock failed"};
    }
    return wrapped_.readBlock(run, block);
  }
  Block makeEmptyBlock() const { return {}; }
  template <typename T>
  void appendToBlock(Block& block, T&& element) const {
    block.push_back(std::forward<T>(element));
  }
  ad_utility::MemorySize memorySizeOfElement(
      [[maybe_unused]] const value_type& element) const {
    return ad_utility::MemorySize::bytes(sizeof(value_type));
  }
};

static_assert(BlockedRunsInput<InstrumentedInput>);

// Merge the `input` split into (at most) `numChunks` chunks and return the
// elements of all output blocks in a single vector.
template <bool moveElements = false, typename Input, typename Comparator>
std::vector<typename Input::value_type> mergeToVector(
    Input input, Comparator comparator, MergeOptions options = {},
    size_t numChunks = 1,
    ad_utility::SharedCancellationHandle cancellationHandle = nullptr) {
  auto splitters = computeSplitters(input, comparator, numChunks);
  auto blocks = serialBlockMergeToRange<moveElements>(
      std::move(input), std::move(comparator), std::move(options),
      std::move(cancellationHandle), std::move(splitters));
  std::vector<typename Input::value_type> result;
  for (auto& block : blocks) {
    // An output block is never empty, no matter how the chunks are laid out.
    EXPECT_FALSE(block.empty());
    for (auto& element : block) {
      result.push_back(std::move(element));
    }
  }
  return result;
}

// Return `MergeOptions` with the given number of elements per output block.
// That number is deliberately small in the tests, such that even a single chunk
// yields several blocks.
MergeOptions optionsWithBlockSize(size_t outputBlockSize = 7) {
  MergeOptions options;
  options.outputBlockSize = OutputBlockSize::numElements(outputBlockSize);
  return options;
}

// Return `numRuns` sorted vectors of random numbers, the sizes of which are
// uniformly distributed in `[minSize, maxSize]`.
std::vector<SizeVec> makeRandomRuns(size_t numRuns, size_t minSize,
                                    size_t maxSize) {
  ad_utility::FastRandomIntGenerator<uint64_t> valueGenerator;
  ad_utility::SlowRandomIntGenerator<size_t> sizeGenerator{minSize, maxSize};
  std::vector<SizeVec> runs;
  for (size_t i = 0; i < numRuns; ++i) {
    SizeVec run(sizeGenerator());
    ql::ranges::generate(run, valueGenerator);
    ql::ranges::sort(run);
    runs.push_back(std::move(run));
  }
  return runs;
}

// Return the sorted concatenation of all `runs`.
SizeVec sortedConcatenation(const std::vector<SizeVec>& runs) {
  SizeVec result;
  for (const auto& run : runs) {
    result.insert(result.end(), run.begin(), run.end());
  }
  ql::ranges::sort(result);
  return result;
}

// Return the concatenation of all `runs`.
template <typename T>
std::vector<T> concatenation(const std::vector<std::vector<T>>& runs) {
  std::vector<T> result;
  for (const auto& run : runs) {
    result.insert(result.end(), run.begin(), run.end());
  }
  return result;
}

// Return the keys (that is, the first components) of the `pairs`.
SizeVec getKeys(const std::vector<Pair>& pairs) {
  SizeVec result;
  for (const auto& pair : pairs) {
    result.push_back(pair.first);
  }
  return result;
}

// The number of runs and the number of elements per run of
// `makeTiedPairRuns()`.
constexpr size_t numTiedRuns = 16;
constexpr size_t numTiedElementsPerRun = 500;

// Return sorted runs of pairs in which many keys occur in several runs, so that
// there are a lot of ties. The second component is unique and therefore makes
// the tie order visible.
std::vector<std::vector<Pair>> makeTiedPairRuns() {
  std::vector<std::vector<Pair>> runs;
  for (size_t run = 0; run < numTiedRuns; ++run) {
    std::vector<Pair> elements;
    for (size_t i = 0; i < numTiedElementsPerRun; ++i) {
      elements.emplace_back(i % 37, run * numTiedElementsPerRun + i);
    }
    ql::ranges::stable_sort(elements, ComparePairs{});
    runs.push_back(std::move(elements));
  }
  return runs;
}
}  // namespace

// _____________________________________________________________________________
TEST(ParallelBlockMerge, binaryMerge) {
  SizeVec v1{1, 3, 5};
  SizeVec v2{2, 4, 6};
  MergeOptions options = optionsWithBlockSize(3);
  auto result = mergeToVector(SizeRuns{{v1, v2}, 3}, std::less<>{}, options);
  EXPECT_THAT(result, ::testing::ElementsAre(1u, 2u, 3u, 4u, 5u, 6u));

  v2 = SizeVec{};
  result = mergeToVector(SizeRuns{{v1, v2}, 3}, std::less<>{}, options);
  EXPECT_THAT(result, ::testing::ElementsAre(1u, 3u, 5u));

  std::swap(v1, v2);
  result = mergeToVector(SizeRuns{{v1, v2}, 3}, std::less<>{}, options);
  EXPECT_THAT(result, ::testing::ElementsAre(1u, 3u, 5u));

  // A merge of zero runs yields nothing.
  EXPECT_THAT(mergeToVector(SizeRuns{{}, 3}, std::less<>{}, options),
              ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, moveOfElements) {
  using V = std::vector<std::string>;
  V v1{"alphaalpha", "deltadelta"};
  V v2{"betabeta", "epsilonepsilon"};
  auto makeInput = [&v1, &v2](bool moveElements) {
    return StringRuns{
        {StringRun{v1.begin(), v1.end()}, StringRun{v2.begin(), v2.end()}},
        3,
        moveElements};
  };
  MergeOptions options = optionsWithBlockSize(3);

  auto result = mergeToVector<false>(makeInput(false), std::less<>{}, options);
  EXPECT_THAT(result, ::testing::ElementsAre("alphaalpha", "betabeta",
                                             "deltadelta", "epsilonepsilon"));
  // The strings weren't moved.
  EXPECT_THAT(v1, ::testing::ElementsAre("alphaalpha", "deltadelta"));
  EXPECT_THAT(v2, ::testing::ElementsAre("betabeta", "epsilonepsilon"));

  result = mergeToVector<true>(makeInput(true), std::less<>{}, options);
  EXPECT_THAT(result, ::testing::ElementsAre("alphaalpha", "betabeta",
                                             "deltadelta", "epsilonepsilon"));
  // The strings were moved out. NOTE: This is an intentional behavior change
  // with respect to the previous `parallelMultiwayMerge`: a block-based input
  // policy never resizes the underlying ranges, it only moves the individual
  // elements out of them, so the vectors still have their original size.
  EXPECT_THAT(v1, ::testing::Each(::testing::IsEmpty()));
  EXPECT_THAT(v2, ::testing::Each(::testing::IsEmpty()));
  EXPECT_EQ(v1.size(), 2u);
  EXPECT_EQ(v2.size(), 2u);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, moveOfElementsWithSeveralChunks) {
  static constexpr size_t numElementsPerRun = 40;
  std::vector<KeyAndPayload> run0;
  std::vector<KeyAndPayload> run1;
  for (size_t i = 0; i < numElementsPerRun; ++i) {
    // The two runs interleave, so that both of them contribute to (almost)
    // every chunk.
    run0.emplace_back(2 * i, "payloadpayloadpayload" + std::to_string(i));
    run1.emplace_back(2 * i + 1, "payloadpayloadpayload" + std::to_string(i));
  }
  // NOTE: The virtual block size is one, such that no input block is shared
  // between two chunks. `VectorRunsInput` hands out blocks that alias the
  // underlying vectors, so a chunk that reads a block from which a neighboring
  // chunk has already moved the elements out would see an empty payload. This
  // is a property of this test-only input policy; a realistic policy returns a
  // freshly decompressed block.
  KeyAndPayloadRuns input{{KeyAndPayloadRun{run0.begin(), run0.end()},
                           KeyAndPayloadRun{run1.begin(), run1.end()}},
                          1,
                          true};
  auto result = mergeToVector<true>(std::move(input), CompareKeys{},
                                    optionsWithBlockSize(3), 8);
  ASSERT_EQ(result.size(), 2 * numElementsPerRun);
  for (size_t i = 0; i < result.size(); ++i) {
    EXPECT_EQ(result.at(i).first, i);
    EXPECT_EQ(result.at(i).second,
              "payloadpayloadpayload" + std::to_string(i / 2));
  }
  // The payloads were moved out of both runs, while their keys are untouched.
  using ::testing::Each;
  using ::testing::Field;
  using ::testing::IsEmpty;
  EXPECT_THAT(run0, Each(Field(&KeyAndPayload::second, IsEmpty())));
  EXPECT_THAT(run1, Each(Field(&KeyAndPayload::second, IsEmpty())));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, randomInputs) {
  auto testRandomInts = [](size_t blockSize, size_t numRuns, size_t minSize,
                           size_t maxSize) {
    auto runs = makeRandomRuns(numRuns, minSize, maxSize);
    auto expected = sortedConcatenation(runs);
    auto result = mergeToVector(SizeRuns{runs, blockSize}, std::less<>{},
                                optionsWithBlockSize(blockSize), 8);
    EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
  };
  testRandomInts(12, 2000, 20, 50);
  testRandomInts(13, 1, 40, 40);
  testRandomInts(5, 2, 40, 50);
  testRandomInts(1, 3, 30, 50);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, totalNumElements) {
  EXPECT_EQ(detail::totalNumElements(SizeRuns{{}, 7}), 0u);
  std::vector<SizeVec> emptyRuns{SizeVec{}, SizeVec{}};
  EXPECT_EQ(detail::totalNumElements(SizeRuns{emptyRuns, 7}), 0u);
  // Blocks that are not completely filled are counted correctly, so the block
  // size must not make a difference.
  auto runs = makeRandomRuns(5, 30, 70);
  size_t expected = sortedConcatenation(runs).size();
  for (size_t blockSize : {1u, 3u, 64u, 1000u}) {
    EXPECT_EQ(detail::totalNumElements(SizeRuns{runs, blockSize}), expected);
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, splittersAreStrictlyIncreasing) {
  auto runs = makeRandomRuns(20, 100, 200);
  SizeRuns input{runs, 7};
  for (size_t numChunks : {2u, 3u, 8u, 64u}) {
    auto keys = computeSplitters(input, std::less<>{}, numChunks).keys();
    EXPECT_LE(keys.size(), numChunks - 1);
    for (size_t i = 1; i < keys.size(); ++i) {
      EXPECT_LT(keys[i - 1], keys[i]);
    }
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, splitterEdgeCases) {
  // All keys are equal, so there is no way to split the input.
  {
    std::vector<SizeVec> runs{SizeVec(100, 42u), SizeVec(100, 42u)};
    EXPECT_THAT(computeSplitters(SizeRuns{runs, 7}, std::less<>{}, 8).keys(),
                ::testing::IsEmpty());
  }
  // Zero runs, and only empty runs.
  {
    EXPECT_THAT(computeSplitters(SizeRuns{{}, 7}, std::less<>{}, 8).keys(),
                ::testing::IsEmpty());
    std::vector<SizeVec> runs{SizeVec{}, SizeVec{}};
    EXPECT_THAT(computeSplitters(SizeRuns{runs, 7}, std::less<>{}, 8).keys(),
                ::testing::IsEmpty());
  }
  // A single chunk requires no splitters at all.
  {
    auto runs = makeRandomRuns(4, 50, 50);
    EXPECT_THAT(computeSplitters(SizeRuns{runs, 7}, std::less<>{}, 1).keys(),
                ::testing::IsEmpty());
    EXPECT_THAT(computeSplitters(SizeRuns{runs, 7}, std::less<>{}, 0).keys(),
                ::testing::IsEmpty());
  }
  // More chunks than blocks: the number of splitters is bounded by the number
  // of distinct block keys.
  {
    std::vector<SizeVec> runs{SizeVec{1, 2, 3, 4, 5, 6}};
    auto keys = computeSplitters(SizeRuns{runs, 2}, std::less<>{}, 100).keys();
    EXPECT_LE(keys.size(), 3u);
    for (size_t i = 1; i < keys.size(); ++i) {
      EXPECT_LT(keys[i - 1], keys[i]);
    }
  }
  // One huge run and many tiny ones. The splitters have to follow the huge run,
  // and the merge still has to be correct.
  {
    std::vector<SizeVec> runs;
    SizeVec huge(10000);
    ql::ranges::generate(huge, [i = size_t{0}]() mutable { return i++; });
    runs.push_back(std::move(huge));
    for (size_t i = 0; i < 50; ++i) {
      runs.push_back(SizeVec{i, i + 1});
    }
    SizeRuns input{runs, 64};
    auto keys = computeSplitters(input, std::less<>{}, 8).keys();
    EXPECT_FALSE(keys.empty());
    EXPECT_LE(keys.size(), 7u);
    // The splitters are spread over the whole range of the huge run and are not
    // all crammed into the range of the tiny ones.
    EXPECT_GT(keys.back(), 1000u);
    auto expected = sortedConcatenation(runs);
    auto result = mergeToVector(SizeRuns{runs, 64}, std::less<>{},
                                optionsWithBlockSize(128), 8);
    EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
  }
  // Runs that are empty are simply skipped.
  {
    std::vector<SizeVec> runs{SizeVec{}, SizeVec{1, 2, 3, 4}, SizeVec{},
                              SizeVec{0, 5}};
    auto result = mergeToVector(SizeRuns{runs, 2}, std::less<>{},
                                optionsWithBlockSize(2), 4);
    EXPECT_THAT(result, ::testing::ElementsAre(0u, 1u, 2u, 3u, 4u, 5u));
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, splittersFromExplicitChunkSizes) {
  // A single run with the keys `0 ... 99`, one element per block, so that the
  // splitters can be predicted exactly.
  SizeVec run(100);
  ql::ranges::generate(run, [i = size_t{0}]() mutable { return i++; });
  std::vector<SizeVec> runs{run};
  SizeRuns input{runs, 1};

  // NOTE: A splitter is the largest key that is still needed to reach a target,
  // so a chunk that is supposed to start after `n` elements starts at the key
  // `n - 1`. That is the same convention as for a uniform number of chunks, see
  // `computeSplitters`, and it is why the chunk sizes below are only exact up
  // to that single element.

  // Uniform chunks of 25 elements each, so the chunks start after `25`, `50`
  // and `75` elements.
  {
    auto keys =
        computeSplitters(input, std::less<>{}, ChunkSizes{{}, 25}).keys();
    EXPECT_THAT(keys, ::testing::ElementsAre(24u, 49u, 74u));
  }
  // Three small leading chunks, then chunks of 40, so the chunks start after
  // `5`, `10`, `20` and `60` elements.
  {
    auto keys =
        computeSplitters(input, std::less<>{}, ChunkSizes{{5, 5, 10}, 40})
            .keys();
    EXPECT_THAT(keys, ::testing::ElementsAre(4u, 9u, 19u, 59u));
  }
  // The leading sizes cover the whole input exactly, so no chunk is left for
  // `remainingChunkSize_`.
  {
    auto keys =
        computeSplitters(input, std::less<>{}, ChunkSizes{{50, 50}, 10}).keys();
    EXPECT_THAT(keys, ::testing::ElementsAre(49u));
  }
  // The leading sizes exceed the input, so the surplus ones are dropped.
  {
    auto keys =
        computeSplitters(input, std::less<>{}, ChunkSizes{{30, 500, 7}, 10})
            .keys();
    EXPECT_THAT(keys, ::testing::ElementsAre(29u));
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, splittersFromChunkSizesEdgeCases) {
  auto runs = makeRandomRuns(4, 50, 50);
  SizeRuns input{runs, 7};

  // A chunk that is at least as large as the whole input needs no splitters.
  {
    EXPECT_THAT(
        computeSplitters(input, std::less<>{}, ChunkSizes{{}, 1000}).keys(),
        ::testing::IsEmpty());
    EXPECT_THAT(
        computeSplitters(input, std::less<>{}, ChunkSizes{{1000}, 10}).keys(),
        ::testing::IsEmpty());
  }
  // Zero runs, and only empty runs.
  {
    EXPECT_THAT(
        computeSplitters(SizeRuns{{}, 7}, std::less<>{}, ChunkSizes{{}, 4})
            .keys(),
        ::testing::IsEmpty());
    std::vector<SizeVec> emptyRuns{SizeVec{}, SizeVec{}};
    EXPECT_THAT(computeSplitters(SizeRuns{emptyRuns, 7}, std::less<>{},
                                 ChunkSizes{{}, 4})
                    .keys(),
                ::testing::IsEmpty());
  }
  // All keys are equal, so there is no way to split the input, no matter which
  // chunk sizes are requested.
  {
    std::vector<SizeVec> equalRuns{SizeVec(100, 42u), SizeVec(100, 42u)};
    EXPECT_THAT(computeSplitters(SizeRuns{equalRuns, 7}, std::less<>{},
                                 ChunkSizes{{2, 2}, 2})
                    .keys(),
                ::testing::IsEmpty());
  }
  // The splitters are strictly increasing, also for very small chunks.
  {
    auto keys =
        computeSplitters(input, std::less<>{}, ChunkSizes{{1, 2, 3}, 1}).keys();
    for (size_t i = 1; i < keys.size(); ++i) {
      EXPECT_LT(keys[i - 1], keys[i]);
    }
  }
  // A size of zero is illegal, because it would describe an empty chunk.
  {
    AD_EXPECT_THROW_WITH_MESSAGE(
        computeSplitters(input, std::less<>{}, ChunkSizes{{}, 0}),
        ::testing::HasSubstr("remainingChunkSize_ > 0"));
    AD_EXPECT_THROW_WITH_MESSAGE(
        computeSplitters(input, std::less<>{}, ChunkSizes{{4, 0, 4}, 4}),
        ::testing::HasSubstr("size > 0"));
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, splittersDescribeTheKeyRangeOfEveryChunk) {
  Splitters<size_t> splitters{SizeVec{10, 20}};
  ASSERT_EQ(splitters.numChunks(), 3u);
  // The lower bound of the first and the upper bound of the last chunk are
  // empty, that is minus and plus infinity.
  EXPECT_FALSE(splitters.getSplittersAt(0).lo_.has_value());
  EXPECT_EQ(splitters.getSplittersAt(0).hi_, 10u);
  EXPECT_EQ(splitters.getSplittersAt(1).lo_, 10u);
  EXPECT_EQ(splitters.getSplittersAt(1).hi_, 20u);
  EXPECT_EQ(splitters.getSplittersAt(2).lo_, 20u);
  EXPECT_FALSE(splitters.getSplittersAt(2).hi_.has_value());
  AD_EXPECT_THROW_WITH_MESSAGE(
      splitters.getSplittersAt(3),
      ::testing::HasSubstr("chunkIndex < numChunks()"));

  // An empty set of splitters describes a single chunk that covers everything.
  Splitters<size_t> single;
  EXPECT_EQ(single.numChunks(), 1u);
  EXPECT_FALSE(single.getSplittersAt(0).lo_.has_value());
  EXPECT_FALSE(single.getSplittersAt(0).hi_.has_value());
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, blockRangeForRun) {
  // Two runs with the blocks `[0, 1, 2]`, `[3, 4, 5]`, `[6, 7, 8]` and
  // `[10, 11, 12]`, plus an empty run.
  std::vector<SizeVec> runs{SizeVec{0, 1, 2, 3, 4, 5, 6, 7, 8},
                            SizeVec{10, 11, 12}, SizeVec{}};
  SizeRuns input{runs, 3};
  const std::less<> comparator;
  auto range = [&input, &comparator](std::optional<size_t> lo,
                                     std::optional<size_t> hi, size_t run) {
    auto result = detail::blockRangeForRun(
        input, comparator, Split<size_t>{std::move(lo), std::move(hi)}, run);
    return std::pair<size_t, size_t>{result.firstBlockIdx_,
                                     result.endBlockIdx_};
  };
  // Without any bounds, all blocks of the run are in the range.
  EXPECT_EQ(range(std::nullopt, std::nullopt, 0), Pair(0u, 3u));
  EXPECT_EQ(range(std::nullopt, std::nullopt, 1), Pair(0u, 1u));
  // An empty run contributes no block at all.
  EXPECT_EQ(range(std::nullopt, std::nullopt, 2), Pair(0u, 0u));
  EXPECT_TRUE(
      detail::blockRangeForRun(input, comparator, Split<size_t>{}, 2).empty());

  // The upper bound `3` is exactly the first key of the second block, so that
  // block is already outside of the range. This pins the exact form of the
  // predicate for the end of the range.
  EXPECT_EQ(range(std::nullopt, 3, 0), Pair(0u, 1u));
  // Symmetrically, the lower bound `3` is greater than the last key of the
  // first block, so that block is outside of the range.
  EXPECT_EQ(range(3, std::nullopt, 0), Pair(1u, 3u));
  // Bounds that lie inside a block keep exactly that block.
  EXPECT_EQ(range(4, 5, 0), Pair(1u, 2u));
  // A bound that lies between two blocks.
  EXPECT_EQ(range(2, 6, 0), Pair(0u, 2u));

  // Bounds that exclude the whole run yield an empty range.
  EXPECT_TRUE(detail::blockRangeForRun(input, comparator,
                                       Split<size_t>{100, std::nullopt}, 0)
                  .empty());
  EXPECT_TRUE(detail::blockRangeForRun(input, comparator,
                                       Split<size_t>{std::nullopt, 0}, 0)
                  .empty());
  // The second run lies completely above the key range `[0, 9)`.
  EXPECT_TRUE(
      detail::blockRangeForRun(input, comparator, Split<size_t>{0, 9}, 1)
          .empty());
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunkBoundaryPredicatesDoNotReadSuperfluousBlocks) {
  // A single run with the three blocks `[0, 1, 2]`, `[3, 4, 5]`, `[6, 7, 8]`.
  SizeVec run{0, 1, 2, 3, 4, 5, 6, 7, 8};
  const std::less<> comparator;
  MergeOptions options = optionsWithBlockSize(100);

  // The upper bound `3` is exactly the first key of the second block, so that
  // block must not be read. This pins the exact form of the predicate for the
  // end of the block range.
  {
    InstrumentedInput input{SizeRuns{{run}, 3}};
    detail::ChunkMerger<false, InstrumentedInput, std::less<>> merger{
        &input, &comparator, options, Split<size_t>{std::nullopt, 3}, nullptr};
    auto block = merger.get();
    ASSERT_TRUE(block.has_value());
    EXPECT_THAT(block.value(), ::testing::ElementsAre(0u, 1u, 2u));
    EXPECT_FALSE(merger.get().has_value());
    EXPECT_THAT(input.state_->readBlocks_,
                ::testing::ElementsAre(::testing::Pair(0u, 0u)));
  }
  // Symmetrically, the lower bound `3` is greater than the last key of the
  // first block, so that block must not be read either.
  {
    InstrumentedInput input{SizeRuns{{run}, 3}};
    detail::ChunkMerger<false, InstrumentedInput, std::less<>> merger{
        &input, &comparator, options, Split<size_t>{3, std::nullopt}, nullptr};
    auto block = merger.get();
    ASSERT_TRUE(block.has_value());
    EXPECT_THAT(block.value(), ::testing::ElementsAre(3u, 4u, 5u, 6u, 7u, 8u));
    EXPECT_FALSE(merger.get().has_value());
    EXPECT_THAT(input.state_->readBlocks_,
                ::testing::ElementsAre(::testing::Pair(0u, 1u),
                                       ::testing::Pair(0u, 2u)));
  }
  // A bound that lies inside a block trims that block, and only that block is
  // read.
  {
    InstrumentedInput input{SizeRuns{{run}, 3}};
    detail::ChunkMerger<false, InstrumentedInput, std::less<>> merger{
        &input, &comparator, options, Split<size_t>{4, 5}, nullptr};
    auto block = merger.get();
    ASSERT_TRUE(block.has_value());
    EXPECT_THAT(block.value(), ::testing::ElementsAre(4u));
    EXPECT_FALSE(merger.get().has_value());
    EXPECT_THAT(input.state_->readBlocks_,
                ::testing::ElementsAre(::testing::Pair(0u, 1u)));
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, resultIsIndependentOfTheChunking) {
  // The chunks partition the key range, so merging them one after the other has
  // to yield exactly the same elements as a single chunk that covers
  // everything. This is the property that a merge which distributes the chunks
  // over several threads relies on.
  static constexpr size_t numRuns = 16;
  static constexpr size_t numElementsPerRun = 500;
  std::vector<std::vector<Pair>> distinctRuns;
  for (size_t run = 0; run < numRuns; ++run) {
    std::vector<Pair> elements;
    for (size_t i = 0; i < numElementsPerRun; ++i) {
      // The keys are pairwise distinct across all runs.
      elements.emplace_back(i * numRuns + run, run);
    }
    distinctRuns.push_back(std::move(elements));
  }
  auto mergeDistinct = [&distinctRuns](size_t numChunks) {
    return mergeToVector(PairRuns{distinctRuns, 32}, ComparePairs{},
                         optionsWithBlockSize(64), numChunks);
  };
  auto singleChunk = mergeDistinct(1);
  ASSERT_EQ(singleChunk.size(), numRuns * numElementsPerRun);
  EXPECT_TRUE(ql::ranges::is_sorted(singleChunk, ComparePairs{}));
  for (size_t numChunks : {2u, 8u, 64u, 10000u}) {
    EXPECT_THAT(mergeDistinct(numChunks),
                ::testing::ElementsAreArray(singleChunk));
  }

  // With ties, the relative order of the tied elements is deliberately *not*
  // specified and may well depend on the chunking, because a chunk only sets up
  // cursors for those runs that actually contribute to it. The keys are still
  // in sorted order, and no element is ever lost or duplicated.
  auto tiedRuns = makeTiedPairRuns();
  auto expected = concatenation(tiedRuns);
  ql::ranges::stable_sort(expected, ComparePairs{});
  auto sortedExpected = expected;
  ql::ranges::sort(sortedExpected);
  for (size_t numChunks : {1u, 2u, 8u, 64u}) {
    auto result = mergeToVector(PairRuns{tiedRuns, 32}, ComparePairs{},
                                optionsWithBlockSize(64), numChunks);
    EXPECT_TRUE(ql::ranges::is_sorted(result, ComparePairs{}));
    EXPECT_THAT(getKeys(result),
                ::testing::ElementsAreArray(getKeys(expected)));
    auto sortedResult = result;
    ql::ranges::sort(sortedResult);
    EXPECT_THAT(sortedResult, ::testing::ElementsAreArray(sortedExpected));
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunksWithoutAnyElementAreSkipped) {
  // A chunk that contains no element at all is perfectly legal and simply
  // yields no output block. The splitters below cannot be produced by
  // `computeSplitters` (which only ever picks keys that actually occur in the
  // data), so they are handed to the merge directly.
  std::vector<SizeVec> runs;
  for (size_t run = 0; run < 4; ++run) {
    SizeVec elements;
    for (size_t i = 0; i < 40; ++i) {
      elements.push_back(100 + run + 4 * i);
    }
    runs.push_back(std::move(elements));
  }
  auto expected = sortedConcatenation(runs);
  auto mergeWithSplitters = [&runs, &expected](SizeVec splitterKeys) {
    auto blocks = serialBlockMergeToRange<false>(
        SizeRuns{runs, 8}, std::less<>{}, optionsWithBlockSize(8), nullptr,
        Splitters<size_t>{std::move(splitterKeys)});
    SizeVec result;
    for (const auto& block : blocks) {
      EXPECT_FALSE(block.empty());
      result.insert(result.end(), block.begin(), block.end());
    }
    EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
  };
  // All elements are greater than `100`, so the first five chunks are empty.
  mergeWithSplitters(SizeVec{1, 2, 3, 4, 5});
  // Empty chunks at the beginning, in the middle, and at the end.
  mergeWithSplitters(SizeVec{1, 2, 1000, 2000, 3000});
  // A single element per chunk, and far more chunks than elements.
  SizeVec manySplitters;
  for (size_t i = 0; i < 300; ++i) {
    manySplitters.push_back(i);
  }
  mergeWithSplitters(std::move(manySplitters));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, exceptionFromChunkPropagates) {
  auto runs = makeRandomRuns(16, 200, 300);
  InstrumentedInput input{SizeRuns{runs, 16}};
  // Throw from the third call to `readBlock` onwards, so that the merge has
  // already yielded some output blocks when the exception arrives.
  input.state_->throwAtRead_ = 3;
  AD_EXPECT_THROW_WITH_MESSAGE(
      mergeToVector(input, std::less<>{}, optionsWithBlockSize(16), 4),
      ::testing::HasSubstr("readBlock failed"));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, cancellation) {
  auto runs = makeRandomRuns(16, 200, 300);
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  handle->cancel(ad_utility::CancellationState::MANUAL);
  for (size_t numChunks : {1u, 4u}) {
    EXPECT_THROW(mergeToVector(SizeRuns{runs, 16}, std::less<>{},
                               optionsWithBlockSize(16), numChunks, handle),
                 ad_utility::CancellationException);
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, outputBlockMemoryLimit) {
  auto runs = makeRandomRuns(4, 100, 100);
  MergeOptions options;
  // Three elements fit into a single output block, and the limit on the number
  // of elements is far away, so it is the memory that finishes a block.
  options.outputBlockSize = OutputBlockSize::both(
      1000, ad_utility::MemorySize::bytes(3 * sizeof(size_t)));
  size_t numElements = 0;
  auto blocks = serialBlockMergeToRange<false>(SizeRuns{runs, 16},
                                               std::less<>{}, options);
  for (const auto& block : blocks) {
    EXPECT_LE(block.size(), 3u);
    EXPECT_FALSE(block.empty());
    numElements += block.size();
  }
  EXPECT_EQ(numElements, 400u);
}
