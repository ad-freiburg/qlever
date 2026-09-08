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
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "util/CancellationHandle.h"
#include "util/Forward.h"
#include "util/GTestHelpers.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ParallelBlockMergeTestHelpers.h"
#include "util/parallelBlockMerge/ParallelBlockMerge.h"

// The tests of the helpers from `MergeHelpers.h` (in particular of
// `computeChunkBoundaries`) live in `MergeHelpersTest.cpp`.
using namespace ad_utility::parallelBlockMerge;
using namespace parallelBlockMergeTestHelpers;

namespace {
static_assert(InputConcept<SizeInput>);
static_assert(InputConcept<VectorInput<Pair>>);

// A string that counts how often it was copied, so that a merge that moves the
// elements out of its input blocks is distinguishable from one that does not.
struct CountingString {
  std::string value_{};
  size_t numCopies_ = 0;

  CountingString() = default;
  explicit CountingString(std::string value) : value_{std::move(value)} {}
  CountingString(const CountingString& other)
      : value_{other.value_}, numCopies_{other.numCopies_ + 1} {}
  CountingString(CountingString&&) = default;
  CountingString& operator=(const CountingString& other) {
    value_ = other.value_;
    numCopies_ = other.numCopies_ + 1;
    return *this;
  }
  CountingString& operator=(CountingString&&) = default;
  ~CountingString() = default;

  bool operator<(const CountingString& other) const {
    return value_ < other.value_;
  }
};

// An element that counts how many instances of it are alive, so that a test can
// observe when the merge releases an input block.
struct LiveCounted {
  size_t value_ = 0;

  static size_t& numLive() {
    static size_t numLive = 0;
    return numLive;
  }

  explicit LiveCounted(size_t value) : value_{value} { ++numLive(); }
  LiveCounted(const LiveCounted& other) : value_{other.value_} { ++numLive(); }
  LiveCounted(LiveCounted&& other) noexcept : value_{other.value_} {
    ++numLive();
  }
  LiveCounted& operator=(const LiveCounted&) = default;
  LiveCounted& operator=(LiveCounted&&) = default;
  ~LiveCounted() { --numLive(); }

  bool operator<(const LiveCounted& other) const {
    return value_ < other.value_;
  }
};

// Return the sorted run of the `LiveCounted` elements `first ... last - 1`.
std::vector<LiveCounted> makeLiveCounted(size_t first, size_t last) {
  std::vector<LiveCounted> result;
  for (size_t i = first; i < last; ++i) {
    result.emplace_back(i);
  }
  return result;
}

// An input policy that wraps a `VectorInput<size_t>` and additionally records
// every call to `getBlock` and (optionally) throws from the `throwAtRead_`-th
// of them. The recorded state is shared between all copies, because the merge
// takes the input by value.
struct InstrumentedInput {
  using value_type = size_t;
  using Element = size_t;
  using Block = SizeVec;

  // The shared state of all copies of an `InstrumentedInput`.
  struct State {
    // NOTE: The `mutex_` is only there because `InputConcept` requires
    // `getBlock` to be thread-safe. A serial merge only ever reads from the
    // consuming thread.
    std::mutex mutex_{};
    std::vector<std::pair<size_t, size_t>> readBlocks_{};
    // The number of the call to `getBlock` that throws. The value `0` means
    // "never throw".
    size_t throwAtRead_ = 0;
  };

  SizeInput wrapped_;
  std::shared_ptr<State> state_ = std::make_shared<State>();

  size_t numRuns() const { return wrapped_.numRuns(); }
  size_t numBlocks(size_t runIdx) const { return wrapped_.numBlocks(runIdx); }
  size_t numElementsInBlock(size_t runIdx, size_t blockIdx) const {
    return wrapped_.numElementsInBlock(runIdx, blockIdx);
  }
  const Element& firstElement(size_t runIdx, size_t blockIdx) const {
    return wrapped_.firstElement(runIdx, blockIdx);
  }
  const Element& lastElement(size_t runIdx, size_t blockIdx) const {
    return wrapped_.lastElement(runIdx, blockIdx);
  }
  Block getBlock(size_t runIdx, size_t blockIdx) const {
    size_t numReads = 0;
    {
      std::lock_guard<std::mutex> lock{state_->mutex_};
      state_->readBlocks_.emplace_back(runIdx, blockIdx);
      numReads = state_->readBlocks_.size();
    }
    if (state_->throwAtRead_ != 0 && numReads >= state_->throwAtRead_) {
      throw std::runtime_error{"getBlock failed"};
    }
    return wrapped_.getBlock(runIdx, blockIdx);
  }
  Block makeEmptyBlock() const { return {}; }
  template <typename T>
  void appendToBlock(Block& block, T&& element) const {
    block.push_back(AD_FWD(element));
  }
  ad_utility::MemorySize memorySizeOfElement(
      [[maybe_unused]] const value_type& element) const {
    return ad_utility::MemorySize::bytes(sizeof(value_type));
  }
};

static_assert(InputConcept<InstrumentedInput>);

// Merge the `input` split into (at most) `numChunks` chunks and return the
// elements of all output blocks in a single vector.
template <bool moveElements = false, typename Input, typename Comparator>
std::vector<typename Input::value_type> mergeToVector(
    Input input, Comparator comparator, MergeOptions options = {},
    size_t numChunks = 1,
    ad_utility::SharedCancellationHandle cancellationHandle =
        std::make_shared<ad_utility::CancellationHandle<>>()) {
  auto boundaries = computeChunkBoundaries(input, comparator, numChunks);
  auto blocks = serialBlockMergeToRange<moveElements>(
      std::move(input), std::move(comparator), std::move(options),
      std::move(cancellationHandle), std::move(boundaries));
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

// Return the `values` as `CountingString`s, all of which have not been copied
// yet.
std::vector<CountingString> makeCountingStrings(
    const std::vector<std::string>& values) {
  std::vector<CountingString> result;
  for (const auto& value : values) {
    result.emplace_back(value);
  }
  return result;
}

// Create the `MergeState` that a single `detail::ChunkMerger` needs, with
// exactly the given chunk `boundary`.
template <typename Input, typename Comparator>
auto makeSingleChunkState(Input input, Comparator comparator,
                          MergeOptions options,
                          ChunkBoundary<typename Input::Element> boundary) {
  using State = detail::MergeState<Input, Comparator>;
  return std::make_shared<const State>(
      std::move(input), std::move(comparator), std::move(options),
      std::make_shared<ad_utility::CancellationHandle<>>(),
      std::vector<ChunkBoundary<typename Input::Element>>{std::move(boundary)});
}
}  // namespace

// _____________________________________________________________________________
TEST(ParallelBlockMerge, binaryMerge) {
  std::vector<SizeVec> runs{SizeVec{1, 3, 5}, SizeVec{2, 4, 6}};
  auto result = mergeToVector(makeVectorInput(runs, 2), std::less<>{},
                              optionsWithBlockSize(2));
  EXPECT_THAT(result, ::testing::ElementsAre(1u, 2u, 3u, 4u, 5u, 6u));
  // A single run is simply passed through, and so is a single element.
  EXPECT_THAT(mergeToVector(makeVectorInput(std::vector<SizeVec>{{1, 3, 5}}, 2),
                            std::less<>{}, optionsWithBlockSize(2)),
              ::testing::ElementsAre(1u, 3u, 5u));
  EXPECT_THAT(mergeToVector(makeVectorInput(std::vector<SizeVec>{{7}}, 2),
                            std::less<>{}, optionsWithBlockSize(2)),
              ::testing::ElementsAre(7u));
  // An input without any element yields no output block at all.
  EXPECT_THAT(mergeToVector(makeVectorInput(std::vector<SizeVec>{}, 2),
                            std::less<>{}, optionsWithBlockSize(2)),
              ::testing::IsEmpty());
  EXPECT_THAT(mergeToVector(makeVectorInput(std::vector<SizeVec>{{}, {}}, 2),
                            std::less<>{}, optionsWithBlockSize(2)),
              ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, moveOfElements) {
  std::vector<std::vector<CountingString>> runs{
      makeCountingStrings({"alphaalpha", "deltadelta", "gammagamma"}),
      makeCountingStrings({"betabeta", "epsilonepsilon"})};
  std::vector<std::string> expectedValues{
      "alphaalpha", "betabeta", "deltadelta", "epsilonepsilon", "gammagamma"};
  // Several chunks, one of which lies inside a block, so that two chunks read
  // the very same block.
  for (size_t numChunks : {1u, 3u}) {
    auto notMoved =
        mergeToVector<false>(makeVectorInput(runs, 2), std::less<>{},
                             optionsWithBlockSize(2), numChunks);
    auto moved = mergeToVector<true>(makeVectorInput(runs, 2), std::less<>{},
                                     optionsWithBlockSize(2), numChunks);
    ASSERT_EQ(moved.size(), expectedValues.size());
    ASSERT_EQ(notMoved.size(), expectedValues.size());
    for (size_t i = 0; i < expectedValues.size(); ++i) {
      EXPECT_EQ(moved.at(i).value_, expectedValues.at(i));
      EXPECT_EQ(notMoved.at(i).value_, expectedValues.at(i));
      // Moving the elements out of the input blocks saves a copy.
      EXPECT_LT(moved.at(i).numCopies_, notMoved.at(i).numCopies_);
    }
    // `getBlock` hands out a copy of a block, so the merge never touches the
    // input itself, no matter how the chunks are laid out. In particular, a
    // block that two neighboring chunks share is still intact for the second of
    // them.
    for (const auto& run : runs) {
      EXPECT_THAT(run, ::testing::Each(::testing::Field(
                           &CountingString::value_,
                           ::testing::Not(::testing::IsEmpty()))));
    }
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, blockOfAnExhaustedRunIsReleasedEarly) {
  // Run 0 is a single block of ten elements, run 1 consists of ten blocks of
  // ten elements, all of which are greater than those of run 0. With an output
  // block size of ten, the first output block exhausts run 0, and the merger
  // must release that run's block right away, instead of keeping it alive until
  // the whole chunk is merged.
  std::vector<std::vector<LiveCounted>> runs;
  runs.push_back(makeLiveCounted(0, 10));
  runs.push_back(makeLiveCounted(100, 200));
  // The `VectorInput` holds its own copy of the runs (`getBlock` then copies
  // again), so the baseline is taken after it has been created.
  auto input = makeVectorInput(runs, 10);
  const size_t numLiveBefore = LiveCounted::numLive();
  ASSERT_EQ(numLiveBefore, 220u);

  auto blocks = serialBlockMergeToRange<false>(std::move(input), std::less<>{},
                                               optionsWithBlockSize(10));
  auto it = blocks.begin();
  ASSERT_NE(it, blocks.end());
  // The first output block holds the ten elements of run 0. Alive beyond the
  // baseline are exactly that output block and the current block of run 1, but
  // not the (exhausted) block of run 0 any more.
  auto firstBlock = std::move(*it);
  ASSERT_EQ(firstBlock.size(), 10u);
  EXPECT_EQ(firstBlock.front().value_, 0u);
  EXPECT_EQ(LiveCounted::numLive(), numLiveBefore + 10 + 10);
  ++it;
  ASSERT_NE(it, blocks.end());
  EXPECT_EQ((*it).front().value_, 100u);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, randomInputs) {
  auto testRandomInts = [](size_t blockSize, size_t numRuns, size_t minSize,
                           size_t maxSize) {
    auto runs = makeRandomRuns(numRuns, minSize, maxSize);
    auto expected = sortedConcatenation(runs);
    auto result = mergeToVector(makeVectorInput(runs, blockSize), std::less<>{},
                                optionsWithBlockSize(blockSize), 8);
    EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
  };
  testRandomInts(12, 2000, 20, 50);
  testRandomInts(13, 1, 40, 40);
  testRandomInts(5, 2, 40, 50);
  testRandomInts(1, 3, 30, 50);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, blocksWithEqualLastElements) {
  // Four identical runs in which every distinct element is the last element of
  // four different blocks, see `runsWithEqualLastElements`. The merge has to
  // yield the same result no matter how many chunks are requested.
  auto runs = runsWithEqualLastElements();
  auto expected = sortedConcatenation(runs);
  for (size_t numChunks : {1u, 2u, 4u, 100u}) {
    EXPECT_THAT(mergeToVector(makeVectorInput(runs, 10), std::less<>{},
                              optionsWithBlockSize(7), numChunks),
                ::testing::ElementsAreArray(expected));
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, edgeCaseInputs) {
  // All elements are equal, so there is no way to actually split the input.
  {
    auto runs = runsWithEqualElements();
    EXPECT_THAT(mergeToVector(makeVectorInput(runs, 7), std::less<>{},
                              optionsWithBlockSize(7), 8),
                ::testing::ElementsAreArray(SizeVec(200, 42u)));
  }
  // One huge run and many tiny ones, the elements of which all lie at the very
  // beginning of the huge one.
  {
    auto runs = oneHugeAndManyTinyRuns();
    auto expected = sortedConcatenation(runs);
    EXPECT_THAT(mergeToVector(makeVectorInput(runs, 64), std::less<>{},
                              optionsWithBlockSize(128), 8),
                ::testing::ElementsAreArray(expected));
  }
  // Runs that are empty are simply skipped.
  {
    std::vector<SizeVec> runs{SizeVec{}, SizeVec{1, 2, 3, 4}, SizeVec{},
                              SizeVec{0, 5}};
    EXPECT_THAT(mergeToVector(makeVectorInput(runs, 2), std::less<>{},
                              optionsWithBlockSize(2), 4),
                ::testing::ElementsAre(0u, 1u, 2u, 3u, 4u, 5u));
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunkBoundaryPredicatesDoNotReadSuperfluousBlocks) {
  // A single run with the three blocks `[0, 1, 2]`, `[3, 4, 5]`, `[6, 7, 8]`.
  std::vector<SizeVec> runs{SizeVec{0, 1, 2, 3, 4, 5, 6, 7, 8}};
  MergeOptions options = optionsWithBlockSize(100);
  using Merger = detail::ChunkMerger<false, InstrumentedInput, std::less<>>;

  auto testChunk = [&runs, &options](ChunkBoundary<size_t> boundary,
                                     const SizeVec& expectedElements,
                                     const std::vector<Pair>& expectedReads) {
    InstrumentedInput input{makeVectorInput(runs, 3)};
    auto state = makeSingleChunkState(input, std::less<>{}, options,
                                      std::move(boundary));
    Merger merger{state, 0};
    auto block = merger.get();
    ASSERT_TRUE(block.has_value());
    EXPECT_THAT(block.value(), ::testing::ElementsAreArray(expectedElements));
    EXPECT_FALSE(merger.get().has_value());
    EXPECT_THAT(input.state_->readBlocks_,
                ::testing::ElementsAreArray(expectedReads));
  };

  // The upper bound `3` is exactly the first element of the second block, so
  // that block must not be read. This pins the exact form of the predicate for
  // the end of the block range.
  testChunk(ChunkBoundary<size_t>{std::nullopt, 3}, SizeVec{0, 1, 2},
            {Pair(0u, 0u)});
  // Symmetrically, the lower bound `3` is greater than the last element of the
  // first block, so that block must not be read either.
  testChunk(ChunkBoundary<size_t>{3, std::nullopt}, SizeVec{3, 4, 5, 6, 7, 8},
            {Pair(0u, 1u), Pair(0u, 2u)});
  // A bound that lies inside a block trims that block, and only that block is
  // read.
  testChunk(ChunkBoundary<size_t>{4, 5}, SizeVec{4}, {Pair(0u, 1u)});
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, resultIsIndependentOfTheChunking) {
  // The chunks partition the range of elements, so merging them one after the
  // other has to yield exactly the same elements as a single chunk that covers
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
    return mergeToVector(makeVectorInput(distinctRuns, 32), ComparePairs{},
                         optionsWithBlockSize(64), numChunks);
  };
  auto singleChunkResult = mergeDistinct(1);
  ASSERT_EQ(singleChunkResult.size(), numRuns * numElementsPerRun);
  EXPECT_TRUE(ql::ranges::is_sorted(singleChunkResult, ComparePairs{}));
  for (size_t numChunks : {2u, 8u, 64u, 10000u}) {
    EXPECT_THAT(mergeDistinct(numChunks),
                ::testing::ElementsAreArray(singleChunkResult));
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
    auto result = mergeToVector(makeVectorInput(tiedRuns, 32), ComparePairs{},
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
  // yields no output block. The split points below cannot be produced by
  // `computeChunkBoundaries` (which only ever picks elements that actually
  // occur in the data), so they are handed to the merge directly.
  std::vector<SizeVec> runs;
  for (size_t run = 0; run < 4; ++run) {
    SizeVec elements;
    for (size_t i = 0; i < 40; ++i) {
      elements.push_back(100 + run + 4 * i);
    }
    runs.push_back(std::move(elements));
  }
  auto expected = sortedConcatenation(runs);
  auto mergeWithSplitPoints = [&runs, &expected](SizeVec splitPoints) {
    auto blocks = serialBlockMergeToRange<false>(
        makeVectorInput(runs, 8), std::less<>{}, optionsWithBlockSize(8),
        std::make_shared<ad_utility::CancellationHandle<>>(),
        detail::chunkBoundariesFromSplitPoints(splitPoints));
    SizeVec result;
    for (const auto& block : blocks) {
      EXPECT_FALSE(block.empty());
      result.insert(result.end(), block.begin(), block.end());
    }
    EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
  };
  // All elements are greater than `100`, so the first five chunks are empty.
  mergeWithSplitPoints(SizeVec{1, 2, 3, 4, 5});
  // Empty chunks at the beginning, in the middle, and at the end.
  mergeWithSplitPoints(SizeVec{1, 2, 1000, 2000, 3000});
  // A single element per chunk, and far more chunks than elements.
  SizeVec manySplitPoints;
  for (size_t i = 0; i < 300; ++i) {
    manySplitPoints.push_back(i);
  }
  mergeWithSplitPoints(std::move(manySplitPoints));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, exceptionFromChunkPropagates) {
  auto runs = makeRandomRuns(16, 200, 300);
  InstrumentedInput input{makeVectorInput(runs, 16)};
  // Throw from the third call to `getBlock` onwards, so that the merge has
  // already yielded some output blocks when the exception arrives.
  input.state_->throwAtRead_ = 3;
  AD_EXPECT_THROW_WITH_MESSAGE(
      mergeToVector(input, std::less<>{}, optionsWithBlockSize(16), 4),
      ::testing::HasSubstr("getBlock failed"));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, cancellation) {
  auto runs = makeRandomRuns(16, 200, 300);
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  handle->cancel(ad_utility::CancellationState::MANUAL);
  for (size_t numChunks : {1u, 4u}) {
    EXPECT_THROW(mergeToVector(makeVectorInput(runs, 16), std::less<>{},
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
  auto blocks = serialBlockMergeToRange<false>(makeVectorInput(runs, 16),
                                               std::less<>{}, options);
  for (const auto& block : blocks) {
    EXPECT_LE(block.size(), 3u);
    EXPECT_FALSE(block.empty());
    numElements += block.size();
  }
  EXPECT_EQ(numElements, 400u);
}
