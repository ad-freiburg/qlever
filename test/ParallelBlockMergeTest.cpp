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

#include <atomic>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/asio.h"
#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ParallelBlockMergeTestHelpers.h"
#include "util/SourceLocation.h"
#include "util/parallelBlockMerge/ParallelBlockMerge.h"

// The tests of the helpers from `MergeHelpers.h` (in particular of
// `computeChunkBoundaries`) live in `MergeHelpersTest.cpp`.
using namespace ad_utility::parallelBlockMerge;
using namespace parallelBlockMergeTestHelpers;

namespace {
static_assert(InputConcept<SizeInput>);
static_assert(InputConcept<VectorInput<Pair>>);
static_assert(SinkConcept<CollectingBlockSink<SizeVec>, SizeVec>);
static_assert(
    SinkConcept<CollectingBlockSink<std::vector<Pair>>, std::vector<Pair>>);

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

// An input policy that derives from `SizeInput` (that is, from
// `VectorInput<size_t>`) and instruments only `getBlock`: it records every call
// to it and (optionally) throws from the `throwAtRead_`-th of them. Everything
// else is inherited from the base class. The recorded state is shared between
// all copies, because the merge takes the input by value.
struct InstrumentedInput : public SizeInput {
  // The shared state of all copies of an `InstrumentedInput`.
  struct State {
    // NOTE: The `mutex_` is required because `InputConcept` requires `getBlock`
    // to be thread-safe. A serial merge only ever reads from the consuming
    // thread, but a parallel one reads from all of its threads.
    std::mutex mutex_{};
    std::vector<std::pair<size_t, size_t>> readBlocks_{};
    // The threads from which `getBlock` was called.
    std::set<std::thread::id> readingThreads_{};
    // The number of the call to `getBlock` that throws. The value `0` means
    // "never throw".
    size_t throwAtRead_ = 0;
    // How long a single `getBlock` is held up, which makes concurrent reads
    // observable in `numOverlappingReads_`.
    std::chrono::milliseconds delayPerRead_{0};
    // The number of `getBlock` calls that are currently in flight, and the
    // number of calls that found at least one other call in flight.
    std::atomic<size_t> numConcurrentReads_{0};
    std::atomic<size_t> numOverlappingReads_{0};
  };

  std::shared_ptr<State> state_ = std::make_shared<State>();

  Block getBlock(size_t runIdx, size_t blockIdx) const {
    size_t numReads = 0;
    {
      std::lock_guard<std::mutex> lock{state_->mutex_};
      state_->readBlocks_.emplace_back(runIdx, blockIdx);
      state_->readingThreads_.insert(std::this_thread::get_id());
      numReads = state_->readBlocks_.size();
    }
    if (++state_->numConcurrentReads_ > 1) {
      ++state_->numOverlappingReads_;
    }
    if (state_->delayPerRead_.count() > 0) {
      std::this_thread::sleep_for(state_->delayPerRead_);
    }
    --state_->numConcurrentReads_;
    if (state_->throwAtRead_ != 0 && numReads >= state_->throwAtRead_) {
      throw std::runtime_error{"getBlock failed"};
    }
    return SizeInput::getBlock(runIdx, blockIdx);
  }
};

static_assert(InputConcept<InstrumentedInput>);

// Return the elements of all `blocks` in a single vector, and check that no
// block is empty. The elements are moved out of the `blocks`, which every call
// site may do because each of them owns its blocks.
template <typename Blocks>
std::vector<ql::ranges::range_value_t<ql::ranges::range_value_t<Blocks>>>
collectBlocks(Blocks&& blocks) {
  std::vector<ql::ranges::range_value_t<ql::ranges::range_value_t<Blocks>>>
      result;
  for (auto& block : blocks) {
    // An output block is never empty, no matter how the chunks are laid out.
    EXPECT_FALSE(block.empty());
    for (auto& element : block) {
      result.push_back(std::move(element));
    }
  }
  return result;
}

// Merge the `input` split into (at most) `numChunks` chunks and return the
// elements of all output blocks in a single vector.
template <bool moveElements = false, typename Input, typename Comparator>
std::vector<typename Input::value_type> mergeToVector(
    Input input, Comparator comparator, MergeOptions options = {},
    size_t numChunks = 1,
    ad_utility::SharedCancellationHandle cancellationHandle =
        detail::freshCancellationHandle()) {
  auto boundaries = computeChunkBoundaries(input, comparator, numChunks);
  auto blocks = serialBlockMergeToRange<moveElements>(
      std::move(input), std::move(comparator), std::move(options),
      std::move(cancellationHandle), std::move(boundaries));
  return collectBlocks(blocks);
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

// The number of runs and the number of elements per run of
// `makeDistinctPairRuns()`.
constexpr size_t numDistinctRuns = 16;
constexpr size_t numDistinctElementsPerRun = 500;

// Return sorted runs of pairs the keys of which are pairwise distinct across
// all runs, so that there is no tie at all and the result of a merge is
// therefore completely determined by the input.
std::vector<std::vector<Pair>> makeDistinctPairRuns() {
  std::vector<std::vector<Pair>> runs;
  for (size_t run = 0; run < numDistinctRuns; ++run) {
    std::vector<Pair> elements;
    for (size_t i = 0; i < numDistinctElementsPerRun; ++i) {
      // The keys are pairwise distinct across all runs.
      elements.emplace_back(i * numDistinctRuns + run, run);
    }
    runs.push_back(std::move(elements));
  }
  return runs;
}

// Check the invariants that the `result` of a merge of the `tiedRuns` has to
// satisfy no matter how the chunks are laid out: the keys of the `result` are
// exactly the keys of the stably sorted concatenation of the `tiedRuns` and in
// that very order, and the `result` consists of exactly the same elements (with
// the same multiplicities). The relative order of the tied elements themselves
// is deliberately *not* specified, so it is not checked here.
void expectSameKeysAndMultiset(
    const std::vector<Pair>& result,
    const std::vector<std::vector<Pair>>& tiedRuns,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  auto expected = concatenation(tiedRuns);
  ql::ranges::stable_sort(expected, ComparePairs{});
  EXPECT_THAT(getKeys(result), ::testing::ElementsAreArray(getKeys(expected)));
  auto sortedResult = result;
  ql::ranges::sort(sortedResult);
  ql::ranges::sort(expected);
  EXPECT_THAT(sortedResult, ::testing::ElementsAreArray(expected));
}

// Return four sorted runs of 40 elements each which together are exactly the
// elements `100 ... 259`, distributed over the runs in a round-robin fashion,
// so that all four runs contribute to (almost) every chunk.
std::vector<SizeVec> runsWithInterleavedElements() {
  std::vector<SizeVec> runs;
  for (size_t run = 0; run < 4; ++run) {
    SizeVec elements;
    for (size_t i = 0; i < 40; ++i) {
      elements.push_back(100 + run + 4 * i);
    }
    runs.push_back(std::move(elements));
  }
  return runs;
}

// Return the split points `0 ... numSplitPoints - 1`, which describe one chunk
// per element of the range `[0, numSplitPoints)` plus a single chunk that holds
// everything that is greater.
SizeVec splitPointsUpTo(size_t numSplitPoints) {
  SizeVec result;
  for (size_t i = 0; i < numSplitPoints; ++i) {
    result.push_back(i);
  }
  return result;
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

// Create the `MergeState` that the `detail::ChunkMerger`s of a merge share (see
// `detail::MergeState`), with exactly the given chunk `boundaries` and with a
// fresh cancellation handle.
template <typename Input, typename Comparator>
auto makeMergeState(
    Input input, Comparator comparator, MergeOptions options,
    std::vector<ChunkBoundary<typename Input::Element>> boundaries) {
  using State = detail::MergeState<Input, Comparator>;
  return std::make_shared<const State>(
      std::move(input), std::move(comparator), std::move(options),
      detail::freshCancellationHandle(), std::move(boundaries));
}

// Create the `MergeState` that a single `detail::ChunkMerger` needs, with
// exactly the given chunk `boundary`.
template <typename Input, typename Comparator>
auto makeSingleChunkState(Input input, Comparator comparator,
                          MergeOptions options,
                          ChunkBoundary<typename Input::Element> boundary) {
  return makeMergeState(
      std::move(input), std::move(comparator), std::move(options),
      std::vector<ChunkBoundary<typename Input::Element>>{std::move(boundary)});
}

// Check that a merge of random runs yields their sorted concatenation. The
// `mergeFn` performs the actual merge; it is called as `mergeFn(input,
// blockSize)` with an input of `numRuns` runs, the sizes of which are
// uniformly distributed in `[minSize, maxSize]`, split into blocks of
// `blockSize` elements. This way the very same check can be run for the serial
// and for the parallel merge.
template <typename MergeFn>
void expectSortedResult(
    MergeFn mergeFn, size_t blockSize, size_t numRuns, size_t minSize,
    size_t maxSize, ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  auto runs = makeRandomRuns(numRuns, minSize, maxSize);
  auto expected = sortedConcatenation(runs);
  auto result = mergeFn(makeVectorInput(runs, blockSize), blockSize);
  ASSERT_EQ(result.size(), expected.size());
  EXPECT_TRUE(ql::ranges::is_sorted(result));
  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}

// Check that an exception that the input of a merge throws is propagated to the
// caller of that merge. The input throws from its third call to `getBlock`
// onwards, so that the merge has already produced some output blocks when the
// exception arrives. The `mergeFn` performs the actual merge of the
// `InstrumentedInput` that it is called with.
template <typename MergeFn>
void expectExceptionPropagates(
    MergeFn mergeFn,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  auto runs = makeRandomRuns(16, 200, 300);
  InstrumentedInput input{makeVectorInput(runs, 16)};
  input.state_->throwAtRead_ = 3;
  AD_EXPECT_THROW_WITH_MESSAGE(mergeFn(input),
                               ::testing::HasSubstr("getBlock failed"));
}

// Check that a merge which is given an already cancelled cancellation handle
// throws a `CancellationException`. The `mergeFn` performs the actual merge; it
// is called as `mergeFn(input, cancellationHandle)`.
template <typename MergeFn>
void expectCancellationThrows(MergeFn mergeFn, ad_utility::source_location loc =
                                                   AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  auto runs = makeRandomRuns(16, 200, 300);
  auto handle = detail::freshCancellationHandle();
  handle->cancel(ad_utility::CancellationState::MANUAL);
  EXPECT_THROW(mergeFn(makeVectorInput(runs, 16), handle),
               ad_utility::CancellationException);
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
  auto mergeSerially = [](SizeInput input, size_t blockSize) {
    return mergeToVector(std::move(input), std::less<>{},
                         optionsWithBlockSize(blockSize), 8);
  };
  expectSortedResult(mergeSerially, 12, 2000, 20, 50);
  expectSortedResult(mergeSerially, 13, 1, 40, 40);
  expectSortedResult(mergeSerially, 5, 2, 40, 50);
  expectSortedResult(mergeSerially, 1, 3, 30, 50);
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
  auto distinctRuns = makeDistinctPairRuns();
  auto mergeDistinct = [&distinctRuns](size_t numChunks) {
    return mergeToVector(makeVectorInput(distinctRuns, 32), ComparePairs{},
                         optionsWithBlockSize(64), numChunks);
  };
  auto singleChunkResult = mergeDistinct(1);
  ASSERT_EQ(singleChunkResult.size(),
            numDistinctRuns * numDistinctElementsPerRun);
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
  for (size_t numChunks : {1u, 2u, 8u, 64u}) {
    auto result = mergeToVector(makeVectorInput(tiedRuns, 32), ComparePairs{},
                                optionsWithBlockSize(64), numChunks);
    EXPECT_TRUE(ql::ranges::is_sorted(result, ComparePairs{}));
    expectSameKeysAndMultiset(result, tiedRuns);
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunksWithoutAnyElementAreSkipped) {
  // A chunk that contains no element at all is perfectly legal and simply
  // yields no output block. The split points below cannot be produced by
  // `computeChunkBoundaries` (which only ever picks elements that actually
  // occur in the data), so they are handed to the merge directly.
  auto runs = runsWithInterleavedElements();
  auto expected = sortedConcatenation(runs);
  auto mergeWithSplitPoints = [&runs, &expected](SizeVec splitPoints) {
    auto blocks = serialBlockMergeToRange<false>(
        makeVectorInput(runs, 8), std::less<>{}, optionsWithBlockSize(8),
        detail::freshCancellationHandle(),
        detail::chunkBoundariesFromSplitPoints(splitPoints));
    EXPECT_THAT(collectBlocks(blocks), ::testing::ElementsAreArray(expected));
  };
  // All elements are greater than or equal to `100`, so the first five chunks
  // are empty.
  mergeWithSplitPoints(SizeVec{1, 2, 3, 4, 5});
  // All elements lie between the split points `2` and `1000`, so all chunks but
  // the third one are empty: two empty chunks before the single non-empty one,
  // and three after it.
  mergeWithSplitPoints(SizeVec{1, 2, 1000, 2000, 3000});
  // A single element per chunk, and far more chunks than elements.
  mergeWithSplitPoints(splitPointsUpTo(300));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, exceptionFromChunkPropagates) {
  // The merge has already yielded some output blocks when the exception
  // arrives, see `expectExceptionPropagates`.
  expectExceptionPropagates([](InstrumentedInput input) {
    return mergeToVector(std::move(input), std::less<>{},
                         optionsWithBlockSize(16), 4);
  });
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, cancellation) {
  for (size_t numChunks : {1u, 4u}) {
    expectCancellationThrows(
        [numChunks](SizeInput input,
                    ad_utility::SharedCancellationHandle handle) {
          return mergeToVector(std::move(input), std::less<>{},
                               optionsWithBlockSize(16), numChunks,
                               std::move(handle));
        });
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

// ___________________________________________________________________________
// The parallel merge.
//
// The tests above pin down the chunking and the merging itself, both of which
// the parallel merge shares with the serial one. The tests below therefore only
// cover what the parallelization adds: that the chunks really are merged
// concurrently, that every chunk nevertheless pushes exactly the blocks that it
// would push in a serial merge, and that the teardown works on every path.
//
// The sink of all these tests is the trivial in-memory `CollectingBlockSink`,
// which buffers everything and hence never applies back-pressure. What such a
// sink does with the blocks (and in particular how it turns them back into a
// single sorted range) is not part of the merge and is tested separately.
// ___________________________________________________________________________

namespace {
// Return the sink factory that a merge requires (see
// `parallelBlockMergeToSink`): it creates a `CollectingBlockSink` that runs on
// the `executor` and stores it in `out`, so that the caller can inspect it
// afterwards. Pass a positive `stopAfterNumBlocks` to make the sink stop the
// merge as soon as that many blocks were pushed.
//
// NOTE: The reference to `out` is only used while the factory is called, which
// happens inside `parallelBlockMergeToSink` and hence before that function
// returns.
template <typename Sink>
auto collectingSinkFactory(ql::any_io_executor executor,
                           std::shared_ptr<Sink>& out,
                           size_t stopAfterNumBlocks = 0) {
  return [executor = std::move(executor), &out,
          stopAfterNumBlocks](size_t numChunks) {
    out = std::make_shared<Sink>(executor, numChunks, stopAfterNumBlocks);
    return out;
  };
}

// Pin down that the factory above models the `SinkFactoryConcept`, and that
// that concept is SFINAE-friendly: for a type that is not a sink factory it has
// to be a plain `false` instead of a compilation error, no matter which of its
// requirements is violated.
using CollectingSinkFactory =
    decltype(collectingSinkFactory<CollectingBlockSink<SizeVec>>(
        ql::any_io_executor{},
        std::declval<std::shared_ptr<CollectingBlockSink<SizeVec>>&>()));
static_assert(SinkFactoryConcept<CollectingSinkFactory, SizeVec>);
// Not callable with a `size_t` at all.
static_assert(!SinkFactoryConcept<int, SizeVec>);
// Callable, but does not return a `std::shared_ptr`.
struct NotAFactory {
  int operator()(size_t) const { return 0; }
};
static_assert(!SinkFactoryConcept<NotAFactory, SizeVec>);
// Returns a `std::shared_ptr`, but not to something that models the
// `SinkConcept`.
struct FactoryOfNonSink {
  std::shared_ptr<int> operator()(size_t) const { return nullptr; }
};
static_assert(!SinkFactoryConcept<FactoryOfNonSink, SizeVec>);
// A sink, but for the wrong block type.
static_assert(!SinkFactoryConcept<CollectingSinkFactory, std::vector<Pair>>);

// Start a parallel merge of the `input` on the `executor` and return its state
// together with the `CollectingBlockSink` that collects its output blocks. Pass
// a positive `stopAfterNumBlocks` to make the sink stop the merge as soon as
// that many blocks were pushed.
template <bool moveElements = false, typename Input, typename Comparator>
auto startParallelMerge(
    ql::any_io_executor executor, Input input, Comparator comparator,
    MergeOptions options, size_t parallelismHint,
    ad_utility::SharedCancellationHandle cancellationHandle =
        detail::freshCancellationHandle(),
    size_t stopAfterNumBlocks = 0) {
  using Sink = CollectingBlockSink<typename Input::Block>;
  std::shared_ptr<Sink> sink;
  options.parallelismHint = parallelismHint;
  auto state = parallelBlockMergeToSink<moveElements>(
      executor, std::move(input), std::move(comparator),
      collectingSinkFactory(executor, sink, stopAfterNumBlocks),
      std::move(options), std::move(cancellationHandle));
  AD_CORRECTNESS_CHECK(sink != nullptr);
  return std::pair{std::move(state), std::move(sink)};
}

// Check the invariants that hold for the sink of every merge all of whose tasks
// are done: a chunk never sends more than one end-of-chunk sentinel, and if the
// merge was not stopped, then every chunk sends exactly one. A merge that was
// stopped in contrast does not dispatch its remaining chunks at all, so those
// send no sentinel.
template <typename Sink>
void expectSentinelsAreConsistent(const Sink& sink) {
  for (const auto& chunk : sink.chunks()) {
    EXPECT_LE(chunk.numSentinels_, 1u);
    if (!sink.stopRequested()) {
      EXPECT_EQ(chunk.numSentinels_, 1u);
    }
    // An output block is never empty, no matter how the chunks are laid out.
    for (const auto& block : chunk.blocks_) {
      EXPECT_FALSE(block.empty());
    }
  }
}

// Wait until every chunk of the merge that pushes to the `sink` has sent its
// end-of-chunk sentinel, check that the sentinels are consistent, and return
// the elements of all output blocks. This is how a test that does not join the
// thread pool of its merge waits for that merge; only use it for a merge that
// was not stopped, see `CollectingBlockSink::waitUntilAllChunksAreFinished`.
template <typename Sink>
auto awaitAndCollect(const Sink& sink, ad_utility::source_location loc =
                                           AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  sink.waitUntilAllChunksAreFinished();
  expectSentinelsAreConsistent(sink);
  return mergedElements(sink);
}

// Merge the `input` on a thread pool with `numThreads` threads and return the
// elements of all output blocks, in the order of the chunks. Rethrow the
// exception of a chunk if there is one.
template <bool moveElements = false, typename Input, typename Comparator>
std::vector<typename Input::value_type> parallelMergeToVector(
    Input input, Comparator comparator, MergeOptions options = {},
    size_t numThreads = 4,
    ad_utility::SharedCancellationHandle cancellationHandle =
        detail::freshCancellationHandle()) {
  net::thread_pool pool{numThreads};
  auto sink = startParallelMerge<moveElements>(
                  pool.get_executor(), std::move(input), std::move(comparator),
                  std::move(options), numThreads, std::move(cancellationHandle))
                  .second;
  // All the tasks that are still in flight have to finish, otherwise this
  // hangs.
  pool.join();
  expectSentinelsAreConsistent(*sink);
  sink->rethrowIfException();
  return mergedElements(*sink);
}

// Return `MergeOptions` with a small output block size and several chunks per
// thread, so that even the small inputs of the tests are split.
MergeOptions parallelOptions(size_t outputBlockSize = 7) {
  MergeOptions options = optionsWithBlockSize(outputBlockSize);
  options.targetChunksPerThread = 2;
  return options;
}
}  // namespace

// _____________________________________________________________________________
TEST(ParallelBlockMerge, parallelMergeYieldsTheSortedResult) {
  auto mergeInParallel = [](SizeInput input, size_t blockSize) {
    return parallelMergeToVector(std::move(input), std::less<>{},
                                 parallelOptions(blockSize), 8);
  };
  expectSortedResult(mergeInParallel, 16, 8, 100, 200);
  // A block size of one, so that no input block is shared between two chunks.
  expectSortedResult(mergeInParallel, 1, 4, 20, 30);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, parallelMergeOfElementsThatAreMoved) {
  std::vector<std::vector<CountingString>> runs;
  for (size_t run = 0; run < 4; ++run) {
    std::vector<std::string> values;
    for (size_t i = 0; i < 100; ++i) {
      // The runs interleave, so that all of them contribute to (almost) every
      // chunk.
      values.push_back("payloadpayloadpayload" +
                       std::to_string(4 * i + run + 1000));
    }
    runs.push_back(makeCountingStrings(values));
  }
  auto notMoved = parallelMergeToVector<false>(
      makeVectorInput(runs, 8), std::less<>{}, parallelOptions(8), 8);
  auto moved = parallelMergeToVector<true>(
      makeVectorInput(runs, 8), std::less<>{}, parallelOptions(8), 8);
  ASSERT_EQ(moved.size(), 400u);
  ASSERT_EQ(notMoved.size(), 400u);
  for (size_t i = 0; i < moved.size(); ++i) {
    EXPECT_EQ(moved.at(i).value_, notMoved.at(i).value_);
    // Moving the elements out of the input blocks saves a copy.
    EXPECT_LT(moved.at(i).numCopies_, notMoved.at(i).numCopies_);
  }
  EXPECT_TRUE(ql::ranges::is_sorted(moved, std::less<>{}));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, deterministicAcrossParallelism) {
  // The order of tied elements is not specified, so the guarantee is only
  // that the result is fully determined by the input and the configuration.
  // Without any ties that means that the result is identical for every number
  // of chunks.
  auto mergeRuns = [](const std::vector<std::vector<Pair>>& runs,
                      size_t numThreads) {
    MergeOptions options = parallelOptions(64);
    options.targetChunksPerThread = 3;
    return parallelMergeToVector(makeVectorInput(runs, 32), ComparePairs{},
                                 options, numThreads);
  };
  auto distinctRuns = makeDistinctPairRuns();
  auto reference = mergeRuns(distinctRuns, 1);
  ASSERT_EQ(reference.size(), numDistinctRuns * numDistinctElementsPerRun);
  EXPECT_TRUE(ql::ranges::is_sorted(reference, ComparePairs{}));
  EXPECT_THAT(mergeRuns(distinctRuns, 2),
              ::testing::ElementsAreArray(reference));
  EXPECT_THAT(mergeRuns(distinctRuns, 8),
              ::testing::ElementsAreArray(reference));

  // With ties, a *fixed* configuration is still perfectly reproducible, also
  // across repeated runs with different thread schedules.
  auto tiedRuns = makeTiedPairRuns();
  auto tiedReference = mergeRuns(tiedRuns, 8);
  EXPECT_TRUE(ql::ranges::is_sorted(tiedReference, ComparePairs{}));
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_THAT(mergeRuns(tiedRuns, 8),
                ::testing::ElementsAreArray(tiedReference));
  }
  // Independently of the tie breaking, no element is ever lost or duplicated.
  expectSameKeysAndMultiset(tiedReference, tiedRuns);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, manyRunsManyChunks) {
  static constexpr size_t numRuns = 50;
  auto runs = makeRandomRuns(numRuns, 500, 1500);
  auto expected = sortedConcatenation(runs);
  MergeOptions options = parallelOptions(64);
  // Several chunks per thread.
  options.targetChunksPerThread = 5;
  auto result = parallelMergeToVector(makeVectorInput(runs, 32), std::less<>{},
                                      options, 8);
  ASSERT_EQ(result.size(), expected.size());
  EXPECT_TRUE(ql::ranges::is_sorted(result));
  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, singleInFlightChunk) {
  // A single in-flight chunk is perfectly legal, because a chunk that cannot
  // push suspends instead of blocking its thread.
  auto runs = makeRandomRuns(16, 200, 300);
  auto expected = sortedConcatenation(runs);
  MergeOptions options = parallelOptions(16);
  options.maxNumChunksInFlight = 1;
  EXPECT_THAT(parallelMergeToVector(makeVectorInput(runs, 16), std::less<>{},
                                    options, 4),
              ::testing::ElementsAreArray(expected));
  options.maxNumChunksInFlight = 2;
  EXPECT_THAT(parallelMergeToVector(makeVectorInput(runs, 16), std::less<>{},
                                    options, 4),
              ::testing::ElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, exceptionFromChunkPropagatesInParallel) {
  // Some of the chunks succeed and others fail, see
  // `expectExceptionPropagates`. The merge must throw and must in particular
  // not call `std::terminate`.
  expectExceptionPropagates([](InstrumentedInput input) {
    return parallelMergeToVector(std::move(input), std::less<>{},
                                 parallelOptions(16), 4);
  });
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, cancellationInParallel) {
  expectCancellationThrows(
      [](SizeInput input, ad_utility::SharedCancellationHandle handle) {
        return parallelMergeToVector(std::move(input), std::less<>{},
                                     parallelOptions(16), 4, std::move(handle));
      });
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunksAreActuallyMergedInParallel) {
  // Make sure that the tests above really exercise the parallel code path.
  auto runs = makeRandomRuns(32, 1000, 2000);
  auto expected = sortedConcatenation(runs);
  InstrumentedInput input{makeVectorInput(runs, 32)};
  auto state = input.state_;
  EXPECT_THAT(parallelMergeToVector(std::move(input), std::less<>{},
                                    parallelOptions(64), 8),
              ::testing::ElementsAreArray(expected));
  EXPECT_GT(state->readingThreads_.size(), 1u);
  // A merge on a single thread in contrast never reads from two threads at the
  // same time, although it also runs on the executor and not in the calling
  // thread.
  InstrumentedInput singleThreaded{makeVectorInput(runs, 32)};
  auto singleThreadedState = singleThreaded.state_;
  EXPECT_THAT(parallelMergeToVector(std::move(singleThreaded), std::less<>{},
                                    parallelOptions(64), 1),
              ::testing::ElementsAreArray(expected));
  EXPECT_EQ(singleThreadedState->numOverlappingReads_.load(), 0u);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunksOverlapForAllOfTheirOutputBlocks) {
  // The stronger version of `chunksAreActuallyMergedInParallel`: the chunks
  // have to keep overlapping in time for *all* of their output blocks, and not
  // only while they are started. A chunk continues on the general executor
  // after every single `asyncPush`, so nothing serializes the chunks once they
  // are under way; a regression shows up as a large majority of
  // *non*-overlapping reads (and, in an optimized build, as a merge that is
  // slower by roughly the degree of parallelism).
  static constexpr size_t numThreads = 8;
  auto runs = makeRandomRuns(4, 4000, 4000);
  auto expected = sortedConcatenation(runs);
  InstrumentedInput input{makeVectorInput(runs, 64)};
  auto state = input.state_;
  // Hold up every single read, so that overlapping reads are easy to observe.
  state->delayPerRead_ = std::chrono::milliseconds{4};
  EXPECT_THAT(parallelMergeToVector(std::move(input), std::less<>{},
                                    parallelOptions(100), numThreads),
              ::testing::ElementsAreArray(expected));
  // Every chunk reads several blocks per output block, so if the chunks really
  // run concurrently then almost every read overlaps with another one. Only a
  // small fraction is allowed to be alone (the very first and the very last
  // reads of the merge).
  const size_t numReads = state->readBlocks_.size();
  ASSERT_GT(numReads, 100u);
  EXPECT_GT(state->numOverlappingReads_.load(), numReads / 2);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, stopStopsTheMerge) {
  auto runs = makeRandomRuns(50, 2000, 2000);
  net::thread_pool pool{8};
  auto stateAndSink =
      startParallelMerge<false>(pool.get_executor(), makeVectorInput(runs, 64),
                                std::less<>{}, parallelOptions(16), 8);
  // Abandon the merge right away. This must neither hang, nor crash, nor leak:
  // the tasks that are still in flight have to finish instead of waiting for a
  // consumer that is gone, and the state has to stay alive until the last of
  // them is done.
  stateAndSink.first->stop();
  stateAndSink.first.reset();
  pool.join();
  const auto& sink = *stateAndSink.second;
  EXPECT_TRUE(sink.stopRequested());
  expectSentinelsAreConsistent(sink);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, sinkThatStopsTheMergeEarly) {
  // A sink may also stop the merge from within an `asyncPush`, which is what a
  // sink whose consumer has abandoned it does. The merge then drops the blocks
  // that are already finished, finishes the chunks that are already running,
  // and does not dispatch the remaining ones at all.
  static constexpr size_t stopAfterNumBlocks = 3;
  auto runs = makeRandomRuns(50, 2000, 2000);
  net::thread_pool pool{8};
  auto sink = startParallelMerge<false>(
                  pool.get_executor(), makeVectorInput(runs, 64), std::less<>{},
                  parallelOptions(16), 8, detail::freshCancellationHandle(),
                  stopAfterNumBlocks)
                  .second;
  pool.join();
  EXPECT_TRUE(sink->stopRequested());
  expectSentinelsAreConsistent(*sink);
  // The blocks that the merge finishes after the stop are dropped, and the
  // input is far too large to be merged into three blocks, so exactly the
  // blocks up to the stop arrive.
  size_t numBlocks = 0;
  for (const auto& chunk : sink->chunks()) {
    numBlocks += chunk.blocks_.size();
  }
  EXPECT_EQ(numBlocks, stopAfterNumBlocks);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunksWithoutAnyOutputBlockStillSendTheirSentinel) {
  // A chunk that yields no output block at all still has to send its
  // end-of-chunk sentinel, otherwise a sink that waits for that chunk waits
  // forever. The split points below cannot be produced by
  // `computeChunkBoundaries` (which only ever picks elements that actually
  // occur in the data), so the `ParallelMergeState` is driven directly. A
  // regression manifests as a hang of this test.
  auto runs = runsWithInterleavedElements();
  auto expected = sortedConcatenation(runs);
  using Sink = CollectingBlockSink<SizeVec>;
  using State = detail::ParallelMergeState<false, SizeInput, std::less<>, Sink>;
  net::thread_pool pool{4};
  auto mergeWithSplitPoints = [&runs, &pool](const SizeVec& splitPoints) {
    auto boundaries = detail::chunkBoundariesFromSplitPoints(splitPoints);
    size_t numChunks = boundaries.size();
    auto mergeState = makeMergeState(makeVectorInput(runs, 8), std::less<>{},
                                     parallelOptions(8), std::move(boundaries));
    auto sink = std::make_shared<Sink>(pool.get_executor(), numChunks);
    auto state = State::create(pool.get_executor(), std::move(mergeState), sink,
                               std::min<size_t>(2, numChunks));
    return awaitAndCollect(*sink);
  };
  // All elements are greater than or equal to `100`, so the first five chunks
  // are empty.
  EXPECT_THAT(mergeWithSplitPoints(SizeVec{1, 2, 3, 4, 5}),
              ::testing::ElementsAreArray(expected));
  // All elements lie between the split points `2` and `1000`, so all chunks but
  // the third one are empty: two empty chunks before the single non-empty one,
  // and three after it.
  EXPECT_THAT(mergeWithSplitPoints(SizeVec{1, 2, 1000, 2000, 3000}),
              ::testing::ElementsAreArray(expected));
  // A single element per chunk, so that there are far more chunks than
  // in-flight slots.
  EXPECT_THAT(mergeWithSplitPoints(splitPointsUpTo(300)),
              ::testing::ElementsAreArray(expected));
  pool.join();
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, defaultExecutorAndParallelism) {
  // A default-constructed executor and a `MergeOptions::parallelismHint` of
  // zero mean "use the process-wide default thread pool of the merge with one
  // thread per hardware thread", see `MergeExecutor.h`.
  auto runs = makeRandomRuns(4, 200, 300);
  auto expected = sortedConcatenation(runs);
  using Sink = CollectingBlockSink<SizeVec>;
  std::shared_ptr<Sink> sink;
  // NOTE: The merge is set up by hand (and not via `startParallelMerge`),
  // because it is exactly the default-constructed executor and the default
  // `MergeOptions::parallelismHint` that are tested here.
  auto state = parallelBlockMergeToSink<false>(
      ql::any_io_executor{}, makeVectorInput(runs, 16), std::less<>{},
      collectingSinkFactory(defaultMergeExecutor(), sink), parallelOptions(16));
  EXPECT_GT(state->numChunks(), 1u);
  // The default pool is shared, so it cannot be joined; wait for the merge
  // itself instead.
  EXPECT_THAT(awaitAndCollect(*sink), ::testing::ElementsAreArray(expected));
}
