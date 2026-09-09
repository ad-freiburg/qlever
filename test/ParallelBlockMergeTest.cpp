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
#include "util/CancellationHandle.h"
#include "util/Forward.h"
#include "util/GTestHelpers.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ParallelBlockMergeTestHelpers.h"
#include "util/parallelBlockMerge/ParallelBlockMerge.h"

// The single test at the very bottom of this file consumes the merge from a
// coroutine and is therefore not available in the C++17 backports mode. The
// merge itself is coroutine-free and available in that mode.
#ifndef QLEVER_CPP_17
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "util/AsyncTestHelpers.h"
#endif

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

// ___________________________________________________________________________
// The parallel merge.
//
// The tests above pin down the chunking and the merging itself, both of which
// the parallel merge shares with the serial one. The tests below therefore only
// cover what the parallelization adds: that the chunks really are merged
// concurrently, that the blocks nevertheless arrive in the global order, and
// that the teardown works on every path.
// ___________________________________________________________________________

namespace {
// Merge the `input` on a thread pool with `numThreads` threads and return the
// elements of all output blocks in a single vector. A `numThreads` of one takes
// the serial fast path, which merges in the calling thread.
template <bool moveElements = false, typename Input, typename Comparator>
std::vector<typename Input::value_type> parallelMergeToVector(
    Input input, Comparator comparator, MergeOptions options = {},
    size_t numThreads = 4,
    ad_utility::SharedCancellationHandle cancellationHandle =
        std::make_shared<ad_utility::CancellationHandle<>>()) {
  net::thread_pool pool{numThreads};
  std::vector<typename Input::value_type> result;
  {
    auto blocks = parallelBlockMergeToRange<moveElements>(
        pool.get_executor(), std::move(input), std::move(comparator),
        std::move(options), numThreads, std::move(cancellationHandle));
    for (auto& block : blocks) {
      for (auto& element : block) {
        result.push_back(std::move(element));
      }
    }
  }
  // All the tasks that are still in flight have to finish, otherwise this
  // hangs.
  pool.join();
  return result;
}

// Return `MergeOptions` that force the parallel code path also for the small
// inputs that are used in the tests.
MergeOptions parallelOptions(size_t outputBlockSize = 7) {
  MergeOptions options = optionsWithBlockSize(outputBlockSize);
  options.serialNumElementsThreshold = 0;
  options.targetChunksPerThread = 2;
  return options;
}
}  // namespace

// _____________________________________________________________________________
TEST(ParallelBlockMerge, parallelMergeYieldsTheSortedResult) {
  auto testRandomInts = [](size_t blockSize, size_t numRuns, size_t minSize,
                           size_t maxSize) {
    auto runs = makeRandomRuns(numRuns, minSize, maxSize);
    auto expected = sortedConcatenation(runs);
    auto result =
        parallelMergeToVector(makeVectorInput(runs, blockSize), std::less<>{},
                              parallelOptions(blockSize), 8);
    ASSERT_EQ(result.size(), expected.size());
    EXPECT_TRUE(ql::ranges::is_sorted(result));
    EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
  };
  testRandomInts(16, 8, 100, 200);
  // A block size of one, so that no input block is shared between two chunks.
  testRandomInts(1, 4, 20, 30);
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
  auto mergeDistinct = [&distinctRuns](size_t numThreads) {
    MergeOptions options = parallelOptions(64);
    options.targetChunksPerThread = 3;
    return parallelMergeToVector(makeVectorInput(distinctRuns, 32),
                                 ComparePairs{}, options, numThreads);
  };
  auto serial = mergeDistinct(1);
  ASSERT_EQ(serial.size(), numRuns * numElementsPerRun);
  EXPECT_TRUE(ql::ranges::is_sorted(serial, ComparePairs{}));
  EXPECT_THAT(mergeDistinct(2), ::testing::ElementsAreArray(serial));
  EXPECT_THAT(mergeDistinct(8), ::testing::ElementsAreArray(serial));

  // With ties, a *fixed* configuration is still perfectly reproducible, also
  // across repeated runs with different thread schedules.
  auto tiedRuns = makeTiedPairRuns();
  auto mergeTied = [&tiedRuns](size_t numThreads) {
    MergeOptions options = parallelOptions(64);
    options.targetChunksPerThread = 3;
    return parallelMergeToVector(makeVectorInput(tiedRuns, 32), ComparePairs{},
                                 options, numThreads);
  };
  auto reference = mergeTied(8);
  EXPECT_TRUE(ql::ranges::is_sorted(reference, ComparePairs{}));
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_THAT(mergeTied(8), ::testing::ElementsAreArray(reference));
  }
  // Independently of the tie breaking, no element is ever lost or duplicated.
  auto expected = concatenation(tiedRuns);
  ql::ranges::stable_sort(expected, ComparePairs{});
  EXPECT_THAT(getKeys(reference),
              ::testing::ElementsAreArray(getKeys(expected)));
  auto sortedReference = reference;
  ql::ranges::sort(sortedReference);
  auto sortedExpected = expected;
  ql::ranges::sort(sortedExpected);
  EXPECT_THAT(sortedReference, ::testing::ElementsAreArray(sortedExpected));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, exceptionFromChunkPropagatesInParallel) {
  auto runs = makeRandomRuns(16, 200, 300);
  InstrumentedInput input{makeVectorInput(runs, 16)};
  // Throw from the third call to `getBlock` onwards, so that some of the
  // chunks succeed and others fail.
  input.state_->throwAtRead_ = 3;
  // This must throw and must in particular not call `std::terminate`.
  AD_EXPECT_THROW_WITH_MESSAGE(
      parallelMergeToVector(input, std::less<>{}, parallelOptions(16), 4),
      ::testing::HasSubstr("getBlock failed"));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, cancellationInParallel) {
  auto runs = makeRandomRuns(16, 200, 300);
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  handle->cancel(ad_utility::CancellationState::MANUAL);
  EXPECT_THROW(parallelMergeToVector(makeVectorInput(runs, 16), std::less<>{},
                                     parallelOptions(16), 4, handle),
               ad_utility::CancellationException);
  // The same holds for the serial fast path.
  auto smallRuns = makeRandomRuns(2, 20, 30);
  EXPECT_THROW(parallelMergeToVector(makeVectorInput(smallRuns, 4),
                                     std::less<>{}, MergeOptions{}, 4, handle),
               ad_utility::CancellationException);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, consumerAbandonsRangeEarly) {
  auto runs = makeRandomRuns(50, 2000, 2000);
  net::thread_pool pool{8};
  // This must neither hang, nor crash, nor leak. The destructor of the range
  // has to abort the merge, and the state has to stay alive until the last task
  // that refers to it is done.
  {
    auto blocks = parallelBlockMergeToRange<false>(
        pool.get_executor(), makeVectorInput(runs, 64), std::less<>{},
        parallelOptions(16), 8);
    auto it = blocks.begin();
    ASSERT_NE(it, blocks.end());
    EXPECT_FALSE(it->empty());
    ++it;
    ASSERT_NE(it, blocks.end());
    EXPECT_FALSE(it->empty());
  }
  // Abandoning the range without consuming anything at all also works.
  {
    auto blocks = parallelBlockMergeToRange<false>(
        pool.get_executor(), makeVectorInput(runs, 64), std::less<>{},
        parallelOptions(16), 8);
  }
  pool.join();
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, manyRunsManyChunks) {
  static constexpr size_t numRuns = 50;
  auto runs = makeRandomRuns(numRuns, 500, 1500);
  auto expected = sortedConcatenation(runs);
  MergeOptions options = parallelOptions(64);
  // Several chunks per thread.
  options.targetChunksPerThread = 5;
  options.bufferedBlocksPerChunk = 1;
  auto result = parallelMergeToVector(makeVectorInput(runs, 32), std::less<>{},
                                      options, 8);
  ASSERT_EQ(result.size(), expected.size());
  EXPECT_TRUE(ql::ranges::is_sorted(result));
  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, singleInFlightChunk) {
  // A single in-flight chunk is perfectly legal and does not fall back to the
  // serial merge, because a chunk whose channel is full suspends instead of
  // blocking its thread.
  auto runs = makeRandomRuns(16, 200, 300);
  auto expected = sortedConcatenation(runs);
  MergeOptions options = parallelOptions(16);
  options.bufferedBlocksPerChunk = 1;
  options.maxInFlightChunks = 1;
  EXPECT_THAT(parallelMergeToVector(makeVectorInput(runs, 16), std::less<>{},
                                    options, 4),
              ::testing::ElementsAreArray(expected));
  options.maxInFlightChunks = 2;
  EXPECT_THAT(parallelMergeToVector(makeVectorInput(runs, 16), std::less<>{},
                                    options, 4),
              ::testing::ElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunksAreActuallyMergedInParallel) {
  // Make sure that the tests above really exercise the parallel code path (and
  // not silently one of the serial fast paths).
  auto runs = makeRandomRuns(32, 1000, 2000);
  auto expected = sortedConcatenation(runs);
  InstrumentedInput input{makeVectorInput(runs, 32)};
  auto state = input.state_;
  EXPECT_THAT(parallelMergeToVector(std::move(input), std::less<>{},
                                    parallelOptions(64), 8),
              ::testing::ElementsAreArray(expected));
  EXPECT_GT(state->readingThreads_.size(), 1u);
  // The serial fast path in contrast only ever reads from the consuming thread.
  InstrumentedInput serialInput{makeVectorInput(runs, 32)};
  auto serialState = serialInput.state_;
  MergeOptions options = parallelOptions(64);
  options.serialNumElementsThreshold = 1'000'000;
  EXPECT_THAT(
      parallelMergeToVector(std::move(serialInput), std::less<>{}, options, 8),
      ::testing::ElementsAreArray(expected));
  EXPECT_EQ(serialState->readingThreads_.size(), 1u);
  EXPECT_EQ(*serialState->readingThreads_.begin(), std::this_thread::get_id());
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunksStayInParallelAfterTheirFirstOutputBlock) {
  // The stronger version of `chunksAreActuallyMergedInParallel`: the chunks
  // have to keep overlapping in time for *all* of their output blocks, and not
  // only while they are started.
  //
  // This pins down that a chunk continues on the general executor after every
  // single `asyncPush`. The completion handler of that push may well run on the
  // strand of the sink, and everything that runs on a strand is serialized, so
  // a chunk that continued to merge right there would hold the strand for a
  // whole output block and thereby serialize the entire merge. Only the *first*
  // output block of a chunk would still be merged in parallel (it is reached
  // from the `net::post` of the dispatch loop), which is exactly why a test
  // that only looks at the peak concurrency does not catch this. See the
  // IMPORTANT note at `detail::ParallelMergeState` and `ChunkTask::step`.
  //
  // A regression shows up as a large majority of *non*-overlapping reads (and,
  // in an optimized build, as a merge that is slower by roughly the degree of
  // parallelism).
  static constexpr size_t numThreads = 8;
  auto runs = makeRandomRuns(4, 4000, 4000);
  auto expected = sortedConcatenation(runs);
  InstrumentedInput input{makeVectorInput(runs, 64)};
  auto state = input.state_;
  // Hold up every single read, so that overlapping reads are easy to observe.
  state->delayPerRead_ = std::chrono::milliseconds{4};
  MergeOptions options = parallelOptions(100);
  // Buffer generously, so that the chunks are not throttled by the consumer.
  // The back-pressure is tested separately, and here it would hide the effect.
  options.bufferedBlocksPerChunk = 40;
  EXPECT_THAT(parallelMergeToVector(std::move(input), std::less<>{}, options,
                                    numThreads),
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
TEST(ParallelBlockMerge, chunksWithoutAnyOutputBlockDoNotHang) {
  // A chunk that yields no output block at all still has to send its
  // end-of-chunk sentinel, otherwise the consumer waits for it forever. The
  // split points below cannot be produced by `computeChunkBoundaries` (which
  // only ever picks elements that actually occur in the data), so the
  // `ParallelMergeState` is driven directly. A regression manifests as a hang
  // of this test.
  std::vector<SizeVec> runs;
  for (size_t run = 0; run < 4; ++run) {
    SizeVec elements;
    for (size_t i = 0; i < 40; ++i) {
      elements.push_back(100 + run + 4 * i);
    }
    runs.push_back(std::move(elements));
  }
  auto expected = sortedConcatenation(runs);
  using State = detail::ParallelMergeState<false, SizeInput, std::less<>>;
  using Range = detail::ParallelMergeRange<false, SizeInput, std::less<>>;
  net::thread_pool pool{4};
  auto mergeWithSplitPoints = [&runs, &pool](SizeVec splitPoints) {
    auto mergeState = std::make_shared<const typename State::SharedMergeState>(
        makeVectorInput(runs, 8), std::less<>{}, parallelOptions(8),
        std::make_shared<ad_utility::CancellationHandle<>>(),
        detail::chunkBoundariesFromSplitPoints(splitPoints));
    auto state = State::create(pool.get_executor(), std::move(mergeState), 2);
    ad_utility::InputRangeTypeErased<SizeVec> blocks{
        std::make_unique<Range>(std::move(state))};
    SizeVec result;
    for (const auto& block : blocks) {
      EXPECT_FALSE(block.empty());
      result.insert(result.end(), block.begin(), block.end());
    }
    return result;
  };
  // All elements are greater than `100`, so the first five chunks are empty.
  EXPECT_THAT(mergeWithSplitPoints(SizeVec{1, 2, 3, 4, 5}),
              ::testing::ElementsAreArray(expected));
  // Empty chunks at the beginning, in the middle, and at the end.
  EXPECT_THAT(mergeWithSplitPoints(SizeVec{1, 2, 1000, 2000, 3000}),
              ::testing::ElementsAreArray(expected));
  // A single element per chunk, so that there are far more chunks than
  // in-flight slots.
  SizeVec manySplitPoints;
  for (size_t i = 0; i < 300; ++i) {
    manySplitPoints.push_back(i);
  }
  EXPECT_THAT(mergeWithSplitPoints(std::move(manySplitPoints)),
              ::testing::ElementsAreArray(expected));
  pool.join();
}

#ifndef QLEVER_CPP_17
// _____________________________________________________________________________
ASYNC_TEST(ParallelBlockMerge, singleThreadedConsumer) {
  // A single thread suffices for an asynchronous consumer, even if many more
  // chunks than that are in flight, because a chunk that has to wait for the
  // consumer suspends instead of blocking the only thread.
  auto runs = makeRandomRuns(8, 300, 400);
  auto expected = sortedConcatenation(runs);
  MergeOptions options = parallelOptions(16);
  options.bufferedBlocksPerChunk = 1;
  auto state = parallelBlockMergeAsync<false>(ioContext.get_executor(),
                                              makeVectorInput(runs, 16),
                                              std::less<>{}, options, 8);
  SizeVec result;
  while (auto block = co_await state->asyncNext(net::use_awaitable)) {
    result.insert(result.end(), block->begin(), block->end());
  }
  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}
#endif  // QLEVER_CPP_17
