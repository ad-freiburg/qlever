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
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Random.h"
#include "util/parallelBlockMerge/ParallelBlockMerge.h"

// The tests of the Boost.Asio based merge at the bottom of this file require
// coroutines and are therefore not available in the C++17 backports mode.
#ifndef QLEVER_CPP_17
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>

#include "util/AsyncTestHelpers.h"
#endif

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
    std::mutex mutex_{};
    std::vector<std::pair<size_t, size_t>> readBlocks_{};
    // The threads from which `readBlock` was called.
    std::set<std::thread::id> readingThreads_{};
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
      state_->readingThreads_.insert(std::this_thread::get_id());
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

// Merge the `input` and return the elements of all output blocks in a single
// vector.
template <bool moveElements = false, typename Input, typename Comparator>
std::vector<typename Input::value_type> mergeToVector(
    Input input, Comparator comparator, MergeOptions options = {},
    SharedMergeScheduler scheduler = defaultMergeScheduler(),
    ad_utility::SharedCancellationHandle cancellationHandle = nullptr) {
  std::vector<typename Input::value_type> result;
  auto blocks = parallelBlockMergeToRange<moveElements>(
      std::move(input), std::move(comparator), std::move(options),
      std::move(scheduler), std::move(cancellationHandle));
  for (auto& block : blocks) {
    for (auto& element : block) {
      result.push_back(std::move(element));
    }
  }
  return result;
}

// Return `MergeOptions` that force the parallel code path also for the small
// inputs that are used in the tests.
MergeOptions parallelOptions(size_t outputBlockSize = 7) {
  MergeOptions options;
  options.outputBlockSize = OutputBlockSize::numElements(outputBlockSize);
  options.serialNumElementsThreshold = 0;
  options.targetChunksPerThread = 2;
  return options;
}

// Return a scheduler with the given number of threads.
SharedMergeScheduler makeScheduler(size_t numThreads) {
  return std::make_shared<TaskQueueMergeScheduler>(numThreads, "test");
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
  MergeOptions options;
  options.outputBlockSize = OutputBlockSize::numElements(3);
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
  MergeOptions options;
  options.outputBlockSize = OutputBlockSize::numElements(3);

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
TEST(ParallelBlockMerge, randomInputs) {
  auto testRandomInts = [](size_t blockSize, size_t numRuns, size_t minSize,
                           size_t maxSize) {
    auto runs = makeRandomRuns(numRuns, minSize, maxSize);
    auto expected = sortedConcatenation(runs);
    MergeOptions options = parallelOptions(blockSize);
    auto result = mergeToVector(SizeRuns{runs, blockSize}, std::less<>{},
                                options, makeScheduler(4));
    EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
  };
  testRandomInts(12, 2000, 20, 50);
  testRandomInts(13, 1, 40, 40);
  testRandomInts(5, 2, 40, 50);
  testRandomInts(1, 3, 30, 50);
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
                                parallelOptions(128), makeScheduler(4));
    EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
  }
  // Runs that are empty are simply skipped.
  {
    std::vector<SizeVec> runs{SizeVec{}, SizeVec{1, 2, 3, 4}, SizeVec{},
                              SizeVec{0, 5}};
    auto result = mergeToVector(SizeRuns{runs, 2}, std::less<>{},
                                parallelOptions(2), makeScheduler(4));
    EXPECT_THAT(result, ::testing::ElementsAre(0u, 1u, 2u, 3u, 4u, 5u));
  }
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
  MergeOptions options;
  options.outputBlockSize = OutputBlockSize::numElements(100);

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
  auto mergeDistinct = [&distinctRuns](SharedMergeScheduler scheduler) {
    MergeOptions options = parallelOptions(64);
    options.targetChunksPerThread = 3;
    return mergeToVector(PairRuns{distinctRuns, 32}, ComparePairs{}, options,
                         std::move(scheduler));
  };
  auto serial = mergeDistinct(std::make_shared<InlineMergeScheduler>());
  ASSERT_EQ(serial.size(), numRuns * numElementsPerRun);
  EXPECT_TRUE(ql::ranges::is_sorted(serial, ComparePairs{}));
  EXPECT_THAT(mergeDistinct(makeScheduler(2)),
              ::testing::ElementsAreArray(serial));
  EXPECT_THAT(mergeDistinct(makeScheduler(8)),
              ::testing::ElementsAreArray(serial));

  // With ties, a *fixed* configuration is still perfectly reproducible, also
  // across repeated runs with different thread schedules.
  auto tiedRuns = makeTiedPairRuns();
  auto mergeTied = [&tiedRuns](SharedMergeScheduler scheduler) {
    MergeOptions options = parallelOptions(64);
    options.targetChunksPerThread = 3;
    return mergeToVector(PairRuns{tiedRuns, 32}, ComparePairs{}, options,
                         std::move(scheduler));
  };
  auto reference = mergeTied(makeScheduler(8));
  EXPECT_TRUE(ql::ranges::is_sorted(reference, ComparePairs{}));
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_THAT(mergeTied(makeScheduler(8)),
                ::testing::ElementsAreArray(reference));
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
TEST(ParallelBlockMerge, exceptionFromChunkPropagates) {
  auto runs = makeRandomRuns(16, 200, 300);
  InstrumentedInput input{SizeRuns{runs, 16}};
  // Throw from the third call to `readBlock` onwards, so that some of the
  // chunks succeed and others fail.
  input.state_->throwAtRead_ = 3;
  // This must throw and must in particular not call `std::terminate`.
  AD_EXPECT_THROW_WITH_MESSAGE(
      mergeToVector(input, std::less<>{}, parallelOptions(16),
                    makeScheduler(4)),
      ::testing::HasSubstr("readBlock failed"));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, exceptionFromChunkPropagatesSerially) {
  auto runs = makeRandomRuns(2, 200, 300);
  InstrumentedInput input{SizeRuns{runs, 16}};
  input.state_->throwAtRead_ = 1;
  MergeOptions options = parallelOptions(16);
  // The input is below the element threshold, so the merge is serial.
  options.serialNumElementsThreshold = 1'000'000;
  AD_EXPECT_THROW_WITH_MESSAGE(
      mergeToVector(input, std::less<>{}, options, makeScheduler(4)),
      ::testing::HasSubstr("readBlock failed"));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, cancellation) {
  auto runs = makeRandomRuns(16, 200, 300);
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  handle->cancel(ad_utility::CancellationState::MANUAL);
  EXPECT_THROW(mergeToVector(SizeRuns{runs, 16}, std::less<>{},
                             parallelOptions(16), makeScheduler(4), handle),
               ad_utility::CancellationException);
  // The same holds for the serial fast path.
  auto smallRuns = makeRandomRuns(2, 20, 30);
  EXPECT_THROW(mergeToVector(SizeRuns{smallRuns, 4}, std::less<>{},
                             MergeOptions{}, makeScheduler(4), handle),
               ad_utility::CancellationException);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, consumerAbandonsRangeEarly) {
  auto runs = makeRandomRuns(50, 2000, 2000);
  // This must neither hang, nor crash, nor leak. The destructor of the range
  // has to abort the sink and to wait for all in-flight chunks, because those
  // refer to the input and the sink that the range owns.
  {
    auto blocks =
        parallelBlockMergeToRange<false>(SizeRuns{runs, 64}, std::less<>{},
                                         parallelOptions(16), makeScheduler(8));
    auto it = blocks.begin();
    ASSERT_NE(it, blocks.end());
    EXPECT_FALSE(it->empty());
    ++it;
    ASSERT_NE(it, blocks.end());
    EXPECT_FALSE(it->empty());
  }
  // Abandoning the range without consuming anything at all also works.
  {
    auto blocks =
        parallelBlockMergeToRange<false>(SizeRuns{runs, 64}, std::less<>{},
                                         parallelOptions(16), makeScheduler(8));
  }
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
  auto result = mergeToVector(SizeRuns{runs, 32}, std::less<>{}, options,
                              makeScheduler(8));
  ASSERT_EQ(result.size(), expected.size());
  EXPECT_TRUE(ql::ranges::is_sorted(result));
  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, maxInFlightChunksIsHonored) {
  auto runs = makeRandomRuns(16, 200, 300);
  auto expected = sortedConcatenation(runs);
  MergeOptions options = parallelOptions(16);
  // A single in-flight chunk cannot be parallelized and therefore falls back to
  // the serial merge, which still has to yield the correct result.
  options.maxInFlightChunks = 1;
  EXPECT_THAT(mergeToVector(SizeRuns{runs, 16}, std::less<>{}, options,
                            makeScheduler(4)),
              ::testing::ElementsAreArray(expected));
  options.maxInFlightChunks = 2;
  EXPECT_THAT(mergeToVector(SizeRuns{runs, 16}, std::less<>{}, options,
                            makeScheduler(4)),
              ::testing::ElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, outputBlockMemoryLimit) {
  auto runs = makeRandomRuns(4, 100, 100);
  MergeOptions options = parallelOptions(1000);
  // Three elements fit into a single output block.
  options.outputBlockSize = OutputBlockSize::both(
      1000, ad_utility::MemorySize::bytes(3 * sizeof(size_t)));
  auto blocks = parallelBlockMergeToRange<false>(
      SizeRuns{runs, 16}, std::less<>{}, options, makeScheduler(4));
  size_t numElements = 0;
  for (const auto& block : blocks) {
    EXPECT_LE(block.size(), 3u);
    EXPECT_FALSE(block.empty());
    numElements += block.size();
  }
  EXPECT_EQ(numElements, 400u);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunksAreActuallyMergedInParallel) {
  // Make sure that the tests above really exercise the parallel code path (and
  // not silently one of the serial fast paths).
  auto runs = makeRandomRuns(32, 1000, 2000);
  auto expected = sortedConcatenation(runs);
  InstrumentedInput input{SizeRuns{runs, 32}};
  auto state = input.state_;
  EXPECT_THAT(mergeToVector(std::move(input), std::less<>{},
                            parallelOptions(64), makeScheduler(8)),
              ::testing::ElementsAreArray(expected));
  EXPECT_GT(state->readingThreads_.size(), 1u);
  // The serial fast path in contrast only ever reads from a single thread.
  InstrumentedInput serialInput{SizeRuns{runs, 32}};
  auto serialState = serialInput.state_;
  MergeOptions options = parallelOptions(64);
  options.serialNumElementsThreshold = 1'000'000;
  EXPECT_THAT(mergeToVector(std::move(serialInput), std::less<>{}, options,
                            makeScheduler(8)),
              ::testing::ElementsAreArray(expected));
  EXPECT_EQ(serialState->readingThreads_.size(), 1u);
  EXPECT_EQ(*serialState->readingThreads_.begin(), std::this_thread::get_id());
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunksWithoutAnyOutputBlockDoNotHang) {
  // A chunk that yields no output block at all leaves the consumer parked
  // inside the sink, where it cannot dispatch anything. The successors of such
  // a chunk therefore have to be dispatched by the completing workers. The
  // splitters below cannot be produced by `computeSplitters` (which only ever
  // picks keys that actually occur in the data), so the `ParallelMergeState` is
  // driven directly. A regression manifests as a hang of this test.
  std::vector<SizeVec> runs;
  for (size_t run = 0; run < 4; ++run) {
    SizeVec elements;
    for (size_t i = 0; i < 40; ++i) {
      elements.push_back(100 + run + 4 * i);
    }
    runs.push_back(std::move(elements));
  }
  auto expected = sortedConcatenation(runs);
  using State = detail::ParallelMergeState<false, SizeRuns, std::less<>>;
  auto mergeWithSplitters = [&runs](SizeVec splitters) {
    ad_utility::InputRangeTypeErased<SizeVec> blocks{std::make_unique<State>(
        SizeRuns{runs, 8}, std::less<>{}, parallelOptions(8), makeScheduler(4),
        nullptr, Splitters<size_t>{std::move(splitters)}, 2)};
    SizeVec result;
    for (const auto& block : blocks) {
      EXPECT_FALSE(block.empty());
      result.insert(result.end(), block.begin(), block.end());
    }
    return result;
  };
  // All elements are greater than `100`, so the first five chunks are empty.
  EXPECT_THAT(mergeWithSplitters(SizeVec{1, 2, 3, 4, 5}),
              ::testing::ElementsAreArray(expected));
  // Empty chunks at the beginning, in the middle, and at the end.
  EXPECT_THAT(mergeWithSplitters(SizeVec{1, 2, 1000, 2000, 3000}),
              ::testing::ElementsAreArray(expected));
  // A single element per chunk, so that there are far more chunks than
  // in-flight slots.
  SizeVec manySplitters;
  for (size_t i = 0; i < 300; ++i) {
    manySplitters.push_back(i);
  }
  EXPECT_THAT(mergeWithSplitters(std::move(manySplitters)),
              ::testing::ElementsAreArray(expected));
}

#ifndef QLEVER_CPP_17
namespace {
// Merge the `input` on a Boost.Asio thread pool with `numThreads` threads and
// return the elements of all output blocks in a single vector.
template <bool moveElements = false, typename Input, typename Comparator>
std::vector<typename Input::value_type> mergeToVectorAsio(
    Input input, Comparator comparator, MergeOptions options = {},
    size_t numThreads = 4,
    ad_utility::SharedCancellationHandle cancellationHandle = nullptr) {
  net::thread_pool pool{numThreads};
  std::vector<typename Input::value_type> result;
  {
    auto blocks = parallelBlockMergeToRangeAsio<moveElements>(
        pool.get_executor(), std::move(input), std::move(comparator),
        std::move(options), numThreads, std::move(cancellationHandle));
    for (auto& block : blocks) {
      for (auto& element : block) {
        result.push_back(std::move(element));
      }
    }
  }
  // All the coroutines that are still in flight have to finish, otherwise this
  // hangs.
  pool.join();
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(AsioParallelBlockMerge, binaryMerge) {
  SizeVec v1{1, 3, 5};
  SizeVec v2{2, 4, 6};
  MergeOptions options = parallelOptions(3);
  auto result =
      mergeToVectorAsio(SizeRuns{{v1, v2}, 3}, std::less<>{}, options);
  EXPECT_THAT(result, ::testing::ElementsAre(1u, 2u, 3u, 4u, 5u, 6u));

  v2 = SizeVec{};
  result = mergeToVectorAsio(SizeRuns{{v1, v2}, 3}, std::less<>{}, options);
  EXPECT_THAT(result, ::testing::ElementsAre(1u, 3u, 5u));

  std::swap(v1, v2);
  result = mergeToVectorAsio(SizeRuns{{v1, v2}, 3}, std::less<>{}, options);
  EXPECT_THAT(result, ::testing::ElementsAre(1u, 3u, 5u));

  // A merge of zero runs yields nothing.
  EXPECT_THAT(mergeToVectorAsio(SizeRuns{{}, 3}, std::less<>{}, options),
              ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(AsioParallelBlockMerge, moveOfElements) {
  using V = std::vector<std::string>;
  V v1{"alphaalpha", "deltadelta"};
  V v2{"betabeta", "epsilonepsilon"};
  // NOTE: The virtual block size is one, such that no input block is shared
  // between two chunks. `VectorRunsInput` hands out blocks that alias the
  // underlying vectors, so a chunk that reads a block from which a neighboring
  // chunk has already moved the elements out would see empty strings and
  // therefore locate its own elements incorrectly. This is a property of this
  // test-only input policy (a realistic policy returns a freshly decompressed
  // block) and it affects `parallelBlockMergeToRange` in exactly the same way.
  StringRuns input{
      {StringRun{v1.begin(), v1.end()}, StringRun{v2.begin(), v2.end()}},
      1,
      true};
  auto result = mergeToVectorAsio<true>(std::move(input), std::less<>{},
                                        parallelOptions(3));
  EXPECT_THAT(result, ::testing::ElementsAre("alphaalpha", "betabeta",
                                             "deltadelta", "epsilonepsilon"));
  // The strings were moved out.
  EXPECT_THAT(v1, ::testing::Each(::testing::IsEmpty()));
  EXPECT_THAT(v2, ::testing::Each(::testing::IsEmpty()));
}

// _____________________________________________________________________________
TEST(AsioParallelBlockMerge, randomInputs) {
  auto testRandomInts = [](size_t blockSize, size_t numRuns, size_t minSize,
                           size_t maxSize) {
    auto runs = makeRandomRuns(numRuns, minSize, maxSize);
    auto expected = sortedConcatenation(runs);
    auto result = mergeToVectorAsio(SizeRuns{runs, blockSize}, std::less<>{},
                                    parallelOptions(blockSize), 4);
    EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
  };
  testRandomInts(12, 2000, 20, 50);
  testRandomInts(13, 1, 40, 40);
  testRandomInts(5, 2, 40, 50);
  testRandomInts(1, 3, 30, 50);
}

// _____________________________________________________________________________
TEST(AsioParallelBlockMerge, manyRunsManyChunks) {
  auto runs = makeRandomRuns(50, 500, 1500);
  auto expected = sortedConcatenation(runs);
  MergeOptions options = parallelOptions(64);
  options.targetChunksPerThread = 5;
  options.bufferedBlocksPerChunk = 1;
  auto result =
      mergeToVectorAsio(SizeRuns{runs, 32}, std::less<>{}, options, 8);
  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(AsioParallelBlockMerge, singleInFlightChunk) {
  // In contrast to `parallelBlockMergeToRange`, a single in-flight chunk is
  // perfectly legal here and does not fall back to the serial merge, because a
  // chunk whose channel is full suspends its coroutine instead of blocking its
  // thread.
  auto runs = makeRandomRuns(16, 200, 300);
  auto expected = sortedConcatenation(runs);
  MergeOptions options = parallelOptions(16);
  options.maxInFlightChunks = 1;
  options.bufferedBlocksPerChunk = 1;
  EXPECT_THAT(mergeToVectorAsio(SizeRuns{runs, 16}, std::less<>{}, options, 4),
              ::testing::ElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(AsioParallelBlockMerge, sameTieOrderAsTheSchedulerBasedMerge) {
  // The chunk boundaries only depend on the input, the comparator, and
  // `parallelismHint * targetChunksPerThread`, so the two implementations have
  // to produce exactly the same order, also for the elements that the
  // comparator considers equal.
  auto runs = makeTiedPairRuns();
  MergeOptions options = parallelOptions(64);
  auto reference = mergeToVector(PairRuns{runs, 32}, ComparePairs{}, options,
                                 makeScheduler(4));
  auto result =
      mergeToVectorAsio(PairRuns{runs, 32}, ComparePairs{}, options, 4);
  EXPECT_THAT(result, ::testing::ElementsAreArray(reference));
  // Make sure that this really compares a non-trivial order.
  EXPECT_EQ(result.size(), numTiedRuns * numTiedElementsPerRun);
  EXPECT_TRUE(ql::ranges::is_sorted(getKeys(result)));
}

// _____________________________________________________________________________
TEST(AsioParallelBlockMerge, exceptionFromChunkPropagates) {
  auto runs = makeRandomRuns(16, 200, 300);
  InstrumentedInput input{SizeRuns{runs, 16}};
  // Throw from the third call to `readBlock` onwards, so that some of the
  // chunks succeed and others fail.
  input.state_->throwAtRead_ = 3;
  // This must throw and must in particular not call `std::terminate`.
  AD_EXPECT_THROW_WITH_MESSAGE(
      mergeToVectorAsio(input, std::less<>{}, parallelOptions(16), 4),
      ::testing::HasSubstr("readBlock failed"));
}

// _____________________________________________________________________________
TEST(AsioParallelBlockMerge, cancellation) {
  auto runs = makeRandomRuns(16, 200, 300);
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  handle->cancel(ad_utility::CancellationState::MANUAL);
  EXPECT_THROW(mergeToVectorAsio(SizeRuns{runs, 16}, std::less<>{},
                                 parallelOptions(16), 4, handle),
               ad_utility::CancellationException);
}

// _____________________________________________________________________________
TEST(AsioParallelBlockMerge, consumerAbandonsRangeEarly) {
  auto runs = makeRandomRuns(50, 2000, 2000);
  net::thread_pool pool{8};
  // This must neither hang, nor crash, nor leak. The destructor of the range
  // has to abort the merge, and the state has to stay alive until the last
  // coroutine that refers to it is done.
  {
    auto blocks = parallelBlockMergeToRangeAsio<false>(
        pool.get_executor(), SizeRuns{runs, 64}, std::less<>{},
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
    auto blocks = parallelBlockMergeToRangeAsio<false>(
        pool.get_executor(), SizeRuns{runs, 64}, std::less<>{},
        parallelOptions(16), 8);
  }
  pool.join();
}

// _____________________________________________________________________________
TEST(AsioParallelBlockMerge, chunksAreActuallyMergedInParallel) {
  // Make sure that the tests above really exercise the parallel code path (and
  // not silently the serial fast path).
  auto runs = makeRandomRuns(32, 1000, 2000);
  auto expected = sortedConcatenation(runs);
  InstrumentedInput input{SizeRuns{runs, 32}};
  auto state = input.state_;
  EXPECT_THAT(mergeToVectorAsio(std::move(input), std::less<>{},
                                parallelOptions(64), 8),
              ::testing::ElementsAreArray(expected));
  EXPECT_GT(state->readingThreads_.size(), 1u);
}

// _____________________________________________________________________________
ASYNC_TEST(AsioParallelBlockMerge, singleThreadedConsumer) {
  // A single thread suffices for the Boost.Asio based merge, even if many more
  // chunks than that are in flight, because a chunk that has to wait for the
  // consumer suspends its coroutine instead of blocking the only thread. The
  // scheduler-based `ParallelMergeState` in contrast needs one thread per
  // in-flight chunk.
  auto runs = makeRandomRuns(8, 300, 400);
  auto expected = sortedConcatenation(runs);
  MergeOptions options = parallelOptions(16);
  options.bufferedBlocksPerChunk = 1;
  auto state = parallelBlockMergeAsio<false>(
      ioContext.get_executor(), SizeRuns{runs, 16}, std::less<>{}, options, 8);
  SizeVec result;
  while (auto block = co_await state->asyncNext()) {
    result.insert(result.end(), block->begin(), block->end());
  }
  EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
}
#endif  // QLEVER_CPP_17
