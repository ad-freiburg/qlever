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
using SizeInput = VectorInput<size_t>;

using Pair = std::pair<size_t, size_t>;

static_assert(InputConcept<SizeInput>);
static_assert(InputConcept<VectorInput<Pair>>);

// A comparator that only looks at the first component of a pair, so that ties
// are visible in the second component.
struct ComparePairs {
  bool operator()(const Pair& a, const Pair& b) const {
    return a.first < b.first;
  }
};

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

// An input policy that wraps a `VectorInput<size_t>` and additionally records
// every call to `readBlock` and (optionally) throws from the `throwAtRead_`-th
// of them. The recorded state is shared between all copies, because the merge
// takes the input by value.
struct InstrumentedInput {
  using value_type = size_t;
  using Element = size_t;
  using Block = SizeVec;

  // The shared state of all copies of an `InstrumentedInput`.
  struct State {
    // NOTE: The `mutex_` is only there because `InputConcept` requires
    // `readBlock` to be thread-safe. A serial merge only ever reads from the
    // consuming thread.
    std::mutex mutex_{};
    std::vector<std::pair<size_t, size_t>> readBlocks_{};
    // The number of the call to `readBlock` that throws. The value `0` means
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
  Block readBlock(size_t runIdx, size_t blockIdx) const {
    size_t numReads = 0;
    {
      std::lock_guard<std::mutex> lock{state_->mutex_};
      state_->readBlocks_.emplace_back(runIdx, blockIdx);
      numReads = state_->readBlocks_.size();
    }
    if (state_->throwAtRead_ != 0 && numReads >= state_->throwAtRead_) {
      throw std::runtime_error{"readBlock failed"};
    }
    return wrapped_.readBlock(runIdx, blockIdx);
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

static_assert(InputConcept<InstrumentedInput>);

// Return the elements at which a new chunk starts, that is the upper bounds of
// all chunks but the last one.
template <typename Element>
std::vector<Element> chunkStarts(
    const std::vector<ChunkBoundary<Element>>& boundaries) {
  std::vector<Element> result;
  for (const auto& boundary : boundaries) {
    if (boundary.hi_.has_value()) {
      result.push_back(boundary.hi_.value());
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
    ad_utility::SharedCancellationHandle cancellationHandle = nullptr) {
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
      std::move(input), std::move(comparator), std::move(options), nullptr,
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
    // `readBlock` hands out a copy of a block, so the merge never touches the
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
TEST(ParallelBlockMerge, totalNumElements) {
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
TEST(ParallelBlockMerge, chunkStartsAreStrictlyIncreasing) {
  auto runs = makeRandomRuns(20, 100, 200);
  auto input = makeVectorInput(runs, 7);
  for (size_t numChunks : {2u, 3u, 8u, 64u}) {
    auto boundaries = computeChunkBoundaries(input, std::less<>{}, numChunks);
    EXPECT_LE(boundaries.size(), numChunks);
    auto keys = chunkStarts(boundaries);
    for (size_t i = 1; i < keys.size(); ++i) {
      EXPECT_LT(keys[i - 1], keys[i]);
    }
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, blocksWithEqualLastElements) {
  // Four identical runs, each of which consists of the four blocks
  // `[0 ... 0]`, `[1 ... 1]`, `[2 ... 2]` and `[3 ... 3]`. Every one of the
  // four distinct elements is therefore the last element of four different
  // blocks, so the entries of the weighted quantile have to be merged; without
  // that, a single element could start more than one chunk, and
  // the quantiles would be off by up to a factor of the number of runs.
  std::vector<SizeVec> runs;
  for (size_t run = 0; run < 4; ++run) {
    SizeVec elements;
    for (size_t value = 0; value < 4; ++value) {
      elements.insert(elements.end(), 10, value);
    }
    runs.push_back(std::move(elements));
  }
  auto input = makeVectorInput(runs, 10);
  auto boundaries = computeChunkBoundaries(input, std::less<>{}, 4);
  // The accumulated weights are `40`, `80`, `120` and `160`, and the targets
  // for four chunks are `40`, `80` and `120`, so every distinct element but the
  // largest one starts a chunk.
  EXPECT_THAT(chunkStarts(boundaries), ::testing::ElementsAre(0u, 1u, 2u));
  // NOTE: The first chunk `[-infinity, 0)` is empty, which is perfectly legal,
  // see `pickChunkStarts`.
  EXPECT_FALSE(boundaries.at(0).lo_.has_value());
  EXPECT_EQ(boundaries.at(0).hi_, 0u);

  auto expected = sortedConcatenation(runs);
  for (size_t numChunks : {1u, 2u, 4u, 100u}) {
    EXPECT_THAT(mergeToVector(makeVectorInput(runs, 10), std::less<>{},
                              optionsWithBlockSize(7), numChunks),
                ::testing::ElementsAreArray(expected));
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunkBoundaryEdgeCases) {
  // All elements are equal, so there is no way to actually split the input.
  // NOTE: The single chunk start that is picked yields an empty first chunk,
  // see `pickChunkStarts`; the second chunk then holds all the elements.
  {
    std::vector<SizeVec> runs{SizeVec(100, 42u), SizeVec(100, 42u)};
    EXPECT_THAT(chunkStarts(computeChunkBoundaries(makeVectorInput(runs, 7),
                                                   std::less<>{}, 8)),
                ::testing::ElementsAre(42u));
    EXPECT_THAT(mergeToVector(makeVectorInput(runs, 7), std::less<>{},
                              optionsWithBlockSize(7), 8),
                ::testing::ElementsAreArray(SizeVec(200, 42u)));
  }
  // Zero runs, and only empty runs.
  {
    EXPECT_THAT(
        chunkStarts(computeChunkBoundaries(
            makeVectorInput(std::vector<SizeVec>{}, 7), std::less<>{}, 8)),
        ::testing::IsEmpty());
    std::vector<SizeVec> runs{SizeVec{}, SizeVec{}};
    EXPECT_THAT(chunkStarts(computeChunkBoundaries(makeVectorInput(runs, 7),
                                                   std::less<>{}, 8)),
                ::testing::IsEmpty());
  }
  // A single chunk requires no chunk start at all.
  {
    auto runs = makeRandomRuns(4, 50, 50);
    for (size_t numChunks : {0u, 1u}) {
      auto boundaries = computeChunkBoundaries(makeVectorInput(runs, 7),
                                               std::less<>{}, numChunks);
      EXPECT_EQ(boundaries.size(), 1u);
      EXPECT_THAT(chunkStarts(boundaries), ::testing::IsEmpty());
    }
  }
  // More chunks than blocks: the number of chunk starts is bounded by the
  // number of distinct last elements of the blocks.
  {
    std::vector<SizeVec> runs{SizeVec{1, 2, 3, 4, 5, 6}};
    auto keys = chunkStarts(
        computeChunkBoundaries(makeVectorInput(runs, 2), std::less<>{}, 100));
    EXPECT_LE(keys.size(), 3u);
    for (size_t i = 1; i < keys.size(); ++i) {
      EXPECT_LT(keys[i - 1], keys[i]);
    }
  }
  // One huge run and many tiny ones. The chunk starts have to follow the huge
  // run, and the merge still has to be correct.
  {
    std::vector<SizeVec> runs;
    SizeVec huge(10000);
    ql::ranges::generate(huge, [i = size_t{0}]() mutable { return i++; });
    runs.push_back(std::move(huge));
    for (size_t i = 0; i < 50; ++i) {
      runs.push_back(SizeVec{i, i + 1});
    }
    auto keys = chunkStarts(
        computeChunkBoundaries(makeVectorInput(runs, 64), std::less<>{}, 8));
    EXPECT_FALSE(keys.empty());
    EXPECT_LE(keys.size(), 7u);
    // The chunk starts are spread over the whole range of the huge run and are
    // not all crammed into the range of the tiny ones.
    EXPECT_GT(keys.back(), 1000u);
    auto expected = sortedConcatenation(runs);
    auto result = mergeToVector(makeVectorInput(runs, 64), std::less<>{},
                                optionsWithBlockSize(128), 8);
    EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
  }
  // Runs that are empty are simply skipped.
  {
    std::vector<SizeVec> runs{SizeVec{}, SizeVec{1, 2, 3, 4}, SizeVec{},
                              SizeVec{0, 5}};
    auto result = mergeToVector(makeVectorInput(runs, 2), std::less<>{},
                                optionsWithBlockSize(2), 4);
    EXPECT_THAT(result, ::testing::ElementsAre(0u, 1u, 2u, 3u, 4u, 5u));
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunkBoundariesFromExplicitChunkSizes) {
  // A single run with the elements `0 ... 99`, one element per block, so that
  // the chunk starts can be predicted exactly.
  SizeVec run(100);
  ql::ranges::generate(run, [i = size_t{0}]() mutable { return i++; });
  std::vector<SizeVec> runs{run};
  auto input = makeVectorInput(runs, 1);
  auto keysFor = [&input](ChunkSizes chunkSizes) {
    return chunkStarts(
        computeChunkBoundaries(input, std::less<>{}, std::move(chunkSizes)));
  };

  // NOTE: A chunk starts at the largest element that is still needed to reach a
  // target, so a chunk that is supposed to start after `n` elements starts at
  // the element `n - 1`. That is the same convention as for a uniform number of
  // chunks, see `computeChunkBoundaries`, and it is why the chunk sizes below
  // are only exact up to that single element.

  // Uniform chunks of 25 elements each, so the chunks start after `25`, `50`
  // and `75` elements.
  EXPECT_THAT(keysFor(ChunkSizes{{}, 25}),
              ::testing::ElementsAre(24u, 49u, 74u));
  // Three small leading chunks, then chunks of 40, so the chunks start after
  // `5`, `10`, `20` and `60` elements.
  EXPECT_THAT(keysFor(ChunkSizes{{5, 5, 10}, 40}),
              ::testing::ElementsAre(4u, 9u, 19u, 59u));
  // The leading sizes cover the whole input exactly, so no chunk is left for
  // `remainingChunkSize_`.
  EXPECT_THAT(keysFor(ChunkSizes{{50, 50}, 10}), ::testing::ElementsAre(49u));
  // The leading sizes exceed the input, so the surplus ones are dropped.
  EXPECT_THAT(keysFor(ChunkSizes{{30, 500, 7}, 10}),
              ::testing::ElementsAre(29u));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, chunkBoundariesFromChunkSizesEdgeCases) {
  auto runs = makeRandomRuns(4, 50, 50);
  auto input = makeVectorInput(runs, 7);
  auto keysFor = [](const auto& theInput, ChunkSizes chunkSizes) {
    return chunkStarts(
        computeChunkBoundaries(theInput, std::less<>{}, std::move(chunkSizes)));
  };

  // A chunk that is at least as large as the whole input needs no chunk start.
  EXPECT_THAT(keysFor(input, ChunkSizes{{}, 1000}), ::testing::IsEmpty());
  EXPECT_THAT(keysFor(input, ChunkSizes{{1000}, 10}), ::testing::IsEmpty());
  // Zero runs, and only empty runs.
  EXPECT_THAT(
      keysFor(makeVectorInput(std::vector<SizeVec>{}, 7), ChunkSizes{{}, 4}),
      ::testing::IsEmpty());
  std::vector<SizeVec> emptyRuns{SizeVec{}, SizeVec{}};
  EXPECT_THAT(keysFor(makeVectorInput(emptyRuns, 7), ChunkSizes{{}, 4}),
              ::testing::IsEmpty());
  // All elements are equal, so there is no way to actually split the input, no
  // matter which chunk sizes are requested. The single chunk start that is
  // picked yields an empty first chunk, see `pickChunkStarts`.
  std::vector<SizeVec> equalRuns{SizeVec(100, 42u), SizeVec(100, 42u)};
  EXPECT_THAT(keysFor(makeVectorInput(equalRuns, 7), ChunkSizes{{2, 2}, 2}),
              ::testing::ElementsAre(42u));
  // The chunk starts are strictly increasing, also for very small chunks.
  {
    auto keys = keysFor(input, ChunkSizes{{1, 2, 3}, 1});
    for (size_t i = 1; i < keys.size(); ++i) {
      EXPECT_LT(keys[i - 1], keys[i]);
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
TEST(ParallelBlockMerge, chunkBoundariesDescribeTheRangeOfEveryChunk) {
  auto boundaries = detail::chunkBoundariesFromChunkStarts(SizeVec{10, 20});
  ASSERT_EQ(boundaries.size(), 3u);
  // The lower bound of the first and the upper bound of the last chunk are
  // empty, that is minus and plus infinity.
  EXPECT_FALSE(boundaries.at(0).lo_.has_value());
  EXPECT_EQ(boundaries.at(0).hi_, 10u);
  EXPECT_EQ(boundaries.at(1).lo_, 10u);
  EXPECT_EQ(boundaries.at(1).hi_, 20u);
  EXPECT_EQ(boundaries.at(2).lo_, 20u);
  EXPECT_FALSE(boundaries.at(2).hi_.has_value());

  // Without any chunk start there is a single chunk that covers everything,
  // which is also what `singleChunk` yields.
  for (const auto& single : {detail::chunkBoundariesFromChunkStarts(SizeVec{}),
                             singleChunk<size_t>()}) {
    ASSERT_EQ(single.size(), 1u);
    EXPECT_FALSE(single.at(0).lo_.has_value());
    EXPECT_FALSE(single.at(0).hi_.has_value());
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, blockRangeForRun) {
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
  // yields no output block. The boundaries below cannot be produced by
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
  auto mergeWithChunkStarts = [&runs, &expected](SizeVec chunkStarts) {
    auto blocks = serialBlockMergeToRange<false>(
        makeVectorInput(runs, 8), std::less<>{}, optionsWithBlockSize(8),
        nullptr, detail::chunkBoundariesFromChunkStarts(chunkStarts));
    SizeVec result;
    for (const auto& block : blocks) {
      EXPECT_FALSE(block.empty());
      result.insert(result.end(), block.begin(), block.end());
    }
    EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
  };
  // All elements are greater than `100`, so the first five chunks are empty.
  mergeWithChunkStarts(SizeVec{1, 2, 3, 4, 5});
  // Empty chunks at the beginning, in the middle, and at the end.
  mergeWithChunkStarts(SizeVec{1, 2, 1000, 2000, 3000});
  // A single element per chunk, and far more chunks than elements.
  SizeVec manyChunkStarts;
  for (size_t i = 0; i < 300; ++i) {
    manyChunkStarts.push_back(i);
  }
  mergeWithChunkStarts(std::move(manyChunkStarts));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, exceptionFromChunkPropagates) {
  auto runs = makeRandomRuns(16, 200, 300);
  InstrumentedInput input{makeVectorInput(runs, 16)};
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
