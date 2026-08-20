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
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"
#include "util/ParallelBlockMerge.h"
#include "util/Random.h"
#include "util/jthread.h"

using namespace ad_utility::parallelBlockMerge;

namespace {
using Block = std::vector<int>;
using Sink = InOrderBlockSink<Block>;

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

static_assert(BlockSink<Sink, Block>);
static_assert(!BlockSink<NotAnInput, Block>);

// Consume all blocks of the `sink` and return them in a single vector.
std::vector<Block> collect(Sink& sink) {
  std::vector<Block> result;
  for (auto& block : sink.blocks()) {
    result.push_back(std::move(block));
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(ParallelBlockMerge, InlineMergeSchedulerRunsTasksInline) {
  InlineMergeScheduler scheduler;
  EXPECT_EQ(scheduler.maxParallelism(), 1u);
  auto callingThread = std::this_thread::get_id();
  bool wasRun = false;
  std::thread::id threadOfTask;
  scheduler.schedule([&wasRun, &threadOfTask] {
    wasRun = true;
    threadOfTask = std::this_thread::get_id();
  });
  // The task has already run when `schedule` returns, and it ran in the calling
  // thread.
  EXPECT_TRUE(wasRun);
  EXPECT_EQ(threadOfTask, callingThread);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, TaskQueueMergeSchedulerRunsAllTasks) {
  static constexpr size_t numTasks = 100;
  std::atomic<size_t> counter = 0;
  {
    TaskQueueMergeScheduler scheduler{4};
    EXPECT_EQ(scheduler.maxParallelism(), 4u);
    for (size_t i = 0; i < numTasks; ++i) {
      scheduler.schedule([&counter] { ++counter; });
    }
    // The destructor of the scheduler waits for all tasks to complete.
  }
  EXPECT_EQ(counter, numTasks);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, TaskQueueMergeSchedulerDefaultNumThreads) {
  TaskQueueMergeScheduler scheduler;
  EXPECT_GE(scheduler.maxParallelism(), 1u);
  EXPECT_NE(defaultMergeScheduler(), nullptr);
  // The default scheduler is a singleton.
  EXPECT_EQ(defaultMergeScheduler(), defaultMergeScheduler());
  EXPECT_GE(defaultMergeScheduler()->maxParallelism(), 1u);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, BorrowedTaskQueueMergeScheduler) {
  ad_utility::TaskQueue<false> queue{8, 2, "borrowed"};
  EXPECT_EQ(queue.maxQueueSize(), 8u);
  std::atomic<size_t> counter = 0;
  {
    BorrowedTaskQueueMergeScheduler scheduler{queue, 2};
    EXPECT_EQ(scheduler.maxParallelism(), 2u);
    for (size_t i = 0; i < 10; ++i) {
      scheduler.schedule([&counter] { ++counter; });
    }
    queue.finish();
  }
  EXPECT_EQ(counter, 10u);
  // The borrowed queue must be able to hold all in-flight tasks.
  ad_utility::TaskQueue<false> tooSmall{1, 1, "tooSmall"};
  EXPECT_ANY_THROW(BorrowedTaskQueueMergeScheduler(tooSmall, 2));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, InOrderBlockSinkSingleThreaded) {
  Sink sink{2};
  sink.setNumChunks(2);
  sink(0, Block{1, 2});
  sink.finishChunk(0);
  sink(1, Block{3});
  sink(1, Block{4});
  sink.finishChunk(1);
  EXPECT_THAT(collect(sink),
              ::testing::ElementsAre(Block{1, 2}, Block{3}, Block{4}));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, InOrderBlockSinkEmpty) {
  Sink sink;
  sink.setNumChunks(0);
  EXPECT_THAT(collect(sink), ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, InOrderBlockSinkMultiThreadedOutOfOrder) {
  static constexpr size_t numChunks = 8;
  static constexpr size_t numBlocksPerChunk = 20;
  Sink sink{3};
  sink.setNumChunks(numChunks);

  // The producers deliberately start with the *highest* chunk index, so that
  // the consumer has to reorder them.
  std::vector<ad_utility::JThread> producers;
  for (size_t i = 0; i < numChunks; ++i) {
    size_t chunkIndex = numChunks - 1 - i;
    producers.emplace_back([&sink, chunkIndex] {
      for (size_t j = 0; j < numBlocksPerChunk; ++j) {
        sink(chunkIndex,
             Block{static_cast<int>(chunkIndex), static_cast<int>(j)});
      }
      sink.finishChunk(chunkIndex);
    });
  }

  auto blocks = collect(sink);
  producers.clear();

  ASSERT_EQ(blocks.size(), numChunks * numBlocksPerChunk);
  // The blocks appear in strict chunk order, and in the order of their
  // creation within a chunk.
  for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    for (size_t j = 0; j < numBlocksPerChunk; ++j) {
      const auto& block = blocks.at(chunkIndex * numBlocksPerChunk + j);
      EXPECT_THAT(block, ::testing::ElementsAre(static_cast<int>(chunkIndex),
                                                static_cast<int>(j)));
    }
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, InOrderBlockSinkPushExceptionSurfaces) {
  Sink sink{2};
  sink.setNumChunks(2);
  sink(0, Block{1});
  ad_utility::JThread producer{[&sink] {
    try {
      throw std::runtime_error{"expected error"};
    } catch (...) {
      sink.pushException(std::current_exception());
    }
  }};
  producer.join();
  auto blocks = sink.blocks();
  AD_EXPECT_THROW_WITH_MESSAGE(([&blocks] {
                                 for ([[maybe_unused]] auto& block : blocks) {
                                 }
                               })(),
                               ::testing::HasSubstr("expected error"));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, InOrderBlockSinkExceptionUnblocksProducers) {
  // A producer that is blocked on a full buffer is unblocked by an exception
  // that is pushed by another producer.
  Sink sink{1};
  sink.setNumChunks(2);
  ad_utility::JThread blockedProducer{[&sink] {
    for (size_t i = 0; i < 100; ++i) {
      sink(1, Block{static_cast<int>(i)});
    }
  }};
  try {
    throw std::runtime_error{"other error"};
  } catch (...) {
    sink.pushException(std::current_exception());
  }
  // This must terminate.
  blockedProducer.join();
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, InOrderBlockSinkBackPressure) {
  // With a buffer of a single block per chunk, the producers of the higher
  // chunks are blocked most of the time. The test must still terminate.
  static constexpr size_t numChunks = 4;
  static constexpr size_t numBlocksPerChunk = 50;
  Sink sink{1};
  sink.setNumChunks(numChunks);

  std::vector<ad_utility::JThread> producers;
  for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    producers.emplace_back([&sink, chunkIndex] {
      for (size_t j = 0; j < numBlocksPerChunk; ++j) {
        sink(chunkIndex, Block{static_cast<int>(chunkIndex)});
      }
      sink.finishChunk(chunkIndex);
    });
  }

  auto blocks = collect(sink);
  producers.clear();
  ASSERT_EQ(blocks.size(), numChunks * numBlocksPerChunk);
  for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    for (size_t j = 0; j < numBlocksPerChunk; ++j) {
      EXPECT_THAT(blocks.at(chunkIndex * numBlocksPerChunk + j),
                  ::testing::ElementsAre(static_cast<int>(chunkIndex)));
    }
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, InOrderBlockSinkAbortUnblocksProducers) {
  // A consumer that stops early must not leave a blocked producer behind. The
  // destructor of the sink aborts, so the producer terminates.
  std::atomic<bool> producerIsDone = false;
  {
    Sink sink{1};
    sink.setNumChunks(1);
    ad_utility::JThread producer{[&sink, &producerIsDone] {
      for (size_t i = 0; i < 1000; ++i) {
        sink(0, Block{static_cast<int>(i)});
      }
      producerIsDone = true;
    }};
    // Consume a single block, then abort.
    auto blocks = sink.blocks();
    auto it = blocks.begin();
    ASSERT_NE(it, blocks.end());
    sink.abort();
    producer.join();
  }
  EXPECT_TRUE(producerIsDone);
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, MergeOptionsDefaults) {
  MergeOptions options;
  EXPECT_EQ(options.outputBlockSize, DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_SIZE);
  EXPECT_EQ(options.maxOutputBlockMemory,
            DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_MEMORY);
  EXPECT_EQ(options.targetChunksPerThread,
            DEFAULT_PARALLEL_MERGE_CHUNKS_PER_THREAD);
  EXPECT_EQ(options.maxInFlightChunks, 0u);
  EXPECT_EQ(options.serialNumRunsThreshold, 2u);
  EXPECT_EQ(options.serialNumElementsThreshold,
            DEFAULT_PARALLEL_MERGE_SERIAL_ELEMENT_THRESHOLD);
  EXPECT_EQ(options.bufferedBlocksPerChunk, 2u);
  EXPECT_FALSE(options.stableTieBreaking);
}

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
  options.outputBlockSize = outputBlockSize;
  options.serialNumRunsThreshold = 0;
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
  options.outputBlockSize = 3;
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
  options.outputBlockSize = 3;

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
    auto splitters = computeSplitters(input, std::less<>{}, numChunks);
    EXPECT_LE(splitters.size(), numChunks - 1);
    for (size_t i = 1; i < splitters.size(); ++i) {
      EXPECT_LT(splitters[i - 1], splitters[i]);
    }
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, splitterEdgeCases) {
  // All keys are equal, so there is no way to split the input.
  {
    std::vector<SizeVec> runs{SizeVec(100, 42u), SizeVec(100, 42u)};
    EXPECT_THAT(computeSplitters(SizeRuns{runs, 7}, std::less<>{}, 8),
                ::testing::IsEmpty());
  }
  // Zero runs, and only empty runs.
  {
    EXPECT_THAT(computeSplitters(SizeRuns{{}, 7}, std::less<>{}, 8),
                ::testing::IsEmpty());
    std::vector<SizeVec> runs{SizeVec{}, SizeVec{}};
    EXPECT_THAT(computeSplitters(SizeRuns{runs, 7}, std::less<>{}, 8),
                ::testing::IsEmpty());
  }
  // A single chunk requires no splitters at all.
  {
    auto runs = makeRandomRuns(4, 50, 50);
    EXPECT_THAT(computeSplitters(SizeRuns{runs, 7}, std::less<>{}, 1),
                ::testing::IsEmpty());
    EXPECT_THAT(computeSplitters(SizeRuns{runs, 7}, std::less<>{}, 0),
                ::testing::IsEmpty());
  }
  // More chunks than blocks: the number of splitters is bounded by the number
  // of distinct block keys.
  {
    std::vector<SizeVec> runs{SizeVec{1, 2, 3, 4, 5, 6}};
    auto splitters = computeSplitters(SizeRuns{runs, 2}, std::less<>{}, 100);
    EXPECT_LE(splitters.size(), 3u);
    for (size_t i = 1; i < splitters.size(); ++i) {
      EXPECT_LT(splitters[i - 1], splitters[i]);
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
    auto splitters = computeSplitters(input, std::less<>{}, 8);
    EXPECT_FALSE(splitters.empty());
    EXPECT_LE(splitters.size(), 7u);
    // The splitters are spread over the whole range of the huge run and are not
    // all crammed into the range of the tiny ones.
    EXPECT_GT(splitters.back(), 1000u);
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
TEST(ParallelBlockMerge, chunkBoundaryPredicatesDoNotReadSuperfluousBlocks) {
  // A single run with the three blocks `[0, 1, 2]`, `[3, 4, 5]`, `[6, 7, 8]`.
  SizeVec run{0, 1, 2, 3, 4, 5, 6, 7, 8};
  const std::less<> comparator;
  MergeOptions options;
  options.outputBlockSize = 100;

  // The upper bound `3` is exactly the first key of the second block, so that
  // block must not be read. This pins the exact form of the predicate for the
  // end of the block range.
  {
    InstrumentedInput input{SizeRuns{{run}, 3}};
    detail::ChunkMerger<false, InstrumentedInput, std::less<>> merger{
        input,  comparator, options, std::nullopt, std::optional<size_t>{3},
        nullptr};
    auto block = merger.nextBlock();
    ASSERT_TRUE(block.has_value());
    EXPECT_THAT(block.value(), ::testing::ElementsAre(0u, 1u, 2u));
    EXPECT_FALSE(merger.nextBlock().has_value());
    EXPECT_THAT(input.state_->readBlocks_,
                ::testing::ElementsAre(::testing::Pair(0u, 0u)));
  }
  // Symmetrically, the lower bound `3` is greater than the last key of the
  // first block, so that block must not be read either.
  {
    InstrumentedInput input{SizeRuns{{run}, 3}};
    detail::ChunkMerger<false, InstrumentedInput, std::less<>> merger{
        input,        comparator, options, std::optional<size_t>{3},
        std::nullopt, nullptr};
    auto block = merger.nextBlock();
    ASSERT_TRUE(block.has_value());
    EXPECT_THAT(block.value(), ::testing::ElementsAre(3u, 4u, 5u, 6u, 7u, 8u));
    EXPECT_FALSE(merger.nextBlock().has_value());
    EXPECT_THAT(input.state_->readBlocks_,
                ::testing::ElementsAre(::testing::Pair(0u, 1u),
                                       ::testing::Pair(0u, 2u)));
  }
  // A bound that lies inside a block trims that block, and only that block is
  // read.
  {
    InstrumentedInput input{SizeRuns{{run}, 3}};
    detail::ChunkMerger<false, InstrumentedInput, std::less<>> merger{
        input,
        comparator,
        options,
        std::optional<size_t>{4},
        std::optional<size_t>{5},
        nullptr};
    auto block = merger.nextBlock();
    ASSERT_TRUE(block.has_value());
    EXPECT_THAT(block.value(), ::testing::ElementsAre(4u));
    EXPECT_FALSE(merger.nextBlock().has_value());
    EXPECT_THAT(input.state_->readBlocks_,
                ::testing::ElementsAre(::testing::Pair(0u, 1u)));
  }
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, stableTieBreaking) {
  // Use pairs of which only the first component is compared, so that the order
  // of the tied elements is visible in the result.
  auto runs = makeTiedPairRuns();
  auto merge = [&runs](SharedMergeScheduler scheduler) {
    MergeOptions options = parallelOptions(64);
    options.targetChunksPerThread = 3;
    options.stableTieBreaking = true;
    return mergeToVector(PairRuns{runs, 32}, ComparePairs{}, options,
                         std::move(scheduler));
  };

  auto serial = merge(std::make_shared<InlineMergeScheduler>());
  ASSERT_EQ(serial.size(), numTiedRuns * numTiedElementsPerRun);
  EXPECT_TRUE(ql::ranges::is_sorted(serial, ComparePairs{}));
  // With `stableTieBreaking` the result is the *stable* merge of the runs, so
  // the ties are resolved in exactly the same way for every number of chunks.
  EXPECT_THAT(merge(makeScheduler(2)), ::testing::ElementsAreArray(serial));
  EXPECT_THAT(merge(makeScheduler(8)), ::testing::ElementsAreArray(serial));
  // The ties are broken by the index of the run, and within a run the original
  // order is preserved, so the result is exactly `std::stable_sort`.
  auto expected = concatenation(runs);
  ql::ranges::stable_sort(expected, ComparePairs{});
  EXPECT_THAT(serial, ::testing::ElementsAreArray(expected));
}

// _____________________________________________________________________________
TEST(ParallelBlockMerge, deterministicAcrossParallelism) {
  // The default configuration does not break ties by the index of the run, so
  // the guarantee is a weaker one: the result is fully determined by the input
  // and the configuration. Without any ties that means that the result is
  // identical for every number of chunks.
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

  // With ties and the default (unstable) tie breaking, a *fixed* configuration
  // is still perfectly reproducible, also across repeated runs with different
  // thread schedules.
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
  // Two runs are below the threshold, so the merge is serial.
  options.serialNumRunsThreshold = 2;
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
  options.maxOutputBlockMemory =
      ad_utility::MemorySize::bytes(3 * sizeof(size_t));
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
        nullptr, std::move(splitters), 2)};
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
