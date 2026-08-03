//   Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <iterator>
#include <thread>

#include "backports/algorithm.h"
#include "util/GTestHelpers.h"
#include "util/ParallelExecutor.h"

// _____________________________________________________________________________
TEST(ParallelExecutor, noTasks) { ad_utility::runTasksInParallel({}); }

// _____________________________________________________________________________
TEST(ParallelExecutor, singleTask) {
  bool executed = false;
  std::vector<std::packaged_task<void()>> tasks;
  tasks.push_back(std::packaged_task{[&executed]() { executed = true; }});
  ad_utility::runTasksInParallel(std::move(tasks));
  EXPECT_TRUE(executed);
}

// _____________________________________________________________________________
TEST(ParallelExecutor, multipleTasks) {
  constexpr size_t NUM_TASKS = 10;
  std::array<bool, NUM_TASKS> executed;
  ql::ranges::fill(executed, false);
  std::vector<std::packaged_task<void()>> tasks;
  for (size_t i = 0; i < NUM_TASKS; ++i) {
    tasks.push_back(
        std::packaged_task{[&executed, i]() { executed.at(i) = true; }});
  }
  ad_utility::runTasksInParallel(std::move(tasks));
  for (size_t i = 0; i < NUM_TASKS; ++i) {
    EXPECT_TRUE(executed.at(i));
  }
}

// _____________________________________________________________________________
TEST(ParallelExecutor, multipleTaskWithOneException) {
  constexpr size_t NUM_TASKS = 10;
  std::array<bool, NUM_TASKS> executed;
  ql::ranges::fill(executed, false);
  std::vector<std::packaged_task<void()>> tasks;
  for (size_t i = 0; i < NUM_TASKS; ++i) {
    tasks.push_back(std::packaged_task{[&executed, i]() {
      executed.at(i) = true;
      if (i == 5) {
        throw std::runtime_error("Error");
      }
    }});
  }
  EXPECT_THROW(ad_utility::runTasksInParallel(std::move(tasks)),
               std::runtime_error);
  for (size_t i = 0; i < NUM_TASKS; ++i) {
    EXPECT_TRUE(executed.at(i));
  }
}

// _____________________________________________________________________________
TEST(ParallelExecutor, multipleTaskWithOnlyExceptions) {
  constexpr size_t NUM_TASKS = 10;
  std::array<bool, NUM_TASKS> executed;
  ql::ranges::fill(executed, false);
  std::vector<std::packaged_task<void()>> tasks;
  for (size_t i = 0; i < NUM_TASKS; ++i) {
    tasks.push_back(std::packaged_task{[&executed, i]() {
      executed.at(i) = true;
      throw std::runtime_error(absl::StrCat("Error ", i));
    }});
  }
  // Only the first error should be rethrown for simplicity.
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      ad_utility::runTasksInParallel(std::move(tasks)),
      ::testing::StrEq("Error 0"), std::runtime_error);
  for (size_t i = 0; i < NUM_TASKS; ++i) {
    EXPECT_TRUE(executed.at(i));
  }
}

namespace {
// A result type for `computeInParallelChunks` that simply remembers all the
// chunks it has seen.
struct Chunks {
  std::vector<std::pair<size_t, size_t>> chunks_;
  // The id of the thread that the chunk was computed in, to check whether the
  // work actually happened in the calling thread or in a separate one.
  std::thread::id threadId_{};

  void mergeWith(const Chunks& other) {
    ql::ranges::copy(other.chunks_, std::back_inserter(chunks_));
  }
};

// A chunk function for `computeInParallelChunks` that simply returns the chunk
// `[begin, end)`.
Chunks makeChunk(size_t begin, size_t end) {
  Chunks chunks;
  chunks.chunks_.emplace_back(begin, end);
  chunks.threadId_ = std::this_thread::get_id();
  return chunks;
}

// Check that the `chunks` are consecutive, non-empty, and exactly cover
// `[0, numElements)`. Note: The partial results are merged in the order of the
// chunks, so the chunks are sorted by their begin.
void expectPartitionOf(const Chunks& chunks, size_t numElements) {
  ASSERT_FALSE(chunks.chunks_.empty());
  size_t expectedBegin = 0;
  for (auto [begin, end] : chunks.chunks_) {
    EXPECT_EQ(begin, expectedBegin);
    EXPECT_LT(begin, end);
    expectedBegin = end;
  }
  EXPECT_EQ(expectedBegin, numElements);
}
}  // namespace

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, emptyRangeDoesNothing) {
  Chunks chunks = ad_utility::computeInParallelChunks(0, 1, &makeChunk);
  EXPECT_THAT(chunks.chunks_, ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, illegalChunkSize) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::computeInParallelChunks(10, 0, &makeChunk),
      ::testing::HasSubstr("chunkSize > 0"));
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, singleChunkRunsInCallingThread) {
  // The range is not larger than the chunk size, so there is exactly one chunk
  // which is processed directly, without spawning a thread and without merging.
  for (size_t numElements : {size_t{1}, size_t{41}, size_t{42}}) {
    Chunks chunks =
        ad_utility::computeInParallelChunks(numElements, 42, &makeChunk);
    EXPECT_THAT(chunks.chunks_,
                ::testing::ElementsAre(std::pair{size_t{0}, numElements}));
    EXPECT_EQ(chunks.threadId_, std::this_thread::get_id());
  }
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, multipleChunksArePartitionAndAreMerged) {
  // A chunk size of one means that there is one chunk per element.
  for (size_t numElements : {size_t{2}, size_t{7}, size_t{1000}}) {
    Chunks chunks =
        ad_utility::computeInParallelChunks(numElements, 1, &makeChunk);
    expectPartitionOf(chunks, numElements);
    EXPECT_THAT(chunks.chunks_, ::testing::SizeIs(numElements));
  }
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, chunksHaveTheGivenSize) {
  // All chunks hold exactly 400 elements, except for the last one which holds
  // the remaining 200.
  Chunks chunks = ad_utility::computeInParallelChunks(1000, 400, &makeChunk);
  EXPECT_THAT(chunks.chunks_,
              ::testing::ElementsAre(std::pair{size_t{0}, size_t{400}},
                                     std::pair{size_t{400}, size_t{800}},
                                     std::pair{size_t{800}, size_t{1000}}));
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, threadBudgetIsRespected) {
  size_t numThreads = std::max(1u, std::thread::hardware_concurrency());
  std::atomic<size_t> numRunning = 0;
  std::atomic<bool> tooManyRunning = false;
  // There are 1000 chunks, but never more than one of them per thread runs at
  // the same time.
  Chunks chunks = ad_utility::computeInParallelChunks(
      1000, 1,
      [&numRunning, &tooManyRunning, numThreads](size_t begin, size_t end) {
        if (++numRunning > numThreads) {
          tooManyRunning = true;
        }
        Chunks chunk = makeChunk(begin, end);
        --numRunning;
        return chunk;
      });
  expectPartitionOf(chunks, 1000);
  EXPECT_FALSE(tooManyRunning.load());
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, explicitNumThreadsIsRespected) {
  // With an explicit thread limit of 2, never more than two chunks run at the
  // same time (in contrast to `threadBudgetIsRespected` above, this test does
  // not depend on the hardware concurrency of the machine).
  std::atomic<size_t> numRunning = 0;
  std::atomic<bool> tooManyRunning = false;
  Chunks chunks = ad_utility::computeInParallelChunks(
      100, 1,
      [&numRunning, &tooManyRunning](size_t begin, size_t end) {
        if (++numRunning > 2) {
          tooManyRunning = true;
        }
        Chunks chunk = makeChunk(begin, end);
        --numRunning;
        return chunk;
      },
      2);
  expectPartitionOf(chunks, 100);
  EXPECT_FALSE(tooManyRunning.load());
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, exceptionInChunkIsPropagated) {
  // Every chunk holds exactly one element, and the chunk for the first element
  // throws.
  auto computeChunk = [](size_t begin, size_t end) {
    if (begin == 0) {
      throw std::runtime_error("Error in the first chunk");
    }
    return makeChunk(begin, end);
  };
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      ad_utility::computeInParallelChunks(2, 1, computeChunk),
      ::testing::StrEq("Error in the first chunk"), std::runtime_error);
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, chunksRunInParallel) {
  size_t numThreads = std::thread::hardware_concurrency();
  if (numThreads < 2) {
    GTEST_SKIP() << "Requires at least two hardware threads";
  }
  // All chunks have to run concurrently, else this test deadlocks (which is a
  // failure that a timeout will catch).
  std::atomic<size_t> numStarted = 0;
  Chunks chunks = ad_utility::computeInParallelChunks(
      numThreads, 1, [&numStarted, numThreads](size_t begin, size_t end) {
        ++numStarted;
        while (numStarted.load() < numThreads) {
        }
        return makeChunk(begin, end);
      });
  expectPartitionOf(chunks, numThreads);
  EXPECT_EQ(chunks.chunks_.size(), numThreads);
}
