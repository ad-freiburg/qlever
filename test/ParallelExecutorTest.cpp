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
  // The id of the thread that `body` was called from, to check whether the work
  // actually happened in the calling thread or in a separate one.
  std::thread::id threadId_{};

  void mergeWith(const Chunks& other) {
    ql::ranges::copy(other.chunks_, std::back_inserter(chunks_));
  }
};

// Add the chunk `[begin, end)` to `chunks`.
void addChunk(Chunks& chunks, size_t begin, size_t end) {
  chunks.chunks_.emplace_back(begin, end);
  chunks.threadId_ = std::this_thread::get_id();
}

// Check that the `chunks` are consecutive, non-empty, and exactly cover
// `[0, numElements)`.
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
  Chunks chunks;
  ad_utility::computeInParallelChunks(0, 1, chunks, &addChunk);
  EXPECT_THAT(chunks.chunks_, ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, illegalChunkSize) {
  Chunks chunks;
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::computeInParallelChunks(10, 0, chunks, &addChunk),
      ::testing::HasSubstr("minChunkSize > 0"));
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, singleChunkRunsInCallingThread) {
  // The range is too small to be split into two chunks of at least 42 elements,
  // so there is exactly one chunk which is processed directly, without spawning
  // a thread and without merging.
  for (size_t numElements : {size_t{1}, size_t{42}, size_t{83}}) {
    Chunks chunks;
    ad_utility::computeInParallelChunks(numElements, 42, chunks, &addChunk);
    EXPECT_THAT(chunks.chunks_,
                ::testing::ElementsAre(std::pair{size_t{0}, numElements}));
    EXPECT_EQ(chunks.threadId_, std::this_thread::get_id());
  }
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, multipleChunksArePartitionAndAreMerged) {
  // A minimal chunk size of one means that the number of chunks is only limited
  // by the hardware concurrency, which we don't know, so we only check the
  // properties that hold for any number of chunks.
  for (size_t numElements : {size_t{2}, size_t{7}, size_t{1000}}) {
    Chunks chunks;
    ad_utility::computeInParallelChunks(numElements, 1, chunks, &addChunk);
    // The chunks are merged in order, so they are sorted by their begin.
    expectPartitionOf(chunks, numElements);
  }
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, chunksRespectTheMinimalChunkSize) {
  Chunks chunks;
  // 1000 elements with a minimal chunk size of 400 give at most two chunks, no
  // matter how many threads are available. Note that the remainder is
  // distributed over the chunks, so the chunks are larger than 400.
  ad_utility::computeInParallelChunks(1000, 400, chunks, &addChunk);
  expectPartitionOf(chunks, 1000);
  EXPECT_THAT(chunks.chunks_, ::testing::SizeIs(::testing::Le(2u)));
  for (auto [begin, end] : chunks.chunks_) {
    EXPECT_GE(end - begin, 400u);
  }
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, exceptionInChunkIsPropagated) {
  Chunks chunks;
  // Every chunk holds exactly one element, and the chunk for the first element
  // throws.
  auto body = [](Chunks& chunkResult, size_t begin, size_t end) {
    if (begin == 0) {
      throw std::runtime_error("Error in the first chunk");
    }
    addChunk(chunkResult, begin, end);
  };
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      ad_utility::computeInParallelChunks(2, 1, chunks, body),
      ::testing::StrEq("Error in the first chunk"), std::runtime_error);
  // Nothing was merged, because the exception is rethrown before the merging.
  EXPECT_THAT(chunks.chunks_, ::testing::IsEmpty());
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
  Chunks chunks;
  ad_utility::computeInParallelChunks(
      numThreads, 1, chunks,
      [&numStarted, numThreads](Chunks& chunkResult, size_t begin, size_t end) {
        ++numStarted;
        while (numStarted.load() < numThreads) {
        }
        addChunk(chunkResult, begin, end);
      });
  expectPartitionOf(chunks, numThreads);
  EXPECT_EQ(chunks.chunks_.size(), numThreads);
}
