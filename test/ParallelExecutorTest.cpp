//   Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>
#include <chrono>
#include <future>
#include <iterator>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "backports/algorithm.h"
#include "util/GTestHelpers.h"
#include "util/HashSet.h"
#include "util/ParallelExecutor.h"

// _____________________________________________________________________________
TEST(ParallelExecutor, noTasks) { ad_utility::runTasksInParallel<void>({}); }

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
TEST(ParallelExecutor, tasksWithResultsAreCollectedInOrder) {
  constexpr size_t NUM_TASKS = 10;
  std::vector<std::packaged_task<size_t()>> tasks;
  for (size_t i = 0; i < NUM_TASKS; ++i) {
    tasks.push_back(std::packaged_task<size_t()>{[i]() { return i * i; }});
  }
  std::vector<size_t> results =
      ad_utility::runTasksInParallel(std::move(tasks));
  ASSERT_EQ(results.size(), NUM_TASKS);
  for (size_t i = 0; i < NUM_TASKS; ++i) {
    EXPECT_EQ(results.at(i), i * i);
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

// A chunk function for `computeInParallelChunks` that simply records the chunk
// `[begin, end)`.
void addChunk(Chunks& chunks, size_t begin, size_t end) {
  chunks.chunks_.emplace_back(begin, end);
  chunks.threadId_ = std::this_thread::get_id();
}

// Check that the `chunks` are consecutive, non-empty, and exactly cover
// `[0, numElements)`. Note: The chunks are handed out to the threads
// dynamically, so they are recorded in an unspecified order and have to be
// sorted first.
void expectPartitionOf(Chunks chunks, size_t numElements) {
  ASSERT_FALSE(chunks.chunks_.empty());
  ql::ranges::sort(chunks.chunks_);
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
  Chunks chunks = ad_utility::computeInParallelChunks(0, 1, &addChunk);
  EXPECT_THAT(chunks.chunks_, ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, illegalChunkSize) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::computeInParallelChunks(10, 0, &addChunk),
      ::testing::HasSubstr("chunkSize > 0"));
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, singleChunkRunsInCallingThread) {
  // The range is not larger than the chunk size, so there is exactly one chunk
  // which is processed directly, without spawning a thread and without merging.
  for (size_t numElements : {size_t{1}, size_t{41}, size_t{42}}) {
    Chunks chunks =
        ad_utility::computeInParallelChunks(numElements, 42, &addChunk);
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
        ad_utility::computeInParallelChunks(numElements, 1, &addChunk);
    expectPartitionOf(chunks, numElements);
    EXPECT_THAT(chunks.chunks_, ::testing::SizeIs(numElements));
  }
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, chunksHaveTheGivenSize) {
  // All chunks hold exactly 400 elements, except for the last one which holds
  // the remaining 200.
  Chunks chunks = ad_utility::computeInParallelChunks(1000, 400, &addChunk);
  EXPECT_THAT(chunks.chunks_, ::testing::UnorderedElementsAre(
                                  std::pair{size_t{0}, size_t{400}},
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
      [&numRunning, &tooManyRunning, numThreads](Chunks& chunks, size_t begin,
                                                 size_t end) {
        if (++numRunning > numThreads) {
          tooManyRunning = true;
        }
        addChunk(chunks, begin, end);
        --numRunning;
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
      [&numRunning, &tooManyRunning](Chunks& chunks, size_t begin, size_t end) {
        if (++numRunning > 2) {
          tooManyRunning = true;
        }
        addChunk(chunks, begin, end);
        --numRunning;
      },
      2);
  expectPartitionOf(chunks, 100);
  EXPECT_FALSE(tooManyRunning.load());
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, onePartialResultPerThreadAndNotPerChunk) {
  // There are 1000 chunks, but only two threads, so the chunks are folded into
  // at most two partial results. This is what keeps the number of (sequential)
  // merges independent of the chunk size.
  std::mutex mutex;
  ad_utility::HashSet<const Chunks*> partialResults;
  Chunks chunks = ad_utility::computeInParallelChunks(
      1000, 1,
      [&mutex, &partialResults](Chunks& chunks, size_t begin, size_t end) {
        {
          std::lock_guard<std::mutex> lock{mutex};
          partialResults.insert(&chunks);
        }
        addChunk(chunks, begin, end);
      },
      2);
  expectPartitionOf(chunks, 1000);
  EXPECT_THAT(partialResults, ::testing::SizeIs(::testing::Le(2u)));
}

// _____________________________________________________________________________
TEST(ComputeInParallelChunks, exceptionInChunkIsPropagated) {
  // Every chunk holds exactly one element, and the chunk for the first element
  // throws.
  auto computeChunk = [](Chunks& chunks, size_t begin, size_t end) {
    if (begin == 0) {
      throw std::runtime_error("Error in the first chunk");
    }
    addChunk(chunks, begin, end);
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
      numThreads, 1,
      [&numStarted, numThreads](Chunks& chunks, size_t begin, size_t end) {
        ++numStarted;
        while (numStarted.load() < numThreads) {
        }
        addChunk(chunks, begin, end);
      });
  expectPartitionOf(chunks, numThreads);
  EXPECT_EQ(chunks.chunks_.size(), numThreads);
}

namespace {
// A small thread pool for the tests of `runIndexedTasksOnExecutor`.
constexpr size_t numPoolThreads = 4;
}  // namespace

// _____________________________________________________________________________
TEST(ParallelExecutor, runIndexedTasksOnExecutorRunsEachTaskOnce) {
  boost::asio::thread_pool pool{numPoolThreads};
  auto run = [&pool](size_t numTasks, const auto& runTask) {
    ad_utility::runIndexedTasksOnExecutor(pool.get_executor(), numPoolThreads,
                                          numTasks, runTask);
  };
  // Without any tasks, nothing is run.
  run(0, [](size_t) { FAIL() << "No task must be run"; });

  for (size_t numTasks : {1, 2, 7, 1000}) {
    std::vector<std::atomic<size_t>> numCalls(numTasks);
    run(numTasks,
        [&numCalls](size_t taskIdx) { numCalls.at(taskIdx).fetch_add(1); });
    for (size_t i = 0; i < numTasks; ++i) {
      EXPECT_EQ(numCalls.at(i).load(), 1u) << i;
    }
  }
}

// _____________________________________________________________________________
TEST(ParallelExecutor, runIndexedTasksOnExecutorRethrowsAfterAllTasksAreDone) {
  boost::asio::thread_pool pool{numPoolThreads};
  constexpr size_t numTasks = 1000;
  std::atomic<size_t> numStarted = 0;
  std::atomic<size_t> numFinished = 0;
  auto runTask = [&](size_t taskIdx) {
    numStarted.fetch_add(1);
    absl::Cleanup finish = [&numFinished] { numFinished.fetch_add(1); };
    if (taskIdx == 5) {
      throw std::runtime_error("task 5 failed");
    }
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::runIndexedTasksOnExecutor(pool.get_executor(), numPoolThreads,
                                            numTasks, runTask),
      ::testing::HasSubstr("task 5 failed"));
  // No task is still running when the exception arrives, and the tasks that
  // were not started before the exception are skipped.
  EXPECT_EQ(numStarted.load(), numFinished.load());
  EXPECT_LT(numStarted.load(), numTasks);
}

// _____________________________________________________________________________
// The calling thread of `runIndexedTasksOnExecutor` also runs tasks itself, so
// it completes even when it is called from *all* the threads of the executor
// at once, such that no thread of the executor is left for the tasks.
TEST(ParallelExecutor, runIndexedTasksOnExecutorFromAllThreadsOfThePool) {
  boost::asio::thread_pool pool{numPoolThreads};
  std::atomic<size_t> numArrived = 0;
  std::atomic<size_t> numTasksRun = 0;
  constexpr size_t numTasksPerCaller = 100;
  std::vector<std::future<void>> futures;
  for ([[maybe_unused]] size_t i : ql::views::iota(size_t{0}, numPoolThreads)) {
    futures.push_back(boost::asio::post(
        pool.get_executor(), boost::asio::use_future([&]() {
          // Wait until every thread of the pool is occupied by one of these
          // callers.
          numArrived.fetch_add(1);
          while (numArrived.load() < numPoolThreads) {
            std::this_thread::yield();
          }
          ad_utility::runIndexedTasksOnExecutor(
              pool.get_executor(), numPoolThreads, numTasksPerCaller,
              [&numTasksRun](size_t) { numTasksRun.fetch_add(1); });
        })));
  }
  for (auto& future : futures) {
    ASSERT_EQ(future.wait_for(std::chrono::minutes(1)),
              std::future_status::ready);
    future.get();
  }
  EXPECT_EQ(numTasksRun.load(), numPoolThreads * numTasksPerCaller);
}
