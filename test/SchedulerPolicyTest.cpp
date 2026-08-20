// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <thread>

#include "util/TaskQueue.h"
#include "util/parallelBlockMerge/SchedulerPolicy.h"

using namespace ad_utility::parallelBlockMerge;

// _____________________________________________________________________________
TEST(SchedulerPolicy, InlineMergeSchedulerRunsTasksInline) {
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
TEST(SchedulerPolicy, TaskQueueMergeSchedulerRunsAllTasks) {
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
TEST(SchedulerPolicy, TaskQueueMergeSchedulerDefaultNumThreads) {
  TaskQueueMergeScheduler scheduler;
  EXPECT_GE(scheduler.maxParallelism(), 1u);
  EXPECT_NE(defaultMergeScheduler(), nullptr);
  // The default scheduler is a singleton.
  EXPECT_EQ(defaultMergeScheduler(), defaultMergeScheduler());
  EXPECT_GE(defaultMergeScheduler()->maxParallelism(), 1u);
}

// _____________________________________________________________________________
TEST(SchedulerPolicy, BorrowedTaskQueueMergeScheduler) {
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
