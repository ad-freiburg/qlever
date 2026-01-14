//   Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

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
