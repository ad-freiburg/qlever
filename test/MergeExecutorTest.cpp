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

#include <algorithm>
#include <boost/asio/post.hpp>
#include <cstddef>
#include <future>
#include <thread>

#include "util/parallelBlockMerge/MergeExecutor.h"

using namespace ad_utility::parallelBlockMerge;

// _____________________________________________________________________________
TEST(MergeExecutor, defaultParallelismIsAtLeastOne) {
  EXPECT_GE(defaultMergeParallelism(), 1u);
  EXPECT_EQ(defaultMergeParallelism(),
            std::max<size_t>(1, std::thread::hardware_concurrency()));
}

// _____________________________________________________________________________
TEST(MergeExecutor, defaultExecutorIsSharedAndRuns) {
  auto executor = defaultMergeExecutor();
  ASSERT_TRUE(static_cast<bool>(executor));
  // The pool is created only once, so the second call yields the same executor.
  EXPECT_EQ(executor, defaultMergeExecutor());

  // The pool really has threads that run the posted work.
  std::promise<std::thread::id> promise;
  auto future = promise.get_future();
  net::post(executor,
            [&promise] { promise.set_value(std::this_thread::get_id()); });
  EXPECT_NE(future.get(), std::this_thread::get_id());
}
