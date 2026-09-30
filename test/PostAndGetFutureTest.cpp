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
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "backports/asio.h"
#include "util/GTestHelpers.h"
#include "util/PostAndGetFuture.h"

namespace {

// A type that is deliberately not default-constructible and not copyable, to
// show that `postAndGetFuture` supports results that
// `ad_utility::runFunctionOnExecutor` cannot transport (see the NOTE at
// `postAndGetFuture`).
struct MoveOnlyNotDefaultConstructible {
  int value_;
  explicit MoveOnlyNotDefaultConstructible(int value) : value_{value} {}
  MoveOnlyNotDefaultConstructible(MoveOnlyNotDefaultConstructible&&) = default;
  MoveOnlyNotDefaultConstructible& operator=(
      MoveOnlyNotDefaultConstructible&&) = default;
};

// A thread pool with `numThreads` threads, together with its executor.
struct Pool {
  boost::asio::thread_pool pool_;
  explicit Pool(size_t numThreads) : pool_{numThreads} {}
  ql::any_io_executor executor() { return pool_.get_executor(); }
};

}  // namespace

// _____________________________________________________________________________
TEST(PostAndGetFuture, returnsTheResultOfTheFunction) {
  Pool pool{2};
  auto future =
      ad_utility::postAndGetFuture(pool.executor(), []() { return 42; });
  EXPECT_EQ(future.get(), 42);
}

// _____________________________________________________________________________
TEST(PostAndGetFuture, runsOnTheExecutorAndNotInTheCallingThread) {
  Pool pool{1};
  auto callingThread = std::this_thread::get_id();
  auto future = ad_utility::postAndGetFuture(
      pool.executor(), []() { return std::this_thread::get_id(); });
  EXPECT_NE(future.get(), callingThread);
}

// _____________________________________________________________________________
TEST(PostAndGetFuture, supportsAVoidResult) {
  Pool pool{1};
  std::atomic<int> counter{0};
  auto future = ad_utility::postAndGetFuture(pool.executor(),
                                             [&counter]() { ++counter; });
  future.get();
  EXPECT_EQ(counter.load(), 1);
}

// _____________________________________________________________________________
TEST(PostAndGetFuture, supportsAResultThatIsNotDefaultConstructible) {
  Pool pool{1};
  auto future = ad_utility::postAndGetFuture(
      pool.executor(), []() { return MoveOnlyNotDefaultConstructible{17}; });
  EXPECT_EQ(future.get().value_, 17);
}

// _____________________________________________________________________________
TEST(PostAndGetFuture, supportsAMoveOnlyFunction) {
  Pool pool{1};
  auto owned = std::make_unique<int>(5);
  auto future = ad_utility::postAndGetFuture(
      pool.executor(), [owned = std::move(owned)]() mutable { return *owned; });
  EXPECT_EQ(future.get(), 5);
}

// _____________________________________________________________________________
TEST(PostAndGetFuture, rethrowsTheExceptionOfTheFunction) {
  Pool pool{1};
  auto future = ad_utility::postAndGetFuture(pool.executor(), []() -> int {
    throw std::runtime_error{"from the pool"};
  });
  AD_EXPECT_THROW_WITH_MESSAGE(future.get(), ::testing::StrEq("from the pool"));
}

// _____________________________________________________________________________
TEST(PostAndGetFuture, manyConcurrentTasks) {
  static constexpr size_t numTasks = 200;
  Pool pool{4};
  std::vector<std::future<size_t>> futures;
  futures.reserve(numTasks);
  for (size_t i = 0; i < numTasks; ++i) {
    futures.push_back(
        ad_utility::postAndGetFuture(pool.executor(), [i]() { return i * i; }));
  }
  for (size_t i = 0; i < numTasks; ++i) {
    EXPECT_EQ(futures[i].get(), i * i);
  }
}
