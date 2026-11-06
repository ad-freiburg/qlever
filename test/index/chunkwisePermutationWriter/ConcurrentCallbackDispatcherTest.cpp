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

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <memory>
#include <optional>
#include <stdexcept>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "index/chunkwisePermutationWriter/ConcurrentCallbackDispatcher.h"

// NOTE: The dispatcher only exists in the C++20 mode of the build.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace {
using namespace chunkwisePermutationWriter;

// Return a block with a single row.
SharedBlock makeSharedBlock() {
  Block block{1, ad_utility::makeUnlimitedAllocator<Id>()};
  block.resize(1);
  block(0, 0) = Id::makeFromInt(42);
  return std::make_shared<const Block>(std::move(block));
}

// A dispatcher on a single-threaded `io_context` (so the tests are
// deterministic), together with everything that its callbacks and its
// `ReportException` function collect. The callbacks store their `done`
// functions instead of calling them, so the tests decide when a callback is
// done.
class DispatcherFixture {
 public:
  boost::asio::io_context ioContext_;
  std::vector<DoneCallback> pendingDones_;
  std::vector<std::exception_ptr> reportedExceptions_;
  std::shared_ptr<ConcurrentCallbackDispatcher> dispatcher_;

  // Create a dispatcher with `numCallbacks` callbacks (which store their
  // `done`) and the given bound on the number of blocks in flight.
  DispatcherFixture(size_t numCallbacks, size_t maxNumBlocksInFlight) {
    std::vector<ConcurrentBlockCallback> callbacks;
    for (size_t i = 0; i < numCallbacks; ++i) {
      callbacks.push_back([this](SharedBlock block, DoneCallback done) {
        EXPECT_NE(block, nullptr);
        pendingDones_.push_back(std::move(done));
      });
    }
    dispatcher_ = std::make_shared<ConcurrentCallbackDispatcher>(
        ioContext_.get_executor(), std::move(callbacks), maxNumBlocksInFlight,
        [this](std::exception_ptr exception) {
          reportedExceptions_.push_back(std::move(exception));
        });
  }

  // Start an `asyncDispatch` of a block, and return the place where its
  // result is stored once it is complete.
  std::shared_ptr<std::optional<bool>> dispatch() {
    auto result = std::make_shared<std::optional<bool>>();
    boost::asio::co_spawn(
        ioContext_, dispatcher_->asyncDispatch(makeSharedBlock(), dispatcher_),
        [result](std::exception_ptr exception, bool value) {
          EXPECT_EQ(exception, nullptr);
          *result = value;
        });
    ioContext_.poll();
    ioContext_.restart();
    return result;
  }

  // Invoke the oldest pending `done` with the given `exception`.
  void finishOldest(std::exception_ptr exception = nullptr) {
    ASSERT_FALSE(pendingDones_.empty());
    auto done = std::move(pendingDones_.front());
    pendingDones_.erase(pendingDones_.begin());
    done(std::move(exception));
    ioContext_.poll();
    ioContext_.restart();
  }
};
}  // namespace

// Test that without callbacks, `asyncDispatch` completes right away and
// nothing is in flight.
TEST(ConcurrentCallbackDispatcher, noCallbacks) {
  DispatcherFixture fixture{0, 1};
  EXPECT_TRUE(fixture.dispatcher_->empty());
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(*fixture.dispatch(), true);
  }
  fixture.dispatcher_->waitUntilIdle();
}

// Test that the number of blocks in flight is bounded, and that a block is in
// flight until all of its callbacks are done.
TEST(ConcurrentCallbackDispatcher, boundedNumberOfBlocksInFlight) {
  DispatcherFixture fixture{2, 2};
  EXPECT_FALSE(fixture.dispatcher_->empty());
  auto first = fixture.dispatch();
  auto second = fixture.dispatch();
  auto third = fixture.dispatch();
  EXPECT_EQ(*first, true);
  EXPECT_EQ(*second, true);
  // The third block has to wait for a free slot.
  EXPECT_FALSE(third->has_value());
  EXPECT_EQ(fixture.pendingDones_.size(), 4u);
  // A single callback of the first block is not enough.
  fixture.finishOldest();
  EXPECT_FALSE(third->has_value());
  fixture.finishOldest();
  EXPECT_EQ(*third, true);
  EXPECT_EQ(fixture.pendingDones_.size(), 4u);
  while (!fixture.pendingDones_.empty()) {
    fixture.finishOldest();
  }
  fixture.dispatcher_->waitUntilIdle();
  EXPECT_TRUE(fixture.reportedExceptions_.empty());
}

// Test that the exceptions of the callbacks are reported, both the ones that
// are passed to `done` and the ones that are thrown, and that a throwing
// callback is treated as done.
TEST(ConcurrentCallbackDispatcher, exceptionsAreReported) {
  boost::asio::io_context ioContext;
  std::vector<std::exception_ptr> reported;
  std::vector<ConcurrentBlockCallback> callbacks;
  callbacks.push_back(
      []([[maybe_unused]] SharedBlock block, DoneCallback done) {
        done(std::make_exception_ptr(std::runtime_error{"via done"}));
      });
  callbacks.push_back([]([[maybe_unused]] SharedBlock block,
                         [[maybe_unused]] DoneCallback done) {
    throw std::runtime_error{"thrown"};
  });
  auto dispatcher = std::make_shared<ConcurrentCallbackDispatcher>(
      ioContext.get_executor(), std::move(callbacks), 1,
      [&reported](std::exception_ptr exception) {
        reported.push_back(std::move(exception));
      });
  for (size_t i = 0; i < 2; ++i) {
    std::optional<bool> result;
    boost::asio::co_spawn(
        ioContext, dispatcher->asyncDispatch(makeSharedBlock(), nullptr),
        [&result](std::exception_ptr, bool value) { result = value; });
    ioContext.poll();
    ioContext.restart();
    // The single slot is free again, because both callbacks are done.
    EXPECT_EQ(result, true);
  }
  dispatcher->waitUntilIdle();
  ASSERT_EQ(reported.size(), 4u);
  AD_EXPECT_THROW_WITH_MESSAGE(std::rethrow_exception(reported.at(0)),
                               ::testing::StrEq("via done"));
  AD_EXPECT_THROW_WITH_MESSAGE(std::rethrow_exception(reported.at(1)),
                               ::testing::StrEq("thrown"));
}

// Test that `cancel` completes a waiting `asyncDispatch` with `false`, that
// later calls return `false` without invoking any callback, and that the
// callbacks that are in flight are not affected.
TEST(ConcurrentCallbackDispatcher, cancel) {
  DispatcherFixture fixture{1, 1};
  auto first = fixture.dispatch();
  auto second = fixture.dispatch();
  EXPECT_EQ(*first, true);
  EXPECT_FALSE(second->has_value());
  fixture.dispatcher_->cancel();
  fixture.ioContext_.poll();
  fixture.ioContext_.restart();
  EXPECT_EQ(*second, false);
  // A slot becomes free, but the dispatcher stays cancelled.
  fixture.finishOldest();
  EXPECT_EQ(*fixture.dispatch(), false);
  EXPECT_TRUE(fixture.pendingDones_.empty());
  fixture.dispatcher_->waitUntilIdle();
}

// Test that the `keepAlive` of a block is held until its last callback is
// done.
TEST(ConcurrentCallbackDispatcher, keepAliveIsHeldUntilDone) {
  DispatcherFixture fixture{2, 1};
  std::weak_ptr<ConcurrentCallbackDispatcher> weak = fixture.dispatcher_;
  auto result = fixture.dispatch();
  EXPECT_EQ(*result, true);
  // The fixture's own pointer plus the one that the pending callbacks hold.
  EXPECT_EQ(weak.use_count(), 2);
  fixture.finishOldest();
  EXPECT_EQ(weak.use_count(), 2);
  fixture.finishOldest();
  EXPECT_EQ(weak.use_count(), 1);
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
