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
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>
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

// Start an `asyncDispatch` of a block on the `dispatcher`, run the `ioContext`
// until it has no more ready work, and return the future of the result.
std::future<bool> dispatchBlock(
    boost::asio::io_context& ioContext,
    std::shared_ptr<ConcurrentCallbackDispatcher> dispatcher) {
  auto future =
      boost::asio::co_spawn(ioContext,
                            ConcurrentCallbackDispatcher::asyncDispatch(
                                std::move(dispatcher), makeSharedBlock()),
                            boost::asio::use_future);
  ioContext.poll();
  ioContext.restart();
  return future;
}

// Return `true` iff the `future` is ready, without blocking.
bool isReady(const std::future<bool>& future) {
  return future.wait_for(std::chrono::seconds{0}) == std::future_status::ready;
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

  // Start an `asyncDispatch` of a block, see `dispatchBlock`.
  std::future<bool> dispatch() {
    return dispatchBlock(ioContext_, dispatcher_);
  }

  // Invoke the oldest pending `done` with the given `exception`.
  void finishOldest(std::exception_ptr exception = nullptr) {
    ASSERT_FALSE(pendingDones_.empty());
    auto done = std::move(pendingDones_.front());
    pendingDones_.erase(pendingDones_.begin());
    std::move(done)(std::move(exception));
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
    EXPECT_TRUE(fixture.dispatch().get());
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
  EXPECT_TRUE(first.get());
  EXPECT_TRUE(second.get());
  // The third block has to wait for a free slot.
  EXPECT_FALSE(isReady(third));
  EXPECT_EQ(fixture.pendingDones_.size(), 4u);
  // A single callback of the first block is not enough.
  fixture.finishOldest();
  EXPECT_FALSE(isReady(third));
  fixture.finishOldest();
  ASSERT_TRUE(isReady(third));
  EXPECT_TRUE(third.get());
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
  callbacks.push_back([]([[maybe_unused]] SharedBlock block,
                         DoneCallback done) {
    std::move(done)(std::make_exception_ptr(std::runtime_error{"via done"}));
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
    auto result = dispatchBlock(ioContext, dispatcher);
    // The single slot is free again, because both callbacks are done.
    ASSERT_TRUE(isReady(result));
    EXPECT_TRUE(result.get());
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
  EXPECT_TRUE(first.get());
  EXPECT_FALSE(isReady(second));
  fixture.dispatcher_->cancel();
  fixture.ioContext_.poll();
  fixture.ioContext_.restart();
  ASSERT_TRUE(isReady(second));
  EXPECT_FALSE(second.get());
  // A slot becomes free, but the dispatcher stays cancelled.
  fixture.finishOldest();
  EXPECT_FALSE(fixture.dispatch().get());
  EXPECT_TRUE(fixture.pendingDones_.empty());
  fixture.dispatcher_->waitUntilIdle();
}

// Test that the dispatcher is kept alive until the last callback of a block is
// done.
TEST(ConcurrentCallbackDispatcher, dispatcherIsKeptAliveUntilDone) {
  DispatcherFixture fixture{2, 1};
  std::weak_ptr<ConcurrentCallbackDispatcher> weak = fixture.dispatcher_;
  auto result = fixture.dispatch();
  EXPECT_TRUE(result.get());
  // The fixture's own pointer plus the one that the pending callbacks hold.
  EXPECT_EQ(weak.use_count(), 2);
  fixture.finishOldest();
  EXPECT_EQ(weak.use_count(), 2);
  fixture.finishOldest();
  EXPECT_EQ(weak.use_count(), 1);
}

// Test the dispatcher with real concurrency: many blocks are dispatched from
// the threads of one pool, and the callbacks are done on the threads of
// another. The bound on the blocks in flight holds throughout, every callback
// is invoked and done for every block, the exceptions of all the blocks are
// reported, and `waitUntilIdle` returns only after the last `done`.
TEST(ConcurrentCallbackDispatcher, concurrentDispatchesAndDones) {
  constexpr size_t NUM_BLOCKS = 300;
  constexpr size_t NUM_CALLBACKS = 3;
  constexpr size_t MAX_BLOCKS_IN_FLIGHT = 4;
  boost::asio::thread_pool dispatchPool{4};
  boost::asio::thread_pool donePool{4};
  // The number of callbacks that are currently in flight, its maximum so far,
  // the number of callbacks that are done, and the number of exceptions that
  // were reported. The first callback of every seventh block (the blocks with
  // the values `0, 7, 14, ...`) reports an exception via its `done`.
  std::atomic<size_t> callbacksInFlight{0};
  std::atomic<size_t> maxCallbacksInFlight{0};
  std::atomic<size_t> numDones{0};
  std::atomic<size_t> numReportedExceptions{0};
  std::vector<ConcurrentBlockCallback> callbacks;
  for (size_t i = 0; i < NUM_CALLBACKS; ++i) {
    callbacks.push_back([&, i](SharedBlock block, DoneCallback done) {
      size_t inFlight = callbacksInFlight.fetch_add(1) + 1;
      maxCallbacksInFlight.store(
          std::max(maxCallbacksInFlight.load(), inFlight));
      bool reportsException = i == 0 && (*block)(0, 0).getInt() % 7 == 0;
      boost::asio::post(
          donePool, [&, done = std::move(done), reportsException]() mutable {
            std::this_thread::sleep_for(std::chrono::microseconds{200});
            callbacksInFlight.fetch_sub(1);
            numDones.fetch_add(1);
            std::move(done)(reportsException ? std::make_exception_ptr(
                                                   std::runtime_error{"failed"})
                                             : nullptr);
          });
    });
  }
  auto dispatcher = std::make_shared<ConcurrentCallbackDispatcher>(
      dispatchPool.get_executor(), std::move(callbacks), MAX_BLOCKS_IN_FLIGHT,
      [&numReportedExceptions](std::exception_ptr exception) {
        EXPECT_NE(exception, nullptr);
        numReportedExceptions.fetch_add(1);
      });

  std::vector<std::future<bool>> results;
  for (size_t i = 0; i < NUM_BLOCKS; ++i) {
    Block block{1, ad_utility::makeUnlimitedAllocator<Id>()};
    block.push_back({Id::makeFromInt(static_cast<int64_t>(i))});
    results.push_back(boost::asio::co_spawn(
        dispatchPool,
        ConcurrentCallbackDispatcher::asyncDispatch(
            dispatcher, std::make_shared<const Block>(std::move(block))),
        boost::asio::use_future));
  }
  for (auto& result : results) {
    EXPECT_TRUE(result.get());
  }
  dispatcher->waitUntilIdle();
  EXPECT_EQ(callbacksInFlight.load(), 0u);
  EXPECT_EQ(numDones.load(), NUM_BLOCKS * NUM_CALLBACKS);
  EXPECT_LE(maxCallbacksInFlight.load(), MAX_BLOCKS_IN_FLIGHT * NUM_CALLBACKS);
  EXPECT_EQ(numReportedExceptions.load(), (NUM_BLOCKS + 6) / 7);
  donePool.join();
  dispatchPool.join();
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
