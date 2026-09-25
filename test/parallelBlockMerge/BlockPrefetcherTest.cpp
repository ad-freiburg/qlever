// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "../util/GTestHelpers.h"

// The `BlockPrefetcher` only exists in C++20 mode, see its header.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>

#include "util/parallelBlockMerge/BlockPrefetcher.h"

namespace {

namespace net = boost::asio;
using ad_utility::parallelBlockMerge::detail::BlockPrefetcher;
using Block = size_t;

// A fake sink whose `asyncGetNextBlock` completes with the given script of
// outcomes, one after the other, and then hangs (just like a sink whose merge
// has not produced the next block yet) until `stop()` is called. It records how
// often it was called and how many of its operations were in flight at the
// same time.
class FakeSink {
 public:
  // The end of the merge, that is a completion with `std::nullopt`.
  struct EndOfMerge {};
  using Outcome = std::variant<Block, std::exception_ptr, EndOfMerge>;
  using Handler = std::function<void(std::exception_ptr, std::optional<Block>)>;

 private:
  net::any_io_executor executor_;
  mutable std::mutex mutex_;
  // All the following members are guarded by `mutex_`.
  std::deque<Outcome> script_;
  size_t numCalls_ = 0;
  size_t numInFlight_ = 0;
  size_t maxNumInFlight_ = 0;
  bool wasStopped_ = false;
  // The handler of the operation that hangs, if any.
  std::optional<Handler> hangingHandler_;

 public:
  FakeSink(net::any_io_executor executor, std::vector<Outcome> script)
      : executor_{std::move(executor)}, script_(script.begin(), script.end()) {}

  // _________________________________________________________________________
  template <typename CompletionToken>
  auto asyncGetNextBlock(CompletionToken&& completionToken) {
    return net::async_initiate<CompletionToken,
                               void(std::exception_ptr, std::optional<Block>)>(
        [this](auto handler) { this->initiate(std::move(handler)); },
        completionToken);
  }

  // Complete the operation that hangs (if any) with `std::nullopt`, and every
  // later operation right away, just like a sink whose merge was stopped.
  void stop() {
    std::unique_lock lock{mutex_};
    wasStopped_ = true;
    if (hangingHandler_.has_value()) {
      auto handler = std::move(hangingHandler_).value();
      hangingHandler_.reset();
      completeLater(std::move(handler), nullptr, std::nullopt);
    }
  }

  // Getters for the statistics.
  size_t numCalls() const {
    std::unique_lock lock{mutex_};
    return numCalls_;
  }
  size_t maxNumInFlight() const {
    std::unique_lock lock{mutex_};
    return maxNumInFlight_;
  }

 private:
  // The initiation of `asyncGetNextBlock`.
  template <typename H>
  void initiate(H handler) {
    std::unique_lock lock{mutex_};
    ++numCalls_;
    ++numInFlight_;
    maxNumInFlight_ = std::max(maxNumInFlight_, numInFlight_);
    // Type-erase the (move-only) handler, such that it can be stored.
    auto executor = net::get_associated_executor(handler, executor_);
    Handler erased = [executor, h = std::make_shared<H>(std::move(handler))](
                         std::exception_ptr exception,
                         std::optional<Block> block) {
      net::post(executor, [h, exception = std::move(exception),
                           block = std::move(block)]() mutable {
        std::move (*h)(std::move(exception), std::move(block));
      });
    };
    if (script_.empty()) {
      if (wasStopped_) {
        completeLater(std::move(erased), nullptr, std::nullopt);
      } else {
        hangingHandler_ = std::move(erased);
      }
      return;
    }
    Outcome outcome = std::move(script_.front());
    script_.pop_front();
    if (auto* block = std::get_if<Block>(&outcome)) {
      completeLater(std::move(erased), nullptr, *block);
    } else if (auto* exception = std::get_if<std::exception_ptr>(&outcome)) {
      completeLater(std::move(erased), *exception, std::nullopt);
    } else {
      completeLater(std::move(erased), nullptr, std::nullopt);
    }
  }

  // Complete the `handler` (which posts itself to its executor) with the given
  // arguments. The operation is no longer in flight from now on.
  //
  // PRECONDITION: `mutex_` is held.
  void completeLater(Handler handler, std::exception_ptr exception,
                     std::optional<Block> block) {
    --numInFlight_;
    handler(std::move(exception), std::move(block));
  }
};

// Return a script of the blocks `0, ..., numBlocks - 1`.
std::vector<FakeSink::Outcome> blocksScript(size_t numBlocks) {
  std::vector<FakeSink::Outcome> script;
  for (size_t i = 0; i < numBlocks; ++i) {
    script.emplace_back(Block{i});
  }
  return script;
}

// Wait (for at most a few seconds) until `condition` holds.
void waitUntil(const std::function<bool()>& condition) {
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
  while (!condition() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  ASSERT_TRUE(condition());
}

using Prefetcher = BlockPrefetcher<Block, FakeSink>;

}  // namespace

// _____________________________________________________________________________
TEST(BlockPrefetcher, contractChecks) {
  net::io_context ioContext;
  auto executor = ioContext.get_executor();
  auto sink = std::make_shared<FakeSink>(executor, blocksScript(0));
  AD_EXPECT_THROW_WITH_MESSAGE(Prefetcher(executor, nullptr, 2),
                               ::testing::HasSubstr("sink != nullptr"));
  AD_EXPECT_THROW_WITH_MESSAGE(Prefetcher(executor, sink, 0),
                               ::testing::HasSubstr("numPrefetchedBlocks > 0"));
}

// _____________________________________________________________________________
// All the blocks arrive in order and are followed by the end of the merge, no
// matter how many blocks are read ahead. The sink is never read concurrently
// and not at all after the end.
TEST(BlockPrefetcher, allBlocksInOrder) {
  for (size_t numPrefetched : {1u, 3u, 1000u}) {
    net::thread_pool pool{4};
    absl::Cleanup joinPool = [&pool] { pool.join(); };
    auto script = blocksScript(100);
    script.emplace_back(FakeSink::EndOfMerge{});
    auto sink = std::make_shared<FakeSink>(pool.get_executor(), script);
    Prefetcher prefetcher{pool.get_executor(), sink, numPrefetched};
    std::vector<Block> result;
    while (auto block = prefetcher.getNextBlock()) {
      result.push_back(block.value());
    }
    std::vector<Block> expected(100);
    std::iota(expected.begin(), expected.end(), Block{0});
    EXPECT_THAT(result, ::testing::ElementsAreArray(expected));
    // The end is sticky.
    EXPECT_EQ(prefetcher.getNextBlock(), std::nullopt);
    prefetcher.shutDown();
    EXPECT_EQ(sink->numCalls(), 101u);
    EXPECT_EQ(sink->maxNumInFlight(), 1u);
    // The read-ahead no longer holds the sink.
    EXPECT_EQ(sink.use_count(), 1);
    // Further calls after the shutdown are harmless.
    prefetcher.shutDown();
    EXPECT_EQ(prefetcher.getNextBlock(), std::nullopt);
  }
}

// _____________________________________________________________________________
// The read-ahead is bounded: with `numPrefetched` blocks it reads exactly
// `numPrefetched + 1` blocks from the sink (the buffered ones and the one that
// waits for room in the buffer) while the consumer idles, and continues as soon
// as the consumer takes a block.
TEST(BlockPrefetcher, readAheadIsBounded) {
  for (size_t numPrefetched : {1u, 4u}) {
    net::thread_pool pool{2};
    absl::Cleanup joinPool = [&pool] { pool.join(); };
    auto sink =
        std::make_shared<FakeSink>(pool.get_executor(), blocksScript(100));
    Prefetcher prefetcher{pool.get_executor(), sink, numPrefetched};
    waitUntil([&] { return sink->numCalls() == numPrefetched + 1; });
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    EXPECT_EQ(sink->numCalls(), numPrefetched + 1);
    EXPECT_EQ(prefetcher.getNextBlock(), Block{0});
    waitUntil([&] { return sink->numCalls() == numPrefetched + 2; });
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    EXPECT_EQ(sink->numCalls(), numPrefetched + 2);
    // The shutdown wakes up the filler, which is suspended because the buffer
    // is full, without stopping the sink. It also drops the buffered blocks.
    prefetcher.shutDown();
    EXPECT_EQ(sink.use_count(), 1);
  }
}

// _____________________________________________________________________________
// An exception of the merge is rethrown after the blocks that precede it, and
// then on every further call. The sink is not read after the exception.
TEST(BlockPrefetcher, exceptionAfterBufferedBlocks) {
  net::thread_pool pool{2};
  absl::Cleanup joinPool = [&pool] { pool.join(); };
  auto script = blocksScript(5);
  script.emplace_back(
      std::make_exception_ptr(std::runtime_error{"merge failed"}));
  auto sink = std::make_shared<FakeSink>(pool.get_executor(), script);
  Prefetcher prefetcher{pool.get_executor(), sink, 10};
  // Let the read-ahead buffer everything before the consumer starts.
  waitUntil([&] { return sink->numCalls() == 6; });
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(prefetcher.getNextBlock(), Block{i});
  }
  AD_EXPECT_THROW_WITH_MESSAGE(prefetcher.getNextBlock(),
                               ::testing::StrEq("merge failed"));
  AD_EXPECT_THROW_WITH_MESSAGE(prefetcher.getNextBlock(),
                               ::testing::StrEq("merge failed"));
  prefetcher.shutDown();
  EXPECT_EQ(sink->numCalls(), 6u);
}

// _____________________________________________________________________________
// A read-ahead whose `asyncGetNextBlock` is still in flight can be shut down
// once the merge was stopped, both explicitly and by its destructor.
TEST(BlockPrefetcher, shutDownWhileOperationIsInFlight) {
  for (bool explicitShutDown : {true, false}) {
    net::thread_pool pool{2};
    absl::Cleanup joinPool = [&pool] { pool.join(); };
    auto sink =
        std::make_shared<FakeSink>(pool.get_executor(), blocksScript(2));
    {
      Prefetcher prefetcher{pool.get_executor(), sink, 10};
      EXPECT_EQ(prefetcher.getNextBlock(), Block{0});
      // The third operation hangs, because the script is exhausted.
      waitUntil([&] { return sink->numCalls() == 3; });
      sink->stop();
      if (explicitShutDown) {
        prefetcher.shutDown();
        EXPECT_EQ(sink.use_count(), 1);
      }
    }
    EXPECT_EQ(sink.use_count(), 1);
    EXPECT_EQ(sink->maxNumInFlight(), 1u);
  }
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
