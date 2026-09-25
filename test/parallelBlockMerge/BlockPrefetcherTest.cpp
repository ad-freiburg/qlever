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
// has not produced the next block yet) until `stop()` is called. Once stopped,
// it completes every operation with `std::nullopt`, no matter what is left of
// the script, just like `InOrderBlockSink`. It records how often it was called
// and how many of its operations were in flight at the same time.
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

  // Call `stop()`, and then complete with nothing.
  template <typename CompletionToken>
  auto asyncStop(CompletionToken&& completionToken) {
    return net::async_initiate<CompletionToken, void(std::exception_ptr)>(
        [this](auto handler) {
          stop();
          auto executor = net::get_associated_executor(handler, executor_);
          net::post(executor, [handler = std::move(handler)]() mutable {
            std::move(handler)(nullptr);
          });
        },
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
  bool wasStopped() const {
    std::unique_lock lock{mutex_};
    return wasStopped_;
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
    if (wasStopped_) {
      completeLater(std::move(erased), nullptr, std::nullopt);
      return;
    }
    if (script_.empty()) {
      hangingHandler_ = std::move(erased);
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
                               ::testing::HasSubstr("sink_ != nullptr"));
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
    // is full, and drops the buffered blocks.
    prefetcher.shutDown();
    EXPECT_TRUE(sink->wasStopped());
    EXPECT_TRUE(sink->wasStopped());
    EXPECT_EQ(sink.use_count(), 1);
    EXPECT_EQ(sink->maxNumInFlight(), 1u);
  }
}

// _____________________________________________________________________________
// A read-ahead whose filler is suspended in `async_send`, because the channel
// is full, can be shut down without stalling. This is the case that a `close`
// followed by a `cancel` of the channel would get wrong, see the NOTE at the
// class comment of `BlockPrefetcher`.
TEST(BlockPrefetcher, shutDownWhileFillerWaitsForRoom) {
  for (size_t numPrefetched : {1u, 2u}) {
    for (size_t numConsumed : {0u, 1u, 5u}) {
      net::thread_pool pool{2};
      absl::Cleanup joinPool = [&pool] { pool.join(); };
      auto sink =
          std::make_shared<FakeSink>(pool.get_executor(), blocksScript(100));
      {
        Prefetcher prefetcher{pool.get_executor(), sink, numPrefetched};
        for (size_t i = 0; i < numConsumed; ++i) {
          EXPECT_EQ(prefetcher.getNextBlock(), Block{i});
        }
        // The buffered blocks plus the one in the suspended `async_send`.
        waitUntil([&] {
          return sink->numCalls() == numConsumed + numPrefetched + 1;
        });
      }
      EXPECT_TRUE(sink->wasStopped());
      EXPECT_EQ(sink.use_count(), 1);
      EXPECT_EQ(sink->maxNumInFlight(), 1u);
    }
  }
}

// _____________________________________________________________________________
// Shutting down after the last value (the end of the merge or an exception) was
// received, or before the consumer has received anything at all, does not
// stall either.
TEST(BlockPrefetcher, shutDownAfterLastValueOrRightAway) {
  net::thread_pool pool{2};
  absl::Cleanup joinPool = [&pool] { pool.join(); };
  std::vector<std::vector<FakeSink::Outcome>> scripts;
  scripts.push_back({FakeSink::EndOfMerge{}});
  scripts.push_back({std::make_exception_ptr(std::runtime_error{"failed"})});
  scripts.push_back({Block{0}, FakeSink::EndOfMerge{}});
  for (const auto& script : scripts) {
    for (bool consumeLastValue : {true, false}) {
      auto sink = std::make_shared<FakeSink>(pool.get_executor(), script);
      {
        Prefetcher prefetcher{pool.get_executor(), sink, 1};
        if (consumeLastValue) {
          try {
            while (prefetcher.getNextBlock().has_value()) {
            }
          } catch (const std::runtime_error& error) {
            EXPECT_STREQ(error.what(), "failed");
          }
        }
      }
      EXPECT_EQ(sink.use_count(), 1);
      EXPECT_EQ(sink->maxNumInFlight(), 1u);
    }
  }
}

// _____________________________________________________________________________
// Stress the shutdown: consume a varying number of blocks from read-aheads of
// different sizes and then destroy them at an arbitrary point in time, with an
// executor that is busy with several read-aheads at once. None of this may
// stall or leak the sink.
TEST(BlockPrefetcher, shutDownStress) {
  net::thread_pool pool{3};
  absl::Cleanup joinPool = [&pool] { pool.join(); };
  for (size_t iteration = 0; iteration < 200; ++iteration) {
    size_t numPrefetched = 1 + iteration % 3;
    size_t numBlocks = iteration % 7;
    size_t numConsumed = iteration % 5;
    std::vector<std::shared_ptr<FakeSink>> sinks;
    std::vector<std::unique_ptr<Prefetcher>> prefetchers;
    for (size_t i = 0; i < 3; ++i) {
      auto script = blocksScript(numBlocks);
      if (i == 1) {
        script.emplace_back(FakeSink::EndOfMerge{});
      }
      sinks.push_back(
          std::make_shared<FakeSink>(pool.get_executor(), std::move(script)));
      prefetchers.push_back(std::make_unique<Prefetcher>(
          pool.get_executor(), sinks.back(), numPrefetched));
    }
    for (size_t i = 0; i < 3; ++i) {
      // Consume only as many blocks as there are, the sinks without an end
      // hang afterwards.
      for (size_t j = 0; j < std::min(numConsumed, numBlocks); ++j) {
        EXPECT_EQ(prefetchers[i]->getNextBlock(), Block{j});
      }
    }
    prefetchers.clear();
    for (const auto& sink : sinks) {
      EXPECT_EQ(sink.use_count(), 1);
      EXPECT_EQ(sink->maxNumInFlight(), 1u);
    }
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
// A read-ahead whose `asyncGetNextBlock` is still in flight (and would hang
// forever) can be shut down, both explicitly and by its destructor, because the
// shutdown stops the sink.
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
