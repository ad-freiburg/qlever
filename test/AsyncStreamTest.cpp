// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <semaphore>
#include <thread>
#include <vector>

#include "util/AsyncStream.h"
#include "util/GTestHelpers.h"

namespace {
using ad_utility::streams::runStreamAsync;

cppcoro::generator<std::string> generateNChars(
    size_t n, std::atomic_size_t& totalProcessed) {
  for (size_t i = 0; i < n; i++) {
    co_yield "A";
    totalProcessed = i + 1;
  }
}

TEST(AsyncStream, EnsureMaximumBufferLimitWorks) {
  std::atomic_size_t totalProcessed = 0;
  size_t bufferLimit = 10;
  auto stream = runStreamAsync(generateNChars(bufferLimit + 2, totalProcessed),
                               bufferLimit);
  auto iterator = stream.begin();

  while (totalProcessed <= bufferLimit) {
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }

  // stream.begin() consumes a single element, and bufferLimit elements are
  // stored in the queue inside of stream.
  ASSERT_EQ(totalProcessed, bufferLimit + 1);

  // One element has been retrieved, so another one may enter the buffer.
  ++iterator;

  while (totalProcessed == bufferLimit + 1) {
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }
  ASSERT_EQ(totalProcessed, bufferLimit + 2);
}

TEST(AsyncStream, EnsureBuffersArePassedCorrectly) {
  const std::vector<std::string> testData{"Abc", "Def", "Ghi"};
  auto generator = runStreamAsync(testData, 2);

  ASSERT_TRUE(ql::ranges::equal(testData.begin(), testData.end(),
                                generator.begin(), generator.end()));
}

// _____________________________________________________________________________
TEST(AsyncStream, ExceptionsInTheRange) {
  auto consume = [](auto&& range) {
    for (auto&& el : range) {
      (void)el;
    }
  };
  auto generator = runStreamAsync(
      []() -> cppcoro::generator<std::string> {
        co_yield "A";
        throw std::runtime_error("Test exception");
        co_yield "B";
      }(),
      1);
  AD_EXPECT_THROW_WITH_MESSAGE(consume(generator),
                               ::testing::HasSubstr("Test exception"));
}

// _____________________________________________________________________________
TEST(AsyncStream, RunOnExecutor) {
  const std::vector<std::string> testData{"Abc", "Def", "Ghi"};
  boost::asio::thread_pool pool{1};
  // The range runs on the single thread of the `pool`, and not on the thread
  // that consumes the result.
  auto consumingThread = std::this_thread::get_id();
  std::atomic<std::thread::id> producingThread;
  auto reportThread = [&producingThread](const std::vector<std::string>& values)
      -> cppcoro::generator<std::string> {
    for (const auto& value : values) {
      producingThread = std::this_thread::get_id();
      co_yield std::string{value};
    }
  };
  auto generator =
      runStreamAsync(reportThread(testData), 2, pool.get_executor());
  ASSERT_TRUE(ql::ranges::equal(testData.begin(), testData.end(),
                                generator.begin(), generator.end()));
  ASSERT_NE(producingThread.load(), consumingThread);

  // The pool can be used again for the next stream, because the destructor of
  // the previous one has waited for its task.
  auto generator2 = runStreamAsync(testData, 2, pool.get_executor());
  ASSERT_TRUE(ql::ranges::equal(testData.begin(), testData.end(),
                                generator2.begin(), generator2.end()));
}

// Regression test: when the range that `runStreamAsync` consumes is destroyed,
// the resources that it owns have to be released before the destructor of the
// returned range returns. Callers rely on this, for example the
// `CompressedExternalIdTableSorter`, which may be `clear()`ed right after its
// sorted output has been destroyed. A task on an executor is destroyed only
// after it has returned, so `runStreamAsync` has to destroy the range itself.
// _____________________________________________________________________________
TEST(AsyncStream, RangeIsDestroyedBeforeTheStreamOnExecutor) {
  // A range of three strings that sets a flag when it is destroyed.
  struct RangeWithDestructor : public std::vector<std::string> {
    std::atomic<bool>* wasDestroyed_;
    explicit RangeWithDestructor(std::atomic<bool>* wasDestroyed)
        : std::vector<std::string>{"Abc", "Def", "Ghi"},
          wasDestroyed_{wasDestroyed} {}
    RangeWithDestructor(RangeWithDestructor&& other) noexcept
        : std::vector<std::string>{std::move(other)},
          wasDestroyed_{std::exchange(other.wasDestroyed_, nullptr)} {}
    ~RangeWithDestructor() {
      if (wasDestroyed_ != nullptr) {
        // Make the (incorrect) case that the destruction happens too late
        // reliably observable instead of flaky.
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
        *wasDestroyed_ = true;
      }
    }
  };

  boost::asio::thread_pool pool{1};
  std::atomic<bool> wasDestroyed = false;
  {
    auto generator = runStreamAsync(RangeWithDestructor{&wasDestroyed}, 2,
                                    pool.get_executor());
    for ([[maybe_unused]] const auto& value : generator) {
    }
    ASSERT_FALSE(wasDestroyed);
  }
  ASSERT_TRUE(wasDestroyed);
}

// _____________________________________________________________________________
TEST(AsyncStream, ExceptionsInTheRangeOnExecutor) {
  boost::asio::thread_pool pool{1};
  auto consume = [](auto&& range) {
    for (auto&& el : range) {
      (void)el;
    }
  };
  auto generator = runStreamAsync(
      []() -> cppcoro::generator<std::string> {
        co_yield "A";
        throw std::runtime_error("Test exception");
        co_yield "B";
      }(),
      1, pool.get_executor());
  AD_EXPECT_THROW_WITH_MESSAGE(consume(generator),
                               ::testing::HasSubstr("Test exception"));
}

// _____________________________________________________________________________
TEST(AsyncStream, PrematureDestructionOnExecutor) {
  boost::asio::thread_pool pool{1};
  auto generator = runStreamAsync(
      []() -> cppcoro::generator<std::string> {
        co_yield "A";
        co_yield "B";
        co_yield "C";
      }(),
      1, pool.get_executor());
  [[maybe_unused]] auto it = generator.begin();
  // Assume no deadlocks when destroying the range without fully consuming it.
}

// _____________________________________________________________________________
TEST(AsyncStream, PrematureDestruction) {
  auto generator = runStreamAsync(
      []() -> cppcoro::generator<std::string> {
        co_yield "A";
        co_yield "B";
        co_yield "C";
      }(),
      1);
  [[maybe_unused]] auto it = generator.begin();
  // Assume no deadlocks when destroying the range without fully consuming it.
}
}  // namespace
