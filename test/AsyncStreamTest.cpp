// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <semaphore>

#include "../src/util/AsyncStream.h"
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
