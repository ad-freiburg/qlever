// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <semaphore>

#include "../src/util/AsyncStream.h"

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

  ASSERT_TRUE(std::ranges::equal(testData.begin(), testData.end(),
                                 generator.begin(), generator.end()));
}
