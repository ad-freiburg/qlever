// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <chrono>
#include <semaphore>

#include "../src/util/AsyncStream.h"

using ad_utility::streams::runStreamAsync;

cppcoro::generator<std::string> generateNChars(size_t n,
                                               size_t& totalProcessed) {
  for (size_t i = 0; i < n; i++) {
    co_yield "A";
    totalProcessed = i + 1;
  }
}

TEST(AsyncStream, EnsureMaximumBufferLimitWorks) {
  size_t totalProcessed = 0;
  size_t bufferLimit = 10;
  auto stream = runStreamAsync<cppcoro::generator<std::string>>(
      generateNChars(bufferLimit + 2, totalProcessed), bufferLimit);
  auto iterator = stream.begin();

  while (totalProcessed <= bufferLimit) {
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }

  ASSERT_EQ(totalProcessed, bufferLimit + 1);

  ++iterator;

  while (totalProcessed == bufferLimit + 1) {
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }
  ASSERT_EQ(totalProcessed, bufferLimit + 2);
}

TEST(AsyncStream, EnsureBuffersArePassedCorrectly) {
  auto generator =
      runStreamAsync<std::vector<std::string>>({"Abc", "Def", "Ghi"}, 3);

  auto iterator = generator.begin();
  ASSERT_NE(iterator, generator.end());
  ASSERT_EQ(*iterator, "Abc");

  ++iterator;
  ASSERT_NE(iterator, generator.end());
  ASSERT_EQ(*iterator, "Def");

  ++iterator;
  ASSERT_NE(iterator, generator.end());
  ASSERT_EQ(*iterator, "Ghi");

  ++iterator;
  ASSERT_EQ(iterator, generator.end());
}
