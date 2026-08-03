// Copyright 2024 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <sstream>
#include <string_view>
#include <thread>
#include <vector>

#include "../test/util/GTestHelpers.h"
#include "util/ProgressBar.h"
#include "util/StringUtils.h"
#include "util/Timer.h"

using ad_utility::ProgressBar;

// Test typical use case with multiple updates and a final update.
TEST(ProgressBar, typicalUsage) {
  auto speedDescriptionFunction = DEFAULT_SPEED_DESCRIPTION_FUNCTION;
  for (auto displayOption : {ProgressBar::UseNewLine, ProgressBar::ReuseLine}) {
    size_t numSteps = 0;
    ProgressBar progressBar(numSteps, "Steps: ", 100'000,
                            speedDescriptionFunction, displayOption);
    // We expect three update strings with a speed of around 3 M/s and a final
    // update string with 303'000 steps.
    //
    // TODO: Why does \\d instead of [0-9] not work in the following regex?
    //
    // NOTE: For macOS, `std::this_thread::sleep_for` can take much longer
    // than indicated, resulting in a much lower speed than expected.
    std::string expectedSpeedRegex =
#ifndef _QLEVER_NO_TIMING_TESTS
        "\\[average speed [234]\\.[0-9] M/s, last batch [1234]\\.[0-9] M/s"
        ", fastest [234]\\.[0-9] M/s, slowest [1234]\\.[0-9] M/s\\] ";
#else
        "\\[average speed [0-9]\\.[0-9] M/s, last batch [0-9]\\.[0-9] M/s"
        ", fastest [0-9]\\.[0-9] M/s, slowest [0-9]\\.[0-9] M/s\\] ";
#endif
    char lastChar = displayOption == ProgressBar::ReuseLine ? '\r' : '\n';
    std::vector<std::string> expectedUpdateRegexes = {
        "Steps: 100,000 " + expectedSpeedRegex + lastChar,
        "Steps: 200,000 " + expectedSpeedRegex + lastChar,
        "Steps: 300,000 " + expectedSpeedRegex + lastChar,
        "Steps: 303,000 " + expectedSpeedRegex + "\n"};
    size_t k = 0;
    for (size_t i = 0; i < 101; ++i) {
      numSteps += 3'000;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      if (progressBar.update()) {
        ASSERT_LT(k + 1, expectedUpdateRegexes.size());
        ASSERT_THAT(progressBar.getProgressString(),
                    MatchesRegex(expectedUpdateRegexes[k]));
        ++k;
      }
    }
    ASSERT_THAT(progressBar.getFinalProgressString(),
                MatchesRegex(expectedUpdateRegexes.back()));
    AD_EXPECT_THROW_WITH_MESSAGE(
        progressBar.getFinalProgressString(),
        ::testing::ContainsRegex("should only be called once"));
  }
}

// Test special case where the number of steps is less than the batch size.
TEST(ProgressBar, numberOfStepsLessThanBatchSize) {
  size_t numSteps = 30'000;
  ProgressBar progressBar(numSteps, "Steps: ", 50'000);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  std::string expectedUpdateRegex =
#ifndef _QLEVER_NO_TIMING_TESTS
      "Steps: 30,000 \\[average speed [234]\\.[0-9] M/s\\] \n";
#else
      "Steps: 30,000 \\[average speed [0-9]\\.[0-9] M/s\\] \n";
#endif
  ASSERT_THAT(progressBar.getFinalProgressString(),
              MatchesRegex(expectedUpdateRegex));
}

// Test `getTimer` by stopping the timer after 10ms, then sleeping for 10ms
// more, and checking that these additional 10ms are not considered in the
// reported average speed.
TEST(ProgressBar, getTimer) {
  size_t numSteps = 30'000;
  ProgressBar progressBar(numSteps, "Steps: ", 50'000);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  progressBar.getTimer().stop();
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  std::string expectedUpdateRegex =
#ifndef _QLEVER_NO_TIMING_TESTS
      "Steps: 30,000 \\[average speed [234]\\.[0-9] M/s\\] \n";
#else
      "Steps: 30,000 \\[average speed [0-9]\\.[0-9] M/s\\] \n";
#endif
  ASSERT_THAT(progressBar.getFinalProgressString(),
              MatchesRegex(expectedUpdateRegex));
}

// Tests for `ConcurrentProgressBar`, see `ProgressBar.h`.

// Single-threaded: an `Update` is returned exactly when the batch size is
// crossed, with the correct counts and percentages; intermediate progress
// strings end with `\r`, only the final one with `\n`.
TEST(ConcurrentProgressBar, singleThreaded) {
  std::ostringstream out;
  ad_utility::ConcurrentProgressBar progressBar{"Steps: ", 100, 10};
  progressBar.add(5);
  EXPECT_FALSE(progressBar.update().has_value());
  progressBar.add(5);
  {
    auto update = progressBar.update();
    ASSERT_TRUE(update.has_value());
    out << update->getProgressString();
  }
  // The display for this batch has been claimed, so no further `Update` is
  // returned before the next multiple of the batch size is reached.
  EXPECT_FALSE(progressBar.update().has_value());
  progressBar.add(90);
  {
    auto update = progressBar.update();
    ASSERT_TRUE(update.has_value());
    out << update->getProgressString();
  }
  out << progressBar.getFinalProgressString();
  std::string s = std::move(out).str();
  EXPECT_THAT(s, ::testing::HasSubstr("Steps: 10 of 100 (10.0%)"));
  EXPECT_THAT(s, ::testing::HasSubstr("Steps: 100 of 100 (100.0%)"));
  EXPECT_EQ(std::count(s.begin(), s.end(), '\r'), 2);
  EXPECT_EQ(std::count(s.begin(), s.end(), '\n'), 1);
  EXPECT_EQ(s.back(), '\n');
}

// A computation with a total of zero steps is reported as trivially complete.
TEST(ConcurrentProgressBar, zeroTotalIsComplete) {
  ad_utility::ConcurrentProgressBar progressBar{"Steps: ", 0};
  EXPECT_THAT(progressBar.getFinalProgressString(),
              ::testing::HasSubstr("Steps: 0 of 0 (100.0%)"));
}

// Concurrent `add` and `update` calls from several threads: the counts are
// summed up correctly, the `Update` objects serialize the writes to the
// shared stream, and the displayed counts never decrease.
TEST(ConcurrentProgressBar, concurrentAddsAndUpdates) {
  std::ostringstream out;
  ad_utility::ConcurrentProgressBar progressBar{"Steps: ", 3000, 100};
  std::vector<std::thread> threads;
  for (size_t i = 0; i < 4; ++i) {
    threads.emplace_back([&progressBar, &out]() {
      // Varying step sizes (cycling through 1..5, 750 steps per thread in
      // total), so that single `add` calls also jump over batch boundaries.
      for (size_t j = 0; j < 250; ++j) {
        progressBar.add(j % 5 + 1);
        if (auto update = progressBar.update()) {
          out << update->getProgressString();
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  out << progressBar.getFinalProgressString();
  std::string s = std::move(out).str();
  EXPECT_THAT(s, ::testing::HasSubstr("Steps: 3,000 of 3,000 (100.0%)"));
  // Every progress string is intact (in particular, not interleaved with
  // another one), and the displayed counts never decrease.
  size_t previousCount = 0;
  size_t numProgressStrings = 0;
  for (std::string_view rest{s}; !rest.empty();) {
    size_t lineEnd = rest.find_first_of("\r\n");
    ASSERT_NE(lineEnd, std::string_view::npos);
    std::string_view line = rest.substr(0, lineEnd);
    rest.remove_prefix(lineEnd + 1);
    if (line.empty()) {
      continue;  // Padding of a `\r` string that was followed by another.
    }
    ASSERT_TRUE(line.starts_with("Steps: "));
    std::string countString{line.substr(7, line.find(" of ") - 7)};
    std::erase(countString, ',');
    size_t count = std::stoul(countString);
    EXPECT_GE(count, previousCount);
    previousCount = count;
    ++numProgressStrings;
  }
  EXPECT_GE(numProgressStrings, 2u);
}

// A progress string that is shorter than its predecessor is padded with
// spaces, so that the `\r` overwrites all leftover characters.
TEST(ConcurrentProgressBar, shorterStringsArePadded) {
  ad_utility::ConcurrentProgressBar progressBar{"Steps: ", 1'000'000, 1'000};
  progressBar.add(1'000);
  auto update = progressBar.update();
  ASSERT_TRUE(update.has_value());
  size_t firstWidth = update->getProgressString().size();
  update.reset();
  std::string finalString = progressBar.getFinalProgressString();
  EXPECT_GE(finalString.size(), firstWidth);
}

// A batch size of zero is rejected (`update()` divides by it).
TEST(ConcurrentProgressBar, zeroBatchSizeIsRejected) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::ConcurrentProgressBar("Steps: ", 100, 0),
      ::testing::HasSubstr("must not be zero"));
}

// With `UseNewLine`, every progress string ends with `\n` (and no padding is
// needed).
TEST(ConcurrentProgressBar, useNewLine) {
  ad_utility::ConcurrentProgressBar progressBar{
      "Steps: ", 100, 10, DEFAULT_SPEED_DESCRIPTION_FUNCTION,
      ad_utility::ProgressBar::UseNewLine};
  progressBar.add(10);
  auto update = progressBar.update();
  ASSERT_TRUE(update.has_value());
  EXPECT_THAT(update->getProgressString(),
              ::testing::HasSubstr("Steps: 10 of 100"));
  EXPECT_EQ(update->getProgressString().back(), '\n');
  update.reset();
  EXPECT_EQ(progressBar.getFinalProgressString().back(), '\n');
}

// The final progress string may only be requested once.
TEST(ConcurrentProgressBar, getFinalProgressStringOnlyOnce) {
  ad_utility::ConcurrentProgressBar progressBar{"Steps: ", 10};
  progressBar.getFinalProgressString();
  AD_EXPECT_THROW_WITH_MESSAGE(
      progressBar.getFinalProgressString(),
      ::testing::HasSubstr("should only be called once"));
}
