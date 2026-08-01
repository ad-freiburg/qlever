// Copyright 2024 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <sstream>
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

// _____________________________________________________________________________
// Tests for `ConcurrentProgress`, see `ProgressBar.h`.

// Single-threaded: lines are printed when the batch size is crossed, with the
// correct counts and percentages; intermediate lines end with `\r`, only the
// final line with `\n`.
TEST(ConcurrentProgress, singleThreaded) {
  std::ostringstream out;
  ad_utility::ConcurrentProgress progress{out, "Steps: ", 100, 10};
  progress.add(5);
  EXPECT_TRUE(out.str().empty());
  progress.add(5);
  progress.add(90);
  progress.finish();
  std::string s = out.str();
  EXPECT_THAT(s, ::testing::HasSubstr("Steps: 10 of 100 (10.0%)"));
  EXPECT_THAT(s, ::testing::HasSubstr("Steps: 100 of 100 (100.0%)"));
  EXPECT_EQ(std::count(s.begin(), s.end(), '\r'), 2);
  EXPECT_EQ(std::count(s.begin(), s.end(), '\n'), 1);
  EXPECT_EQ(s.back(), '\n');
}

// A phase with a total of zero steps is reported as trivially complete.
TEST(ConcurrentProgress, zeroTotalIsComplete) {
  std::ostringstream out;
  ad_utility::ConcurrentProgress progress{out, "Steps: ", 0};
  progress.finish();
  EXPECT_THAT(out.str(), ::testing::HasSubstr("Steps: 0 of 0 (100.0%)"));
}

// Concurrent `add` calls from several threads are summed up correctly.
TEST(ConcurrentProgress, concurrentAdds) {
  std::ostringstream out;
  // Batch size larger than the total, so only `finish` prints.
  ad_utility::ConcurrentProgress progress{out, "Steps: ", 1000, 100'000};
  std::vector<std::thread> threads;
  for (size_t i = 0; i < 4; ++i) {
    threads.emplace_back([&progress]() {
      for (size_t j = 0; j < 250; ++j) {
        progress.add(1);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  progress.finish();
  EXPECT_THAT(out.str(),
              ::testing::HasSubstr("Steps: 1,000 of 1,000 (100.0%)"));
}

// A line that is shorter than its predecessor is padded with spaces, so that
// the `\r` overwrites all leftover characters of the previous line.
TEST(ConcurrentProgress, shorterLinesArePadded) {
  std::ostringstream out;
  // The line for 1,000 of 1,000,000 is longer than the final line would
  // naturally be for the prefix and count alone, so the final line must be
  // padded to at least the same width.
  ad_utility::ConcurrentProgress progress{out, "Steps: ", 1'000'000, 1'000};
  progress.add(1'000);
  progress.finish();
  std::string s = out.str();
  size_t carriageReturn = s.find('\r');
  ASSERT_NE(carriageReturn, std::string::npos);
  size_t newline = s.find('\n');
  ASSERT_NE(newline, std::string::npos);
  EXPECT_GE(newline - carriageReturn - 1, carriageReturn);
}
