// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <chrono>
#include <regex>
#include <thread>

#include "../test/util/GTestHelpers.h"
#include "util/ProgressBar.h"
#include "util/StringUtils.h"

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
        "\\[average speed [234]\\.[0-9] M/s, last batch [234]\\.[0-9] M/s"
        ", fastest [234]\\.[0-9] M/s, slowest [234]\\.[0-9] M/s\\] ";
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
                    ::testing::MatchesRegex(expectedUpdateRegexes[k]));
        ++k;
      }
    }
    ASSERT_THAT(progressBar.getFinalProgressString(),
                ::testing::MatchesRegex(expectedUpdateRegexes.back()));
    AD_EXPECT_THROW_WITH_MESSAGE(
        progressBar.getFinalProgressString(),
        ::testing::ContainsRegex("should only be called once"));
  }
}

// Test special case where the number of steps is less than the batch size.
TEST(ProgressBar, numberOfStepsLessThanBatchSize) {
  size_t numSteps = 3'000;
  ProgressBar progressBar(numSteps, "Steps: ", 5'000);
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  std::string expectedUpdateRegex =
#ifndef _QLEVER_NO_TIMING_TESTS
      "Steps: 3,000 \\[average speed [234]\\.[0-9] M/s\\] \n";
#else
      "Steps: 3,000 \\[average speed [0-9]\\.[0-9] M/s\\] \n";
#endif
  ASSERT_THAT(progressBar.getFinalProgressString(),
              ::testing::MatchesRegex(expectedUpdateRegex));
}
