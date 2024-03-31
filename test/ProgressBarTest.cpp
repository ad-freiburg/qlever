// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <chrono>
#include <regex>
#include <thread>

#include "util/ProgressBar.h"
#include "util/StringUtils.h"

// _____________________________________________________________________________
TEST(ProgressBar, typicalUsage) {
  size_t numSteps = 0;
  ad_utility::ProgressBar progressBar(numSteps, "Steps: ", 100'000,
                                      DEFAULT_SPEED_DESCRIPTION_FUNCTION,
                                      ad_utility::ProgressBar::UseNewLine);
  ASSERT_TRUE(std::regex_match("[4 M/s]", std::regex(R"(\[\d M/s\])")));
  // We expect three update strings with a speed of around 3 M/s and a final
  // update string with 303'000 steps.
  std::string expectedSpeedRegex =
      R"(\[average speed [234]\.\d+ M/s, last batch [234]\.\d+ M/s)"
      R"(, fastest [234]\.\d+ M/s, slowest [234]\.\d+ M/s\])";
  std::vector<std::string> expectedUpdateRegexes = {
      "Steps: 100,000 " + expectedSpeedRegex,
      "Steps: 200,000 " + expectedSpeedRegex,
      "Steps: 300,000 " + expectedSpeedRegex,
      "Steps: 303,000 " + expectedSpeedRegex};
  // Helper lamba to check that a progress string matches the expected regex.
  auto checkProgressString = [&](const std::string& progressString,
                                 const std::string& expectedRegex) {
    ASSERT_TRUE(std::regex_search(progressString, std::regex(expectedRegex)))
        << "Expected: " << expectedRegex << std::endl
        << "Got     : " << progressString;
  };
  size_t numUpdateStrings = 0;
  for (size_t i = 0; i < 101; ++i) {
    numSteps += 3'000;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    if (progressBar.update()) {
      ASSERT_LT(numUpdateStrings + 1, expectedUpdateRegexes.size());
      checkProgressString(progressBar.getProgressString(),
                          expectedUpdateRegexes[numUpdateStrings]);
      ++numUpdateStrings;
    }
  }
  checkProgressString(progressBar.getFinalProgressString(),
                      expectedUpdateRegexes.back());
}
