//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/OnDestruction.h"

TEST(OnDestruction, terminateIfThrows) {
  auto alwaysThrow = []() { throw 42; };
  EXPECT_DEATH(ad_utility::terminateIfThrows(alwaysThrow, "A function "),
               "A function that should never throw");

  auto alwaysThrowException = []() {
    throw std::runtime_error("throwing in test");
  };
  EXPECT_DEATH(ad_utility::terminateIfThrows(alwaysThrowException,
                                             "test for terminating"),
               "A function that should never throw");

  auto noThrowThenExit = []() {
    ad_utility::terminateIfThrows([]() {}, "");
    std::exit(42);
  };
  EXPECT_EXIT(noThrowThenExit(), ::testing::ExitedWithCode(42), ::testing::_);
}
