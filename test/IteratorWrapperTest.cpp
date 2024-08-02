//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include <vector>

#include "util/IteratorWrapper.h"

using ad_utility::IteratorWrapper;

TEST(IteratorWrapper, transparentWrapper) {
  std::vector vec{1, 2, 3};
  int numIterations = 0;
  for (auto value : IteratorWrapper{vec}) {
    EXPECT_EQ(value, numIterations + 1);
    numIterations++;
  }
  EXPECT_EQ(numIterations, 3);
}

// _____________________________________________________________________________

struct TestIterable {
  std::vector<int> vec_{1, 2, 3};
  bool value1_ = false;
  int value2_ = 0;
  std::string value3_ = "";

  auto begin(bool value1, int value2, std::string value3) {
    value1_ = value1;
    value2_ = value2;
    value3_ = std::move(value3);
    return vec_.begin();
  }

  auto end() { return vec_.end(); }
};

TEST(IteratorWrapper, verifyArgumentsArePassed) {
  TestIterable testIterable;
  int numIterations = 0;
  for (auto value : IteratorWrapper{testIterable, true, 42, "Hi"}) {
    EXPECT_EQ(value, numIterations + 1);
    numIterations++;
  }
  EXPECT_EQ(numIterations, 3);
  EXPECT_TRUE(testIterable.value1_);
  EXPECT_EQ(testIterable.value2_, 42);
  EXPECT_EQ(testIterable.value3_, "Hi");
}
