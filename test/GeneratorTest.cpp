//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "util/Generator.h"

struct Details {
  bool begin_ = false;
  bool end_ = false;
};

// A simple generator that first yields three numbers and then adds a detail
// value, that we can then extract after iterating over it.
cppcoro::generator<int, Details> simpleGen(double detailValue) {
  auto& details = co_await cppcoro::getDetails;
  details.begin_ = true;
  co_yield 1;
  co_yield 42;
  co_yield 43;
  details.end_ = true;
};

// Test the behavior of the `simpleGen` above
TEST(Generator, details) {
  auto gen = simpleGen(17.3);
  int result{};
  // The first detail is only added after the call to `begin()` in the for loop
  // below
  ASSERT_FALSE(gen.details().begin_);
  ASSERT_FALSE(gen.details().end_);
  for (int i : gen) {
    result += i;
    ASSERT_TRUE(gen.details().begin_);
    ASSERT_FALSE(gen.details().end_);
  }
  ASSERT_EQ(result, 86);
  ASSERT_FALSE(gen.details().begin_);
  ASSERT_TRUE(gen.details().end_);
}
