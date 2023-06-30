//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "util/Generator.h"

// A simple generator that first yields three numbers and then adds a detail
// value, that we can then extract after iterating over it.
cppcoro::generator<int> simpleGen(double detailValue) {
  co_await cppcoro::AddDetail{"started", true};
  co_yield 1;
  co_yield 42;
  co_yield 43;
  co_await cppcoro::AddDetail{"detail", detailValue};
};

// Test the behavior of the `simpleGen` above
TEST(Generator, details) {
  auto gen = simpleGen(17.3);
  int result{};
  // The first detail is only added after the call to `begin()` in the for loop
  // below
  ASSERT_TRUE(gen.details().empty());
  for (int i : gen) {
    result += i;
    // The detail is only
    ASSERT_EQ(gen.details().size(), 1);
    ASSERT_TRUE(gen.details().at("started"));
  }
  ASSERT_EQ(result, 86);
  ASSERT_EQ(gen.details().size(), 2);
  ASSERT_EQ(gen.details().at("detail"), 17.3);
}
