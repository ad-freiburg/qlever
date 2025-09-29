//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "backports/three_way_comparison.h"
#include "util/Generator.h"

struct Details {
  bool begin_ = false;
  bool end_ = false;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Details, begin_, end_)
};

// A simple generator that first yields three numbers and then adds a detail
// value, that we can then extract after iterating over it.
cppcoro::generator<int, Details> simpleGen() {
  Details& details = co_await cppcoro::getDetails;
  details.begin_ = true;
  co_yield 1;
  co_yield 42;
  co_yield 43;
  details.end_ = true;
};

auto blubbBlabb() {
  return []() -> cppcoro::generator<int> { co_yield 12340; };
}

// Test the behavior of the `simpleGen` above.
TEST(Generator, details) {
  auto gen = simpleGen();
  int result{};
  // `details().begin_` is only set to true after the call to `begin()` in the
  // for loop below
  ASSERT_FALSE(gen.details().begin_);
  ASSERT_FALSE(gen.details().end_);
  for (int i : gen) {
    result += i;
    ASSERT_TRUE(gen.details().begin_);
    ASSERT_FALSE(gen.details().end_);
  }
  ASSERT_EQ(result, 86);
  ASSERT_TRUE(gen.details().begin_);
  ASSERT_TRUE(gen.details().end_);
}

// Test the behavior of the `simpleGen` with an explicit external details
// object.
TEST(Generator, externalDetails) {
  auto gen = simpleGen();
  Details details{};
  ASSERT_NE(&details, &gen.details());
  gen.setDetailsPointer(&details);
  ASSERT_EQ(&details, &gen.details());
  int result{};
  // `details().begin_` is only set to true after the call to `begin()` in the
  // for loop below
  ASSERT_FALSE(gen.details().begin_);
  ASSERT_FALSE(details.begin_);
  ASSERT_FALSE(gen.details().end_);
  ASSERT_FALSE(details.end_);
  for (int i : gen) {
    result += i;
    ASSERT_TRUE(gen.details().begin_);
    ASSERT_TRUE(details.begin_);
    ASSERT_FALSE(gen.details().end_);
    ASSERT_FALSE(details.end_);
  }
  ASSERT_EQ(result, 86);
  ASSERT_TRUE(gen.details().begin_);
  ASSERT_TRUE(details.begin_);
  ASSERT_TRUE(gen.details().end_);
  ASSERT_TRUE(details.end_);
  ASSERT_EQ(&details, &gen.details());

  // Setting a `nullptr` is illegal
  ASSERT_ANY_THROW(gen.setDetailsPointer(nullptr));
}

// Test that a default-constructed generator still has a valid `Details` object.
TEST(Generator, detailsForDefaultConstructedGenerator) {
  cppcoro::generator<int, Details> gen;
  ASSERT_EQ(gen.details(), Details());
  ASSERT_EQ(std::as_const(gen).details(), Details());
}

TEST(Generator, getSingleElement) {
  // The generator yields more than a single element -> throw
  EXPECT_ANY_THROW(cppcoro::getSingleElement(simpleGen()));

  // The generator yields exactly one element -> return the element
  auto gen2 = []() -> cppcoro::generator<int> { co_yield 1; }();
  EXPECT_EQ(1, cppcoro::getSingleElement(std::move(gen2)));
  auto gen3 = []() -> cppcoro::generator<int> {
    co_yield 1;
    co_yield 3;
  }();
  EXPECT_ANY_THROW(cppcoro::getSingleElement(std::move(gen3)));
}
