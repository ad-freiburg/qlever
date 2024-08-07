//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "util/CacheableGenerator.h"
#include "util/Generator.h"

using ad_utility::wrapGeneratorWithCache;
using cppcoro::generator;
using ::testing::Optional;

generator<uint32_t> testGenerator(uint32_t range) {
  for (uint32_t i = 0; i < range; i++) {
    co_yield i;
  }
}

// _____________________________________________________________________________
TEST(CacheableGenerator, testAggregation) {
  bool called = false;
  auto gen = wrapGeneratorWithCache(
      testGenerator(4),
      [](std::optional<uint32_t>& optionalValue, const uint32_t& newValue) {
        if (optionalValue.has_value()) {
          optionalValue.value() += newValue;
        } else {
          optionalValue.emplace(newValue);
        }
        return true;
      },
      [&called](std::optional<uint32_t> value) {
        called = true;
        EXPECT_THAT(value, Optional(6));
      });
  uint32_t counter = 0;
  for (uint32_t element : gen) {
    EXPECT_EQ(counter, element);
    ++counter;
  }
  EXPECT_EQ(counter, 4);
  EXPECT_TRUE(called);
}

// _____________________________________________________________________________
TEST(CacheableGenerator, testEmptyGenerator) {
  bool called = false;
  auto gen = wrapGeneratorWithCache(
      testGenerator(0),
      [&called](std::optional<uint32_t>&, uint32_t) {
        called = true;
        return true;
      },
      [&called](std::optional<uint32_t>) { called = true; });
  uint32_t tracker = 0;
  for (uint32_t element : gen) {
    tracker += element;
  }
  EXPECT_EQ(tracker, 0);
  EXPECT_FALSE(called);
}

// _____________________________________________________________________________
TEST(CacheableGenerator, testAggregationCutoff) {
  uint32_t callCounter = 0;
  bool called = false;
  auto gen = wrapGeneratorWithCache(
      testGenerator(2),
      [&callCounter](std::optional<uint32_t>&, uint32_t) {
        ++callCounter;
        return false;
      },
      [&called](std::optional<uint32_t>) { called = true; });
  uint32_t loopCounter = 0;
  for (uint32_t element : gen) {
    EXPECT_EQ(element, loopCounter);
    ++loopCounter;
  }
  EXPECT_EQ(loopCounter, 2);
  EXPECT_EQ(callCounter, 1);
  EXPECT_FALSE(called);
}
