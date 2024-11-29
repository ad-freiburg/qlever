//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "util/GTestHelpers.h"
#include "util/Generator.h"
#include "util/Generators.h"
#include "util/Views.h"

using ad_utility::generatorFromActionWithCallback;
using ad_utility::wrapGeneratorWithCache;
using cppcoro::generator;
using ::testing::Optional;

generator<uint32_t> testGenerator(uint32_t range) {
  for (uint32_t i = 0; i < range; i++) {
    co_yield i;
  }
}

// _____________________________________________________________________________
TEST(Generators, testAggregation) {
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
TEST(Generators, testEmptyGenerator) {
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
TEST(Generators, testAggregationCutoff) {
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

// _____________________________________________________________________________
TEST(Generators, generatorFromActionWithCallbackCreatesProperGenerator) {
  auto generator = generatorFromActionWithCallback<int>([](auto callback) {
    callback(0);
    callback(1);
    callback(2);
  });
  int counter = 0;
  for (int element : generator) {
    EXPECT_EQ(element, counter);
    ++counter;
  }
  EXPECT_EQ(counter, 3);
}

// Test that in `generatorFromActionWithCallback` the inner and outer thread
// run mutually exclusive.
TEST(Generators, generatorFromActionWithCallbackThreadSafety) {
  size_t counter = 0;
  static constexpr size_t numValues = 20'000;
  auto generator =
      generatorFromActionWithCallback<int>([&counter](auto callback) {
        for (size_t i = 0; i < numValues; ++i) {
          // The counter is modified concurrently directly here, as well as by
          // the outer thread.
          ++counter;
          callback(1);
        }
      });
  for (auto element : generator) {
    counter += element;
  }
  EXPECT_EQ(counter, 2 * numValues);
}

// _____________________________________________________________________________
TEST(Generators, generatorFromActionWithCallbackAbortsProperly) {
  bool unreachableReached = false;
  auto functionWithCallback = [&](auto callback) {
    callback(634);
    unreachableReached = true;
    callback(1);
  };
  {
    auto generator = generatorFromActionWithCallback<int>(functionWithCallback);
    auto iterator = generator.begin();
    ASSERT_NE(iterator, generator.end());
    EXPECT_EQ(*iterator, 634);
    // Prematurely destroy the generator (simulate cancellation).
  }
  ASSERT_FALSE(unreachableReached);

  // Destruction of the generator without consuming any values
  {
    auto generator = generatorFromActionWithCallback<int>(functionWithCallback);
  }
  ASSERT_FALSE(unreachableReached);

  // Exception after consuming the first value
  auto consumeAndThrow = [&]() {
    auto generator = generatorFromActionWithCallback<int>(functionWithCallback);
    auto iterator = generator.begin();
    ASSERT_NE(iterator, generator.end());
    EXPECT_EQ(*iterator, 634);
    throw std::runtime_error("bum");
    // Prematurely destroy the generator (simulate cancellation).
  };
  AD_EXPECT_THROW_WITH_MESSAGE(consumeAndThrow(), ::testing::StrEq("bum"));
}

// _____________________________________________________________________________
TEST(Generators, generatorFromActionWithCallbackPropagatesException) {
  auto generator = generatorFromActionWithCallback<int>([](auto callback) {
    callback(0);
    throw std::runtime_error{"Test Exception"};
  });
  auto iterator = generator.begin();
  ASSERT_NE(iterator, generator.end());
  EXPECT_EQ(*iterator, 0);
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      ++iterator, ::testing::StrEq("Test Exception"), std::runtime_error);
}
