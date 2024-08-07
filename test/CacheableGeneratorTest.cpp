//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "util/CacheableGenerator.h"
#include "util/Generator.h"

using ad_utility::wrapGeneratorWithCache;
using cppcoro::generator;
using namespace std::chrono_literals;

generator<uint32_t> testGenerator(uint32_t range) {
  for (uint32_t i = 0; i < range; i++) {
    co_yield i;
  }
}

TEST(CacheableGenerator, placeholder) {
  auto test = wrapGeneratorWithCache(
      testGenerator(10),
      [](std::optional<uint32_t>& optionalValue, const uint32_t& newValue) {
        if (optionalValue.has_value()) {
          optionalValue.value() += newValue;
        } else {
          optionalValue.emplace(newValue);
        }
        return true;
      },
      [](const std::optional<uint32_t>&) {});
  EXPECT_EQ(1, 1);
}
