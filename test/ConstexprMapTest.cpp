// Copyright 2021 - 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "./util/GTestHelpers.h"
#include "util/ConstexprMap.h"

using namespace ad_utility;

// _____________________________________________________________________________
TEST(ConstexprMap, AllTests) {
  using P = ConstexprMapPair<int, int>;
  ad_utility::ConstexprMap map{std::array{P{3, 36}, P{1, 3}}};
  ASSERT_EQ(map.at(1), 3);
  ASSERT_EQ(map.at(3), 36);
  ASSERT_TRUE(map.contains(1));
  ASSERT_TRUE(map.contains(3));
  ASSERT_FALSE(map.contains(2));
  ASSERT_FALSE(map.contains(4));
  AD_EXPECT_THROW_WITH_MESSAGE(map.at(4),
                               ::testing::HasSubstr("was not found"));

  // Now the same tests at compile time.
  static constexpr ad_utility::ConstexprMap mapC{std::array{P{3, 36}, P{1, 3}}};
  static_assert(mapC.at(1) == 3);
  static_assert(mapC.at(3) == 36);
  static_assert(!mapC.contains(2));
  static_assert(!mapC.contains(4));

  // The following map throws during construction because it has duplicate keys.
  auto illegalMap = []() {
    return ConstexprMap{std::array{P{1, 3}, P{3, 36}, P{1, 5}}};
  };

  AD_EXPECT_THROW_WITH_MESSAGE(illegalMap(),
                               ::testing::HasSubstr("all the keys are unique"));
}
