//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <bitset>

#include "../src/util/Date.h"

TEST(Date, Size) {
  ASSERT_EQ(sizeof(Date), 8);
  ASSERT_EQ(8, Date::remainingBits);
}

TEST(Date, example) {
  Date d{2022, 7, 13};
  std::bitset<64> bits{d.toBytes()};
  std::cout << bits << std::endl;
}