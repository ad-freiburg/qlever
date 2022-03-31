//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/util/BitUtils.h"

using namespace ad_utility;

TEST(BitUtils, bitMaskForLowerBits) {
  static_assert(bitMaskForLowerBits(0) == 0);
  static_assert(bitMaskForLowerBits(1) == 1);
  static_assert(bitMaskForLowerBits(2) == 3);

  for (size_t i = 1; i <= 64; ++i) {
    auto expected = static_cast<uint64_t>(std::pow(2, i)) - 1;
    ASSERT_EQ(bitMaskForLowerBits(i), expected);
  }

  for (size_t i = 65; i < 2048; ++i) {
    ASSERT_THROW(bitMaskForLowerBits(i), std::out_of_range);
  }
}

TEST(BitUtils, bitMaskForHigherBits) {
  for (size_t i = 1; i <= 64; ++i) {
    auto max = std::numeric_limits<uint64_t>::max();
    auto expected = max - (static_cast<uint64_t>(std::pow(2, i)) - 1);
    ASSERT_EQ(bitMaskForHigherBits(i), expected);
  }

  for (size_t i = 65; i < 2048; ++i) {
    ASSERT_THROW(bitMaskForHigherBits(i), std::out_of_range);
  }
}

TEST(BitUtils, unsignedTypeForNumberOfBits) {
  static_assert(std::is_same_v<uint8_t, unsignedTypeForNumberOfBits<0>>);
  static_assert(std::is_same_v<uint8_t, unsignedTypeForNumberOfBits<1>>);
  static_assert(std::is_same_v<uint8_t, unsignedTypeForNumberOfBits<7>>);
  static_assert(std::is_same_v<uint8_t, unsignedTypeForNumberOfBits<8>>);
  static_assert(std::is_same_v<uint16_t, unsignedTypeForNumberOfBits<9>>);
  static_assert(std::is_same_v<uint16_t, unsignedTypeForNumberOfBits<16>>);
  static_assert(std::is_same_v<uint32_t, unsignedTypeForNumberOfBits<17>>);
  static_assert(std::is_same_v<uint32_t, unsignedTypeForNumberOfBits<32>>);
  static_assert(std::is_same_v<uint64_t, unsignedTypeForNumberOfBits<33>>);
  static_assert(std::is_same_v<uint64_t, unsignedTypeForNumberOfBits<64>>);
}
