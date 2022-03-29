//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/util/BitUtils.h"

using namespace ad_utility;

TEST(BitPacking, numBitsRequired) {
  static_assert(numBitsRequired(0) == 1);
  static_assert(numBitsRequired(3) == 2);
  static_assert(numBitsRequired(4) == 2);
  static_assert(numBitsRequired(5) == 3);
  static_assert(numBitsRequired(8) == 3);
  static_assert(numBitsRequired(9) == 4);
  static_assert(numBitsRequired(16) == 4);
  static_assert(numBitsRequired(17) == 5);
  static_assert(numBitsRequired(32) == 5);
  static_assert(numBitsRequired(33) == 6);
  static_assert(numBitsRequired(1024) == 10);
  static_assert(numBitsRequired(1025) == 11);

  for (size_t i = 1025; i <= 2048; ++i) {
    ASSERT_EQ(numBitsRequired(i), 11);
  }
  static_assert(numBitsRequired(2049) == 12);
}

TEST(BitPacking, bitMaskForLowerBits) {
  static_assert(bitMaskForLowerBits(0) == 0);
  static_assert(bitMaskForLowerBits(1) == 1);
  static_assert(bitMaskForLowerBits(2) == 3);

  for (size_t i = 1; i <= 64; ++i) {
    auto numBits = static_cast<uint64_t>(std::pow(2, i)) - 1;
    ASSERT_EQ(bitMaskForLowerBits(i), numBits);
  }

  for (size_t i = 65; i < 2048; ++i) {
    ASSERT_THROW(bitMaskForLowerBits(i), std::out_of_range);
  }
}

TEST(BitPacking, unsignedTypeForNumberOfBits) {
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
