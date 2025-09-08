//  Copyright 2022 - 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//           Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/util/BitUtils.h"

namespace {
using namespace ad_utility;

// _____________________________________________________________________________
TEST(BitUtils, bitMaskForLowerBits) {
  static_assert(bitMaskForLowerBits(0) == 0);
  static_assert(bitMaskForLowerBits(1) == 1);
  static_assert(bitMaskForLowerBits(2) == 3);

  for (size_t i = 0; i < 64; ++i) {
    auto expected = static_cast<uint64_t>(std::pow(2, i)) - 1;
    ASSERT_EQ(bitMaskForLowerBits(i), expected);
  }
  ASSERT_EQ(bitMaskForLowerBits(64), std::numeric_limits<uint64_t>::max());

  for (size_t i = 65; i < 2048; ++i) {
    ASSERT_THROW(bitMaskForLowerBits(i), std::out_of_range);
  }
}

// _____________________________________________________________________________
TEST(BitUtils, bitMaskForHigherBits) {
  constexpr static auto max = std::numeric_limits<uint64_t>::max();
  static_assert(bitMaskForHigherBits(0) == 0);
  static_assert(bitMaskForHigherBits(64) == max);
  static_assert(bitMaskForHigherBits(63) == max - 1);
  static_assert(bitMaskForHigherBits(62) == max - 3);

  for (size_t i = 0; i <= 64; ++i) {
    auto expected = max - bitMaskForLowerBits(64 - i);
    ASSERT_EQ(bitMaskForHigherBits(i), expected);
  }

  for (size_t i = 65; i < 2048; ++i) {
    ASSERT_THROW(bitMaskForHigherBits(i), std::out_of_range);
  }
}

// _____________________________________________________________________________
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

// _____________________________________________________________________________
TEST(BitUtils, bitMaskSize) {
  // Test that (a) bitMaskSizeForValue returns the right result and (b) can be
  // evaluated at compile time.
  static_assert(bitMaskSizeForValue(0) == 0);
  static_assert(bitMaskSizeForValue(1) == 1);
  static_assert(bitMaskSizeForValue(2) == 2);
  static_assert(bitMaskSizeForValue(3) == 2);
  static_assert(bitMaskSizeForValue(4) == 3);
  static_assert(bitMaskSizeForValue(-1ull) == 64);
  static_assert(bitMaskSizeForValue(std::numeric_limits<uint64_t>::max()) ==
                64);
  // Also some runtime tests to make codecov happy, who doesn't see the
  // static_assert because they happen at compile time.
  ASSERT_EQ(bitMaskSizeForValue(0), 0);
  ASSERT_EQ(bitMaskSizeForValue(1), 1);
  ASSERT_EQ(bitMaskSizeForValue(4), 3);
}

}  // namespace
