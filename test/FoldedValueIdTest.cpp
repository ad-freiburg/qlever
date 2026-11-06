//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

// Tests that only make sense for the legacy `ValueId`, which packs the datatype
// and the payload into a single 64-bit word. The tests for both layouts are in
// `IdBaseTest.cpp`.

#include <gtest/gtest.h>

#include "./ValueIdTestHelpers.h"
#include "global/FoldedValueId.h"
#include "util/BitUtils.h"

namespace foldedValueId {

// Test that the bits of a `ValueId` are the datatype in the highest
// `numDatatypeBits` bits and the payload in the lower `numDataBits` bits.
TEST(FoldedValueId, bitsArePackedIntoOneWord) {
  static_assert(ValueId::numDatatypeBits == 4);
  static_assert(ValueId::numDataBits == 60);
  for (auto id : makeRandomIds()) {
    EXPECT_EQ(id.getBits(),
              (uint64_t{id.getDatatypeBits()} << ValueId::numDataBits) |
                  id.getPayloadBits());
  }
  // A negative integer has all payload bits set, but none of the datatype
  // bits.
  EXPECT_EQ(ValueId::makeFromInt(-1).getBits(),
            (static_cast<uint64_t>(Datatype::Int) << ValueId::numDataBits) |
                ad_utility::bitMaskForLowerBits(ValueId::numDataBits));

  // `incremented` is the increment of this single word, so the overflow of the
  // payload carries into the datatype bits.
  for (auto id : {ValueId::makeUndefined(), ValueId::makeFromInt(-42),
                  ValueId::makeFromDouble(1.5), makeVocabId(5),
                  makeVocabId(ValueId::maxIndex)}) {
    EXPECT_EQ(id.incremented().getBits(), id.getBits() + 1);
  }
}

// Test that an integer outside of the 60 bits of the payload overflows
// according to the semantics of `NBitInteger`.
TEST(FoldedValueId, makeFromIntOverflows) {
  auto testOverflow = [](auto generator) {
    using I = ValueId::IntegerType;
    for (size_t i = 0; i < 10'000; ++i) {
      auto value = generator();
      auto id = ValueId::makeFromInt(value);
      ASSERT_EQ(id.getDatatype(), Datatype::Int);
      ASSERT_EQ(id.getInt(), I::fromNBit(I::toNBit(value)));
      ASSERT_NE(id.getInt(), value);
    }
  };
  testOverflow(overflowingNBitGenerator);
  testOverflow(underflowingNBitGenerator);
}

// Test that an index above `maxIndex` (60 bits) is rejected with an
// `IndexTooLargeException`.
TEST(FoldedValueId, indexTooLarge) {
  EXPECT_THROW(makeVocabId(ValueId::maxIndex + 1),
               ValueId::IndexTooLargeException);
}

}  // namespace foldedValueId
