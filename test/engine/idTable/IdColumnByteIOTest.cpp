// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <vector>

#include "./IdColumnTestHelpers.h"
#include "engine/idTable/IdColumnByteIO.h"
#include "engine/idTable/IdColumnVector.h"

using namespace columnBasedIdTable;
using testHelpers::sampleIds;
using testHelpers::testAllocator;

// _____________________________________________________________________________
TEST(IdColumnByteIOTest, packAndUnpackBytesRoundtrip) {
  auto ids = sampleIds();
  IdColumnVector vec{ids.begin(), ids.end(), testAllocator()};

  auto bytes = packIdColumnToBytes(vec.asConstView());
  ASSERT_EQ(bytes.size(), ids.size() * BYTES_PER_ID_COLUMN_ENTRY);

  IdColumnVector roundtripped{ids.size(), testAllocator()};
  unpackBytesToIdColumn(ql::span<const char>{bytes}, roundtripped.asView());
  for (size_t i = 0; i < ids.size(); ++i) {
    EXPECT_EQ(static_cast<Id>(roundtripped[i]), ids.at(i));
  }
}

// _____________________________________________________________________________
TEST(IdColumnByteIOTest, packOfEmptyColumnIsEmpty) {
  const IdColumnVector empty{testAllocator()};
  const auto bytes = packIdColumnToBytes(empty.asConstView());
  EXPECT_TRUE(bytes.empty());
}

// _____________________________________________________________________________
TEST(IdColumnByteIOTest, unpackRejectsWronglySizedBuffers) {
  auto ids = sampleIds();
  IdColumnVector roundtripped{ids.size(), testAllocator()};

  // An empty buffer doesn't match `ids.size() * BYTES_PER_ID_COLUMN_ENTRY`
  // (since `ids` is non-empty), so this must be rejected rather than
  // silently reading out of bounds or leaving some elements untouched.
  ASSERT_THROW(unpackBytesToIdColumn({}, roundtripped.asView()),
               ad_utility::Exception);

  // One byte short of the correct size is rejected too, not just "empty".
  auto correctBytes = packIdColumnToBytes(
      IdColumnVector{ids.begin(), ids.end(), testAllocator()}.asConstView());
  correctBytes.pop_back();
  ASSERT_THROW(unpackBytesToIdColumn(ql::span<const char>{correctBytes},
                                     roundtripped.asView()),
               ad_utility::Exception);
}
