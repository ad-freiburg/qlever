// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "engine/idTable/IdColumnByteIO.h"
#include "global/Id.h"
#include "rdfTypes/GeoPoint.h"

using namespace ad_utility::idColumnByteIO;

namespace {
// `Id`s of the datatypes whose bits are not a pointer, including payloads that
// use all of their bits.
std::vector<Id> sampleIds() {
  return {Id::makeUndefined(),
          Id::makeFromBool(true),
          Id::makeFromInt(0),
          Id::makeFromInt(-1),
          Id::makeFromInt(Id::maxInt),
          Id::makeFromDouble(-0.5),
          Id::makeFromDouble(std::numeric_limits<double>::infinity()),
          Id::makeFromVocabIndex(VocabIndex::make(123456789)),
          Id::makeFromBlankNodeIndex(BlankNodeIndex::make(7)),
          Id::makeFromGeoPoint(GeoPoint{48.0, 7.8})};
}

// The bytes that `useBytes` is called with, as a `std::string`.
std::string bytesAsString(ql::span<const char> bytes) {
  return std::string(bytes.begin(), bytes.end());
}

// The bytes of the trivially copyable `value`.
template <typename T>
std::string bytesOf(const T& value) {
  return std::string{reinterpret_cast<const char*>(&value), sizeof(value)};
}

template <typename F>
class IdColumnByteIOTest : public ::testing::Test {};
using Formats = ::testing::Types<LegacyFormat, SplitFormat>;
TYPED_TEST_SUITE(IdColumnByteIOTest, Formats);
}  // namespace

// Test that `decode` restores exactly the `Id`s that `encode` was called with,
// and that both pass `numBytesPerId` bytes per `Id` to their callback.
TYPED_TEST(IdColumnByteIOTest, encodeAndDecode) {
  using TestedFormat = TypeParam;
  for (const auto& ids : {std::vector<Id>{}, sampleIds()}) {
    std::string bytes = TestedFormat::encode(ids, &bytesAsString);
    EXPECT_EQ(bytes.size(), ids.size() * TestedFormat::numBytesPerId);

    std::vector<Id> decoded(ids.size());
    TestedFormat::decode(decoded, [&bytes](ql::span<char> target) {
      ASSERT_EQ(target.size(), bytes.size());
      ql::ranges::copy(bytes, target.begin());
    });
    for (size_t i = 0; i < ids.size(); ++i) {
      EXPECT_EQ(decoded[i].getBits(), ids[i].getBits()) << i;
    }
  }
}

// Test that the `LegacyFormat` passes the memory of the column itself to the
// callbacks, so the bytes on disk are exactly the `Id`s in memory and nothing
// is copied.
TEST(IdColumnByteIO, legacyFormatUsesTheMemoryOfTheColumn) {
  static_assert(LegacyFormat::numBytesPerId == sizeof(Id));
  auto ids = sampleIds();
  const void* memory = ids.data();
  size_t numBytes = ids.size() * sizeof(Id);
  LegacyFormat::encode(ids, [memory, numBytes](ql::span<const char> bytes) {
    EXPECT_EQ(static_cast<const void*>(bytes.data()), memory);
    EXPECT_EQ(bytes.size(), numBytes);
  });
  LegacyFormat::decode(ids, [memory, numBytes](ql::span<char> bytes) {
    EXPECT_EQ(static_cast<const void*>(bytes.data()), memory);
    EXPECT_EQ(bytes.size(), numBytes);
  });
}

// Test that the `SplitFormat` stores the payloads of all the `Id`s, followed by
// their datatypes.
TEST(IdColumnByteIO, splitFormatStoresPayloadsAndThenDatatypes) {
  static_assert(SplitFormat::numBytesPerId == 9);
  std::vector<Id> ids{Id::makeFromInt(3),
                      Id::makeFromVocabIndex(VocabIndex::make(42))};
  EXPECT_EQ(SplitFormat::encode(ids, &bytesAsString),
            absl::StrCat(bytesOf(uint64_t{3}), bytesOf(uint64_t{42}),
                         bytesOf(static_cast<uint8_t>(Datatype::Int)),
                         bytesOf(static_cast<uint8_t>(Datatype::VocabIndex))));
}
