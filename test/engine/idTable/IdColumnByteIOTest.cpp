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
#include <utility>
#include <vector>

#include "../../util/AllocatorTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/span.h"
#include "engine/idTable/IdColumnByteIO.h"
#include "engine/idTable/IdTable.h"
#include "engine/idTable/splitLayout/IdColumnVector.h"
#include "global/Id.h"
#include "rdfTypes/GeoPoint.h"

using namespace ad_utility::idColumnByteIO;
namespace splitLayout = columnBasedIdTable::splitLayout;

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

// The two kinds of columns of an `IdTable`: a `ql::span<Id>` and a column of
// the split layout. Both work with the `Id` of every build.
struct SpanColumn {
  using Owner = std::vector<Id>;
  static constexpr size_t bytesPerId = sizeof(Id);
  static Owner make(const std::vector<Id>& ids) { return ids; }
  static Owner makeWithSize(size_t size) { return Owner(size); }
  static ql::span<const Id> constView(const Owner& owner) { return owner; }
  static ql::span<Id> view(Owner& owner) { return owner; }
};
struct SplitColumn {
  using Allocator = ad_utility::AllocatorWithLimit<Id>;
  using Owner = splitLayout::IdColumnVector<Allocator>;
  static constexpr size_t bytesPerId =
      sizeof(Id::PayloadType) + sizeof(Id::DatatypeType);
  static Owner make(const std::vector<Id>& ids) {
    return Owner{ids.begin(), ids.end(), ad_utility::testing::makeAllocator()};
  }
  static Owner makeWithSize(size_t size) {
    return Owner{size, ad_utility::testing::makeAllocator()};
  }
  static splitLayout::ConstIdColumnRef constView(const Owner& owner) {
    return owner.asConstView();
  }
  static splitLayout::IdColumnRef view(Owner& owner) { return owner.asView(); }
};

// The bytes that `useBytes` is called with, as a `std::string`.
std::string bytesAsString(ql::span<const char> bytes) {
  return std::string(bytes.begin(), bytes.end());
}

// The bytes of the trivially copyable `value`.
template <typename T>
std::string bytesOf(const T& value) {
  return std::string{reinterpret_cast<const char*>(&value), sizeof(value)};
}

template <typename Column>
class IdColumnByteIOTest : public ::testing::Test {};
using Columns = ::testing::Types<SpanColumn, SplitColumn>;
TYPED_TEST_SUITE(IdColumnByteIOTest, Columns);
}  // namespace

// Test that `decode` restores exactly the `Id`s that `encode` was called with,
// and that both pass `bytesPerId` bytes per `Id` to their callback.
TYPED_TEST(IdColumnByteIOTest, encodeAndDecode) {
  using Column = TypeParam;
  for (const auto& ids : {std::vector<Id>{}, sampleIds()}) {
    const auto source = Column::make(ids);
    std::string bytes = encode(Column::constView(source), &bytesAsString);
    EXPECT_EQ(bytes.size(), ids.size() * Column::bytesPerId);

    auto target = Column::makeWithSize(ids.size());
    decode(Column::view(target), [&bytes](ql::span<char> buffer) {
      ASSERT_EQ(buffer.size(), bytes.size());
      ql::ranges::copy(bytes, buffer.begin());
    });
    for (size_t i = 0; i < ids.size(); ++i) {
      EXPECT_EQ(static_cast<Id>(target[i]).getBits(), ids[i].getBits()) << i;
    }
  }
}

// Test that the memory of a `ql::span<Id>` is passed to the callbacks without a
// copy, so its bytes on disk are exactly the `Id`s in memory.
TEST(IdColumnByteIO, spanColumnIsNotCopied) {
  auto ids = sampleIds();
  const void* memory = ids.data();
  size_t numBytes = ids.size() * sizeof(Id);
  encode(ql::span<const Id>{ids},
         [memory, numBytes](ql::span<const char> bytes) {
           EXPECT_EQ(static_cast<const void*>(bytes.data()), memory);
           EXPECT_EQ(bytes.size(), numBytes);
         });
  decode(ql::span<Id>{ids}, [memory, numBytes](ql::span<char> bytes) {
    EXPECT_EQ(static_cast<const void*>(bytes.data()), memory);
    EXPECT_EQ(bytes.size(), numBytes);
  });
}

// Test that the bytes of a column of the split layout are the payloads of all
// the `Id`s, followed by their datatypes.
TEST(IdColumnByteIO, splitColumnIsPayloadsThenDatatypes) {
  auto column = SplitColumn::make(
      {Id::makeFromInt(3), Id::makeFromVocabIndex(VocabIndex::make(42))});
  EXPECT_EQ(encode(SplitColumn::constView(column), &bytesAsString),
            absl::StrCat(bytesOf(uint64_t{3}), bytesOf(uint64_t{42}),
                         bytesOf(static_cast<uint8_t>(Datatype::Int)),
                         bytesOf(static_cast<uint8_t>(Datatype::VocabIndex))));
}

// Test that a column of an `IdTable` has `BYTES_PER_ID_COLUMN_ENTRY` bytes per
// `Id` on disk, which the callers use to compute the number of rows of a
// block from its number of bytes.
TEST(IdColumnByteIO, idTableColumnsHaveBytesPerIdColumnEntry) {
  IdTable table{1, ad_utility::testing::makeAllocator()};
  for (Id id : sampleIds()) {
    table.push_back({id});
  }
  EXPECT_EQ(encode(std::as_const(table).getColumn(0), &bytesAsString).size(),
            table.numRows() * BYTES_PER_ID_COLUMN_ENTRY);
}
