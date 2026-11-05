// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "../../util/AllocatorTestHelpers.h"
#include "backports/span.h"
#include "engine/idTable/IdColumnByteIO.h"
#include "engine/idTable/IdTable.h"
#include "engine/idTable/splitLayout/IdColumnVector.h"
#include "global/Id.h"
#include "rdfTypes/GeoPoint.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"

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
  static Owner make(const std::vector<Id>& ids) { return ids; }
  static Owner makeWithSize(size_t size) { return Owner(size); }
  static ql::span<const Id> constView(const Owner& owner) { return owner; }
  static ql::span<Id> view(Owner& owner) { return owner; }
};
struct SplitColumn {
  using Allocator = ad_utility::AllocatorWithLimit<Id>;
  using Owner = splitLayout::IdColumnVector<Allocator>;
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

template <typename Column>
class IdColumnByteIOTest : public ::testing::Test {};
using Columns = ::testing::Types<SpanColumn, SplitColumn>;
TYPED_TEST_SUITE(IdColumnByteIOTest, Columns);
}  // namespace

// Test that `decode` restores the `Id`s that `encode` was called with, when the
// chunks go through `ZstdWrapper` like in all the callers, also for an empty
// column.
TYPED_TEST(IdColumnByteIOTest, roundTrip) {
  using Column = TypeParam;
  for (const auto& ids : {std::vector<Id>{}, sampleIds()}) {
    const auto source = Column::make(ids);
    auto compressed = encode(Column::constView(source), [](const auto& chunks) {
      return ZstdWrapper::compress(chunks);
    });
    auto target = Column::makeWithSize(ids.size());
    decode(Column::view(target), [&compressed](const auto& buffers) {
      ZstdWrapper::decompressToBuffers(compressed, buffers);
    });
    for (size_t i = 0; i < ids.size(); ++i) {
      EXPECT_EQ(static_cast<Id>(target[i]).getBits(), ids[i].getBits()) << i;
    }
  }
}

// Test that a `ql::span<Id>` is a single chunk that points to the `Id`s in
// memory, so it is not copied and its bytes on disk are exactly the `Id`s.
TEST(IdColumnByteIO, spanColumnIsOneChunkWithoutCopy) {
  auto ids = sampleIds();
  auto check = [&ids](const auto& chunks) {
    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_EQ(static_cast<const void*>(chunks[0].data()), ids.data());
    EXPECT_EQ(chunks[0].size(), ids.size() * sizeof(Id));
  };
  encode(ql::span<const Id>{ids}, check);
  decode(ql::span<Id>{ids}, check);
}

// Test that a column of the split layout is two chunks that point to the
// payloads and then to the datatypes of the column, so it is not copied and
// its bytes on disk are the payloads followed by the datatypes.
TEST(IdColumnByteIO, splitColumnIsPayloadsThenDatatypesWithoutCopy) {
  auto column = SplitColumn::make(sampleIds());
  auto view = SplitColumn::view(column);
  auto check = [&view](const auto& chunks) {
    ASSERT_EQ(chunks.size(), 2u);
    EXPECT_EQ(static_cast<const void*>(chunks[0].data()),
              view.rawPayloads().data());
    EXPECT_EQ(chunks[0].size(), view.size() * sizeof(Id::PayloadType));
    EXPECT_EQ(static_cast<const void*>(chunks[1].data()),
              view.rawDatatypes().data());
    EXPECT_EQ(chunks[1].size(), view.size() * sizeof(Id::DatatypeType));
  };
  encode(SplitColumn::constView(column), check);
  decode(view, check);
}

// Test that a column of an `IdTable` has `BYTES_PER_ID_COLUMN_ENTRY` bytes per
// `Id` on disk, which the callers use to compute the number of rows of a
// block from its number of bytes.
TEST(IdColumnByteIO, idTableColumnsHaveBytesPerIdColumnEntry) {
  IdTable table{1, ad_utility::testing::makeAllocator()};
  for (Id id : sampleIds()) {
    table.push_back({id});
  }
  size_t numBytes = 0;
  encode(std::as_const(table).getColumn(0), [&numBytes](const auto& chunks) {
    for (auto chunk : chunks) {
      numBytes += chunk.size();
    }
  });
  EXPECT_EQ(numBytes, table.numRows() * BYTES_PER_ID_COLUMN_ENTRY);
}
