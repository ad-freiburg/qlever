// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "IdColumnTestHelpers.h"
#include "backports/algorithm.h"
#include "engine/idTable/splitLayout/IdColumn.h"
#include "engine/idTable/splitLayout/IdColumnVector.h"
#include "util/serializer/ByteBufferSerializer.h"

using namespace columnBasedIdTable::splitLayout;
using namespace ad_utility::serialization;
using testHelpers::sampleIds;
using testHelpers::testAllocator;

namespace {
// Write a `char`, the column of the `ids` and the number 42 with a `Writer`.
// The `char` makes the column start at a position that is not aligned for the
// payloads.
template <typename Writer>
auto writeColumn(const std::vector<Id>& ids) {
  Writer writer;
  writer << 'x';
  writer
      << IdColumnVector{ids.begin(), ids.end(), testAllocator()}.asConstView();
  writer << uint64_t{42};
  return std::move(writer).data();
}

// Read what `writeColumn` wrote with a `Reader`, using `readColumn` for the
// column, and check that the column has the `ids` and that the number after
// it is read correctly.
template <typename Reader, typename Bytes, typename ReadColumn>
void expectColumnIsRead(const std::vector<Id>& ids, Bytes bytes,
                        ReadColumn readColumn) {
  Reader reader{std::move(bytes)};
  char c = 0;
  reader >> c;
  EXPECT_TRUE(ql::ranges::equal(readColumn(reader), ids));
  uint64_t number = 0;
  reader >> number;
  EXPECT_EQ(number, 42u);
}
}  // namespace

// Test that a column is written as its size, then the payloads of all its
// `Id`s, then their datatypes.
TEST(IdColumnSerialization, format) {
  const auto ids = sampleIds();
  const IdColumnVector column{ids.begin(), ids.end(), testAllocator()};
  const auto view = column.asConstView();
  ByteBufferWriteSerializer writer;
  writer << view;

  ByteBufferWriteSerializer expected;
  expected << view.size();
  expected.serializeBytes(
      reinterpret_cast<const char*>(view.rawPayloads().data()),
      view.rawPayloads().size_bytes());
  expected.serializeBytes(
      reinterpret_cast<const char*>(view.rawDatatypes().data()),
      view.rawDatatypes().size_bytes());
  EXPECT_EQ(writer.data(), expected.data());
}

// Test that reading into a view of the right size restores the `Id`s, with
// and without aligned serialization.
TEST(IdColumnSerialization, roundTrip) {
  for (const auto& ids : {std::vector<Id>{}, sampleIds()}) {
    auto readInto = [&ids](auto& reader) {
      IdColumnVector column{ids.size(), testAllocator()};
      reader >> column.asView();
      return column;
    };
    expectColumnIsRead<ByteBufferReadSerializer>(
        ids, writeColumn<ByteBufferWriteSerializer>(ids), readInto);
    expectColumnIsRead<AlignedByteBufferReadSerializer>(
        ids, writeColumn<AlignedByteBufferWriteSerializer>(ids), readInto);
  }
}

// Test that reading into a view of another size throws, like for a
// `ql::span`.
TEST(IdColumnSerialization, readingIntoAViewOfAnotherSizeThrows) {
  const auto ids = sampleIds();
  for (size_t size : {size_t{0}, ids.size() - 1, ids.size() + 1}) {
    ByteBufferReadSerializer reader{
        writeColumn<ByteBufferWriteSerializer>(ids)};
    char c = 0;
    reader >> c;
    IdColumnVector column{size, testAllocator()};
    EXPECT_ANY_THROW(reader >> column.asView()) << size;
  }
}

// Test that `zeroCopyDeserializeIdColumn` returns a view whose two arrays
// point into the buffer of the serializer, without a copy.
TEST(IdColumnSerialization, zeroCopyDeserialization) {
  for (const auto& ids : {std::vector<Id>{}, sampleIds()}) {
    auto bytes = writeColumn<AlignedByteBufferWriteSerializer>(ids);
    const char* begin = bytes.data();
    const char* end = begin + bytes.size();
    auto pointsIntoBuffer = [begin, end](const auto* pointer) {
      const auto* byte = reinterpret_cast<const char*>(pointer);
      return begin <= byte && byte < end;
    };
    expectColumnIsRead<AlignedByteBufferReadSerializer>(
        ids, std::move(bytes), [&](auto& reader) {
          ConstIdColumnRef column = zeroCopyDeserializeIdColumn(reader);
          if (!column.empty()) {
            EXPECT_TRUE(pointsIntoBuffer(column.rawPayloads().data()));
            EXPECT_TRUE(pointsIntoBuffer(column.rawDatatypes().data()));
          }
          return column;
        });
  }
}
