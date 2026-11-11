// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <array>
#include <type_traits>

#include "global/Id.h"
#include "global/ValueIdComparators.h"

TEST(TripleTermId, RoundTripAndTypeSeparation) {
  auto id = Id::makeFromTripleTermIndex(TripleTermIndex::make(17));
  EXPECT_EQ(id.getDatatype(), Datatype::TripleTermIndex);
  EXPECT_EQ(id.getTripleTermIndex().get(), 17);
  EXPECT_EQ(id, Id::fromBits(id.getBits()));
  EXPECT_NE(id, Id::makeFromInt(17));
  EXPECT_NE(id, Id::makeFromVocabIndex(VocabIndex::make(17)));
  EXPECT_FALSE(id.isTrivial());
  EXPECT_TRUE(id.canBeComparedBitwise());
  EXPECT_EQ(toString(id.getDatatype()), "TripleTermIndex");
  EXPECT_TRUE(id.visit([](const auto& value) {
    return std::is_same_v<std::decay_t<decltype(value)>, TripleTermIndex>;
  }));
}

TEST(TripleTermId, PayloadBoundsAndOrdering) {
  auto zero = Id::makeFromTripleTermIndex(TripleTermIndex::make(0));
  auto maximum =
      Id::makeFromTripleTermIndex(TripleTermIndex::make(Id::maxIndex));
  EXPECT_LT(zero, maximum);
  EXPECT_EQ(maximum.getTripleTermIndex().get(), Id::maxIndex);
  EXPECT_THROW(
      Id::makeFromTripleTermIndex(TripleTermIndex::make(Id::maxIndex + 1)),
      Id::IndexTooLargeException);
}

TEST(TripleTermId, EqualityRangeDoesNotMatchOtherDatatypes) {
  auto term = Id::makeFromTripleTermIndex(TripleTermIndex::make(17));
  std::vector<Id> values{Id::makeFromInt(17),
                         Id::makeFromVocabIndex(VocabIndex::make(17)), term};
  auto ranges = valueIdComparators::getRangesForId(
      values.begin(), values.end(), term, valueIdComparators::Comparison::EQ);
  ASSERT_EQ(ranges.size(), 1);
  EXPECT_EQ(ranges[0].first, values.begin() + 2);
  EXPECT_EQ(ranges[0].second, values.end());
}

// These tags are part of existing index files and binary query results.
// Appending a datatype must preserve the previous numeric assignments.
TEST(TripleTermId, ExistingDatatypeTagsRemainStable) {
  constexpr auto existing = std::to_array<Datatype>(
      {Datatype::Undefined, Datatype::Bool, Datatype::Int, Datatype::Double,
       Datatype::VocabIndex, Datatype::LocalVocabIndex,
       Datatype::SecondaryVocabIndex, Datatype::TextRecordIndex, Datatype::Date,
       Datatype::GeoPoint, Datatype::WordVocabIndex, Datatype::BlankNodeIndex,
       Datatype::EncodedVal});
  for (size_t tag = 0; tag < existing.size(); ++tag) {
    EXPECT_EQ(static_cast<size_t>(existing[tag]), tag);
    // Decode raw legacy IDs without going through a datatype-specific factory.
    auto bits = (static_cast<Id::T>(tag) << Id::numDataBits) | 17;
    auto id = Id::fromBits(bits);
    EXPECT_EQ(id.getDatatype(), existing[tag]);
    EXPECT_EQ(id.getBits(), bits);
  }
  EXPECT_EQ(static_cast<size_t>(Datatype::TripleTermIndex), existing.size());
  EXPECT_EQ(sizeof(Id), sizeof(uint64_t));
}
