// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "engine/idTable/splitLayout/IdRef.h"

using namespace columnBasedIdTable::splitLayout;

// Test the construction from a datatype and a payload.
TEST(SplitLayoutIdBitRepresentationTest, construction) {
  constexpr int idValue = 42;
  auto [datatype, payload] = getBitsCompat(Id::makeFromInt(idValue));
  const SplitLayoutIdBitRepresentation representation{datatype, payload};
  EXPECT_EQ(static_cast<Datatype>(representation.datatype_), Datatype::Int);
  EXPECT_EQ(representation.payload_, static_cast<uint64_t>(idValue));
}

// Test `incremented`, with and without a carry into the datatype.
TEST(SplitLayoutIdBitRepresentationTest, incremented) {
  // Without a carry, only the payload is incremented.
  constexpr int idValue = 42;
  auto [datatype, payload] = getBitsCompat(Id::makeFromInt(idValue));
  const SplitLayoutIdBitRepresentation representation{datatype, payload};
  const auto [incDatatype, incPayload] = representation.incremented();
  EXPECT_EQ(static_cast<Datatype>(incDatatype), Datatype::Int);
  EXPECT_EQ(incPayload, static_cast<uint64_t>(idValue) + 1);

  // The maximal payload carries into the datatype.
  constexpr SplitLayoutIdBitRepresentation maxPayload{
      static_cast<uint8_t>(Datatype::Int),
      std::numeric_limits<uint64_t>::max()};
  constexpr auto carried = maxPayload.incremented();
  EXPECT_EQ(carried.datatype_, static_cast<uint8_t>(Datatype::Int) + 1);
  EXPECT_EQ(carried.payload_, 0u);
}
