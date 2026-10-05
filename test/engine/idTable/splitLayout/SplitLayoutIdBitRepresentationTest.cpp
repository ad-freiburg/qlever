// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/iostreams/filter/zlib.hpp>

#include "backports/algorithm.h"
#include "engine/idTable/splitLayout/IdRef.h"
#include "util/AllocatorWithLimit.h"
#include "util/json/Writer.h"

using namespace columnBasedIdTable::splitLayout;
// _____________________________________________________________________________
TEST(SplitLayoutIdBitRepresentationTest, construction) {
  constexpr int idValue = 42;
  constexpr ValueId id = Id::makeFromInt(idValue);
  auto [datatype, payload] = getBitsCompat(id);
  const SplitLayoutIdBitRepresentation representation{datatype, payload};

  ASSERT_EQ(static_cast<Datatype>(representation.datatype_), Datatype::Int);
  ASSERT_EQ(representation.payload_, idValue);
}

// _____________________________________________________________________________
TEST(SplitLayoutIdBitRepresentationTest, incremented) {
  constexpr int idValue = 42;
  constexpr ValueId id = Id::makeFromInt(idValue);
  auto [datatype, payload] = getBitsCompat(id);
  const SplitLayoutIdBitRepresentation representation{datatype, payload};

  ASSERT_EQ(static_cast<Datatype>(representation.datatype_), Datatype::Int);
  ASSERT_EQ(representation.payload_, idValue);

  const SplitLayoutIdBitRepresentation incrementedRepresentation =
      representation.incremented();
  const auto [inc_datatype, inc_payload] = incrementedRepresentation;

  ASSERT_EQ(static_cast<Datatype>(inc_datatype), Datatype::Int);
  ASSERT_EQ(inc_payload, idValue + 1);
}

// _____________________________________________________________________________
TEST(SplitLayoutIdBitRepresentationTest, incremented_maxValue) {
  constexpr SplitLayoutIdBitRepresentation representation{
      static_cast<uint8_t>(Datatype::Int),
      std::numeric_limits<uint64_t>::max()};

  ASSERT_EQ(static_cast<Datatype>(representation.datatype_), Datatype::Int);
  ASSERT_EQ(representation.payload_, std::numeric_limits<uint64_t>::max());

  constexpr SplitLayoutIdBitRepresentation incrementedRepresentation =
      representation.incremented();
  const auto [inc_datatype, inc_payload] = incrementedRepresentation;

  ASSERT_EQ(inc_datatype, static_cast<uint8_t>(Datatype::Int) + 1);
  ASSERT_EQ(inc_payload, 0u);
}
