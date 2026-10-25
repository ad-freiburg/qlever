// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Pascal Keßler <kesslerp@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <sstream>

#include "IdColumnTestHelpers.h"
#include "engine/idTable/splitLayout/SplitLayoutIdBitRepresentation.h"
#include "util/HashSet.h"

using namespace columnBasedIdTable::splitLayout;
using testHelpers::getBitsCompat;

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
  //
  // NOTE: Keep `maxPayload` non-const. For a `const` object with a constant
  // initializer, clang evaluates the `constexpr` call at compile time (even
  // at `-O0`), so the carry would never run at runtime and the coverage report
  // would not see it.
  SplitLayoutIdBitRepresentation maxPayload{
      static_cast<uint8_t>(Datatype::Int),
      std::numeric_limits<uint64_t>::max()};
  const auto carried = maxPayload.incremented();
  EXPECT_EQ(carried.datatype_, static_cast<uint8_t>(Datatype::Int) + 1);
  EXPECT_EQ(carried.payload_, 0u);
}

// Test the output via `operator<<` and `absl::StrCat`.
TEST(SplitLayoutIdBitRepresentationTest, toString) {
  const SplitLayoutIdBitRepresentation representation{3, 42};
  std::ostringstream stream;
  stream << representation;
  EXPECT_EQ(stream.str(), "3:42");
  EXPECT_EQ(absl::StrCat(representation), "3:42");
}

// Test `std::hash` and the hash of abseil: equal representations have the same
// hash, and both the datatype and the payload change it.
TEST(SplitLayoutIdBitRepresentationTest, hash) {
  using Rep = SplitLayoutIdBitRepresentation;
  const std::hash<Rep> hash;
  EXPECT_EQ(hash(Rep{3, 42}), hash(Rep{3, 42}));
  EXPECT_NE(hash(Rep{3, 42}), hash(Rep{4, 42}));
  EXPECT_NE(hash(Rep{3, 42}), hash(Rep{3, 43}));

  const ad_utility::HashSet<Rep> set{Rep{3, 42}, Rep{3, 42}, Rep{4, 42}};
  EXPECT_EQ(set.size(), 2u);
  EXPECT_TRUE(set.contains(Rep{4, 42}));
  EXPECT_FALSE(set.contains(Rep{3, 43}));
}
