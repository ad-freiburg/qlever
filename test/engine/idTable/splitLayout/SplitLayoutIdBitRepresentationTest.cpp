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

#include <algorithm>
#include <array>
#include <cstddef>
#include <limits>
#include <new>
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

// Test that the bytes of a bit representation are completely determined by its
// datatype and payload: the padding after the datatype is always zero, also if
// the memory held other bytes before.
TEST(SplitLayoutIdBitRepresentationTest, bytesAreDeterministic) {
  using Rep = SplitLayoutIdBitRepresentation;
  static_assert(sizeof(Rep) == 16);

  // Construct the bit representations in memory that is not zero.
  alignas(Rep) std::array<unsigned char, sizeof(Rep)> memory;
  auto paddingIsZero = [&memory](auto makeRep) {
    memory.fill(0xAB);
    new (memory.data()) Rep{makeRep()};
    auto padding = memory.begin() + offsetof(Rep, padding_);
    return std::all_of(padding, padding + sizeof(Rep::padding_),
                       [](unsigned char byte) { return byte == 0; });
  };
  EXPECT_TRUE(paddingIsZero([] { return Rep{3, 42}; }));
  EXPECT_TRUE(paddingIsZero([] { return Rep{3, 42}.incremented(); }));
  EXPECT_TRUE(paddingIsZero([] {
    return Rep{3, std::numeric_limits<uint64_t>::max()}.incremented();
  }));
}

// Test that the comparison is datatype-major, although the `payload_` is
// declared before the `datatype_`.
TEST(SplitLayoutIdBitRepresentationTest, compare) {
  using Rep = SplitLayoutIdBitRepresentation;
  constexpr auto maxPayload = std::numeric_limits<uint64_t>::max();
  // A smaller datatype decides, even if the payload is larger.
  EXPECT_TRUE(Rep(3, maxPayload) < Rep(4, 0));
  EXPECT_TRUE(Rep(3, maxPayload) <= Rep(4, 0));
  EXPECT_TRUE(Rep(4, 0) > Rep(3, maxPayload));
  EXPECT_TRUE(Rep(4, 0) >= Rep(3, maxPayload));
  EXPECT_TRUE(ql::compareThreeWay(Rep(4, 0), Rep(3, maxPayload)) ==
              ql::strong_ordering::greater);
  // For the same datatype, the payload decides.
  EXPECT_TRUE(Rep(3, 42) < Rep(3, 43));
  EXPECT_TRUE(Rep(3, 43) > Rep(3, 42));
  EXPECT_TRUE(ql::compareThreeWay(Rep(3, 42), Rep(3, 43)) ==
              ql::strong_ordering::less);
  // Equality requires both the same datatype and the same payload.
  EXPECT_TRUE(Rep(3, 42) == Rep(3, 42));
  EXPECT_TRUE(Rep(3, 42) <= Rep(3, 42));
  EXPECT_TRUE(Rep(3, 42) >= Rep(3, 42));
  EXPECT_TRUE(ql::compareThreeWay(Rep(3, 42), Rep(3, 42)) ==
              ql::strong_ordering::equal);
  EXPECT_TRUE(Rep(3, 42) != Rep(4, 42));
  EXPECT_TRUE(Rep(3, 42) != Rep(3, 43));
  EXPECT_FALSE(Rep(3, 42) != Rep(3, 42));
}

// Test that the bit representations are ordered like the `Id`s they belong to
// (none of the sample `Id`s is a `LocalVocabIndex`, see the class comment).
TEST(SplitLayoutIdBitRepresentationTest, compareLikeIds) {
  auto toRep = [](const Id id) {
    auto [datatype, payload] = getBitsCompat(id);
    return SplitLayoutIdBitRepresentation{datatype, payload};
  };
  for (const Id a : testHelpers::sampleIds()) {
    for (const Id b : testHelpers::sampleIds()) {
      EXPECT_TRUE(ql::compareThreeWay(toRep(a), toRep(b)) ==
                  a.compareWithoutLocalVocab(b))
          << a << " " << b;
    }
  }
}

// Test `incremented`, with and without a carry into the datatype.
TEST(SplitLayoutIdBitRepresentationTest, incremented) {
  // Without a carry, only the payload is incremented.
  constexpr int idValue = 42;
  auto [datatype, payload] = getBitsCompat(Id::makeFromInt(idValue));
  const SplitLayoutIdBitRepresentation representation{datatype, payload};
  const auto incremented = representation.incremented();
  EXPECT_EQ(static_cast<Datatype>(incremented.datatype_), Datatype::Int);
  EXPECT_EQ(incremented.payload_, static_cast<uint64_t>(idValue) + 1);

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
