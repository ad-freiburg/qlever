//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "index/vocabulary/VocabularyType.h"
#include "util/HashMap.h"

using namespace ad_utility;
// Simple tests for the glorified enum `VocabularyType`.
TEST(VocabularyType, allTests) {
  using E = VocabularyType::Enum;
  using T = VocabularyType;
  T t{};
  EXPECT_EQ(t.value(), E::InMemoryUncompressed);
  for (auto e : T::all()) {
    EXPECT_EQ(T{e}.value(), e);
  }

  t = T::fromString("on-disk-compressed");
  EXPECT_EQ(t.value(), E::OnDiskCompressed);

  EXPECT_ANY_THROW(T::fromString("kartoffelsalat"));

  EXPECT_EQ(T{E::OnDiskUncompressed}.toString(), "on-disk-uncompressed");

  using namespace ::testing;
  EXPECT_THAT(T::getListOfSupportedValues(),
              AllOf(HasSubstr("in-memory-uncompressed"),
                    HasSubstr(", on-disk-uncompressed")));

  for (auto e : T::all()) {
    nlohmann::json j = T{e};
    t = j.get<T>();
    EXPECT_EQ(t.value(), e);
  }
}

// Test the random sampling.
TEST(VocabularyType, random) {
  ad_utility::HashMap<size_t, size_t> counts;
  size_t numSamples = 100'000;
  for (size_t i = 0; i < numSamples; ++i) {
    counts[static_cast<size_t>(VocabularyType::random().value())]++;
  }
  for (const auto& [_, count] : counts) {
    EXPECT_GE(count, numSamples / VocabularyType::all().size() / 3);
  }
}

// _____________________________________________________________________________
// Test the two vocabulary types with holes, which (in contrast to all the other
// types) cannot be used for regular index building.
TEST(VocabularyType, vocabularyTypesWithHoles) {
  using E = VocabularyType::Enum;
  using T = VocabularyType;
  EXPECT_EQ(T::numValues(), 7);

  EXPECT_EQ(T::fromString("in-memory-uncompressed-with-holes").value(),
            E::InMemoryUncompressedWithHoles);
  EXPECT_EQ(T::fromString("in-memory-compressed-with-holes").value(),
            E::InMemoryCompressedWithHoles);
  EXPECT_EQ(T{E::InMemoryUncompressedWithHoles}.toString(),
            "in-memory-uncompressed-with-holes");
  EXPECT_EQ(T{E::InMemoryCompressedWithHoles}.toString(),
            "in-memory-compressed-with-holes");
  EXPECT_EQ(T::InMemoryUncompressedWithHoles.value(),
            E::InMemoryUncompressedWithHoles);
  EXPECT_EQ(T::InMemoryCompressedWithHoles.value(),
            E::InMemoryCompressedWithHoles);

  // The types with holes are not part of the types for index building, and
  // hence are never returned by `randomForIndexBuilding`.
  auto typesForIndexBuilding = T::allForIndexBuilding();
  EXPECT_EQ(typesForIndexBuilding.size(), 5);
  using namespace ::testing;
  EXPECT_THAT(typesForIndexBuilding,
              Not(Contains(E::InMemoryUncompressedWithHoles)));
  EXPECT_THAT(typesForIndexBuilding,
              Not(Contains(E::InMemoryCompressedWithHoles)));
  for (size_t i = 0; i < 1000; ++i) {
    EXPECT_THAT(typesForIndexBuilding,
                Contains(T::randomForIndexBuilding().value()));
  }
}
