//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "index/vocabulary/VocabularyType.h"

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
