// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "index/vocabulary/VocabularyType.h"
#include "util/HashMap.h"

// _____________________________________________________________________________
TEST(EnumWithStrings, VocabularyTypeEnum) {
  using V = ad_utility::VocabularyType;
  EXPECT_EQ(V::InMemoryUncompressed, V::fromString("in-memory-uncompressed"));
  EXPECT_EQ(V::OnDiskCompressed, V::fromString("on-disk-compressed"));
  EXPECT_EQ("in-memory-uncompressed", V::InMemoryUncompressed.toString());
  EXPECT_EQ("on-disk-compressed", V::OnDiskCompressed.toString());

  nlohmann::json j;
  j = V::InMemoryUncompressed;
  EXPECT_EQ(V::InMemoryUncompressed, j.get<V>());
  j = V::OnDiskCompressed;
  EXPECT_EQ(V::OnDiskCompressed, j.get<V>());

  std::vector<V> all(V::all().begin(), V::all().end());
  EXPECT_THAT(all, ::testing::ElementsAre(
                       V::InMemoryUncompressed, V::OnDiskUncompressed,
                       V::InMemoryCompressed, V::OnDiskCompressed,
                       V::OnDiskCompressedGeoSplit));

  ad_utility::HashMap<V, size_t> h;
  for (size_t i = 0; i < 50000; ++i) {
    h[V::random()]++;
  }
  for (auto v : all) {
    EXPECT_GT(h[v], 7000);
    EXPECT_LT(h[v], 13000);
  }

  EXPECT_THROW(V::fromString("not-a-valid-type"), std::runtime_error);
}
