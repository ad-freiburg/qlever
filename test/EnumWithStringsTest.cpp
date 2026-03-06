// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <../src/util/compression/CompressionAlgorithm.h>
#include <gmock/gmock.h>

#include "util/HashMap.h"

TEST(EnumWithStrings, CompressionAlgorithmEnum) {
  using C = CompressionAlgorithm;
  EXPECT_EQ(C::Lz4, C::fromString("lz4"));
  EXPECT_EQ(C::Zstd, C::fromString("zstd"));
  EXPECT_EQ("zstd", C::Zstd.toString());
  EXPECT_EQ("lz4", C::Lz4.toString());

  nlohmann::json j;
  j = C::Zstd;
  EXPECT_EQ(C::Zstd, j.get<C>());
  j = C::Lz4;
  EXPECT_EQ(C::Lz4, j.get<C>());

  std::vector<C> all(C::all().begin(), C::all().end());
  EXPECT_THAT(all, ::testing::ElementsAre(C::Zstd, C::Lz4));

  ad_utility::HashMap<C, size_t> h;
  for (size_t i = 0; i < 10000; ++i) {
    h[C::random()]++;
  }
  for (auto i : all) {
    EXPECT_GT(h[i], 4000);
    EXPECT_LT(h[i], 6000);
  }
}
