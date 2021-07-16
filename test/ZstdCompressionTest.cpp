// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>

#include "../src/util/CompressionUsingZstd/ZstdWrapper.h"

TEST(CompressionTest, Basic) {
  std::vector<int> x{1, 2, 3, 4};
  std::vector<char> comp;
  ZstdWrapper::compress(x.data(), x.size() * sizeof(int),
                        [&comp](auto result) { comp = std::move(result); });
  auto decomp = ZstdWrapper::decompress<int>(comp.data(), comp.size(), 4);
  ASSERT_EQ(x, decomp);
}