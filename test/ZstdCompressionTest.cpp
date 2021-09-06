// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>

#include "../src/util/CompressionUsingZstd/ZstdWrapper.h"

// _____________________________________________________________________________
TEST(CompressionTest, Basic) {
  std::vector<int> x{1, 2, 3, 4};
  std::vector<char> comp =
      ZstdWrapper::compress(x.data(), x.size() * sizeof(int));
  auto decomp = ZstdWrapper::decompress<int>(comp.data(), comp.size(), 4);
  ASSERT_EQ(x, decomp);
}

// _____________________________________________________________________________
TEST(CompressionTest, DecompressToBuffer) {
  std::vector<int> x{1, 2, 3, 4};
  std::vector<char> comp =
      ZstdWrapper::compress(x.data(), x.size() * sizeof(int));
  std::vector<int> decomp(4);
  auto numBytesDecompressed = ZstdWrapper::decompressToBuffer<int>(
      comp.data(), comp.size(), decomp.data(), decomp.size() * sizeof(int));
  ASSERT_EQ(x, decomp);
  ASSERT_EQ(4ul * sizeof(int), numBytesDecompressed);
}
