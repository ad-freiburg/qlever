// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifdef QLEVER_HAS_LZ4

#include <gtest/gtest.h>

#include "util/CompressionUsingLz4/Lz4Wrapper.h"

// _____________________________________________________________________________
TEST(Lz4CompressionTest, Basic) {
  std::vector<int> x{1, 2, 3, 4};
  std::vector<char> comp =
      Lz4Wrapper::compress(x.data(), x.size() * sizeof(int));
  auto decomp = Lz4Wrapper::decompress<int>(comp.data(), comp.size(), 4);
  ASSERT_EQ(x, decomp);
}

// _____________________________________________________________________________
TEST(Lz4CompressionTest, DecompressToBuffer) {
  std::vector<int> x{1, 2, 3, 4};
  std::vector<char> comp =
      Lz4Wrapper::compress(x.data(), x.size() * sizeof(int));
  std::vector<int> decomp(4);
  auto numBytesDecompressed = Lz4Wrapper::decompressToBuffer<int>(
      comp.data(), comp.size(), decomp.data(), decomp.size() * sizeof(int));
  ASSERT_EQ(x, decomp);
  ASSERT_EQ(4ul * sizeof(int), numBytesDecompressed);
}

// _____________________________________________________________________________
TEST(Lz4CompressionTest, LargeData) {
  // Test with a larger data set to ensure compression actually reduces size.
  std::vector<int> x(10000);
  for (size_t i = 0; i < x.size(); ++i) {
    x[i] = static_cast<int>(i % 256);
  }
  std::vector<char> comp =
      Lz4Wrapper::compress(x.data(), x.size() * sizeof(int));
  ASSERT_LT(comp.size(), x.size() * sizeof(int));
  auto decomp = Lz4Wrapper::decompress<int>(comp.data(), comp.size(), x.size());
  ASSERT_EQ(x, decomp);
}

#endif  // QLEVER_HAS_LZ4
