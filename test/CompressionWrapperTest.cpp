// Copyright 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "util/GTestHelpers.h"
#include "util/compression/CompressionAlgorithm.h"
#include "util/compression/ZstdWrapper.h"
#ifdef QLEVER_HAS_LZ4
#include "util/compression/Lz4Wrapper.h"
#endif

// A parametrized test fixture over `CompressionAlgorithm`.
class CompressionWrapperTest
    : public ::testing::TestWithParam<CompressionAlgorithm> {};

// Build the list of algorithms to test.
static auto allAlgorithms() {
  std::vector<CompressionAlgorithm> algos{CompressionAlgorithm::Zstd};
#ifdef QLEVER_HAS_LZ4
  algos.push_back(CompressionAlgorithm::Lz4);
#endif
  return algos;
}

// Pretty-print the algorithm name for test output.
static std::string nameGenerator(
    const ::testing::TestParamInfo<CompressionAlgorithm>& info) {
  return std::string{info.param.toString()};
}

INSTANTIATE_TEST_SUITE_P(AllAlgorithms, CompressionWrapperTest,
                         ::testing::ValuesIn(allAlgorithms()), nameGenerator);

// _____________________________________________________________________________
TEST_P(CompressionWrapperTest, Basic) {
  std::vector<int> x{1, 2, 3, 4};
  std::vector<char> comp =
      GetParam().compress(x.data(), x.size() * sizeof(int));
  auto decomp = GetParam().decompress<int>(comp.data(), comp.size(), 4);
  ASSERT_EQ(x, decomp);

  // Decompression with a wrong `knownOriginalSize` fails.
  // Note: we deliberately use `EXPECT_ANY_THROW`, as the thrown message might
  // depend on the details of the concrete compression wrapper.
  EXPECT_ANY_THROW((GetParam().decompress<int>(comp.data(), comp.size(), 5)));
}

TEST_P(CompressionWrapperTest, EmptyInput) {
  std::vector<char> comp = GetParam().compress(nullptr, 0);
  // Note: it is not guaranteed that an empty input compresses to an empty
  // output, so there is nothing to assert here, but decompression should work
  // nonet
  auto decomp = GetParam().decompress<int>(comp.data(), comp.size(), 0);
  EXPECT_TRUE(decomp.empty());

  // Also test decompression of empty inputs via the `decompressToBuffer` API.
  decomp.resize(3);
  auto res = GetParam().decompressToBuffer<int>(comp.data(), comp.size(),
                                                decomp.data(), 3);
  EXPECT_EQ(0ul, res);
}

// _____________________________________________________________________________
TEST_P(CompressionWrapperTest, DecompressToBuffer) {
  std::vector<int> x{1, 2, 3, 4};
  std::vector<char> comp =
      GetParam().compress(x.data(), x.size() * sizeof(int));
  std::vector<int> decomp(4);
  auto numBytesDecompressed = GetParam().decompressToBuffer<int>(
      comp.data(), comp.size(), decomp.data(), decomp.size() * sizeof(int));
  ASSERT_EQ(x, decomp);
  ASSERT_EQ(4ul * sizeof(int), numBytesDecompressed);

  // Decompression of data that was not actually compressed is expected to throw
  // an exception (the details of the exception are implementation details of
  // the underlying implementations, and thus not asserted here).
  std::vector<char> garbage{'1', 'a', 'u'};
  EXPECT_ANY_THROW(GetParam().decompressToBuffer<int>(
      garbage.data(), garbage.size(), decomp.data(),
      decomp.size() * sizeof(int)));
}

// _____________________________________________________________________________
TEST_P(CompressionWrapperTest, LargeData) {
  std::vector<int> x(10000);
  for (size_t i = 0; i < x.size(); ++i) {
    x[i] = static_cast<int>(i % 256);
  }
  std::vector<char> comp =
      GetParam().compress(x.data(), x.size() * sizeof(int));
  ASSERT_LT(comp.size(), x.size() * sizeof(int));
  auto decomp = GetParam().decompress<int>(comp.data(), comp.size(), x.size());
  ASSERT_EQ(x, decomp);
}

// _____________________________________________________________________________
TEST(CompressionAlgorithm, CornerCases) {
  CompressionAlgorithm invalid{static_cast<CompressionAlgorithm::Enum>(-1)};
  AD_EXPECT_THROW_WITH_MESSAGE(invalid.compress(nullptr, 0),
                               ::testing::HasSubstr("should be unreachable"));
  EXPECT_EQ(CompressionAlgorithm::typeName(), "compression algorithm");
}
