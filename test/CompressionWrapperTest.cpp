// Copyright 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "../src/util/compression/CompressionAlgorithm.h"
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
