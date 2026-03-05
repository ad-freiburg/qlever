// Copyright 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "util/CompressionAlgorithm.h"
#include "util/compression/ZstdWrapper.h"
#ifdef QLEVER_HAS_LZ4
#include "util/compression/Lz4Wrapper.h"
#endif

// A parametrized test fixture over `CompressionAlgorithm`.
class CompressionWrapperTest
    : public ::testing::TestWithParam<CompressionAlgorithm> {
 protected:
  // Compress `numBytes` starting at `src` using the current algorithm.
  std::vector<char> compress(const void* src, size_t numBytes) {
    switch (GetParam().value()) {
      case CompressionAlgorithm::Enum::Zstd:
        return ZstdWrapper::compress(src, numBytes);
      case CompressionAlgorithm::Enum::Lz4:
#ifdef QLEVER_HAS_LZ4
        return Lz4Wrapper::compress(src, numBytes);
#else
        ADD_FAILURE() << "LZ4 not available";
        return {};
#endif
    }
    ADD_FAILURE() << "Unknown algorithm";
    return {};
  }

  // Decompress to a `std::vector<T>`.
  template <typename T>
  std::vector<T> decompress(void* src, size_t numBytes,
                            size_t knownOriginalSize) {
    switch (GetParam().value()) {
      case CompressionAlgorithm::Enum::Zstd:
        return ZstdWrapper::decompress<T>(src, numBytes, knownOriginalSize);
      case CompressionAlgorithm::Enum::Lz4:
#ifdef QLEVER_HAS_LZ4
        return Lz4Wrapper::decompress<T>(src, numBytes, knownOriginalSize);
#else
        ADD_FAILURE() << "LZ4 not available";
        return {};
#endif
    }
    ADD_FAILURE() << "Unknown algorithm";
    return {};
  }

  // Decompress to a given buffer.
  template <typename T>
  size_t decompressToBuffer(const char* src, size_t numBytes, T* buffer,
                            size_t bufferCapacity) {
    switch (GetParam().value()) {
      case CompressionAlgorithm::Enum::Zstd:
        return ZstdWrapper::decompressToBuffer(src, numBytes, buffer,
                                               bufferCapacity);
      case CompressionAlgorithm::Enum::Lz4:
#ifdef QLEVER_HAS_LZ4
        return Lz4Wrapper::decompressToBuffer(src, numBytes, buffer,
                                              bufferCapacity);
#else
        ADD_FAILURE() << "LZ4 not available";
        return 0;
#endif
    }
    ADD_FAILURE() << "Unknown algorithm";
    return 0;
  }
};

// Build the list of algorithms to test.
static auto allAlgorithms() {
  std::vector<CompressionAlgorithm> algos{
      CompressionAlgorithm{CompressionAlgorithm::Enum::Zstd}};
#ifdef QLEVER_HAS_LZ4
  algos.push_back(CompressionAlgorithm{CompressionAlgorithm::Enum::Lz4});
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
  std::vector<char> comp = compress(x.data(), x.size() * sizeof(int));
  auto decomp = decompress<int>(comp.data(), comp.size(), 4);
  ASSERT_EQ(x, decomp);
}

// _____________________________________________________________________________
TEST_P(CompressionWrapperTest, DecompressToBuffer) {
  std::vector<int> x{1, 2, 3, 4};
  std::vector<char> comp = compress(x.data(), x.size() * sizeof(int));
  std::vector<int> decomp(4);
  auto numBytesDecompressed = decompressToBuffer<int>(
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
  std::vector<char> comp = compress(x.data(), x.size() * sizeof(int));
  ASSERT_LT(comp.size(), x.size() * sizeof(int));
  auto decomp = decompress<int>(comp.data(), comp.size(), x.size());
  ASSERT_EQ(x, decomp);
}
