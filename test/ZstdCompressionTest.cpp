// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../test/util/GTestHelpers.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"

using ::testing::HasSubstr;

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

// _____________________________________________________________________________
TEST(CompressionTest, GetUncompressedSize) {
  std::vector<int> x{1, 2, 3, 4};
  constexpr size_t numBytes = 4 * sizeof(int);
  std::vector<char> comp = ZstdWrapper::compress(x.data(), numBytes);
  EXPECT_EQ(ZstdWrapper::getUncompressedSize(comp.data(), comp.size()),
            numBytes);

  // The empty input also yields a valid frame, with a stored size of zero.
  std::vector<char> emptyComp = ZstdWrapper::compress(x.data(), 0);
  EXPECT_EQ(
      ZstdWrapper::getUncompressedSize(emptyComp.data(), emptyComp.size()), 0u);

  // Data that does not start with a valid ZSTD frame header is rejected, ...
  std::vector<char> garbage(64, 'x');
  AD_EXPECT_THROW_WITH_MESSAGE(
      ZstdWrapper::getUncompressedSize(garbage.data(), garbage.size()),
      HasSubstr("does not start with a valid ZSTD frame header"));
  // ... as is a buffer that is too short to even hold a frame header.
  AD_EXPECT_THROW_WITH_MESSAGE(
      ZstdWrapper::getUncompressedSize(comp.data(), 1),
      HasSubstr("does not start with a valid ZSTD frame header"));
}

// _____________________________________________________________________________
// A frame that was written without the content size flag (which `ZstdWrapper`
// itself never does, but other ZSTD compressors may) does not store the size of
// its uncompressed data.
TEST(CompressionTest, GetUncompressedSizeOfFrameWithoutContentSize) {
  std::vector<int> x{1, 2, 3, 4};
  constexpr size_t numBytes = 4 * sizeof(int);
  std::vector<char> comp(ZSTD_compressBound(numBytes));

  ZSTD_CCtx* context = ZSTD_createCCtx();
  ASSERT_NE(context, nullptr);
  absl::Cleanup cleanup = [context] { ZSTD_freeCCtx(context); };
  ASSERT_FALSE(
      ZSTD_isError(ZSTD_CCtx_setParameter(context, ZSTD_c_contentSizeFlag, 0)));
  auto compressedSize =
      ZSTD_compress2(context, comp.data(), comp.size(), x.data(), numBytes);
  ASSERT_FALSE(ZSTD_isError(compressedSize));

  AD_EXPECT_THROW_WITH_MESSAGE(
      ZstdWrapper::getUncompressedSize(comp.data(), compressedSize),
      HasSubstr("does not store that size in its header"));
}
