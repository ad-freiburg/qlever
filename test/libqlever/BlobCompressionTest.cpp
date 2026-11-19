// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <zstd.h>

#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "../util/GTestHelpers.h"
#include "./QleverTestHelpers.h"
#include "libqlever/BlobCompression.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"

using namespace qlever::blobCompression;
using namespace testing;
using ad_utility::testing::decompressOrFail;

namespace {
using Bytes = std::vector<char>;

// The size of the skippable frame with the `BlobIndex` at the end of a blob,
// and the offsets of its fields (relative to the start of the frame).
constexpr size_t indexFrameSize = 38;
constexpr size_t indexVersionOffset = 16;
constexpr size_t indexPrefixSizeOffset = 22;
constexpr size_t indexBlocksEndOffset = 30;

// The stable prefix requires the sequence API of ZSTD 1.5.5 (see
// `BlobCompression.h`).
bool stablePrefixIsSupported() { return ZSTD_versionNumber() >= 10505; }

// Data that resembles a serialized vocabulary: words, each followed by its
// offset as a 64-bit integer, and every 100 words some zero bytes (like the
// padding for the alignment of a serialized array). It is compressible, but
// not trivially so.
Bytes vocabularyLike(size_t numBytes, unsigned seed) {
  std::mt19937 generator{seed};
  Bytes result;
  for (size_t i = 0; result.size() < numBytes; ++i) {
    auto word = absl::StrCat("<http://example.org/entity_", generator() % 50000,
                             "_", generator() % 7, ">");
    result.insert(result.end(), word.begin(), word.end());
    uint64_t offset = result.size();
    const char* bytes = reinterpret_cast<const char*>(&offset);
    result.insert(result.end(), bytes, bytes + sizeof(offset));
    if (i % 100 == 0) {
      result.insert(result.end(), 24, '\0');
    }
  }
  result.resize(numBytes);
  return result;
}

// `numBytes` pseudo-random bytes (deterministic for a given `seed`).
Bytes randomBytes(size_t numBytes, unsigned seed) {
  std::mt19937 generator{seed};
  Bytes result(numBytes);
  for (auto& byte : result) {
    byte = static_cast<char>(generator());
  }
  return result;
}

// Return the concatenation of `a` and `b`.
Bytes concat(const Bytes& a, const Bytes& b) {
  Bytes result{a};
  result.insert(result.end(), b.begin(), b.end());
  return result;
}

// Return a copy of `bytes` in which the `T` at `offset` is replaced by
// `value`.
template <typename T>
Bytes withValueAt(Bytes bytes, size_t offset, T value) {
  std::memcpy(bytes.data() + offset, &value, sizeof(value));
  return bytes;
}

// Decompress `compressed` like a reader of blobs does (also a reader that
// precedes the stable prefix and the index), and check that the result is
// `expected`, and that the blob is a single frame followed by the index.
void expectDecompressesTo(
    const Bytes& compressed, const Bytes& expected,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto decompressed = decompressOrFail(compressed, {}, l);
  EXPECT_TRUE(ql::ranges::equal(decompressed, expected));
  EXPECT_EQ(ZSTD_getFrameContentSize(compressed.data(), compressed.size()),
            expected.size());
  EXPECT_EQ(ZSTD_findFrameCompressedSize(compressed.data(), compressed.size()),
            compressed.size() - indexFrameSize);
}

// Return the `BlobIndex` of `compressed` (fail the test if it has none).
BlobIndex indexOf(const Bytes& compressed,
                  ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto index = readBlobIndex(compressed);
  EXPECT_TRUE(index.has_value());
  return index.value_or(BlobIndex{});
}
}  // namespace

// _____________________________________________________________________________
TEST(BlobCompression, withoutStablePrefix) {
  auto data = vocabularyLike(300'000, 1);
  for (int level : {-5, 1, 3, 19}) {
    auto compressed = compressBlob(data, BlobEncoding{level, 0});
    expectDecompressesTo(compressed, data);
    auto index = indexOf(compressed);
    EXPECT_EQ(index.encoding_.compressionLevel_, level);
    EXPECT_EQ(index.encoding_.stablePrefixSize_, 0u);
    EXPECT_EQ(index.stablePrefixBlocksEnd_, 0u);
  }
  // The default level is 1, and a higher level compresses better.
  auto defaultCompressed = compressBlob(data);
  EXPECT_EQ(indexOf(defaultCompressed).encoding_.compressionLevel_, 1);
  EXPECT_GT(defaultCompressed.size(),
            compressBlob(data, BlobEncoding{19, 0}).size());
  // The `NamedCachedQueryBlobManager` uses the same compression.
  EXPECT_EQ(qlever::NamedCachedQueryBlobManager::compressBlob(data),
            defaultCompressed);

  // Empty data.
  auto empty = compressBlob(Bytes{});
  expectDecompressesTo(empty, Bytes{});
}

// _____________________________________________________________________________
TEST(BlobCompression, stablePrefix) {
  auto prefix = vocabularyLike(200'000, 1);
  auto data = concat(prefix, vocabularyLike(100'000, 2));
  for (int level : {1, 3}) {
    BlobEncoding encoding{level, prefix.size()};
    auto compressed = compressBlob(data, encoding);
    expectDecompressesTo(compressed, data);
    auto index = indexOf(compressed);
    EXPECT_EQ(index.encoding_.compressionLevel_, level);
    if (!stablePrefixIsSupported()) {
      EXPECT_EQ(index.encoding_.stablePrefixSize_, 0u);
      continue;
    }
    EXPECT_EQ(index.encoding_.stablePrefixSize_, prefix.size());
    EXPECT_GT(index.stablePrefixBlocksEnd_, 13u);
    EXPECT_LT(index.stablePrefixBlocksEnd_, compressed.size());
    // NOTE: The blob with a stable prefix may be larger than a plain one,
    // because the remainder cannot refer to the stable prefix. For real blobs
    // this hardly matters, but this synthetic data is very repetitive.
    EXPECT_LT(compressed.size(), data.size() / 2);
  }
}

// _____________________________________________________________________________
TEST(BlobCompression, stablePrefixIsCopied) {
  auto prefix = vocabularyLike(200'000, 1);
  auto base = concat(prefix, vocabularyLike(100'000, 2));
  auto target = concat(prefix, vocabularyLike(150'000, 3));
  BlobEncoding encoding{1, prefix.size()};
  auto baseCompressed = compressBlob(base, encoding);
  auto fresh = compressBlob(target, encoding);
  // Copying the stable prefix gives the same result as encoding it again.
  auto copied =
      compressBlob(target, encoding, ReusableBlob{baseCompressed, base});
  expectDecompressesTo(copied, target);
  EXPECT_EQ(copied, fresh);
  if (stablePrefixIsSupported()) {
    auto end = indexOf(copied).stablePrefixBlocksEnd_;
    EXPECT_EQ(end, indexOf(baseCompressed).stablePrefixBlocksEnd_);
    EXPECT_TRUE(std::equal(copied.begin() + 13, copied.begin() + end,
                           baseCompressed.begin() + 13));
  }

  // The stable prefix is not copied (but encoded) if it differs (in size or
  // content) from the one of the reusable blob, or if the reusable blob has no
  // stable prefix. The result is the same.
  auto otherPrefix = prefix;
  otherPrefix[1000] ^= 0x01;
  auto otherBase = concat(otherPrefix, vocabularyLike(100'000, 2));
  auto shorterEncoding = BlobEncoding{1, prefix.size() - 10'000};
  for (const auto& [reusableData, reusableCompressed] :
       std::vector<std::pair<Bytes, Bytes>>{
           {otherBase, compressBlob(otherBase, encoding)},
           {base, compressBlob(base, shorterEncoding)},
           {base, compressBlob(base, BlobEncoding{1, 0})},
           {base, ZstdWrapper::compress(base.data(), base.size())}}) {
    auto result = compressBlob(target, encoding,
                               ReusableBlob{reusableCompressed, reusableData});
    EXPECT_EQ(result, fresh);
  }
  // The decompressed reusable blob is shorter than the prefix.
  EXPECT_EQ(compressBlob(target, encoding,
                         ReusableBlob{baseCompressed,
                                      ql::span<const char>{base}.first(100)}),
            fresh);

  if (!stablePrefixIsSupported()) {
    return;
  }
  // An index whose end of the stable prefix is not at a block boundary, or at
  // the frame header, is not used.
  const size_t indexStart = baseCompressed.size() - indexFrameSize;
  auto blocksEnd = indexOf(baseCompressed).stablePrefixBlocksEnd_;
  for (uint64_t wrongEnd : {blocksEnd - 1, uint64_t{13}, uint64_t{5}}) {
    auto corrupted = withValueAt(baseCompressed,
                                 indexStart + indexBlocksEndOffset, wrongEnd);
    EXPECT_EQ(compressBlob(target, encoding, ReusableBlob{corrupted, base}),
              fresh);
  }
}

// _____________________________________________________________________________
TEST(BlobCompression, noStablePrefixForUnsuitableInput) {
  auto data = concat(vocabularyLike(200'000, 1), vocabularyLike(1'000, 2));
  // A prefix that is too small, the whole blob, or larger than the blob.
  for (uint64_t prefixSize :
       {minStablePrefixSize - 1, uint64_t{data.size()}, uint64_t{1} << 40}) {
    auto compressed = compressBlob(data, BlobEncoding{1, prefixSize});
    expectDecompressesTo(compressed, data);
    EXPECT_EQ(indexOf(compressed).encoding_.stablePrefixSize_, 0u);
  }
  // A random prefix has no matches (in particular none with the offsets 8, 4,
  // and 1), so it cannot be encoded as a stable prefix.
  auto randomPrefix = randomBytes(100'000, 3);
  auto randomData = concat(randomPrefix, vocabularyLike(1'000, 2));
  auto compressed =
      compressBlob(randomData, BlobEncoding{1, randomPrefix.size()});
  expectDecompressesTo(compressed, randomData);
  EXPECT_EQ(indexOf(compressed).encoding_.stablePrefixSize_, 0u);
  // Neither can a prefix whose only suitable matches are far from its end
  // (all the following bytes would be compressed without matches).
  auto farPrefix = concat(vocabularyLike(10'000, 4), randomBytes(400'000, 5));
  auto farData = concat(farPrefix, vocabularyLike(1'000, 2));
  auto farCompressed = compressBlob(farData, BlobEncoding{1, farPrefix.size()});
  expectDecompressesTo(farCompressed, farData);
  EXPECT_EQ(indexOf(farCompressed).encoding_.stablePrefixSize_, 0u);
  // But a prefix whose suitable matches are not too far from its end can be
  // encoded, also if the following literals span several blocks.
  auto nearPrefix = concat(vocabularyLike(200'000, 4), randomBytes(200'000, 5));
  auto nearData = concat(nearPrefix, vocabularyLike(1'000, 2));
  auto nearCompressed =
      compressBlob(nearData, BlobEncoding{1, nearPrefix.size()});
  expectDecompressesTo(nearCompressed, nearData);
  if (stablePrefixIsSupported()) {
    EXPECT_EQ(indexOf(nearCompressed).encoding_.stablePrefixSize_,
              nearPrefix.size());
  }
}

// _____________________________________________________________________________
TEST(BlobCompression, readBlobIndexRejectsInvalidInput) {
  auto prefix = vocabularyLike(50'000, 1);
  auto data = concat(prefix, vocabularyLike(10'000, 2));
  auto compressed = compressBlob(data, BlobEncoding{1, prefix.size()});
  ASSERT_TRUE(readBlobIndex(compressed).has_value());
  const size_t indexStart = compressed.size() - indexFrameSize;

  // No index at all: garbage, too short, or a plain ZSTD frame (as written by
  // previous versions of QLever).
  EXPECT_EQ(readBlobIndex(Bytes(100, 'x')), std::nullopt);
  EXPECT_EQ(readBlobIndex(Bytes(10, 'x')), std::nullopt);
  EXPECT_EQ(readBlobIndex(Bytes{}), std::nullopt);
  EXPECT_EQ(readBlobIndex(ZstdWrapper::compress(data.data(), data.size())),
            std::nullopt);
  // Corrupted magic numbers, size, or version of the index.
  for (size_t offset : {size_t{0}, size_t{4}, size_t{8}, indexVersionOffset}) {
    auto corrupted = compressed;
    corrupted[indexStart + offset] ^= 0x10;
    EXPECT_EQ(readBlobIndex(corrupted), std::nullopt) << offset;
  }
  // The index does not directly follow the frame.
  auto withGap = compressed;
  withGap.insert(withGap.begin() + indexStart, 'x');
  EXPECT_EQ(readBlobIndex(withGap), std::nullopt);
  // The index alone.
  EXPECT_EQ(readBlobIndex(ql::span<const char>{compressed}.subspan(indexStart)),
            std::nullopt);
  // Implausible values of the stable prefix.
  auto prefixSizeAt = indexStart + indexPrefixSizeOffset;
  auto blocksEndAt = indexStart + indexBlocksEndOffset;
  EXPECT_EQ(readBlobIndex(
                withValueAt(compressed, prefixSizeAt, uint64_t{data.size()})),
            std::nullopt);
  EXPECT_EQ(
      readBlobIndex(withValueAt(compressed, blocksEndAt, uint64_t{indexStart})),
      std::nullopt);
  // Without a stable prefix, the end of its blocks has to be zero.
  auto withoutPrefix = compressBlob(data, BlobEncoding{1, 0});
  ASSERT_TRUE(readBlobIndex(withoutPrefix).has_value());
  EXPECT_EQ(readBlobIndex(withValueAt(
                withoutPrefix,
                withoutPrefix.size() - indexFrameSize + indexBlocksEndOffset,
                uint64_t{20})),
            std::nullopt);
}
