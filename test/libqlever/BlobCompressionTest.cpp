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

#include <string>
#include <vector>

#include "../util/GTestHelpers.h"
#include "./QleverTestHelpers.h"
#include "libqlever/BlobCompression.h"
#include "libqlever/Qlever.h"
#include "util/compressionUsingZstd/SegmentedZstdFrame.h"

using namespace qlever;
using namespace qlever::blobCompression;
using ad_utility::testing::buildTestIndex;
using ad_utility::testing::decompressOrFail;
using ad_utility::zstd::SegmentedFrame;

namespace {
using Bytes = std::vector<char>;

// Bytes that resemble a serialized vocabulary: words, each followed by some
// zero bytes. Deterministic for a given `seed`.
Bytes vocabularyLike(size_t numBytes, unsigned seed) {
  Bytes result;
  for (size_t i = 0; result.size() < numBytes; ++i) {
    auto word =
        absl::StrCat("<http://example.org/entity_", (i * seed) % 50000, ">");
    result.insert(result.end(), word.begin(), word.end());
    result.insert(result.end(), i % 5, '\0');
  }
  result.resize(numBytes);
  return result;
}

// Some triples with distinct subjects and objects.
std::string manyTriples(size_t n) {
  std::string result;
  for (size_t i = 0; i < n; ++i) {
    absl::StrAppend(&result, "<s", i, "> <p> <o", i, "> .\n");
  }
  return result;
}
}  // namespace

// Without segments, a blob is an ordinary ZSTD frame without an index. With
// segments, it is a segmented frame, which also every ZSTD decompressor reads.
TEST(BlobCompression, withAndWithoutSegments) {
  auto data = vocabularyLike(100'000, 1);
  auto plain = compressBlob(data);
  EXPECT_EQ(ZSTD_findFrameCompressedSize(plain.data(), plain.size()),
            plain.size());
  EXPECT_FALSE(SegmentedFrame::read(plain).has_value());
  EXPECT_EQ(decompressOrFail(plain, {}).size(), data.size());

  auto segmented = compressBlob(
      data, BlobEncoding{1, std::vector<uint64_t>{60'000, 40'000}});
  auto frame = SegmentedFrame::read(segmented);
  ASSERT_TRUE(frame.has_value());
  EXPECT_EQ(frame->numSegments(), 2u);
  auto decompressed = decompressOrFail(segmented, {});
  EXPECT_TRUE(ql::ranges::equal(decompressed, data));

  // The unchanged segment of a reusable base is copied, which gives the same
  // result as compressing it again. A base that is not segmented is ignored.
  auto changed = data;
  changed[70'000] ^= 1;
  BlobEncoding encoding{1, std::vector<uint64_t>{60'000, 40'000}};
  auto expected = compressBlob(changed, encoding);
  EXPECT_EQ(compressBlob(changed, encoding, segmented), expected);
  EXPECT_EQ(compressBlob(changed, encoding, plain), expected);
}

// `BlobSerializationConfig::writeSegments_` writes a blob with three segments
// (the vocabulary, the secondary vocabulary, and the named results), which
// loads like any other blob.
TEST(BlobCompression, managerWritesSegments) {
  auto config = buildTestIndex(
      manyTriples(500), ad_utility::VocabularyType::InMemoryUncompressed);
  Qlever qlever{EngineConfig{config}};
  qlever.queryAndPinResultWithName(
      "all", "SELECT ?s ?p ?o WHERE { ?s ?p ?o } INTERNAL SORT BY ?s ?p ?o");
  BlobSerializationConfig blobConfig;
  auto plain = qlever.serializeVocabAndNamedCacheToCompressedBlob(blobConfig);
  blobConfig.writeSegments_ = true;
  auto segmented =
      qlever.serializeVocabAndNamedCacheToCompressedBlob(blobConfig);

  EXPECT_FALSE(SegmentedFrame::read(plain).has_value());
  auto frame = SegmentedFrame::read(segmented);
  ASSERT_TRUE(frame.has_value());
  EXPECT_EQ(frame->numSegments(), 3u);
  // Both blobs have the same content.
  EXPECT_TRUE(ql::ranges::equal(decompressOrFail(plain, {}),
                                decompressOrFail(segmented, {})));
  Qlever loaded{EngineConfig{}, /*skipLoading=*/true};
  EXPECT_NO_THROW(
      loaded.deserializeVocabAndNamedCacheFromCompressedBlob(segmented));
}
