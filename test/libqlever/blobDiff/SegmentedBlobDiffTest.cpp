// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include <cstring>
#include <random>
#include <string>
#include <variant>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../QleverTestHelpers.h"
#include "backports/algorithm.h"
#include "libqlever/BlobCompression.h"
#include "libqlever/Qlever.h"
#include "libqlever/blobDiff/BlobDiff.h"
#include "libqlever/blobDiff/SegmentedBlobDiff.h"
#include "util/compressionUsingZstd/SegmentedZstdFrame.h"

using namespace qlever;
using namespace qlever::blobDiff;
using namespace testing;
using ad_utility::testing::buildTestIndex;
using ad_utility::zstd::SegmentedFrame;

namespace {
using Bytes = std::vector<char>;
using Sizes = std::vector<uint64_t>;
using ErrorType = BlobDiffErrorType;

constexpr ad_utility::MemorySize maxTargetSize =
    ad_utility::MemorySize::gigabytes(1);

// The offsets of some fields of a segmented diff file (see the file format in
// `SegmentedBlobDiff.h`).
constexpr size_t headerSize = 8 + 2 + 4 + 8;
constexpr size_t entrySize = 8 + 32 + 1 + 8 + 8;
constexpr size_t kindOffset = 8 + 32;
constexpr size_t baseSegmentOffset = kindOffset + 1;

// Bytes that resemble typical binary data: numbers as text, separated by
// runs of zero bytes. Deterministic for a given `seed`.
Bytes binaryLike(size_t numBytes, unsigned seed) {
  std::mt19937 generator{seed};
  Bytes result;
  while (result.size() < numBytes) {
    auto word = std::to_string(generator() % 100000);
    result.insert(result.end(), word.begin(), word.end());
    result.insert(result.end(), generator() % 6, '\0');
  }
  result.resize(numBytes);
  return result;
}

// Return the concatenation of the `parts`.
Bytes concat(const std::vector<Bytes>& parts) {
  Bytes result;
  for (const auto& part : parts) {
    result.insert(result.end(), part.begin(), part.end());
  }
  return result;
}

// Compress the concatenation of the `parts` as a segmented blob with one
// segment per part.
Bytes segmentedBlob(const std::vector<Bytes>& parts) {
  Sizes sizes;
  for (const auto& part : parts) {
    sizes.push_back(part.size());
  }
  return blobCompression::compressBlob(concat(parts),
                                       blobCompression::BlobEncoding{1, sizes});
}

// Return a copy of `bytes` in which the `T` at `offset` is replaced by
// `value`.
template <typename T>
Bytes withValueAt(Bytes bytes, size_t offset, T value) {
  std::memcpy(bytes.data() + offset, &value, sizeof(value));
  return bytes;
}

// Matcher for a failed result with the given error `type` and a message that
// contains `messageSubstring`.
auto isError(ErrorType type, std::string_view messageSubstring) {
  return VariantWith<BlobDiffError>(
      AllOf(AD_FIELD(BlobDiffError, type_, Eq(type)),
            AD_FIELD(BlobDiffError, message_,
                     HasSubstr(std::string{messageSubstring}))));
}

// Matcher for a successful result that is exactly the `expected` blob.
auto isBlob(const Bytes& expected) { return VariantWith<Bytes>(Eq(expected)); }

// The parts of a base blob and of a target blob in which the first and the
// third part are unchanged, the second part is changed, and a fourth part is
// new.
struct BaseAndTarget {
  std::vector<Bytes> baseParts_{binaryLike(200'000, 1), binaryLike(50'000, 2),
                                binaryLike(100'000, 3)};
  std::vector<Bytes> targetParts_{baseParts_[0], binaryLike(50'000, 2),
                                  baseParts_[2], binaryLike(30'000, 4)};
  Bytes base_ = segmentedBlob(baseParts_);
  Bytes target_;
  BaseAndTarget() {
    targetParts_[1][1000] ^= 1;
    target_ = segmentedBlob(targetParts_);
  }
};
}  // namespace

// The unchanged segments are copied, the others are patched, and applying the
// diff gives exactly the target blob.
TEST(SegmentedBlobDiff, syntheticSegments) {
  BaseAndTarget blobs;
  auto diff = computeSegmentedBlobDiff(blobs.base_, blobs.target_);
  EXPECT_TRUE(isSegmentedBlobDiff(diff));
  // The patch of the second segment is tiny, the new segment is compressed
  // without a reference.
  EXPECT_LT(diff.size(), headerSize + 4 * entrySize + 15'000);
  EXPECT_THAT(describeSegmentedBlobDiff(diff),
              AllOf(HasSubstr("in 4 segments, 2 of which"),
                    HasSubstr("(300000 bytes) are copied"),
                    HasSubstr("2 are patched")));
  EXPECT_EQ(describeBlobDiff(diff), describeSegmentedBlobDiff(diff));

  // Applying the diff gives exactly the target, also via the generic
  // `applyBlobDiffToCompressedBase`.
  EXPECT_THAT(applySegmentedBlobDiff(blobs.base_, diff, maxTargetSize),
              isBlob(blobs.target_));
  EXPECT_THAT(applyBlobDiffToCompressedBase(blobs.base_, diff, maxTargetSize),
              isBlob(blobs.target_));

  // The diff between identical blobs only consists of copies.
  auto identity = computeSegmentedBlobDiff(blobs.base_, blobs.base_);
  EXPECT_EQ(identity.size(), headerSize + 3 * entrySize);
  EXPECT_THAT(applySegmentedBlobDiff(blobs.base_, identity, maxTargetSize),
              isBlob(blobs.base_));
  // A diff to an empty blob.
  auto empty = segmentedBlob({});
  EXPECT_THAT(applySegmentedBlobDiff(
                  blobs.base_, computeSegmentedBlobDiff(blobs.base_, empty),
                  maxTargetSize),
              isBlob(empty));
}

// A copied segment that is not a complete unit of the base (here: the base
// segments are copied in a different order) is decompressed and compressed
// again, which also gives exactly the target.
TEST(SegmentedBlobDiff, copiesThatAreNotCompleteUnits) {
  BaseAndTarget blobs;
  std::vector<Bytes> reordered{blobs.baseParts_[2], blobs.baseParts_[0],
                               blobs.baseParts_[1]};
  auto target = segmentedBlob(reordered);
  auto diff = computeSegmentedBlobDiff(blobs.base_, target);
  EXPECT_EQ(diff.size(), headerSize + 3 * entrySize);
  EXPECT_THAT(applySegmentedBlobDiff(blobs.base_, diff, maxTargetSize),
              isBlob(target));
}

// The diff only applies to its base, and every corruption of the diff is
// detected.
TEST(SegmentedBlobDiff, errors) {
  BaseAndTarget blobs;
  auto diff = computeSegmentedBlobDiff(blobs.base_, blobs.target_);

  // A wrong base: another segmented blob, or a blob that is not segmented.
  auto otherBase = segmentedBlob({binaryLike(1000, 7), binaryLike(1000, 8)});
  EXPECT_THAT(applySegmentedBlobDiff(otherBase, diff, maxTargetSize),
              isError(ErrorType::wrongBase, "does not belong"));
  auto plain = blobCompression::compressBlob(concat(blobs.baseParts_));
  EXPECT_THAT(applySegmentedBlobDiff(plain, diff, maxTargetSize),
              isError(ErrorType::wrongBase, "not segmented"));
  EXPECT_ANY_THROW(computeSegmentedBlobDiff(plain, blobs.target_));

  // A garbage, truncated, or extended diff file.
  EXPECT_THAT(applySegmentedBlobDiff(blobs.base_, Bytes(5, 'x'), maxTargetSize),
              isError(ErrorType::invalidMagicBytes, "too small"));
  EXPECT_THAT(
      applySegmentedBlobDiff(blobs.base_, Bytes(100, 'x'), maxTargetSize),
      isError(ErrorType::invalidVersion, "not a segmented blob diff"));
  auto truncated = diff;
  truncated.pop_back();
  EXPECT_THAT(applySegmentedBlobDiff(blobs.base_, truncated, maxTargetSize),
              isError(ErrorType::invalidPayloadSize, "truncated"));
  auto extended = diff;
  extended.push_back('x');
  EXPECT_THAT(applySegmentedBlobDiff(blobs.base_, extended, maxTargetSize),
              isError(ErrorType::invalidPayloadSize, "trailing bytes"));
  auto tooManySegments = withValueAt(diff, headerSize - 8, uint64_t{1} << 40);
  EXPECT_THAT(
      applySegmentedBlobDiff(blobs.base_, tooManySegments, maxTargetSize),
      isError(ErrorType::invalidPayloadSize, "truncated"));

  // A corrupted entry: an invalid kind, a missing base segment, a copy whose
  // hash does not match the base, and a corrupted payload.
  const size_t secondEntry = headerSize + entrySize;
  EXPECT_THAT(
      applySegmentedBlobDiff(
          blobs.base_, withValueAt(diff, secondEntry + kindOffset, uint8_t{7}),
          maxTargetSize),
      isError(ErrorType::corruptedPayload, "Invalid kind"));
  EXPECT_THAT(
      applySegmentedBlobDiff(
          blobs.base_,
          withValueAt(diff, headerSize + baseSegmentOffset, uint64_t{17}),
          maxTargetSize),
      isError(ErrorType::wrongBase, "is missing"));
  EXPECT_THAT(
      applySegmentedBlobDiff(
          blobs.base_,
          withValueAt(diff, headerSize + baseSegmentOffset, uint64_t{2}),
          maxTargetSize),
      isError(ErrorType::wrongBase, "different hash"));
  auto corruptedPayload = diff;
  corruptedPayload[headerSize + 4 * entrySize + 20] ^= 1;
  EXPECT_THAT(
      applySegmentedBlobDiff(blobs.base_, corruptedPayload, maxTargetSize),
      isError(ErrorType::corruptedPayload, "cannot be decompressed"));

  // A target that is too large.
  EXPECT_THAT(applySegmentedBlobDiff(blobs.base_, diff,
                                     ad_utility::MemorySize::bytes(1000)),
              isError(ErrorType::invalidTargetSize, "maximum target size"));
  EXPECT_ANY_THROW(describeSegmentedBlobDiff(truncated));
}

// The diff between the segmented blobs of a real index before and after an
// update: the vocabulary is copied, the diff is small, and the result is
// exactly the target blob, which loads and answers queries.
TEST(SegmentedBlobDiff, realBlobsAfterUpdate) {
  std::string triples;
  for (size_t i = 0; i < 2000; ++i) {
    absl::StrAppend(&triples, "<s", i, "> <p> <o", i, "> .\n");
  }
  auto config =
      buildTestIndex(triples, ad_utility::VocabularyType::InMemoryUncompressed);
  Qlever source{EngineConfig{config}};
  BlobSerializationConfig blobConfig;
  blobConfig.writeSegments_ = true;
  // Both blobs have the same format, so that their headers (which are part of
  // the first segment) are equal.
  blobConfig.alwaysUseNewFormat_ = true;
  auto pinAndSerialize = [&source, &blobConfig]() {
    source.queryAndPinResultWithName(
        "all", "SELECT ?s ?p ?o WHERE { ?s ?p ?o } INTERNAL SORT BY ?s ?p ?o");
    return source.serializeVocabAndNamedCacheToCompressedBlob(blobConfig);
  };
  auto base = pinAndSerialize();
  ad_utility::testing::applyUpdateToEngine(
      source, "INSERT DATA { <s10> <p> <o20> . <new1> <p> <o20> }");
  ad_utility::testing::applyUpdateToEngine(source,
                                           "DELETE DATA { <s5> <p> <o5> }");
  auto target = pinAndSerialize();

  auto diff = computeSegmentedBlobDiff(base, target);
  EXPECT_LT(diff.size(), target.size() / 4);
  // The vocabulary is copied, the new word is in the secondary vocabulary.
  EXPECT_THAT(describeSegmentedBlobDiff(diff), HasSubstr("1 of which"));
  auto result = applyBlobDiffToCompressedBase(base, diff, maxTargetSize);
  ASSERT_THAT(result, isBlob(target));

  Qlever loaded{EngineConfig{}, /*skipLoading=*/true};
  loaded.deserializeVocabAndNamedCacheFromCompressedBlob(
      std::get<Bytes>(result));
  auto loadedResult = loaded.query(
      "SELECT ?s ?p ?o WHERE { SERVICE ql:cached-result-with-name-all {} }",
      ad_utility::MediaType::tsv);
  EXPECT_THAT(loadedResult, HasSubstr("<new1>"));
  EXPECT_THAT(loadedResult, Not(HasSubstr("<s5>\t<p>\t<o5>")));
}
