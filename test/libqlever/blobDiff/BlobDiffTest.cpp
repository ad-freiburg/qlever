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
#include "libqlever/Qlever.h"
#include "libqlever/blobDiff/BlobDiff.h"

using namespace qlever;
using namespace qlever::blobDiff;
using namespace testing;
using ad_utility::testing::buildTestIndex;
using ad_utility::testing::decompressOrFail;

namespace {
using Bytes = std::vector<char>;
using ErrorType = BlobDiffErrorType;

// The offsets of some fields of the header of a diff file (see the file format
// in `BlobDiff.h`), used to corrupt them.
constexpr size_t versionOffset = 8;
constexpr size_t targetSizeOffset = versionOffset + 2 + 8 + 32;
constexpr size_t targetChecksumOffset = targetSizeOffset + 8;
constexpr size_t payloadSizeOffset = targetChecksumOffset + 32;
constexpr size_t payloadOffset = payloadSizeOffset + 8;

// The `maxTargetSize` that is passed to the functions in `BlobDiff.h`, unless a
// test checks the limit itself. It is much larger than all targets in the
// tests.
constexpr ad_utility::MemorySize maxTargetSize =
    ad_utility::MemorySize::gigabytes(1);

// Return a copy of the decompressed `compressedBlob` as `Bytes` (fail the test
// on error).
Bytes decompressToBytes(
    ql::span<const char> compressedBlob,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto bytes = decompressOrFail(compressedBlob, {}, l);
  return {bytes.begin(), bytes.end()};
}

// Some triples with distinct subjects and objects.
std::string manyTriples(size_t n) {
  std::string result;
  for (size_t i = 0; i < n; ++i) {
    absl::StrAppend(&result, "<s", i, "> <p> <o", i, "> .\n<s", i,
                    "> <q> \"literal ", i, "\" .\n");
  }
  return result;
}

// Return the lines of `text` in sorted order.
std::vector<std::string> sortedLines(std::string_view text) {
  std::vector<std::string> lines = absl::StrSplit(text, '\n');
  ql::ranges::sort(lines);
  return lines;
}

// `numBytes` pseudo-random bytes (deterministic for a given `seed`).
Bytes randomBytes(size_t numBytes, unsigned seed) {
  std::mt19937 generator{seed};
  std::uniform_int_distribution<int> distribution{0, 255};
  Bytes result(numBytes);
  for (auto& byte : result) {
    byte = static_cast<char>(distribution(generator));
  }
  return result;
}

// Return a copy of `bytes` in which the `uint64_t` at `offset` is replaced by
// `value`.
Bytes withUint64At(Bytes bytes, size_t offset, uint64_t value) {
  std::memcpy(bytes.data() + offset, &value, sizeof(value));
  return bytes;
}

// Matcher for a `BlobDiffError` of the given `type` whose message contains
// `messageSubstring`.
auto blobDiffErrorWith(ErrorType type, std::string_view messageSubstring) {
  return AllOf(AD_FIELD(BlobDiffError, type_, Eq(type)),
               AD_FIELD(BlobDiffError, message_,
                        HasSubstr(std::string{messageSubstring})));
}

// Matcher for the result of one of the functions in `BlobDiff.h` that failed
// with a `BlobDiffError` (see `blobDiffErrorWith`).
auto isError(ErrorType type, std::string_view messageSubstring) {
  return VariantWith<BlobDiffError>(blobDiffErrorWith(type, messageSubstring));
}

// Matcher for the successful result of `applyBlobDiffToCompressedBase` that
// yields `expected`.
auto isBlob(const Bytes& expected) { return VariantWith<Bytes>(Eq(expected)); }

// Matcher for the successful result of `applyBlobDiffToDecompressedBase` that
// yields `expected`.
auto isDecompressedBlob(ql::span<const char> expected) {
  return VariantWith<DecompressedBlob>(ElementsAreArray(expected));
}

// Compute the diff of `base` and `target`, check that applying it to `base`
// yields `target` and that the header is consistent, and return it.
Bytes diffAndCheck(ql::span<const char> base, ql::span<const char> target,
                   int compressionLevel = defaultBlobDiffCompressionLevel,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto diff = computeBlobDiff(base, target, compressionLevel);
  EXPECT_THAT(applyBlobDiffToDecompressedBase(base, diff, maxTargetSize),
              isDecompressedBlob(target));
  EXPECT_THAT(
      readBlobDiffHeader(diff, maxTargetSize),
      VariantWith<BlobDiffHeader>(AllOf(
          AD_FIELD(BlobDiffHeader, baseSize_, Eq(base.size())),
          AD_FIELD(BlobDiffHeader, targetSize_, Eq(target.size())),
          AD_FIELD(BlobDiffHeader, baseChecksum_, Eq(blobChecksum(base))),
          AD_FIELD(BlobDiffHeader, targetChecksum_, Eq(blobChecksum(target))),
          AD_FIELD(BlobDiffHeader, payloadSize_,
                   AllOf(Gt(0u), Eq(diff.size() - payloadOffset))))));
  return diff;
}

// Pin the query `SELECT ?s ?p ?o WHERE { ?s ?p ?o }` sorted by all its
// variables under the name `all` and return the compressed blob.
Bytes pinAndSerialize(Qlever& qlever) {
  qlever.queryAndPinResultWithName(
      "all", "SELECT ?s ?p ?o WHERE { ?s ?p ?o } INTERNAL SORT BY ?s ?p ?o");
  return qlever.serializeVocabAndNamedCacheToCompressedBlob();
}
}  // namespace

// Test the diff of synthetic byte sequences: a target made of pieces of the
// base gives a tiny diff, unrelated data a diff slightly larger than the
// target, and the edge cases (identical, empty, highly repetitive inputs, all
// compression levels) round-trip.
TEST(BlobDiff, syntheticBytes) {
  // A target that mostly consists of pieces of the base gives a tiny diff,
  // no matter where the pieces are.
  auto base = randomBytes(200'000, 42);
  Bytes target;
  target.insert(target.end(), base.begin() + 150'000, base.end());
  auto inserted = randomBytes(1'000, 7);
  target.insert(target.end(), inserted.begin(), inserted.end());
  target.insert(target.end(), base.begin(), base.begin() + 100'000);
  auto diff = diffAndCheck(base, target);
  EXPECT_THAT(describeBlobDiff(diff), HasSubstr("payload"));
  // The random inserted bytes are incompressible, the rest costs almost
  // nothing.
  EXPECT_THAT(diff.size(), AllOf(Gt(1'000u), Lt(1'000u + 500u)));

  // Unrelated random data cannot be compressed at all, the diff is then a
  // little larger than the target.
  auto unrelated = randomBytes(50'000, 99);
  EXPECT_THAT(diffAndCheck(base, unrelated).size(),
              AllOf(Gt(unrelated.size()), Lt(unrelated.size() + 1'000)));

  // Edge cases: identical inputs (also in memory, which ZSTD itself does not
  // handle, see `ZstdWrapper::compressWithReference`), empty base, empty
  // target, both empty.
  EXPECT_LT(diffAndCheck(base, base).size(), 200u);
  EXPECT_LT(diffAndCheck(base, Bytes{base}).size(), 200u);
  diffAndCheck({}, base);
  diffAndCheck(base, {});
  diffAndCheck({}, {});

  // A highly repetitive target is much larger than the base and the payload
  // together, which is still a valid (and plausible) target size.
  auto zeros = Bytes(10'000'000, '\0');
  EXPECT_LT(diffAndCheck({}, zeros).size(), 2'000u);

  // All compression levels work.
  for (int level : {1, 3, 9, 22}) {
    diffAndCheck(base, target, level);
  }
}

// Test that a wrong base, garbage, a wrong version, a truncated or extended
// file, a corrupted target size, frame header, payload or target checksum are
// all reported with the right `BlobDiffErrorType`, and nothing throws.
TEST(BlobDiff, invalidInputsAreReported) {
  auto base = randomBytes(10'000, 1);
  auto other = randomBytes(10'000, 2);
  auto target = randomBytes(10'000, 3);
  auto diff = diffAndCheck(base, target);
  Bytes garbage(100, 'x');

  // The diff only fits its base (same size, different content).
  EXPECT_THAT(
      applyBlobDiffToDecompressedBase(other, diff, maxTargetSize),
      isError(ErrorType::wrongBase, "does not belong to the given base"));
  // Garbage instead of a diff, also when applied.
  EXPECT_THAT(readBlobDiffHeader(garbage, maxTargetSize),
              isError(ErrorType::invalidMagicBytes, "not a blob diff file"));
  EXPECT_THAT(applyBlobDiffToDecompressedBase(base, garbage, maxTargetSize),
              isError(ErrorType::invalidMagicBytes, "not a blob diff file"));
  EXPECT_THAT(readBlobDiffHeader(Bytes(5, 'x'), maxTargetSize),
              isError(ErrorType::invalidMagicBytes, "too small"));
  AD_EXPECT_THROW_WITH_MESSAGE(describeBlobDiff(garbage),
                               HasSubstr("not a blob diff file"));
  // A wrong version.
  auto wrongVersion = diff;
  wrongVersion[versionOffset] = 1;
  EXPECT_THAT(
      readBlobDiffHeader(wrongVersion, maxTargetSize),
      isError(ErrorType::invalidVersion, "unsupported format version 1"));
  // Truncated and extended diffs.
  auto truncated = diff;
  truncated.pop_back();
  EXPECT_THAT(applyBlobDiffToDecompressedBase(base, truncated, maxTargetSize),
              isError(ErrorType::invalidPayloadSize,
                      "truncated or has trailing bytes"));
  auto extended = diff;
  extended.push_back('x');
  EXPECT_THAT(applyBlobDiffToDecompressedBase(base, extended, maxTargetSize),
              isError(ErrorType::invalidPayloadSize,
                      "truncated or has trailing bytes"));
  // A corrupted size of the target is detected before the target is
  // allocated, both if it is huge and if it only differs a little from the
  // size in the ZSTD frame.
  for (uint64_t targetSize : {uint64_t{1} << 60, uint64_t{10'001}}) {
    EXPECT_THAT(
        readBlobDiffHeader(withUint64At(diff, targetSizeOffset, targetSize),
                           maxTargetSize),
        isError(ErrorType::invalidTargetSize, "but its payload encodes"));
  }
  // A corrupted header of the ZSTD frame is detected.
  auto corruptedFrameHeader = diff;
  corruptedFrameHeader[payloadOffset] ^= 0x55;
  EXPECT_THAT(readBlobDiffHeader(corruptedFrameHeader, maxTargetSize),
              isError(ErrorType::corruptedPayload, "not a valid ZSTD frame"));
  // A corrupted payload is detected (by ZSTD or by the checksum of the
  // target).
  auto corrupted = diff;
  corrupted[corrupted.size() / 2] ^= 0x55;
  corrupted.back() ^= 0x55;
  EXPECT_THAT(applyBlobDiffToDecompressedBase(base, corrupted, maxTargetSize),
              isError(ErrorType::corruptedPayload, "corrupted"));
  // A corrupted checksum of the target is detected.
  auto wrongTarget = diff;
  wrongTarget[targetChecksumOffset] ^= 0x55;
  EXPECT_THAT(applyBlobDiffToDecompressedBase(base, wrongTarget, maxTargetSize),
              isError(ErrorType::corruptedPayload, "checksum of the result"));
}

// Test that a forged diff whose header and ZSTD frame consistently announce a
// huge target is rejected via `maxTargetSize` before the target is allocated.
TEST(BlobDiff, implausibleTargetSizeIsReported) {
  // A ZSTD frame of a few bytes whose header consistently announces a huge
  // content size, as could be forged. The size exceeds the `maxTargetSize`, so
  // the diff is rejected before the target is allocated.
  auto diff = computeBlobDiff({}, {});
  // Replace the payload by a frame header that announces `2^60` bytes (the
  // single segment flag is not set, the frame content size field has 8 bytes,
  // and the window descriptor is the smallest possible one), followed by an
  // empty last raw block.
  Bytes forged(diff.begin(), diff.begin() + payloadOffset);
  Bytes frame{'\x28', '\xb5', '\x2f', '\xfd', '\xc0', '\x00'};
  Bytes contentSize(8, '\0');
  uint64_t hugeSize = uint64_t{1} << 60;
  std::memcpy(contentSize.data(), &hugeSize, sizeof(hugeSize));
  frame.insert(frame.end(), contentSize.begin(), contentSize.end());
  frame.insert(frame.end(), {'\x01', '\x00', '\x00'});
  forged.insert(forged.end(), frame.begin(), frame.end());
  forged = withUint64At(std::move(forged), targetSizeOffset, hugeSize);
  forged = withUint64At(std::move(forged), payloadSizeOffset, frame.size());
  EXPECT_THAT(
      readBlobDiffHeader(forged, maxTargetSize),
      isError(ErrorType::invalidTargetSize, "exceeds the maximum target size"));
}

// Test that `maxTargetSize` admits a target of exactly that size and rejects
// a target one byte larger, for all three reading functions.
TEST(BlobDiff, maxTargetSizeIsRespected) {
  using namespace ad_utility::memory_literals;
  auto base = randomBytes(10'000, 1);
  auto target = randomBytes(10'000, 3);
  auto diff = diffAndCheck(base, target);
  auto compressedBase = qlever::NamedCachedQueryBlobManager::compressBlob(base);

  // A limit that is at least the size of the target is fine.
  for (auto limit : {10'000_B, 1_MB}) {
    EXPECT_THAT(readBlobDiffHeader(diff, limit),
                VariantWith<BlobDiffHeader>(
                    AD_FIELD(BlobDiffHeader, targetSize_, Eq(10'000u))));
    EXPECT_THAT(applyBlobDiffToDecompressedBase(base, diff, limit),
                isDecompressedBlob(target));
    EXPECT_THAT(applyBlobDiffToCompressedBase(compressedBase, diff, limit),
                VariantWith<Bytes>(SizeIs(Gt(0u))));
  }

  // A smaller limit is reported, also when applying the diff.
  auto matcher =
      isError(ErrorType::invalidTargetSize, "exceeds the maximum target size");
  EXPECT_THAT(readBlobDiffHeader(diff, 9'999_B), matcher);
  EXPECT_THAT(applyBlobDiffToDecompressedBase(base, diff, 9'999_B), matcher);
  EXPECT_THAT(applyBlobDiffToCompressedBase(compressedBase, diff, 9'999_B),
              matcher);
}

// Test the diff between the blobs of a real index before and after an update
// with new words: the diff is small, applying it to the compressed base gives
// the compressed target byte for byte, the result loads and answers queries,
// and a wrong base or garbage is rejected.
TEST(BlobDiff, realBlobsAfterUpdate) {
  auto config = buildTestIndex(
      manyTriples(2000), ad_utility::VocabularyType::InMemoryUncompressed);
  Qlever source{EngineConfig{config}};
  auto baseCompressed = pinAndSerialize(source);
  // Insert and delete a few triples, with and without new words.
  ad_utility::testing::applyUpdateToEngine(
      source,
      "INSERT DATA { <s10> <q> <o20> . <s100> <p> <o1> . <new1> <p> <o20> . "
      "<s7> <p> \"new literal\" }");
  ad_utility::testing::applyUpdateToEngine(
      source, "DELETE DATA { <s5> <p> <o5> . <s1500> <q> \"literal 1500\" }");
  auto targetCompressed = pinAndSerialize(source);

  auto base = decompressToBytes(baseCompressed);
  auto target = decompressToBytes(targetCompressed);
  auto diff = diffAndCheck(base, target);
  // The diff is a lot smaller than the (compressed) target.
  EXPECT_LT(diff.size(), targetCompressed.size() / 10);

  // Applying the diff to the compressed base gives the compressed target (byte
  // for byte, because the recompression is deterministic), and the result is a
  // standard blob that answers queries.
  auto result =
      applyBlobDiffToCompressedBase(baseCompressed, diff, maxTargetSize);
  ASSERT_THAT(result, isBlob(targetCompressed));
  const auto& resultCompressed = std::get<Bytes>(result);
  Qlever loaded{EngineConfig{}, /*skipLoading=*/true};
  EXPECT_NO_THROW(
      loaded.deserializeVocabAndNamedCacheFromCompressedBlob(resultCompressed));
  auto expected = source.query("SELECT ?s ?p ?o WHERE { ?s ?p ?o }",
                               ad_utility::MediaType::tsv);
  auto loadedResult = loaded.query(
      "SELECT ?s ?p ?o WHERE { SERVICE ql:cached-result-with-name-all {} }",
      ad_utility::MediaType::tsv);
  EXPECT_THAT(sortedLines(loadedResult), Eq(sortedLines(expected)));
  EXPECT_THAT(expected, HasSubstr("<new1>"));
  EXPECT_THAT(expected, Not(HasSubstr("<s5>\t<p>\t<o5>")));

  // The diff only fits its base, and garbage is rejected.
  EXPECT_THAT(
      applyBlobDiffToCompressedBase(targetCompressed, diff, maxTargetSize),
      isError(ErrorType::wrongBase, "does not belong to the given base"));
  Bytes garbage(100, 'x');
  EXPECT_THAT(applyBlobDiffToCompressedBase(garbage, diff, maxTargetSize),
              isError(ErrorType::baseNotDecompressible,
                      "base blob cannot be decompressed"));
  EXPECT_THAT(
      applyBlobDiffToCompressedBase(baseCompressed, garbage, maxTargetSize),
      isError(ErrorType::invalidMagicBytes, "not a blob diff file"));
}

// Test that `applyBlobDiffToCompressedBase` validates the diff before the
// base, and detects a base of the wrong size (via its frame header) as well as
// a base of the right size but wrong content (via its checksum).
TEST(BlobDiff, compressedBaseIsCheckedAfterTheDiff) {
  using Manager = qlever::NamedCachedQueryBlobManager;
  auto base = randomBytes(10'000, 1);
  auto target = randomBytes(10'000, 3);
  auto diff = diffAndCheck(base, target);
  Bytes garbage(100, 'x');

  // The diff is validated before the base, so an invalid diff is reported even
  // if the base is garbage as well.
  EXPECT_THAT(applyBlobDiffToCompressedBase(garbage, garbage, maxTargetSize),
              isError(ErrorType::invalidMagicBytes, "not a blob diff file"));

  // A base of the wrong size is detected (from the header of its ZSTD frame),
  // and a base of the right size, but with the wrong content, by its checksum.
  for (const auto& wrongBase :
       {randomBytes(9'999, 1), randomBytes(10'000, 2)}) {
    EXPECT_THAT(
        applyBlobDiffToCompressedBase(Manager::compressBlob(wrongBase), diff,
                                      maxTargetSize),
        isError(ErrorType::wrongBase, "does not belong to the given base"));
  }
}
