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
#include <zstd.h>

#include <algorithm>
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
constexpr size_t checksumTypeOffset = versionOffset + 2;
constexpr size_t targetSizeOffset = checksumTypeOffset + 1 + 8 + 32;
constexpr size_t targetChecksumOffset = targetSizeOffset + 8;
constexpr size_t payloadSizeOffset = targetChecksumOffset + 32 + 4 + 8;
constexpr size_t payloadOffset = payloadSizeOffset + 8;

// The options of `computeBlobDiff` with the given checksum `type` (and the
// default values otherwise).
BlobDiffOptions withChecksums(BlobChecksumType type) {
  BlobDiffOptions options;
  options.checksumType_ = type;
  return options;
}

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
                   const BlobDiffOptions& options = {},
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto diff = computeBlobDiff(base, target, options);
  EXPECT_THAT(applyBlobDiffToDecompressedBase(base, diff, maxTargetSize),
              isDecompressedBlob(target));
  auto type = options.checksumType_;
  EXPECT_THAT(
      readBlobDiffHeader(diff, maxTargetSize),
      VariantWith<BlobDiffHeader>(AllOf(
          AD_FIELD(BlobDiffHeader, checksumType_, Eq(type)),
          AD_FIELD(BlobDiffHeader, baseSize_, Eq(base.size())),
          AD_FIELD(BlobDiffHeader, targetSize_, Eq(target.size())),
          AD_FIELD(BlobDiffHeader, baseChecksum_, Eq(blobChecksum(base, type))),
          AD_FIELD(BlobDiffHeader, targetChecksum_,
                   Eq(blobChecksum(target, type))),
          AD_FIELD(
              BlobDiffHeader, targetEncoding_,
              AllOf(AD_FIELD(blobCompression::BlobEncoding, compressionLevel_,
                             Eq(options.targetEncoding_.compressionLevel_)),
                    AD_FIELD(blobCompression::BlobEncoding, stablePrefixSize_,
                             Eq(options.targetEncoding_.stablePrefixSize_)))),
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

  // All compression levels, checksum types, and target encodings work.
  for (int level : {1, 3, 9, 22}) {
    BlobDiffOptions options;
    options.compressionLevel_ = level;
    diffAndCheck(base, target, options);
  }
  for (auto type : {BlobChecksumType::none, BlobChecksumType::crc32c,
                    BlobChecksumType::sha256}) {
    BlobDiffOptions options = withChecksums(type);
    options.targetEncoding_ = {-5, 12345};
    diffAndCheck(base, target, options);
  }
}

// Test the names of the checksum types, and the checksums themselves.
TEST(BlobDiff, checksumTypes) {
  for (auto type : {BlobChecksumType::none, BlobChecksumType::crc32c,
                    BlobChecksumType::sha256}) {
    EXPECT_THAT(blobChecksumTypeFromString(toString(type)), Optional(type));
  }
  EXPECT_EQ(toString(BlobChecksumType::crc32c), "crc32c");
  EXPECT_EQ(blobChecksumTypeFromString("md5"), std::nullopt);

  // The checksums of an empty input: only the first 4 bytes of a CRC32C are
  // used, and nothing of `none`.
  Bytes empty;
  BlobDiffHeader::Checksum zeros{};
  EXPECT_EQ(blobChecksum(empty, BlobChecksumType::none), zeros);
  EXPECT_EQ(blobChecksum(empty, BlobChecksumType::crc32c), zeros);
  std::string abc = "abc";
  ql::span<const char> abcSpan{abc.data(), abc.size()};
  auto crc = blobChecksum(abcSpan, BlobChecksumType::crc32c);
  // The CRC32C of `abc` is `0x364B3FB7`.
  uint32_t value;
  std::memcpy(&value, crc.data(), sizeof(value));
  EXPECT_EQ(value, 0x364B3FB7u);
  EXPECT_TRUE(
      std::all_of(crc.begin() + 4, crc.end(), [](char c) { return c == 0; }));
  // The SHA-256 of `abc` starts with `ba7816bf`.
  auto sha = blobChecksum(abcSpan, BlobChecksumType::sha256);
  EXPECT_EQ(sha, blobChecksum(abcSpan));
  EXPECT_EQ(static_cast<uint8_t>(sha[0]), 0xBAu);
  EXPECT_EQ(static_cast<uint8_t>(sha[1]), 0x78u);
}

// Test that a wrong base, garbage, a wrong version, a truncated or extended
// file, a corrupted target size, frame header, payload or target checksum are
// all reported with the right `BlobDiffErrorType`, and nothing throws.
TEST(BlobDiff, invalidInputsAreReported) {
  auto base = randomBytes(10'000, 1);
  auto other = randomBytes(10'000, 2);
  // The target consists of pieces of the base, so applying the diff to the
  // wrong base yields a wrong result (which is detected).
  Bytes target{base.begin() + 5'000, base.end()};
  target.insert(target.end(), base.begin(), base.begin() + 5'000);
  auto diff =
      diffAndCheck(base, target, withChecksums(BlobChecksumType::sha256));
  Bytes garbage(200, 'x');

  // The diff only fits its base (same size, different content), which is
  // detected by the checksums. Without checksums, it is detected by the
  // checksum of the payload.
  EXPECT_THAT(
      applyBlobDiffToDecompressedBase(other, diff, maxTargetSize),
      isError(ErrorType::wrongBase, "does not belong to the given base"));
  auto crcDiff =
      diffAndCheck(base, target, withChecksums(BlobChecksumType::crc32c));
  EXPECT_THAT(
      applyBlobDiffToDecompressedBase(other, crcDiff, maxTargetSize),
      isError(ErrorType::wrongBase, "does not belong to the given base"));
  auto noChecksums = diffAndCheck(base, target);
  EXPECT_THAT(
      applyBlobDiffToDecompressedBase(other, noChecksums, maxTargetSize),
      isError(ErrorType::corruptedPayload, "cannot be decompressed"));
  // A base of the wrong size is always detected.
  EXPECT_THAT(
      applyBlobDiffToDecompressedBase(randomBytes(9'999, 1), noChecksums,
                                      maxTargetSize),
      isError(ErrorType::wrongBase, "does not belong to the given base"));
  // An unknown checksum type.
  auto wrongType = diff;
  wrongType[checksumTypeOffset] = 3;
  EXPECT_THAT(
      readBlobDiffHeader(wrongType, maxTargetSize),
      isError(ErrorType::invalidChecksumType, "unknown checksum type 3"));
  // Garbage instead of a diff, also when applied.
  EXPECT_THAT(readBlobDiffHeader(garbage, maxTargetSize),
              isError(ErrorType::invalidMagicBytes, "not a blob diff file"));
  EXPECT_THAT(applyBlobDiffToDecompressedBase(base, garbage, maxTargetSize),
              isError(ErrorType::invalidMagicBytes, "not a blob diff file"));
  EXPECT_THAT(readBlobDiffHeader(Bytes(5, 'x'), maxTargetSize),
              isError(ErrorType::invalidMagicBytes, "too small"));
  EXPECT_THAT(readBlobDiffHeader(Bytes(110, 'x'), maxTargetSize),
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
  Bytes garbage(200, 'x');
  EXPECT_THAT(applyBlobDiffToCompressedBase(garbage, diff, maxTargetSize),
              isError(ErrorType::baseNotDecompressible,
                      "base blob cannot be decompressed"));
  EXPECT_THAT(
      applyBlobDiffToCompressedBase(baseCompressed, garbage, maxTargetSize),
      isError(ErrorType::invalidMagicBytes, "not a blob diff file"));
}

// Test that applying a diff to a compressed base copies the compressed stable
// prefix of the base (see `BlobCompression.h`), and yields the compressed
// target that was written with the same settings byte for byte.
TEST(BlobDiff, stablePrefixIsCopied) {
  auto config = buildTestIndex(
      manyTriples(2000), ad_utility::VocabularyType::InMemoryUncompressed);
  Qlever source{EngineConfig{config}};
  BlobSerializationConfig serializationConfig;
  serializationConfig.writeStablePrefix_ = true;
  serializationConfig.compressionLevel_ = 2;
  source.queryAndPinResultWithName(
      "all", "SELECT ?s ?p ?o WHERE { ?s ?p ?o } INTERNAL SORT BY ?s ?p ?o");
  auto baseCompressed =
      source.serializeVocabAndNamedCacheToCompressedBlob(serializationConfig);
  // An update without new words keeps the vocabulary (and thereby the stable
  // prefix) unchanged.
  ad_utility::testing::applyUpdateToEngine(source,
                                           "INSERT DATA { <s10> <q> <o20> }");
  ad_utility::testing::applyUpdateToEngine(source,
                                           "DELETE DATA { <s5> <p> <o5> }");
  source.queryAndPinResultWithName(
      "all", "SELECT ?s ?p ?o WHERE { ?s ?p ?o } INTERNAL SORT BY ?s ?p ?o");
  auto targetCompressed =
      source.serializeVocabAndNamedCacheToCompressedBlob(serializationConfig);

  auto targetIndex = blobCompression::readBlobIndex(targetCompressed);
  ASSERT_TRUE(targetIndex.has_value());
  EXPECT_EQ(targetIndex->encoding_.compressionLevel_, 2);
  BlobDiffOptions options;
  options.targetEncoding_ = targetIndex->encoding_;
  auto diff = diffAndCheck(decompressToBytes(baseCompressed),
                           decompressToBytes(targetCompressed), options);
  auto result =
      applyBlobDiffToCompressedBase(baseCompressed, diff, maxTargetSize);
  ASSERT_THAT(result, isBlob(targetCompressed));

  // The stable prefix requires the sequence API of ZSTD 1.5.5 (see
  // `BlobCompression.h`).
  if (ZSTD_versionNumber() < 10505) {
    EXPECT_EQ(targetIndex->encoding_.stablePrefixSize_, 0u);
    return;
  }
  // The compressed stable prefix of the result is the one of the base.
  auto prefixEnd = targetIndex->stablePrefixBlocksEnd_;
  EXPECT_GT(targetIndex->encoding_.stablePrefixSize_, 4096u);
  auto baseIndex = blobCompression::readBlobIndex(baseCompressed);
  ASSERT_TRUE(baseIndex.has_value());
  EXPECT_EQ(baseIndex->stablePrefixBlocksEnd_, prefixEnd);
  // The frame headers (13 bytes) differ in the size of the content.
  const auto& resultBytes = std::get<Bytes>(result);
  constexpr size_t frameHeaderSize = 13;
  EXPECT_TRUE(std::equal(resultBytes.begin() + frameHeaderSize,
                         resultBytes.begin() + prefixEnd,
                         baseCompressed.begin() + frameHeaderSize));
}

// Test that `applyBlobDiffToCompressedBase` validates the diff before the
// base, and detects a base of the wrong size (via its frame header) as well as
// a base of the right size but wrong content (via its checksum).
TEST(BlobDiff, compressedBaseIsCheckedAfterTheDiff) {
  using Manager = qlever::NamedCachedQueryBlobManager;
  auto base = randomBytes(10'000, 1);
  auto target = randomBytes(10'000, 3);
  auto diff =
      diffAndCheck(base, target, withChecksums(BlobChecksumType::crc32c));
  Bytes garbage(200, 'x');

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
