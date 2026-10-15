// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../QleverTestHelpers.h"
#include "backports/algorithm.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "libqlever/Qlever.h"
#include "libqlever/blobDiff/BlobDiff.h"
#include "util/File.h"

using namespace qlever;
using namespace testing;

namespace {
using Manager = NamedCachedQueryBlobManager;
using Bytes = std::vector<char>;

// Write the `turtleContents` to a turtle file, build an index with an
// in-memory vocabulary from it, and return the `IndexBuilderConfig`. The
// basename is derived from the name of the running test.
IndexBuilderConfig buildTestIndex(std::string_view turtleContents) {
  std::string basename = gtestCurrentTestName();
  // Updates that were persisted by a previous run of the test must not be
  // applied to the new index.
  ad_utility::deleteFile(absl::StrCat(basename, ".update-triples"), false);
  std::string sourceFilename = absl::StrCat(basename, ".ttl");
  {
    auto ofs = ad_utility::makeOfstream(sourceFilename);
    ofs << turtleContents;
  }
  absl::Cleanup cleanup = [&sourceFilename] {
    ad_utility::deleteFile(sourceFilename);
  };
  IndexBuilderConfig config;
  config.inputFiles_.push_back(
      {sourceFilename, Filetype::Turtle, std::nullopt});
  config.baseName_ = std::move(basename);
  config.vocabType_ = ad_utility::VocabularyType::InMemoryUncompressed;
  EXPECT_NO_THROW(Qlever::buildIndex(config));
  return config;
}

// Decompress `compressedBlob` (fail the test on error).
Bytes decompressOrFail(ql::span<const char> compressedBlob) {
  auto result = Manager::tryToDecompressBlob(compressedBlob, {});
  if (const auto* error = std::get_if<Manager::BlobError>(&result)) {
    ADD_FAILURE() << error->message_;
    return {};
  }
  const auto& bytes = std::get<0>(result);
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

// Compute the diff of `base` and `target`, check that applying it to `base`
// yields `target` and that the header is consistent, and return it.
Bytes diffAndCheck(const Bytes& base, const Bytes& target,
                   int compressionLevel = defaultBlobDiffCompressionLevel) {
  auto diff = computeBlobDiff(base, target, compressionLevel);
  EXPECT_EQ(applyBlobDiffToDecompressed(base, diff), target);
  auto header = readBlobDiffHeader(diff);
  EXPECT_EQ(header.baseSize_, base.size());
  EXPECT_EQ(header.targetSize_, target.size());
  EXPECT_EQ(header.baseChecksum_, blobChecksum(base));
  EXPECT_EQ(header.targetChecksum_, blobChecksum(target));
  EXPECT_GT(header.payloadSize_, 0u);
  EXPECT_LT(header.payloadSize_, diff.size());
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

// _____________________________________________________________________________
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
  std::cout << describeBlobDiff(diff) << std::endl;
  EXPECT_THAT(describeBlobDiff(diff), HasSubstr("payload"));
  // The random inserted bytes are incompressible, the rest costs almost
  // nothing.
  EXPECT_LT(diff.size(), 1'000u + 500u);
  EXPECT_GT(diff.size(), 1'000u);

  // Unrelated random data cannot be compressed at all, the diff is then a
  // little larger than the target.
  auto unrelated = randomBytes(50'000, 99);
  auto largeDiff = diffAndCheck(base, unrelated);
  EXPECT_GT(largeDiff.size(), unrelated.size());
  EXPECT_LT(largeDiff.size(), unrelated.size() + 1'000);

  // Edge cases: identical inputs (also in memory, which ZSTD itself does not
  // handle, see `ZstdWrapper::compressWithReference`), empty base, empty
  // target, both empty.
  EXPECT_LT(diffAndCheck(base, base).size(), 200u);
  EXPECT_LT(diffAndCheck(base, Bytes{base}).size(), 200u);
  diffAndCheck({}, base);
  diffAndCheck(base, {});
  diffAndCheck({}, {});

  // All compression levels work.
  for (int level : {1, 3, 9, 22}) {
    diffAndCheck(base, target, level);
  }
}

// _____________________________________________________________________________
TEST(BlobDiff, invalidInputsThrow) {
  auto base = randomBytes(10'000, 1);
  auto other = randomBytes(10'000, 2);
  auto target = randomBytes(10'000, 3);
  auto diff = diffAndCheck(base, target);

  // The diff only fits its base (same size, different content).
  AD_EXPECT_THROW_WITH_MESSAGE(applyBlobDiffToDecompressed(other, diff),
                               HasSubstr("does not belong to the given base"));
  // Garbage instead of a diff.
  Bytes garbage(100, 'x');
  AD_EXPECT_THROW_WITH_MESSAGE(readBlobDiffHeader(garbage),
                               HasSubstr("not a blob diff file"));
  AD_EXPECT_THROW_WITH_MESSAGE(readBlobDiffHeader(Bytes(5, 'x')),
                               HasSubstr("too small"));
  // A wrong version.
  auto wrongVersion = diff;
  wrongVersion[8] = 1;
  AD_EXPECT_THROW_WITH_MESSAGE(readBlobDiffHeader(wrongVersion),
                               HasSubstr("unsupported format version 1"));
  // Truncated and extended diffs.
  auto truncated = diff;
  truncated.pop_back();
  AD_EXPECT_THROW_WITH_MESSAGE(applyBlobDiffToDecompressed(base, truncated),
                               HasSubstr("truncated or has trailing bytes"));
  auto extended = diff;
  extended.push_back('x');
  AD_EXPECT_THROW_WITH_MESSAGE(applyBlobDiffToDecompressed(base, extended),
                               HasSubstr("truncated or has trailing bytes"));
  // A corrupted payload is detected (by ZSTD or by the checksum of the
  // target).
  auto corrupted = diff;
  corrupted[corrupted.size() / 2] ^= 0x55;
  corrupted.back() ^= 0x55;
  EXPECT_ANY_THROW(applyBlobDiffToDecompressed(base, corrupted));
  // A corrupted checksum of the target is detected.
  auto wrongTarget = diff;
  wrongTarget[8 + 2 + 8 + 32 + 8] ^= 0x55;
  AD_EXPECT_THROW_WITH_MESSAGE(applyBlobDiffToDecompressed(base, wrongTarget),
                               HasSubstr("checksum of the result"));
}

// _____________________________________________________________________________
TEST(BlobDiff, realBlobsAfterUpdate) {
  auto config = buildTestIndex(manyTriples(2000));
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

  auto base = decompressOrFail(baseCompressed);
  auto target = decompressOrFail(targetCompressed);
  auto diff = diffAndCheck(base, target);
  std::cout << "Diff for an update (compressed target has "
            << targetCompressed.size() << " bytes): " << describeBlobDiff(diff)
            << std::endl;
  // The diff is a lot smaller than the (compressed) target.
  EXPECT_LT(diff.size(), targetCompressed.size() / 10);

  // Applying the diff to the compressed base gives the compressed target (byte
  // for byte, because the recompression is deterministic), and the result is a
  // standard blob that answers queries.
  auto resultCompressed = applyBlobDiff(baseCompressed, diff);
  EXPECT_EQ(resultCompressed, targetCompressed);
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
  AD_EXPECT_THROW_WITH_MESSAGE(applyBlobDiff(targetCompressed, diff),
                               HasSubstr("does not belong to the given base"));
  Bytes garbage(100, 'x');
  AD_EXPECT_THROW_WITH_MESSAGE(applyBlobDiff(garbage, diff),
                               HasSubstr("base blob cannot be decompressed"));
  AD_EXPECT_THROW_WITH_MESSAGE(applyBlobDiff(baseCompressed, garbage),
                               HasSubstr("not a blob diff file"));
}
