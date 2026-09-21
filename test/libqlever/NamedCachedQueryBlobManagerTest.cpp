// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <gmock/gmock.h>

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "QleverTestHelpers.h"
#include "backports/memory_resource.h"
#include "backports/span.h"
#include "index/IndexImpl.h"
#include "index/vocabulary/SecondaryVocabulary.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "libqlever/Qlever.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/json.h"

using namespace qlever;
using namespace testing;

namespace {
using Manager = NamedCachedQueryBlobManager;
using ad_utility::VocabularyType;

// A `ql::pmr::memory_resource` that counts the allocations routed through it,
// used to verify that a caller-provided allocator is actually used for the
// decompressed blob buffer.
class CountingMemoryResource : public ql::pmr::memory_resource {
 public:
  size_t numAllocations_ = 0;
  size_t bytesAllocated_ = 0;

 private:
  void* do_allocate(size_t bytes, size_t alignment) override {
    ++numAllocations_;
    bytesAllocated_ += bytes;
    return ql::pmr::new_delete_resource()->allocate(bytes, alignment);
  }
  void do_deallocate(void* p, size_t bytes, size_t alignment) override {
    ql::pmr::new_delete_resource()->deallocate(p, bytes, alignment);
  }
  bool do_is_equal(
      const ql::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }
};

// Write the `turtleContents` to a turtle file, build an index from it with the
// given vocabulary `type`, and return the corresponding `IndexBuilderConfig`.
// The basename of the index is derived from the name of the currently running
// test and the optional `suffix`, so that concurrently running tests do not
// interfere with each other, and so that a single test can build several
// distinct indexes. The turtle input file is deleted again immediately after
// the index was built.
//
// NOTE: The default vocabulary type is the in-memory, uncompressed one,
// because `serializeVocabAndNamedCacheToCompressedBlob` currently requires it
// unless vocabulary entries are excluded (see
// `Vocabulary::writeAsZeroCopyBlob`).
IndexBuilderConfig buildTestIndex(
    std::string_view turtleContents,
    VocabularyType type = VocabularyType::InMemoryUncompressed,
    std::string_view suffix = "") {
  std::string basename = absl::StrCat(gtestCurrentTestName(), suffix);
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
  config.vocabType_ = type;
  EXPECT_NO_THROW(Qlever::buildIndex(config));
  return config;
}

// The turtle data used by the tests for the filtered vocabulary export below:
// one triple whose subject and object survive the filtering, and one whose
// subject and object are excluded from the blob (they contain `dropped`, which
// is what the regexes below match).
constexpr std::string_view filterTestData =
    "<keptSubject> <filterPredicate> \"kept literal\".\n"
    "<droppedSubject> <filterPredicate> \"dropped literal\".";

// The name under which the tests below pin the result of their source query.
constexpr std::string_view filterPinName = "filterPin";

// The query whose result the tests below pin under `filterPinName`.
constexpr std::string_view filterSourceQuery =
    "SELECT ?s ?o WHERE { ?s <filterPredicate> ?o }";

// The query that returns the pinned result (see `filterPinName`) of a blob.
constexpr std::string_view filterPinQuery =
    "SELECT ?s ?o WHERE { SERVICE ql:cached-result-with-name-filterPin {}}";

// The regex with which the tests below exclude the vocabulary entries of
// `filterTestData` that contain `dropped`.
constexpr std::string_view droppedEntriesRegex = ".*dropped.*";

// Return a `BlobSerializationConfig` that excludes all vocabulary entries
// matching one of the given `regexes`. Note that the regexes are matched
// against the complete entry, hence the leading and trailing `.*` of
// `droppedEntriesRegex` above, which thus excludes the IRI `<droppedSubject>`
// as well as the literal `"dropped literal"`.
BlobSerializationConfig excludeConfig(std::vector<std::string> regexes) {
  BlobSerializationConfig config;
  config.excludedEntryRegexes_ = std::move(regexes);
  return config;
}

// Return the vocabulary index of the unique vocabulary entry of `qlever` that
// contains the `substring`.
uint64_t vocabIndexOfEntryContaining(const Qlever& qlever,
                                     std::string_view substring) {
  std::optional<uint64_t> result;
  const auto& vocabulary = qlever.indexAndViewsSnapshot()->index_.getVocab();
  for (const IndexAndWord& entry : vocabulary.scanAll()) {
    if (absl::StrContains(entry.word_, substring)) {
      EXPECT_FALSE(result.has_value()) << entry.word_;
      result = entry.index_;
    }
  }
  EXPECT_TRUE(result.has_value()) << substring;
  return result.value_or(0);
}

// Return a read serializer for the (already decompressed) blob in `data`. Note
// that the serializer only stores a view of the `data`, which therefore has to
// outlive it.
auto makeBlobReader(ql::span<const char> data) {
  return ad_utility::serialization::ByteBufferReadSerializerT<
      true, ql::span<const char>>{data};
}

// Return a read serializer for the payload of the given chunk `region` of the
// (already decompressed) blob in `data`. Note that the payload of a chunk
// begins at a multiple of `alignof(std::max_align_t)`, so the returned
// serializer is properly aligned.
auto makeChunkPayloadReader(ql::span<const char> data,
                            const Manager::BlobLayout::Region& region) {
  return makeBlobReader(region.payloadSpan(data));
}

// Decompress the `compressedBlob` and return the index metadata JSON that is
// stored in its first chunk (see
// `NamedCachedQueryBlobManager::parseBlobLayout`).
nlohmann::json metadataFromBlob(ql::span<const char> compressedBlob) {
  auto uncompressed = Manager::decompressBlob(compressedBlob, {});
  auto layout = Manager::parseBlobLayout(uncompressed);
  auto reader = makeChunkPayloadReader(uncompressed, layout.metadata_);
  std::string metadataJson;
  reader >> metadataJson;
  return nlohmann::json::parse(metadataJson);
}

// Load the `compressedBlob` into a fresh `Qlever` instance that has NO index
// files on disk at all (constructed with `skipLoading`), and return the result
// of running the `query` on that instance in TSV format.
std::string queryBlobInFreshInstance(ql::span<const char> compressedBlob,
                                     std::string_view query) {
  Qlever target{EngineConfig{}, /*skipLoading=*/true};
  EXPECT_NO_THROW(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob));
  return target.query(std::string{query}, ad_utility::MediaType::tsv);
}

// The result of `serializeFilterTestBlob` below.
struct FilterTestBlob {
  std::vector<char> blob_;
  uint64_t droppedSubjectIndex_ = 0;
  uint64_t droppedObjectIndex_ = 0;
  uint64_t keptSubjectIndex_ = 0;
};

// Open a `Qlever` instance on the index described by `sourceConfig`, pin the
// result of `filterSourceQuery` under `filterPinName`, and serialize the
// vocabulary and the named result cache to a blob from which all entries
// matching `droppedEntriesRegex` are excluded. Also return the original
// vocabulary indices of the entries that the tests below inspect.
FilterTestBlob serializeFilterTestBlob(const IndexBuilderConfig& sourceConfig) {
  Qlever source{EngineConfig{sourceConfig}};
  source.queryAndPinResultWithName(std::string{filterPinName},
                                   std::string{filterSourceQuery});
  FilterTestBlob result;
  result.droppedSubjectIndex_ =
      vocabIndexOfEntryContaining(source, "droppedSubject");
  result.droppedObjectIndex_ =
      vocabIndexOfEntryContaining(source, "dropped literal");
  result.keptSubjectIndex_ = vocabIndexOfEntryContaining(source, "keptSubject");
  result.blob_ = source.serializeVocabAndNamedCacheToCompressedBlob(
      excludeConfig({std::string{droppedEntriesRegex}}));
  return result;
}
}  // namespace

// _____________________________________________________________________________
// Test the compression utility and its inverse in isolation, for several
// buffer sizes (including the empty buffer).
TEST(NamedCachedQueryBlobManager, compressAndDecompressBlob) {
  for (const std::string& original :
       {std::string{}, std::string{"x"}, std::string{"a short blob"},
        std::string(100'000, 'q')}) {
    std::vector<char> compressed =
        Manager::compressBlob(ql::span<const char>{original});
    // The size of the uncompressed data is stored in the ZSTD frame header, so
    // the compressed blob is a plain ZSTD frame without any extra bookkeeping.
    EXPECT_EQ(
        ZstdWrapper::getUncompressedSize(compressed.data(), compressed.size()),
        original.size());

    auto roundTripped = Manager::decompressBlob(compressed, {});
    EXPECT_THAT(roundTripped, ::testing::ElementsAreArray(original));
  }
}

// _____________________________________________________________________________
// Test that `writeBlobHeader` and `skipAndVerifyBlobHeader` mirror each other,
// and that an invalid header is rejected.
TEST(NamedCachedQueryBlobManager, writeAndVerifyBlobHeader) {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  Manager::writeBlobHeader(writer);
  // Append a payload so that we can check the reader is positioned correctly
  // after the header.
  writer << std::string_view{"payload"};
  auto data = std::move(writer).data();

  auto reader = makeBlobReader(data);
  EXPECT_NO_THROW(Manager::skipAndVerifyBlobHeader(reader));
  std::string payload;
  reader >> payload;
  EXPECT_EQ(payload, "payload");

  // A buffer that does not start with the expected magic header is rejected.
  ad_utility::serialization::AlignedByteBufferWriteSerializer wrongWriter;
  wrongWriter << std::array<char, 8>{'X', 'X', 'X', 'X', 'X', 'X', 'X', 'X'};
  wrongWriter << uint16_t{1};
  auto wrongData = std::move(wrongWriter).data();
  auto wrongReader = makeBlobReader(wrongData);
  AD_EXPECT_THROW_WITH_MESSAGE(Manager::skipAndVerifyBlobHeader(wrongReader),
                               HasSubstr("was not written by"));
}

// _____________________________________________________________________________
// Test that a blob with the correct magic bytes but an incompatible format
// version is rejected.
TEST(NamedCachedQueryBlobManager, skipAndVerifyBlobHeaderRejectsWrongVersion) {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  // The correct magic bytes (see `blobMagicBytes`), followed by a format
  // version that is definitely not the current one.
  writer << std::array<char, 8>{'Q', 'L', 'V', 'R', 'B', 'L', 'O', 'B'};
  writer << uint16_t{63999};
  auto data = std::move(writer).data();

  auto reader = makeBlobReader(data);
  AD_EXPECT_THROW_WITH_MESSAGE(Manager::skipAndVerifyBlobHeader(reader),
                               HasSubstr("incompatible version"));
}

// _____________________________________________________________________________
// Test that a blob with the correct magic bytes but a truncated header is
// rejected with our own message, instead of with a cryptic message from the
// serializer.
TEST(NamedCachedQueryBlobManager, skipAndVerifyBlobHeaderRejectsShortInput) {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  writer << std::array<char, 4>{'Q', 'L', 'V', 'R'};
  auto data = std::move(writer).data();

  auto reader = makeBlobReader(data);
  AD_EXPECT_THROW_WITH_MESSAGE(Manager::skipAndVerifyBlobHeader(reader),
                               HasSubstr("was not written by"));
}

// _____________________________________________________________________________
// Test that input which is not a ZSTD frame at all is rejected with our own
// message, rather than with a cryptic ZSTD error, and that in particular no
// attempt is made to allocate a buffer of an arbitrary size read from garbage.
TEST(NamedCachedQueryBlobManager, decompressBlobRejectsNonZstdInput) {
  // Input that is too short to even hold a ZSTD frame header.
  std::vector<char> tooShort(3, 'x');
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(Manager::decompressBlob(tooShort, {}),
                                        HasSubstr("was not written by"),
                                        ad_utility::Exception);

  // Longer input that does not start with the ZSTD magic number. Note that
  // interpreting any eight of its bytes as the size of the uncompressed data
  // would yield about 18 exabytes.
  std::vector<char> garbage(1024, '\xFF');
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(Manager::decompressBlob(garbage, {}),
                                        HasSubstr("was not written by"),
                                        ad_utility::Exception);
}

// _____________________________________________________________________________
// Test that a truncated blob (the typical result of an incomplete download) is
// rejected with our own message, rather than with a cryptic ZSTD error. Note
// that the frame header of such a blob is intact, so the size of the
// uncompressed data can be read, and the failure only occurs during the actual
// decompression.
TEST(NamedCachedQueryBlobManager, decompressBlobRejectsTruncatedInput) {
  const std::string original(10'000, 'q');
  std::vector<char> compressed =
      Manager::compressBlob(ql::span<const char>{original});
  ASSERT_GT(compressed.size(), 1u);
  EXPECT_EQ(ZstdWrapper::getUncompressedSize(compressed.data(),
                                             compressed.size() - 1),
            original.size());

  compressed.pop_back();
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(Manager::decompressBlob(compressed, {}),
                                        HasSubstr("was not written by"),
                                        ad_utility::Exception);
}

// _____________________________________________________________________________
// Test that a blob (index metadata + vocabulary + named result cache), written
// from one `Qlever` instance, can be loaded into a completely separate instance
// that has NO index files on disk at all (constructed with `skipLoading`), and
// there produce correct query results without loading any permutations.
TEST(NamedCachedQueryBlobManager, combinedBlob) {
  IndexBuilderConfig sourceConfig = buildTestIndex(
      "<combinedBlobSubject> <combinedBlobPredicate> "
      "\"combined blob literal\".");

  const std::vector<char> compressedBlob = [&sourceConfig]() {
    Qlever source{EngineConfig{sourceConfig}};
    source.queryAndPinResultWithName(
        "blobPin", "SELECT ?s ?o WHERE { ?s <combinedBlobPredicate> ?o }");
    auto blob = source.serializeVocabAndNamedCacheToCompressedBlob();
    EXPECT_FALSE(blob.empty());
    return blob;
  }();

  // A completely fresh instance with NO index files on disk (`skipLoading`);
  // everything needed to answer the cached-result query comes from the blob.
  Qlever target{EngineConfig{}, /*skipLoading=*/true};

  // Before loading the blob, the named result cache is empty.
  std::string cachedResultQuery =
      "SELECT ?s ?o WHERE { SERVICE ql:cached-result-with-name-blobPin {}}";
  AD_EXPECT_THROW_WITH_MESSAGE(
      target.query(cachedResultQuery, ad_utility::MediaType::tsv),
      HasSubstr("is not contained in the named result cache"));

  EXPECT_NO_THROW(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob));

  // The named cached result, and the vocabulary needed to correctly export
  // its IDs as strings, now come entirely from the blob.
  auto res = target.query(cachedResultQuery, ad_utility::MediaType::tsv);
  EXPECT_EQ(res, "?s\t?o\n<combinedBlobSubject>\t\"combined blob literal\"\n");

  // Permutations are not part of the blob, so a query that needs them (i.e.
  // any query with actual triples) is unsupported on a blob-only instance and
  // throws.
  //
  // NOTE: The exception thrown in this case (no index loaded, but the query
  // requires one) is currently an internal `AD_CORRECTNESS_CHECK` (an
  // `ad_utility::Exception`) rather than a user-facing error message. We accept
  // this for now, as turning it into a graceful error would require changes to
  // a lot of code paths.
  EXPECT_THROW(target.query("SELECT ?s WHERE { ?s <combinedBlobPredicate> ?o }",
                            ad_utility::MediaType::tsv),
               ad_utility::Exception);

  // Loading a second blob on the same instance must throw.
  AD_EXPECT_THROW_WITH_MESSAGE(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob),
      HasSubstr("must not be called more than once"));
}

// _____________________________________________________________________________
// Test that the `allocator` passed to
// `deserializeVocabAndNamedCacheFromCompressedBlob` is in fact used to allocate
// the (large) decompressed blob buffer.
TEST(NamedCachedQueryBlobManager, blobUsesProvidedAllocator) {
  IndexBuilderConfig sourceConfig = buildTestIndex(
      "<allocatorBlobSubject> <allocatorBlobPredicate> "
      "\"allocator blob literal\".");

  const std::vector<char> compressedBlob = [&sourceConfig]() {
    Qlever source{EngineConfig{sourceConfig}};
    source.queryAndPinResultWithName(
        "blobPin", "SELECT ?s ?o WHERE { ?s <allocatorBlobPredicate> ?o }");
    return source.serializeVocabAndNamedCacheToCompressedBlob();
  }();

  CountingMemoryResource resource;
  Qlever target{EngineConfig{}, /*skipLoading=*/true};
  EXPECT_NO_THROW(target.deserializeVocabAndNamedCacheFromCompressedBlob(
      compressedBlob, ql::pmr::polymorphic_allocator<char>{&resource}));

  // The decompressed blob buffer must have been allocated via `resource`.
  EXPECT_GT(resource.numAllocations_, 0u);
  EXPECT_GT(resource.bytesAllocated_, 0u);

  // The instance still answers queries correctly from the resource-backed
  // buffer.
  auto res = target.query(
      "SELECT ?s ?o WHERE { SERVICE ql:cached-result-with-name-blobPin {}}",
      ad_utility::MediaType::tsv);
  EXPECT_EQ(res,
            "?s\t?o\n<allocatorBlobSubject>\t\"allocator blob literal\"\n");
}

// _____________________________________________________________________________
// Test that loading a blob that does not carry a valid header is rejected.
TEST(NamedCachedQueryBlobManager, deserializeRejectsInvalidBlob) {
  // A validly ZSTD-compressed blob whose decompressed content does not start
  // with the expected magic header.
  std::vector<char> bogus(64, 'X');
  std::vector<char> compressedBlob = Manager::compressBlob(bogus);

  Qlever target{EngineConfig{}, /*skipLoading=*/true};
  AD_EXPECT_THROW_WITH_MESSAGE(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob),
      HasSubstr("was not written by"));
}

// _____________________________________________________________________________
// Test that a blob with a valid header, but with contents that cannot be read,
// is rejected with our own message, rather than with a cryptic message from
// deep inside the deserialization.
TEST(NamedCachedQueryBlobManager, deserializeRejectsBlobWithInvalidContents) {
  // A blob that consists of nothing but a valid header, so that reading the
  // index metadata JSON that is expected to follow it fails.
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  Manager::writeBlobHeader(writer);
  auto headerOnly = std::move(writer).data();
  std::vector<char> compressedBlob =
      Manager::compressBlob(ql::span<const char>{headerOnly});

  Qlever target{EngineConfig{}, /*skipLoading=*/true};
  AD_EXPECT_THROW_WITH_MESSAGE(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob),
      HasSubstr("Error while reading the contents of a blob"));
}

// _____________________________________________________________________________
// End-to-end test for a blob that carries a spatial (s2) index in its named
// result cache: build an index (with in-memory vocabulary), pin a query result
// together with a cached geometry index, serialize everything to a blob, load
// it into a fresh instance that has NO index files on disk, and run a spatial
// join that uses the cached geometry index from the blob.
TEST(NamedCachedQueryBlobManager, blobWithSpatialIndex) {
  // Four rail segments (linestrings) that are pinned as a cached s2 geometry
  // index. The query point used below lies within 1 km of all four segments
  // (see `SpatialJoinCachedIndexTest`).
  IndexBuilderConfig sourceConfig = buildTestIndex(
      "<s1> <asWKT> \"LINESTRING(7.8428469 47.9995367,7.8413293 "
      "47.9974942)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<s2> <asWKT> \"LINESTRING(7.8409068 47.9975041,7.8420114 "
      "47.9989233)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<s3> <asWKT> \"LINESTRING(7.8427369 47.9995806,7.8411672 "
      "47.9975175)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n"
      "<s4> <asWKT> \"LINESTRING(7.8422376 47.9990144,7.8411016 "
      "47.9975307)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .\n");

  const std::vector<char> compressedBlob = [&sourceConfig]() {
    Qlever source{EngineConfig{sourceConfig}};
    // Pin the linestrings together with a cached s2 geometry index on `?geo2`.
    source.queryAndPinResultWithName(
        QueryExecutionContext::PinResultWithName{"geoPin", Variable{"?geo2"}},
        "SELECT * { ?s2 <asWKT> ?geo2 }");
    auto blob = source.serializeVocabAndNamedCacheToCompressedBlob();
    EXPECT_FALSE(blob.empty());
    return blob;
  }();

  // A spatial join whose right side is the cached geometry index (from the
  // blob) and whose left side is a single point provided inline via `VALUES`,
  // so that no permutations (and hence no on-disk index) are needed.
  std::string spatialQuery =
      "PREFIX qlss: <https://qlever.cs.uni-freiburg.de/spatialSearch/> "
      "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "
      "SELECT ?s2 WHERE { "
      "VALUES ?geo1 { \"POINT(7.841295 47.997731)\"^^geo:wktLiteral } "
      "SERVICE qlss: { "
      "_:config qlss:right ?geo2 ; "
      "qlss:left ?geo1 ; "
      "qlss:maxDistance 1000 ; "
      "qlss:algorithm qlss:experimentalPointPolyline ; "
      "qlss:experimentalRightCacheName \"geoPin\" . "
      "} }";

  // A fresh instance with no index files on disk. Before loading the blob the
  // cached geometry index does not exist, so the spatial query fails.
  Qlever target{EngineConfig{}, /*skipLoading=*/true};
  AD_EXPECT_THROW_WITH_MESSAGE(
      target.query(spatialQuery, ad_utility::MediaType::tsv),
      HasSubstr("is not contained in the named result cache"));

  // After loading the blob, the cached geometry index comes from the blob and
  // the spatial join succeeds, relating the query point to all four segments.
  EXPECT_NO_THROW(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob));
  auto res = target.query(spatialQuery, ad_utility::MediaType::tsv);
  EXPECT_THAT(res, HasSubstr("<s1>"));
  EXPECT_THAT(res, HasSubstr("<s2>"));
  EXPECT_THAT(res, HasSubstr("<s3>"));
  EXPECT_THAT(res, HasSubstr("<s4>"));

  // The pinned result itself is also queryable directly from the blob.
  auto cachedRes = target.query(
      "SELECT ?s2 ?geo2 WHERE { SERVICE ql:cached-result-with-name-geoPin {} }",
      ad_utility::MediaType::tsv);
  EXPECT_THAT(cachedRes, HasSubstr("<s1>"));
}

// A test suite for the round trip of a blob from which some of the vocabulary
// entries were excluded, parameterized by the vocabulary type of the source
// index and the vocabulary type that the resulting blob is expected to have.
class BlobFilterTest
    : public testing::TestWithParam<std::pair<VocabularyType, VocabularyType>> {
};

// _____________________________________________________________________________
// Test a round trip of a blob from which some of the vocabulary entries were
// excluded, for each of the source vocabulary types that supports the
// filtering. The pinned query result references both a kept and an excluded
// entry, so that the preservation of the original vocabulary indices is
// actually exercised: the kept entries have to resolve to their original
// strings, the excluded ones to `placeholderForMissingVocabIndex`.
TEST_P(BlobFilterTest, blobWithExcludedVocabularyEntries) {
  const auto& [sourceType, expectedBlobType] = GetParam();
  auto sourceConfig = buildTestIndex(filterTestData, sourceType);
  auto [compressedBlob, droppedSubjectIndex, droppedObjectIndex,
        keptSubjectIndex] = serializeFilterTestBlob(sourceConfig);

  // The type of the vocabulary in the blob is recorded in its metadata JSON,
  // so that the reading side does not need to know about the filtering.
  EXPECT_EQ(metadataFromBlob(compressedBlob)["vocabulary-type"],
            expectedBlobType.toString());

  auto result = queryBlobInFreshInstance(compressedBlob, filterPinQuery);
  // The kept entries resolve to their original strings, and in particular the
  // kept subject kept its original vocabulary index (which is larger than the
  // index of the dropped subject, so the surviving vocabulary has a hole).
  EXPECT_GT(keptSubjectIndex, droppedSubjectIndex);
  EXPECT_THAT(result, HasSubstr("<keptSubject>\t\"kept literal\""));
  // The excluded entries resolve to the placeholder for their original index.
  EXPECT_THAT(result,
              HasSubstr(ad_utility::vocabulary::placeholderForMissingVocabIndex(
                  droppedSubjectIndex)));
  EXPECT_THAT(result,
              HasSubstr(ad_utility::vocabulary::placeholderForMissingVocabIndex(
                  droppedObjectIndex)));
}

INSTANTIATE_TEST_SUITE_P(
    VocabularyTypes, BlobFilterTest,
    testing::ValuesIn(std::vector<std::pair<VocabularyType, VocabularyType>>{
        {VocabularyType::InMemoryUncompressed,
         VocabularyType::InMemoryUncompressedWithHoles},
        {VocabularyType::OnDiskUncompressed,
         VocabularyType::InMemoryUncompressedWithHoles},
        {VocabularyType::InMemoryCompressed,
         VocabularyType::InMemoryCompressedWithHoles},
        {VocabularyType::OnDiskCompressed,
         VocabularyType::InMemoryCompressedWithHoles}}),
    [](const testing::TestParamInfo<BlobFilterTest::ParamType>& info) {
      // A gtest test name may only consist of alphanumeric characters and
      // underscores, so the `-` of the string representation of the vocabulary
      // type has to be replaced.
      return absl::StrReplaceAll(info.param.first.toString(), {{"-", "_"}});
    });

// _____________________________________________________________________________
// Test that a blob written with a list of regexes that matches no vocabulary
// entry at all is functionally equivalent to (though not byte-identical with)
// the unfiltered blob, and that an empty list of regexes produces a blob in the
// original format (i.e. with the original vocabulary type).
TEST(NamedCachedQueryBlobManager, blobWithRegexesThatMatchNothing) {
  auto sourceConfig = buildTestIndex(filterTestData);

  std::vector<char> unfilteredBlob;
  std::vector<char> filteredBlob;
  {
    Qlever source{EngineConfig{sourceConfig}};
    source.queryAndPinResultWithName(std::string{filterPinName},
                                     std::string{filterSourceQuery});
    unfilteredBlob = source.serializeVocabAndNamedCacheToCompressedBlob();
    filteredBlob = source.serializeVocabAndNamedCacheToCompressedBlob(
        excludeConfig({"thisMatchesNoVocabularyEntry"}));
  }

  // An empty list of regexes leaves the format (and hence the vocabulary type
  // in the metadata JSON) untouched, a non-empty list switches to a vocabulary
  // with holes, so the two blobs are not byte-identical.
  EXPECT_EQ(metadataFromBlob(unfilteredBlob)["vocabulary-type"],
            ad_utility::VocabularyType::InMemoryUncompressed.toString());
  EXPECT_EQ(
      metadataFromBlob(filteredBlob)["vocabulary-type"],
      ad_utility::VocabularyType::InMemoryUncompressedWithHoles.toString());
  EXPECT_NE(unfilteredBlob, filteredBlob);

  // Both blobs are functionally equivalent: no entry was excluded, so all
  // strings resolve to their original values.
  std::string expected =
      "?s\t?o\n<droppedSubject>\t\"dropped literal\"\n<keptSubject>\t\"kept "
      "literal\"\n";
  for (const std::vector<char>& blob : {unfilteredBlob, filteredBlob}) {
    EXPECT_EQ(queryBlobInFreshInstance(blob, filterPinQuery), expected);
  }
}

// _____________________________________________________________________________
// Test that excluding vocabulary entries from a blob is rejected with a
// descriptive message if the source vocabulary is a geo-split vocabulary (whose
// marker-encoded indices cannot be represented by a vocabulary with holes).
TEST(NamedCachedQueryBlobManager, blobWithExcludedEntriesRejectsGeoSplitVocab) {
  auto sourceConfig =
      buildTestIndex(filterTestData, VocabularyType::OnDiskCompressedGeoSplit);

  Qlever source{EngineConfig{sourceConfig}};
  source.queryAndPinResultWithName(std::string{filterPinName},
                                   std::string{filterSourceQuery});
  AD_EXPECT_THROW_WITH_MESSAGE(
      source.serializeVocabAndNamedCacheToCompressedBlob(
          excludeConfig({std::string{droppedEntriesRegex}})),
      HasSubstr("on-disk-compressed-geo-split"));
}

// `applyUpdateToEngine` (used below) is defined in `QleverTestHelpers.h`; see
// the comment there for why the update has to be parsed separately and for
// the thread-safety caveat of taking the snapshot only here.
using ad_utility::testing::applyUpdateToEngine;

namespace {
// The media type in which the tests below export their query results.
constexpr ad_utility::MediaType tsv = ad_utility::MediaType::tsv;

// The turtle data used by the tests below, which write a blob or a diff after
// an update and inspect the chunk layout of a blob.
constexpr std::string_view updateTestData =
    "<s1> <p1> \"l1\".\n"
    "<s2> <p1> \"l2\".\n"
    "<s1> <p2> <o1>.";

// The queries whose results the tests below pin (under the names `q1`, `q2`,
// and `q3`), and the corresponding queries that return those pinned results.
// NOTE: `q3` returns the same triples as `q2`, but binds the object to a
// different variable, so that a join of `q1` and `q3` joins on the subject
// alone.
constexpr std::string_view sourceQuery1 = "SELECT ?s ?o WHERE { ?s <p1> ?o }";
constexpr std::string_view sourceQuery2 = "SELECT ?s ?o WHERE { ?s <p2> ?o }";
constexpr std::string_view sourceQuery3 = "SELECT ?s ?o2 WHERE { ?s <p2> ?o2 }";
constexpr std::string_view cachedQuery1 =
    "SELECT ?s ?o WHERE { SERVICE ql:cached-result-with-name-q1 {} }";
constexpr std::string_view cachedQuery2 =
    "SELECT ?s ?o WHERE { SERVICE ql:cached-result-with-name-q2 {} }";

// Pin the results of `sourceQuery1` and `sourceQuery2` under the names `q1` and
// `q2`. Note that the pinning has to be repeated after every update, because
// the results (and not only the query result cache) change.
void pinSourceQueries(Qlever& engine) {
  engine.queryAndPinResultWithName("q1", std::string{sourceQuery1});
  engine.queryAndPinResultWithName("q2", std::string{sourceQuery2});
}

// Open a `Qlever` instance on the index described by `builderConfig`, with the
// persisting of updates switched off (the tests apply updates in memory only).
Qlever makeSourceEngine(const IndexBuilderConfig& builderConfig) {
  EngineConfig config{builderConfig};
  config.persistUpdates_ = false;
  return Qlever{config};
}

// Build the test index (`updateTestData`), open a `Qlever` instance on it via
// `makeSourceEngine`, and pin the queries via `pinSourceQueries`. This is the
// setup that most of the tests below need before they apply their own updates
// and (re-)pin queries.
Qlever makePinnedSourceEngine() {
  auto builderConfig = buildTestIndex(updateTestData);
  Qlever source = makeSourceEngine(builderConfig);
  pinSourceQueries(source);
  return source;
}

// Run the two cached queries (see `cachedQuery1`, `cachedQuery2`) on `engine`
// and return their results in TSV format.
std::pair<std::string, std::string> cachedResultsOf(Qlever& engine) {
  return {engine.query(std::string{cachedQuery1}, tsv),
          engine.query(std::string{cachedQuery2}, tsv)};
}

// Load `blob`, with the `diffs` applied to it in the given order, into a fresh
// `Qlever` instance that has NO index files on disk at all (constructed with
// `skipLoading`).
std::unique_ptr<Qlever> loadBlob(
    ql::span<const char> blob,
    const std::vector<ql::span<const char>>& diffs = {}) {
  auto target = std::make_unique<Qlever>(EngineConfig{}, /*skipLoading=*/true);
  target->deserializeVocabAndNamedCacheFromCompressedBlob(
      blob, ql::span<const ql::span<const char>>{diffs});
  return target;
}

// Return the secondary vocabulary of the index of `engine`, which must have
// one.
const SecondaryVocabulary& secondaryVocabularyOf(const Qlever& engine) {
  const auto* secondaryVocabulary =
      engine.indexAndViewsSnapshot()->index_.getImpl().secondaryVocab();
  AD_CONTRACT_CHECK(secondaryVocabulary != nullptr);
  return *secondaryVocabulary;
}
}  // namespace

// _____________________________________________________________________________
// End-to-end test of the diff mechanism: pin two queries, write a base blob,
// apply an update that inserts triples with words that are not part of the
// vocabulary of the index (and that deletes one of the triples of the first
// query), re-pin the very same queries, and write a diff against the base blob.
// Applying that diff has to yield a blob that produces exactly the same results
// as the source engine.
TEST(NamedCachedQueryBlobManager, diffAfterUpdate) {
  Qlever source = makePinnedSourceEngine();
  std::vector<char> base = source.serializeVocabAndNamedCacheToCompressedBlob();

  applyUpdateToEngine(source,
                      "INSERT DATA { <newSubject> <p1> \"new literal\" }");
  applyUpdateToEngine(source, "DELETE DATA { <s2> <p1> \"l2\" }");
  pinSourceQueries(source);
  std::vector<char> diff =
      source.serializeVocabAndNamedCacheDiffToCompressedBlob(base);

  // The unchanged parts (in particular the vocabulary) are copied from the base
  // blob, the changed ones are inserted.
  auto statistics = Manager::describeDiff(diff);
  EXPECT_GE(statistics.numCopyInstructions_, 1u);
  EXPECT_GE(statistics.numInsertInstructions_, 1u);
  EXPECT_GT(statistics.numCopiedBytes_, 0u);
  EXPECT_GT(statistics.numInsertedBytes_, 0u);

  // The patched blob is a complete blob again, and loading it into a fresh
  // instance with no index files on disk reproduces the results of the source
  // engine, including the word that only exists in the secondary vocabulary of
  // the blob.
  std::vector<char> patched = Qlever::applyDiffToCompressedBlob(base, diff);
  auto expected = cachedResultsOf(source);
  EXPECT_EQ(expected.first,
            "?s\t?o\n<newSubject>\t\"new literal\"\n<s1>\t\"l1\"\n");
  EXPECT_EQ(expected.second, "?s\t?o\n<s1>\t<o1>\n");

  auto target = loadBlob(patched);
  EXPECT_EQ(cachedResultsOf(*target), expected);
  EXPECT_EQ(secondaryVocabularyOf(*target).numSegments(), 1u);

  // A `FILTER` on the new IRI works in the target, i.e. the `Id` of the
  // secondary vocabulary is found for the IRI of the query.
  EXPECT_EQ(target->query("SELECT ?s ?o WHERE { SERVICE "
                          "ql:cached-result-with-name-q1 {} FILTER(?s = "
                          "<newSubject>) }",
                          tsv),
            "?s\t?o\n<newSubject>\t\"new literal\"\n");

  // Applying the diff while loading the base blob is equivalent to loading the
  // patched blob.
  auto targetFromDiff = loadBlob(base, {diff});
  EXPECT_EQ(cachedResultsOf(*targetFromDiff), expected);
}

// _____________________________________________________________________________
// Test that diffs chain: a second update yields a diff against the blob that
// resulted from the first diff, and the `Id`s that the first diff assigned to
// the new words stay valid.
TEST(NamedCachedQueryBlobManager, incrementalDiffs) {
  Qlever source = makePinnedSourceEngine();
  source.queryAndPinResultWithName("q3", std::string{sourceQuery3});
  std::vector<char> base = source.serializeVocabAndNamedCacheToCompressedBlob();

  auto rePinAll = [&source]() {
    pinSourceQueries(source);
    source.queryAndPinResultWithName("q3", std::string{sourceQuery3});
  };

  applyUpdateToEngine(source,
                      "INSERT DATA { <newSubject> <p1> \"new literal\" }");
  rePinAll();
  std::vector<char> diff1 =
      source.serializeVocabAndNamedCacheDiffToCompressedBlob(base);
  std::vector<char> patched1 = Qlever::applyDiffToCompressedBlob(base, diff1);

  // The second update adds another new word, and reuses the new IRI of the
  // first update in a triple of the second query.
  applyUpdateToEngine(source,
                      "INSERT DATA { <newSubject> <p2> <anotherNewObject> }");
  rePinAll();
  std::vector<char> diff2 =
      source.serializeVocabAndNamedCacheDiffToCompressedBlob(patched1);
  std::vector<char> patched2 =
      Qlever::applyDiffToCompressedBlob(patched1, diff2);

  auto expected = cachedResultsOf(source);
  auto target1 = loadBlob(patched1);
  auto target2 = loadBlob(patched2);
  EXPECT_EQ(cachedResultsOf(*target2), expected);
  EXPECT_EQ(target2->query(std::string{cachedQuery2}, tsv),
            "?s\t?o\n<newSubject>\t<anotherNewObject>\n<s1>\t<o1>\n");

  // The second diff appended a second segment to the secondary vocabulary, and
  // the word of the first segment kept its `Id`.
  EXPECT_EQ(secondaryVocabularyOf(*target1).numSegments(), 1u);
  EXPECT_EQ(secondaryVocabularyOf(*target2).numSegments(), 2u);
  auto idInTarget1 = secondaryVocabularyOf(*target1).getId("<newSubject>");
  auto idInTarget2 = secondaryVocabularyOf(*target2).getId("<newSubject>");
  ASSERT_TRUE(idInTarget1.has_value());
  ASSERT_TRUE(idInTarget2.has_value());
  EXPECT_EQ(idInTarget1.value().get(), idInTarget2.value().get());

  // A join of two cached results on the new IRI, which only works if that IRI
  // is represented by the very same `Id` in both of them.
  EXPECT_EQ(target2->query("SELECT ?s ?o ?o2 WHERE { SERVICE "
                           "ql:cached-result-with-name-q1 {} SERVICE "
                           "ql:cached-result-with-name-q3 {} }",
                           tsv),
            "?s\t?o\t?o2\n<s1>\t\"l1\"\t<o1>\n<newSubject>\t\"new "
            "literal\"\t<anotherNewObject>\n");

  // Both diffs can also be applied while loading the base blob.
  auto targetFromDiffs = loadBlob(base, {diff1, diff2});
  EXPECT_EQ(cachedResultsOf(*targetFromDiffs), expected);
}

// _____________________________________________________________________________
// Test that a complete blob can be written even after an update that introduced
// new words, which previously threw ("cannot be serialized") because those
// words only existed as local vocab entries. They are now stored in the
// secondary vocabulary of the blob, which the loaded instance picks up.
TEST(NamedCachedQueryBlobManager, fullBlobAfterUpdate) {
  auto builderConfig = buildTestIndex(updateTestData);
  Qlever source = makeSourceEngine(builderConfig);
  applyUpdateToEngine(source,
                      "INSERT DATA { <newSubject> <p1> \"new literal\" }");
  pinSourceQueries(source);

  std::vector<char> blob;
  EXPECT_NO_THROW(blob = source.serializeVocabAndNamedCacheToCompressedBlob());
  auto target = loadBlob(blob);
  EXPECT_EQ(cachedResultsOf(*target), cachedResultsOf(source));
  EXPECT_EQ(secondaryVocabularyOf(*target).numSegments(), 1u);
}

// _____________________________________________________________________________
// Test that a word which an update introduced (and which hence is not part of
// the vocabulary of the index) is found in the secondary vocabulary of the
// loaded blob, so that a `FILTER` on that word inside a cached result works,
// i.e. the `Id` of the secondary vocabulary is found for the IRI of the query.
TEST(NamedCachedQueryBlobManager, newWordsInSecondaryVocabularyOfBlob) {
  auto builderConfig = buildTestIndex(updateTestData);
  Qlever source = makeSourceEngine(builderConfig);
  applyUpdateToEngine(source,
                      "INSERT DATA { <newSubject> <p1> \"new literal\" }");
  pinSourceQueries(source);
  std::vector<char> blob = source.serializeVocabAndNamedCacheToCompressedBlob();

  auto target = loadBlob(blob);
  EXPECT_TRUE(secondaryVocabularyOf(*target).getId("<newSubject>").has_value());
  EXPECT_EQ(target->query("SELECT ?s ?o WHERE { SERVICE "
                          "ql:cached-result-with-name-q1 {} FILTER(?s = "
                          "<newSubject>) }",
                          tsv),
            "?s\t?o\n<newSubject>\t\"new literal\"\n");
}

// _____________________________________________________________________________
// Test that a diff which is applied to a base blob other than the one it was
// created against is rejected, and that input which is not a diff at all is
// rejected with our own message.
TEST(NamedCachedQueryBlobManager, applyDiffRejectsWrongInput) {
  auto builderConfig = buildTestIndex(updateTestData);
  auto otherBuilderConfig =
      buildTestIndex("<otherSubject> <otherPredicate> \"other literal\".",
                     VocabularyType::InMemoryUncompressed, "other");

  std::vector<char> base;
  std::vector<char> diff;
  {
    Qlever source = makeSourceEngine(builderConfig);
    pinSourceQueries(source);
    base = source.serializeVocabAndNamedCacheToCompressedBlob();
    applyUpdateToEngine(source,
                        "INSERT DATA { <newSubject> <p1> \"new literal\" }");
    pinSourceQueries(source);
    diff = source.serializeVocabAndNamedCacheDiffToCompressedBlob(base);
  }

  // A blob of a completely different index, which the diff was not created
  // against.
  std::vector<char> otherBase;
  {
    Qlever other = makeSourceEngine(otherBuilderConfig);
    other.queryAndPinResultWithName(
        "q1", "SELECT ?s ?o WHERE { ?s <otherPredicate> ?o }");
    otherBase = other.serializeVocabAndNamedCacheToCompressedBlob();
  }
  AD_EXPECT_THROW_WITH_MESSAGE(
      Qlever::applyDiffToCompressedBlob(otherBase, diff),
      HasSubstr("checksum of the base does not match"));

  // Garbage is not a diff at all.
  std::vector<char> garbage(1024, '\xFF');
  AD_EXPECT_THROW_WITH_MESSAGE(Qlever::applyDiffToCompressedBlob(base, garbage),
                               HasSubstr("The given diff was not written by"));
  AD_EXPECT_THROW_WITH_MESSAGE(Manager::describeDiff(garbage),
                               HasSubstr("The given diff was not written by"));
}

// _____________________________________________________________________________
// Test that neither a blob nor a diff can be created from an index that already
// has a secondary vocabulary of its own (which in particular is the case for an
// instance that was itself loaded from a blob that contained new words).
TEST(NamedCachedQueryBlobManager, serializeRejectsSecondaryVocabulary) {
  Qlever source = makePinnedSourceEngine();
  std::vector<char> base = source.serializeVocabAndNamedCacheToCompressedBlob();

  auto snapshot = source.indexAndViewsSnapshot();
  snapshot->index_.getImpl().setSecondaryVocab(
      std::make_shared<const SecondaryVocabulary>(
          std::vector<std::string>{"<aWordThatIsNotInTheIndex>"}));

  AD_EXPECT_THROW_WITH_MESSAGE(
      source.serializeVocabAndNamedCacheToCompressedBlob(),
      HasSubstr("without a secondary vocabulary"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      source.serializeVocabAndNamedCacheDiffToCompressedBlob(base),
      HasSubstr("without a secondary vocabulary"));
}

// _____________________________________________________________________________
// Whitebox test of the chunk layout of a blob: the chunks are found in the
// expected order, and every one of them begins at a multiple of the alignment
// that makes them position independent (see
// `NamedCachedQueryBlobManager::parseBlobLayout`).
TEST(NamedCachedQueryBlobManager, parseBlobLayoutOfFullBlob) {
  auto builderConfig = buildTestIndex(updateTestData);
  std::vector<char> blob;
  {
    Qlever source = makeSourceEngine(builderConfig);
    pinSourceQueries(source);
    blob = source.serializeVocabAndNamedCacheToCompressedBlob();
  }
  auto uncompressed = Manager::decompressBlob(blob, {});
  auto layout = Manager::parseBlobLayout(uncompressed);

  // No update was applied, so all words are contained in the vocabulary of the
  // index and the secondary vocabulary of the blob is empty.
  EXPECT_THAT(layout.segments_, IsEmpty());
  std::vector<std::string> keys;
  for (const auto& entry : layout.entries_) {
    keys.push_back(entry.key_);
  }
  EXPECT_THAT(keys, ElementsAre("q1", "q2"));

  std::vector<Manager::BlobLayout::Region> regions{
      layout.metadata_, layout.vocabulary_, layout.segmentCount_,
      layout.entryCount_};
  for (const auto& entry : layout.entries_) {
    regions.push_back(entry.region_);
  }
  size_t previousEnd = 0;
  for (const Manager::BlobLayout::Region& region : regions) {
    EXPECT_EQ(region.begin_ % alignof(std::max_align_t), 0u);
    EXPECT_EQ(region.payloadBegin() % alignof(std::max_align_t), 0u);
    EXPECT_GT(region.size(), Manager::BlobLayout::chunkHeaderSize);
    EXPECT_GE(region.begin_, previousEnd);
    previousEnd = region.end_;
  }
  EXPECT_LE(previousEnd, uncompressed.size());
}

// _____________________________________________________________________________
// Test that a diff against a base blob whose contents did not change at all
// reproduces exactly the bytes of that base blob. This is what the position
// independence of the chunks is about (see
// `NamedCachedQueryBlobManager::parseBlobLayout`): the complete blob is
// assembled from instructions that copy the chunks of the base blob to
// (possibly) different offsets.
TEST(NamedCachedQueryBlobManager, diffWithoutChanges) {
  Qlever source = makePinnedSourceEngine();
  std::vector<char> base = source.serializeVocabAndNamedCacheToCompressedBlob();

  // Re-pin the very same queries, without any update in between.
  pinSourceQueries(source);
  std::vector<char> diff =
      source.serializeVocabAndNamedCacheDiffToCompressedBlob(base);

  // Only the header is inserted; all chunks are copied from the base blob, and
  // because they are contiguous there, those copies are merged into a single
  // instruction.
  auto statistics = Manager::describeDiff(diff);
  EXPECT_EQ(statistics.numInsertInstructions_, 1u);
  EXPECT_EQ(statistics.numCopyInstructions_, 1u);
  EXPECT_EQ(statistics.numInsertedBytes_, 10u);

  auto uncompressedBase = Manager::decompressBlob(base, {});
  auto uncompressedPatched = Manager::decompressBlob(
      Qlever::applyDiffToCompressedBlob(base, diff), {});
  EXPECT_THAT(uncompressedPatched, ElementsAreArray(uncompressedBase));
}
