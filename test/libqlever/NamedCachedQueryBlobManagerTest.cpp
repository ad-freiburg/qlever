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
#include <gmock/gmock.h>

#include <string_view>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "backports/memory_resource.h"
#include "backports/span.h"
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

// The turtle data used by the tests for the filtered vocabulary export below:
// one triple whose subject and object survive the filtering, and one whose
// subject and object are excluded from the blob (they contain `dropped`, which
// is what the regexes below match).
constexpr std::string_view filterTestData =
    "<keptSubject> <filterPredicate> \"kept literal\".\n"
    "<droppedSubject> <filterPredicate> \"dropped literal\".";

// The name under which the tests below pin the query result that contains both
// a kept and an excluded vocabulary entry.
constexpr std::string_view filterPinName = "filterPin";

// The query that returns the pinned result (see `filterPinName`) of a blob.
constexpr std::string_view filterPinQuery =
    "SELECT ?s ?o WHERE { SERVICE ql:cached-result-with-name-filterPin {}}";

// Build a small index (see `filterTestData`) with the given vocabulary `type`
// at the `basename`, and return the corresponding `IndexBuilderConfig`. The
// turtle input file is deleted again immediately after the index was built.
IndexBuilderConfig buildFilterTestIndex(const std::string& basename,
                                        ad_utility::VocabularyType type) {
  std::string sourceFilename = absl::StrCat(basename, ".ttl");
  {
    auto ofs = ad_utility::makeOfstream(sourceFilename);
    ofs << filterTestData;
  }
  absl::Cleanup cleanup = [&sourceFilename] {
    ad_utility::deleteFile(sourceFilename);
  };
  IndexBuilderConfig config;
  config.inputFiles_.push_back(
      {sourceFilename, Filetype::Turtle, std::nullopt});
  config.baseName_ = basename;
  config.vocabType_ = type;
  Qlever::buildIndex(config);
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

// Return the index metadata JSON that is stored at the beginning of the
// `compressedBlob` (see `NamedCachedQueryBlobManager::serialize`).
nlohmann::json metadataFromBlob(ql::span<const char> compressedBlob) {
  auto uncompressed = Manager::decompressBlob(compressedBlob, {});
  ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                       ql::span<const char>>
      reader{ql::span<const char>{uncompressed}};
  Manager::skipAndVerifyBlobHeader(reader);
  std::string metadataJson;
  reader >> metadataJson;
  return nlohmann::json::parse(metadataJson);
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

  ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                       ql::span<const char>>
      reader{ql::span<const char>{data}};
  EXPECT_NO_THROW(Manager::skipAndVerifyBlobHeader(reader));
  std::string payload;
  reader >> payload;
  EXPECT_EQ(payload, "payload");

  // A buffer that does not start with the expected magic header is rejected.
  ad_utility::serialization::AlignedByteBufferWriteSerializer wrongWriter;
  wrongWriter << std::array<char, 8>{'X', 'X', 'X', 'X', 'X', 'X', 'X', 'X'};
  wrongWriter << uint16_t{1};
  auto wrongData = std::move(wrongWriter).data();
  ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                       ql::span<const char>>
      wrongReader{ql::span<const char>{wrongData}};
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

  ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                       ql::span<const char>>
      reader{ql::span<const char>{data}};
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

  ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                       ql::span<const char>>
      reader{ql::span<const char>{data}};
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
  std::string basename = gtestCurrentTestName();
  std::string sourceFilename = basename + ".ttl";
  {
    auto ofs = ad_utility::makeOfstream(sourceFilename);
    ofs << "<combinedBlobSubject> <combinedBlobPredicate> "
           "\"combined blob literal\".";
  }
  absl::Cleanup cleanup = [&sourceFilename] {
    ad_utility::deleteFile(sourceFilename);
  };
  IndexBuilderConfig sourceConfig;
  sourceConfig.inputFiles_.push_back(
      {sourceFilename, Filetype::Turtle, std::nullopt});
  sourceConfig.baseName_ = basename;
  // `serializeVocabAndNamedCacheToCompressedBlob` currently requires the
  // in-memory, uncompressed vocabulary implementation (see
  // `Vocabulary::writeAsZeroCopyBlob`).
  sourceConfig.vocabType_ = ad_utility::VocabularyType::InMemoryUncompressed;
  EXPECT_NO_THROW(Qlever::buildIndex(sourceConfig));

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
  std::string basename = gtestCurrentTestName();
  std::string sourceFilename = basename + ".ttl";
  {
    auto ofs = ad_utility::makeOfstream(sourceFilename);
    ofs << "<allocatorBlobSubject> <allocatorBlobPredicate> "
           "\"allocator blob literal\".";
  }
  absl::Cleanup cleanup = [&sourceFilename] {
    ad_utility::deleteFile(sourceFilename);
  };
  IndexBuilderConfig sourceConfig;
  sourceConfig.inputFiles_.push_back(
      {sourceFilename, Filetype::Turtle, std::nullopt});
  sourceConfig.baseName_ = basename;
  sourceConfig.vocabType_ = ad_utility::VocabularyType::InMemoryUncompressed;
  EXPECT_NO_THROW(Qlever::buildIndex(sourceConfig));

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
  std::string basename = gtestCurrentTestName();
  std::string sourceFilename = basename + ".ttl";
  {
    auto ofs = ad_utility::makeOfstream(sourceFilename);
    ofs << "<s1> <asWKT> \"LINESTRING(7.8428469 47.9995367,7.8413293 "
           "47.9974942)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> "
           ".\n"
           "<s2> <asWKT> \"LINESTRING(7.8409068 47.9975041,7.8420114 "
           "47.9989233)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> "
           ".\n"
           "<s3> <asWKT> \"LINESTRING(7.8427369 47.9995806,7.8411672 "
           "47.9975175)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> "
           ".\n"
           "<s4> <asWKT> \"LINESTRING(7.8422376 47.9990144,7.8411016 "
           "47.9975307)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> "
           ".\n";
  }
  absl::Cleanup cleanup = [&sourceFilename] {
    ad_utility::deleteFile(sourceFilename);
  };
  IndexBuilderConfig sourceConfig;
  sourceConfig.inputFiles_.push_back(
      {sourceFilename, Filetype::Turtle, std::nullopt});
  sourceConfig.baseName_ = basename;
  sourceConfig.vocabType_ = ad_utility::VocabularyType::InMemoryUncompressed;
  EXPECT_NO_THROW(Qlever::buildIndex(sourceConfig));

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

// _____________________________________________________________________________
// Test a round trip of a blob from which some of the vocabulary entries were
// excluded, for each of the source vocabulary types that supports the
// filtering. The pinned query result references both a kept and an excluded
// entry, so that the preservation of the original vocabulary indices is
// actually exercised: the kept entries have to resolve to their original
// strings, the excluded ones to `placeholderForMissingVocabIndex`.
TEST(NamedCachedQueryBlobManager, blobWithExcludedVocabularyEntries) {
  using ad_utility::VocabularyType;
  for (const auto& [sourceType, expectedBlobType] :
       std::vector<std::pair<VocabularyType, VocabularyType>>{
           {VocabularyType::InMemoryUncompressed,
            VocabularyType::InMemoryUncompressedWithHoles},
           {VocabularyType::OnDiskUncompressed,
            VocabularyType::InMemoryUncompressedWithHoles},
           {VocabularyType::InMemoryCompressed,
            VocabularyType::InMemoryCompressedWithHoles},
           {VocabularyType::OnDiskCompressed,
            VocabularyType::InMemoryCompressedWithHoles}}) {
    std::string basename =
        absl::StrCat(gtestCurrentTestName(), ".", sourceType.toString());
    auto sourceConfig = buildFilterTestIndex(basename, sourceType);

    uint64_t droppedSubjectIndex = 0;
    uint64_t droppedObjectIndex = 0;
    uint64_t keptSubjectIndex = 0;
    const std::vector<char> compressedBlob =
        [&sourceConfig, &droppedSubjectIndex, &droppedObjectIndex,
         &keptSubjectIndex]() {
          Qlever source{EngineConfig{sourceConfig}};
          source.queryAndPinResultWithName(
              std::string{filterPinName},
              "SELECT ?s ?o WHERE { ?s <filterPredicate> ?o }");
          droppedSubjectIndex =
              vocabIndexOfEntryContaining(source, "droppedSubject");
          droppedObjectIndex =
              vocabIndexOfEntryContaining(source, "dropped literal");
          keptSubjectIndex = vocabIndexOfEntryContaining(source, "keptSubject");
          BlobSerializationConfig config;
          // The regexes are matched against the complete entry, hence the
          // leading and trailing `.*`. This excludes the IRI
          // `<droppedSubject>` as well as the literal `"dropped literal"`.
          config.excludedEntryRegexes_ = {".*dropped.*"};
          return source.serializeVocabAndNamedCacheToCompressedBlob(config);
        }();

    // The type of the vocabulary in the blob is recorded in its metadata JSON,
    // so that the reading side does not need to know about the filtering.
    EXPECT_EQ(metadataFromBlob(compressedBlob)["vocabulary-type"],
              expectedBlobType.toString());

    Qlever target{EngineConfig{}, /*skipLoading=*/true};
    EXPECT_NO_THROW(
        target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob));
    auto result =
        target.query(std::string{filterPinQuery}, ad_utility::MediaType::tsv);
    // The kept entries resolve to their original strings, and in particular the
    // kept subject kept its original vocabulary index (which is larger than the
    // index of the dropped subject, so the surviving vocabulary has a hole).
    EXPECT_GT(keptSubjectIndex, droppedSubjectIndex);
    EXPECT_THAT(result, HasSubstr("<keptSubject>\t\"kept literal\""));
    // The excluded entries resolve to the placeholder for their original index.
    EXPECT_THAT(
        result,
        HasSubstr(ad_utility::vocabulary::placeholderForMissingVocabIndex(
            droppedSubjectIndex)));
    EXPECT_THAT(
        result,
        HasSubstr(ad_utility::vocabulary::placeholderForMissingVocabIndex(
            droppedObjectIndex)));
  }
}

// _____________________________________________________________________________
// Test that a blob written with a list of regexes that matches no vocabulary
// entry at all is functionally equivalent to (though not byte-identical with)
// the unfiltered blob, and that an empty list of regexes produces a blob in the
// original format (i.e. with the original vocabulary type).
TEST(NamedCachedQueryBlobManager, blobWithRegexesThatMatchNothing) {
  std::string basename = gtestCurrentTestName();
  auto sourceConfig = buildFilterTestIndex(
      basename, ad_utility::VocabularyType::InMemoryUncompressed);

  std::vector<char> unfilteredBlob;
  std::vector<char> filteredBlob;
  {
    Qlever source{EngineConfig{sourceConfig}};
    source.queryAndPinResultWithName(
        std::string{filterPinName},
        "SELECT ?s ?o WHERE { ?s <filterPredicate> ?o }");
    unfilteredBlob = source.serializeVocabAndNamedCacheToCompressedBlob();
    BlobSerializationConfig config;
    config.excludedEntryRegexes_ = {"thisMatchesNoVocabularyEntry"};
    filteredBlob = source.serializeVocabAndNamedCacheToCompressedBlob(config);
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
    Qlever target{EngineConfig{}, /*skipLoading=*/true};
    EXPECT_NO_THROW(
        target.deserializeVocabAndNamedCacheFromCompressedBlob(blob));
    EXPECT_EQ(
        target.query(std::string{filterPinQuery}, ad_utility::MediaType::tsv),
        expected);
  }
}

// _____________________________________________________________________________
// Test that excluding vocabulary entries from a blob is rejected with a
// descriptive message if the source vocabulary is a geo-split vocabulary (whose
// marker-encoded indices cannot be represented by a vocabulary with holes).
TEST(NamedCachedQueryBlobManager, blobWithExcludedEntriesRejectsGeoSplitVocab) {
  std::string basename = gtestCurrentTestName();
  auto sourceConfig = buildFilterTestIndex(
      basename, ad_utility::VocabularyType::OnDiskCompressedGeoSplit);

  Qlever source{EngineConfig{sourceConfig}};
  source.queryAndPinResultWithName(
      std::string{filterPinName},
      "SELECT ?s ?o WHERE { ?s <filterPredicate> ?o }");
  BlobSerializationConfig config;
  config.excludedEntryRegexes_ = {".*dropped.*"};
  AD_EXPECT_THROW_WITH_MESSAGE(
      source.serializeVocabAndNamedCacheToCompressedBlob(config),
      HasSubstr("on-disk-compressed-geo-split"));
}
