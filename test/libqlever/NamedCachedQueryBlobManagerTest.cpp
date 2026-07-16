// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <algorithm>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "backports/memory_resource.h"
#include "backports/span.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "libqlever/Qlever.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/Serializer/ByteBufferSerializer.h"

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
}  // namespace

// _____________________________________________________________________________
// Test the compression utility and its inverse in isolation, for several
// buffer sizes (including the empty buffer).
TEST(NamedCachedQueryBlobManager, compressAndDecompressWithTrailingSizeInfo) {
  for (const std::string& original :
       {std::string{}, std::string{"x"}, std::string{"a short blob"},
        std::string(100'000, 'q')}) {
    std::vector<char> uncompressed(original.begin(), original.end());
    std::vector<char> compressed =
        Manager::compressBlobAndAddTrailingSizeInfo(uncompressed);
    // The trailing `uint64_t` with the uncompressed size is always present.
    ASSERT_GE(compressed.size(), sizeof(uint64_t));

    auto roundTripped =
        Manager::decompressBlobWithTrailingSizeInfo(compressed, {});
    ASSERT_EQ(roundTripped.size(), uncompressed.size());
    EXPECT_TRUE(std::equal(roundTripped.begin(), roundTripped.end(),
                           uncompressed.begin()));
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
  writer << std::string{"payload"};
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
// Test that a blob (index metadata + vocabulary + named result cache), written
// from one `Qlever` instance, can be loaded into a completely separate instance
// that has NO index files on disk at all (constructed with `skipLoading`), and
// there produce correct query results without loading any permutations.
TEST(NamedCachedQueryBlobManager, combinedBlob) {
  std::string sourceFilename = "libQleverCombinedBlobSource.ttl";
  {
    auto ofs = ad_utility::makeOfstream(sourceFilename);
    ofs << "<combinedBlobSubject> <combinedBlobPredicate> "
           "\"combined blob literal\".";
  }
  IndexBuilderConfig sourceConfig;
  sourceConfig.inputFiles_.push_back(
      {sourceFilename, Filetype::Turtle, std::nullopt});
  sourceConfig.baseName_ = "NamedCachedQueryBlobManager.combinedBlobSource";
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
  EXPECT_ANY_THROW(
      target.query("SELECT ?s WHERE { ?s <combinedBlobPredicate> ?o }",
                   ad_utility::MediaType::tsv));

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
  std::string sourceFilename = "libQleverBlobAllocatorSource.ttl";
  {
    auto ofs = ad_utility::makeOfstream(sourceFilename);
    ofs << "<allocatorBlobSubject> <allocatorBlobPredicate> "
           "\"allocator blob literal\".";
  }
  IndexBuilderConfig sourceConfig;
  sourceConfig.inputFiles_.push_back(
      {sourceFilename, Filetype::Turtle, std::nullopt});
  sourceConfig.baseName_ = "NamedCachedQueryBlobManager.blobAllocatorSource";
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
  // A validly ZSTD-compressed blob (with a correct trailing uncompressed size)
  // whose decompressed content does not start with the expected magic header.
  std::vector<char> bogus(64, 'X');
  std::vector<char> compressedBlob =
      Manager::compressBlobAndAddTrailingSizeInfo(bogus);

  Qlever target{EngineConfig{}, /*skipLoading=*/true};
  AD_EXPECT_THROW_WITH_MESSAGE(
      target.deserializeVocabAndNamedCacheFromCompressedBlob(compressedBlob),
      HasSubstr("was not written by"));
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
  std::string sourceFilename = "libQleverBlobSpatialSource.ttl";
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
  IndexBuilderConfig sourceConfig;
  sourceConfig.inputFiles_.push_back(
      {sourceFilename, Filetype::Turtle, std::nullopt});
  sourceConfig.baseName_ = "NamedCachedQueryBlobManager.blobSpatialSource";
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
