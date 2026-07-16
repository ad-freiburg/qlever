// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "backports/memory_resource.h"
#include "engine/ExternalValues.h"
#include "libqlever/Qlever.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"

using namespace qlever;
using namespace testing;

// _____________________________________________________________________________
TEST(LibQlever, buildIndexAndRunQuery) {
  std::string filename = "libQleverbuildIndexAndRunQuery.ttl";
  {
    auto ofs = ad_utility::makeOfstream(filename);
    ofs << "<s> <p> <o>. <s2> <p> \"kartoffel und salat\".";
  }

  IndexBuilderConfig c;
  c.inputFiles_.push_back({filename, Filetype::Turtle, std::nullopt});
  c.baseName_ = "LibQlever.buildIndexAndRunQuery";

  // Test the activation of the memory limit
  c.memoryLimit_ = ad_utility::MemorySize::bytes(0);
  AD_EXPECT_THROW_WITH_MESSAGE(Qlever::buildIndex(c),
                               ::testing::HasSubstr("memory limit"));

  c.memoryLimit_ = std::nullopt;

  c.parserBufferSize_ = ad_utility::MemorySize::bytes(0);
  AD_EXPECT_THROW_WITH_MESSAGE(Qlever::buildIndex(c),
                               ::testing::HasSubstr("buffer size"));

  c.parserBufferSize_ = std::nullopt;

  // Test materialized views to be written at index build time.
  c.writeMaterializedViews_ = {{"demoView", "SELECT ?s { ?s <p> <o> }"}};

  EXPECT_NO_THROW(Qlever::buildIndex(c));

  {
    EngineConfig ec{c};
    Qlever engine{ec};
    // Run a simple query.
    std::string query = "SELECT ?s WHERE { ?s <p> <o> }";
    auto res = engine.query(query, ad_utility::MediaType::tsv);
    EXPECT_EQ(res, "?s\n<s>\n");

    // Run with a different media type.
    res = engine.query("SELECT * WHERE { <s> <p> ?o }",
                       ad_utility::MediaType::csv);
    EXPECT_EQ(res, "o\no\n");

    // Separately run the planning and the query.
    auto plan = engine.parseAndPlanQuery("SELECT * WHERE { <s> <p> ?o }");
    res = engine.query(plan, ad_utility::MediaType::csv);
    EXPECT_EQ(res, "o\no\n");

    // Test the explicit query cache.
    engine.queryAndPinResultWithName("pin1", query);
    std::string serviceQuery =
        "SELECT ?s WHERE { SERVICE ql:cached-result-with-name-pin1 {}}";
    std::string serviceQuery2 =
        "SELECT ?s WHERE { SERVICE ql:cached-result-with-name-pin2 {}}";
    res = engine.query(serviceQuery, ad_utility::MediaType::tsv);
    EXPECT_EQ(res, "?s\n<s>\n");
    engine.eraseResultWithName("pin1");
    auto notPinned =
        ::testing::HasSubstr("is not contained in the named result cache");
    AD_EXPECT_THROW_WITH_MESSAGE(engine.query(serviceQuery), notPinned);

    // Pin again.
    engine.queryAndPinResultWithName("pin1", query);
    engine.queryAndPinResultWithName("pin2", query);
    EXPECT_NO_THROW(engine.query(serviceQuery));
    EXPECT_NO_THROW(engine.query(serviceQuery2));

    // Clearing erases all queries.
    engine.clearNamedResultCache();
    AD_EXPECT_THROW_WITH_MESSAGE(engine.query(serviceQuery), notPinned);
    AD_EXPECT_THROW_WITH_MESSAGE(engine.query(serviceQuery2), notPinned);

    // Test that the requested materialized view exists.
    EXPECT_NO_THROW(engine.loadMaterializedView("demoView"));

    // A non-positive `geoIndexSimplificationInMeters_` is rejected.
    AD_EXPECT_THROW_WITH_MESSAGE(
        engine.queryAndPinResultWithName(
            QueryExecutionContext::PinResultWithName{"pin3", std::nullopt,
                                                     -1.0},
            query),
        ::testing::HasSubstr(
            "`geoIndexSimplificationInMeters_` must be a positive "
            "floating-point number of meters."));
  }

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  c.addWordsFromLiterals_ = true;

  // Note: Currently the `addWordsFromLiterals` feature is broken, but
  // @flixtastic has a fix for this.
  EXPECT_NO_THROW(Qlever::buildIndex(c));
  EngineConfig ec{c};
  ec.loadTextIndex_ = true;
  Qlever engine{ec};
#endif
}

// _____________________________________________________________________________
TEST(LibQlever, fulltextIndex) {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  GTEST_SKIP_(
      "Fulltext index not available in the reduced feature set (at least for "
      "now)");
#endif
  auto basename = "libQleverFulltextIndex";
  std::string filename = absl::StrCat(basename, ".ttl");
  std::string wordsfileName = absl::StrCat(basename, ".words");
  std::string docsFileName = absl::StrCat(basename, ".docs");
  {
    auto ofs = ad_utility::makeOfstream(filename);
    ofs << "<s> <p> <o>. <s2> <p> \"kartoffel und salat\".";
    auto words = ad_utility::makeOfstream(wordsfileName);
    words << "kartoffel\t0\t13\t1\n<s>\t1\t13\t1\n";
    auto docs = ad_utility::makeOfstream(docsFileName);
    docs << "13\tKartoffeln sind ein schönes Gemüse🥔";
  }

  IndexBuilderConfig c;
  c.inputFiles_.push_back({filename, Filetype::Turtle, std::nullopt});
  c.wordsfile_ = wordsfileName;
  c.docsfile_ = docsFileName;
  c.baseName_ = "LibQlever.fulltextIndex";
  EXPECT_NO_THROW(Qlever::buildIndex(c));
  {
    EngineConfig ec{c};
    ec.loadTextIndex_ = true;
    Qlever engine{ec};
    // Run a simple query.
    auto res = engine.query(
        "SELECT ?s ?p ?o ?t WHERE { ?t ql:contains-word \"kartoff*\". ?t "
        "ql:contains-entity ?s. ?s ?p ?o }",
        ad_utility::MediaType::tsv);
    EXPECT_EQ(res,
              "?s\t?p\t?o\t?t\n<s>\t<p>\t<o>\tKartoffeln sind ein schönes "
              "Gemüse🥔\n");
  }

  // Now the same test with separately building the RDF and the text index
  c.docsfile_ = "";
  c.wordsfile_ = "";
  c.baseName_ = "testIndexWithSeparateTextIndex";
  EXPECT_NO_THROW(Qlever::buildIndex(c));

  // Separately add the text index.
  c.onlyAddTextIndex_ = true;
  c.wordsfile_ = wordsfileName;
  c.docsfile_ = docsFileName;
  EXPECT_NO_THROW(Qlever::buildIndex(c));
  {
    EngineConfig ec{c};
    ec.loadTextIndex_ = true;
    Qlever engine{ec};
    // Run a simple query.
    auto res = engine.query(
        "SELECT ?s ?p ?o ?t WHERE { ?t ql:contains-word \"kartoff*\". ?t "
        "ql:contains-entity ?s. ?s ?p ?o }",
        ad_utility::MediaType::tsv);
    EXPECT_EQ(res,
              "?s\t?p\t?o\t?t\n<s>\t<p>\t<o>\tKartoffeln sind ein schönes "
              "Gemüse🥔\n");
  }
}

// _____________________________________________________________________________
TEST(IndexBuilderConfig, validate) {
  IndexBuilderConfig c;
  EXPECT_NO_THROW(c.validate());

  c.kScoringParam_ = -3;
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(), HasSubstr("must be >= 0"));

  c = IndexBuilderConfig{};
  c.bScoringParam_ = -3;
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(), HasSubstr("must be between"));
  c.bScoringParam_ = 1.1;
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(), HasSubstr("must be between"));

  c = IndexBuilderConfig{};
  c.wordsfile_ = "blibb";
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(),
                               HasSubstr("Only specified wordsfile"));
  c.docsfile_ = "blabb";
  EXPECT_NO_THROW(c.validate());
  c.wordsfile_ = "";
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(),
                               HasSubstr("Only specified docsfile"));
}

// _____________________________________________________________________________
TEST(LibQlever, loadIndexWithoutPermutations) {
  std::string filename = "libQleverLoadIndexWithoutPermutations.ttl";
  {
    auto ofs = ad_utility::makeOfstream(filename);
    ofs << "<s> <p> <o>. <s2> <p2> \"literal\".";
  }

  IndexBuilderConfig c;
  c.inputFiles_.push_back({filename, Filetype::Turtle, std::nullopt});
  c.baseName_ = "LibQlever.loadIndexWithoutPermutations";
  c.memoryLimit_ = std::nullopt;

  // Build the index normally.
  EXPECT_NO_THROW(Qlever::buildIndex(c));

  // Load the index with `doNotLoadPermutations` set to true.
  EngineConfig ec{c};
  ec.doNotLoadPermutations_ = true;
  Qlever engine{ec};

  // Test that the `setKbName` function silently does nothing, if we have no
  // index.
  EXPECT_NO_THROW(
      engine.indexAndViewsSnapshot()->index_.setKbName("we have no triples!"));

  // Run a query that doesn't need to access permutations (constant expression).
  std::string query = "SELECT (3 + 5 AS ?result) {}";
  auto res = engine.query(query, ad_utility::MediaType::tsv);
  // The result should contain the computed value.
  EXPECT_THAT(res, HasSubstr("8"));

  // Try a query that would need to access permutations and verify it throws.
  std::string queryNeedingPermutations = "SELECT ?s WHERE { ?s <p> <o> }";
  AD_EXPECT_THROW_WITH_MESSAGE(
      engine.query(queryNeedingPermutations, ad_utility::MediaType::tsv),
      HasSubstr("permutation to be loaded"));
}

// _____________________________________________________________________________
// Test that a blob (index metadata + vocabulary + named result cache), written
// from one `Qlever` instance, can be loaded into a completely separate instance
// that has NO index files on disk at all (constructed with `skipLoading`), and
// there produce correct query results without loading any permutations.
TEST(LibQlever, combinedBlob) {
  std::string sourceFilename = "libQleverCombinedBlobSource.ttl";
  {
    auto ofs = ad_utility::makeOfstream(sourceFilename);
    ofs << "<combinedBlobSubject> <combinedBlobPredicate> "
           "\"combined blob literal\".";
  }
  IndexBuilderConfig sourceConfig;
  sourceConfig.inputFiles_.push_back(
      {sourceFilename, Filetype::Turtle, std::nullopt});
  sourceConfig.baseName_ = "LibQlever.combinedBlobSource";
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

// _____________________________________________________________________________
// Test that the `allocator` passed to
// `deserializeVocabAndNamedCacheFromCompressedBlob` is in fact used to allocate
// the (large) decompressed blob buffer.
TEST(LibQlever, blobUsesProvidedAllocator) {
  std::string sourceFilename = "libQleverBlobAllocatorSource.ttl";
  {
    auto ofs = ad_utility::makeOfstream(sourceFilename);
    ofs << "<allocatorBlobSubject> <allocatorBlobPredicate> "
           "\"allocator blob literal\".";
  }
  IndexBuilderConfig sourceConfig;
  sourceConfig.inputFiles_.push_back(
      {sourceFilename, Filetype::Turtle, std::nullopt});
  sourceConfig.baseName_ = "LibQlever.blobAllocatorSource";
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
TEST(LibQlever, deserializeRejectsInvalidBlob) {
  // A validly ZSTD-compressed blob (with a correct trailing uncompressed size)
  // whose decompressed content does not start with the expected magic header.
  std::vector<char> bogus(64, 'X');
  std::vector<char> compressedBlob =
      ZstdWrapper::compress(bogus.data(), bogus.size());
  uint64_t uncompressedSize = bogus.size();
  const char* sizeBytes = reinterpret_cast<const char*>(&uncompressedSize);
  compressedBlob.insert(compressedBlob.end(), sizeBytes,
                        sizeBytes + sizeof(uncompressedSize));

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
TEST(LibQlever, blobWithSpatialIndex) {
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
  sourceConfig.baseName_ = "LibQlever.blobSpatialSource";
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
TEST(LibQlever, disableCaching) {
  std::string filename = "libQleverDisableCaching.ttl";
  {
    auto ofs = ad_utility::makeOfstream(filename);
    ofs << "<s> <p> <o>. <s2> <p2> \"literal\".";
  }

  IndexBuilderConfig c;
  c.inputFiles_.push_back({filename, Filetype::Turtle, std::nullopt});
  c.baseName_ = "LibQlever.disableCaching";
  c.memoryLimit_ = std::nullopt;

  // Build the index normally.
  EXPECT_NO_THROW(Qlever::buildIndex(c));

  EngineConfig ec{c};
  {
    // Load the index with `disableCaching` set to true.
    ec.disableCaching_ = QueryExecutionContext::DisableCaching::True;
    Qlever engine{ec};
    auto plan = engine.parseAndPlanQuery("SELECT ?s WHERE {?x <p> ?o}");
    auto& qec = plan.queryExecutionContext();
    EXPECT_TRUE(qec.disableCaching());
  }
  {
    // Load the index with `disableCaching` set to false.
    ec.disableCaching_ = QueryExecutionContext::DisableCaching::False;
    Qlever engine{ec};
    {
      auto plan = engine.parseAndPlanQuery("SELECT ?s WHERE {?x <p> ?o}");
      auto& qec = plan.queryExecutionContext();
      EXPECT_FALSE(qec.disableCaching());
    }
  }

  {
    // Load the index with `disableCaching` set to `FromRuntimeParameter`
    // (default value)..
    ec.disableCaching_ =
        QueryExecutionContext::DisableCaching::FromRuntimeParameter;
    Qlever engine{ec};
    {
      auto plan = engine.parseAndPlanQuery("SELECT ?s WHERE {?x <p> ?o}");
      auto& qec = plan.queryExecutionContext();
      EXPECT_FALSE(qec.disableCaching());
    }
    // Now after the fact disable the caching for new operations via the runtime
    // parameters:
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::disableCaching_>(true);
    {
      auto plan = engine.parseAndPlanQuery("SELECT ?s WHERE {?x <p> ?o}");
      auto& qec = plan.queryExecutionContext();
      EXPECT_TRUE(qec.disableCaching());
    }
  }
}

// _____________________________________________________________________________
TEST(LibQlever, externallySpecifiedValues) {
  std::string filename = "libQleverExternalValues.ttl";
  {
    auto ofs = ad_utility::makeOfstream(filename);
    ofs << "<s1> <p> 1 . <s2> <p> 2 . <s3> <p> 3 .";
  }

  IndexBuilderConfig c;
  c.inputFiles_.push_back({filename, Filetype::Turtle, std::nullopt});
  c.baseName_ = "testIndexForExternalValues";
  EXPECT_NO_THROW(Qlever::buildIndex(c));

  EngineConfig ec{c};
  // Caching must be disabled for externally specified values.
  ec.disableCaching_ = QueryExecutionContext::DisableCaching::True;
  Qlever engine{ec};

  // Parse a query that uses externally specified values joined with the index.
  // Use both syntaxes, the preferred one, and the deprecated one kept for
  // BMW.
  std::array<std::string, 2> queries = {
      R"(
    SELECT ?x ?o WHERE {
      ?x <p> ?o .
      SERVICE <https://qlever.cs.uni-freiburg.de/external-values/> {
        [] <name> "myValues" .
        [] <variable> ?x .
      }
    } ORDER BY ?x
  )",
      R"(
    SELECT ?x ?o WHERE {
      ?x <p> ?o .
      SERVICE <https://qlever.cs.uni-freiburg.de/external-values-myValues> {
        [] <variable> ?x .
      }
    } ORDER BY ?x
  )"};

  for (const auto& query : queries) {
    auto plan = engine.parseAndPlanQuery(query);
    auto& qet = plan.queryExecutionTree();
    auto& qec = plan.queryExecutionContext();

    // Collect the ExternalValues operations from the tree.
    std::vector<ExternalValues*> externalValues;
    qet.getRootOperation()->getExternalValues(externalValues);
    ASSERT_EQ(externalValues.size(), 1u);
    EXPECT_EQ(externalValues[0]->getName(), "myValues");
    EXPECT_EQ(externalValues[0]->getResultWidth(), 1u);

    // Supply values and execute the query.
    using TC = TripleComponent;
    parsedQuery::SparqlValues newValues;
    newValues._variables = {Variable{"?x"}};
    newValues._values = {{TC::Iri::fromIriref("<s1>")},
                         {TC::Iri::fromIriref("<s3>")}};
    externalValues[0]->updateValues(std::move(newValues));

    auto res = qet.getResult();
    auto i = &Id::makeFromInt;
    auto getId = ad_utility::testing::makeGetId(qec.getIndex());
    // The order of the two columns `?x` and `?o` might not be deterministic.
    auto expected =
        makeIdTableFromVector({{getId("<s1>"), i(1)}, {getId("<s3>"), i(3)}});
    if (qet.getVariableColumn(Variable{"?x"}) != 0) {
      expected.swapColumns(0, 1);
    }
    EXPECT_THAT(res->idTableView(), matchesIdTable(expected));
  }
}
