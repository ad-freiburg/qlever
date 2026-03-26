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
#include "engine/ExternallySpecifiedValues.h"
#include "libqlever/Qlever.h"

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
  c.baseName_ = "testIndexForLibQlever";

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
  c.baseName_ = "testIndexForLibQlever";
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
  c.baseName_ = "testIndexWithoutPermutations";
  c.memoryLimit_ = std::nullopt;

  // Build the index normally.
  EXPECT_NO_THROW(Qlever::buildIndex(c));

  // Load the index with `doNotLoadPermutations` set to true.
  EngineConfig ec{c};
  ec.doNotLoadPermutations_ = true;
  Qlever engine{ec};

  // Test that the `setKbName` function silently does nothing, if we have no
  // index.
  EXPECT_NO_THROW(engine.index().setKbName("we have no triples!"));

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
TEST(LibQlever, disableCaching) {
  std::string filename = "libQleverDisableCaching.ttl";
  {
    auto ofs = ad_utility::makeOfstream(filename);
    ofs << "<s> <p> <o>. <s2> <p2> \"literal\".";
  }

  IndexBuilderConfig c;
  c.inputFiles_.push_back({filename, Filetype::Turtle, std::nullopt});
  c.baseName_ = "testIndexWithoutPermutations";
  c.memoryLimit_ = std::nullopt;

  // Build the index normally.
  EXPECT_NO_THROW(Qlever::buildIndex(c));

  EngineConfig ec{c};
  {
    // Load the index with `disableCaching` set to true.
    ec.disableCaching_ = QueryExecutionContext::DisableCaching::True;
    Qlever engine{ec};
    auto plan = engine.parseAndPlanQuery("SELECT ?s WHERE {?x <p> ?o}");
    auto& qec = std::get<1>(plan);
    EXPECT_TRUE(qec->disableCaching());
  }
  {
    // Load the index with `disableCaching` set to false.
    ec.disableCaching_ = QueryExecutionContext::DisableCaching::False;
    Qlever engine{ec};
    {
      auto plan = engine.parseAndPlanQuery("SELECT ?s WHERE {?x <p> ?o}");
      auto& qec = std::get<1>(plan);
      EXPECT_FALSE(qec->disableCaching());
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
      auto& qec = std::get<1>(plan);
      EXPECT_FALSE(qec->disableCaching());
    }
    // Now after the fact disable the caching for new operations via the runtime
    // parameters:
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::disableCaching_>(true);
    {
      auto plan = engine.parseAndPlanQuery("SELECT ?s WHERE {?x <p> ?o}");
      auto& qec = std::get<1>(plan);
      EXPECT_TRUE(qec->disableCaching());
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
      SERVICE <https://qlever.cs.uni-freiburg/external-values/> {
        [] <identifier> "myValues" .
        [] <variables> ?x .
      }
    } ORDER BY ?x
  )",
      R"(
    SELECT ?x ?o WHERE {
      ?x <p> ?o .
      SERVICE <https://qlever.cs.uni-freiburg.de/external-values-myValues> {
        [] <variables> ?x .
      }
    } ORDER BY ?x
  )"};

  for (const auto& query : queries) {
    auto plan = engine.parseAndPlanQuery(query);
    auto& [qet, qec, parsedQuery] = plan;

    // Collect the ExternallySpecifiedValues operations from the tree.
    std::vector<ExternallySpecifiedValues*> externalValues;
    qet->getRootOperation()->getExternallySpecifiedValues(externalValues);
    ASSERT_EQ(externalValues.size(), 1u);
    EXPECT_EQ(externalValues[0]->getIdentifier(), "myValues");
    EXPECT_EQ(externalValues[0]->getResultWidth(), 1u);

    // Supply values and execute the query.
    using TC = TripleComponent;
    parsedQuery::SparqlValues newValues;
    newValues._variables = {Variable{"?x"}};
    newValues._values = {{TC::Iri::fromIriref("<s1>")},
                         {TC::Iri::fromIriref("<s3>")}};
    externalValues[0]->updateValues(std::move(newValues));

    auto res = qet->getResult();
    auto i = &Id::makeFromInt;
    auto getId = ad_utility::testing::makeGetId(qec->getIndex());
    // The order of the two columns `?x` and `?o` might not be deterministic.
    auto expected =
        makeIdTableFromVector({{getId("<s1>"), i(1)}, {getId("<s3>"), i(3)}});
    if (qet->getVariableColumn(Variable{"?x"}) != 0) {
      expected.swapColumns(0, 1);
    }
    EXPECT_THAT(res->idTable(), matchesIdTable(expected));
  }
}
