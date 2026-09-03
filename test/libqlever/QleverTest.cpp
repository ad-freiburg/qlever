// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include <memory>

#include "../util/FileTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "backports/filesystem.h"
#include "engine/ExternalValues.h"
#include "engine/MaterializedViews.h"
#include "engine/UpdateMetadata.h"
#include "global/FileSuffixConstants.h"
#include "global/RuntimeParameters.h"
#include "index/DeltaTriples.h"
#include "index/IndexImpl.h"
#include "libqlever/Qlever.h"
#include "parser/SparqlParser.h"
#include "util/BlankNodeManager.h"
#include "util/FilesystemHelpers.h"

using namespace qlever;
using namespace testing;

namespace {
// Write `turtleContents` to a turtle file, build an index from it with all
// settings at their default, and return an `EngineConfig` for that index. The
// base name of both the turtle file and the index is derived from the name of
// the currently running test and the optional `suffix`, so that a single test
// can build several distinct indexes. The turtle file is deleted again before
// this returns; the files of the index itself remain on disk.
//
// NOTE: An index that cannot be built throws, which `gtest` reports as a
// failure of the running test. This is deliberately not an `EXPECT_NO_THROW`,
// which would let the test continue with a nonexistent index.
EngineConfig buildTestIndex(std::string_view turtleContents,
                            std::string_view suffix = "") {
  std::string basename = absl::StrCat(gtestCurrentTestName(), suffix);
  std::string filename = absl::StrCat(basename, ".ttl");
  ad_utility::makeOfstream(filename) << turtleContents;
  absl::Cleanup cleanup = [&filename] { ad_utility::deleteFile(filename); };

  IndexBuilderConfig config;
  config.inputFiles_.push_back({filename, Filetype::Turtle, std::nullopt});
  config.baseName_ = basename;
  Qlever::buildIndex(config);
  return EngineConfig{config};
}
}  // namespace

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

  // The vocabulary types with "holes" cannot be built word by word and hence
  // are rejected, all other types are accepted.
  c = IndexBuilderConfig{};
  for (auto type : ad_utility::VocabularyType::all()) {
    c.vocabType_ = ad_utility::VocabularyType{type};
    if (c.vocabType_.isSupportedForIndexBuilding()) {
      EXPECT_NO_THROW(c.validate());
    } else {
      AD_EXPECT_THROW_WITH_MESSAGE(
          c.validate(), AllOf(HasSubstr("cannot be used for index building"),
                              HasSubstr(c.vocabType_.toString()),
                              HasSubstr("on-disk-compressed")));
    }
  }

  // `Qlever::buildIndex` validates its config, such that the informative error
  // message is also reported when the library API is used directly (and not
  // via `IndexBuilderMain`, which validates the config explicitly).
  c = IndexBuilderConfig{};
  c.vocabType_ = ad_utility::VocabularyType{
      ad_utility::VocabularyType::Enum::InMemoryCompressedWithHoles};
  AD_EXPECT_THROW_WITH_MESSAGE(Qlever::buildIndex(c),
                               HasSubstr("cannot be used for index building"));
}

// _____________________________________________________________________________
TEST(LibQlever, loadIndexWithoutPermutations) {
  EngineConfig ec = buildTestIndex("<s> <p> <o>. <s2> <p2> \"literal\".");

  // Load the index with `doNotLoadPermutations` set to true.
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
// Test that `swapIndexAndViews` refuses to swap the index snapshot while the
// named result cache is not empty (its entries are only valid for one specific
// snapshot). Uses `FRIEND_TEST` to reach the otherwise private method.
TEST(LibQlever, swapIndexAndViewsThrowsWithNonEmptyNamedCache) {
  Qlever qlever{buildTestIndex("<s> <p> <o>.")};

  // With an empty named result cache, swapping (here: with the current
  // snapshot) is allowed.
  EXPECT_NO_THROW(qlever.swapIndexAndViews(qlever.indexAndViewsSnapshot()));

  // Pin a named result, so the named result cache is no longer empty.
  qlever.queryAndPinResultWithName("swapPin",
                                   "SELECT ?s ?o WHERE { ?s <p> ?o }");

  // Now swapping the index snapshot must throw.
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlever.swapIndexAndViews(qlever.indexAndViewsSnapshot()),
      HasSubstr("named result cache is not empty"));
}

// _____________________________________________________________________________
TEST(LibQlever, disableCaching) {
  EngineConfig ec = buildTestIndex("<s> <p> <o>. <s2> <p2> \"literal\".");
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
  EngineConfig ec = buildTestIndex("<s1> <p> 1 . <s2> <p> 2 . <s3> <p> 3 .");
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

namespace {
// The pieces produced by `setUpRebuild` below: the base name of the "old"
// index, the base name of the freshly "rebuilt" index (in a temporary
// directory), and the in-memory `IndexAndViews` for the rebuilt index. The
// latter is owned via a `shared_ptr` because `IndexAndViews` is neither
// copyable nor movable.
struct RebuildSetup {
  std::string oldBase_;
  std::string rebuiltBase_;
  std::shared_ptr<qlever::Qlever::IndexAndViews> indexAndViews_;
};

// Build an "old" index at `<baseFolder>/index` and a freshly "rebuilt" index at
// `<baseFolder>/rebuild.tmp/index` (a distinct temporary directory), and return
// them for use with `Qlever::moveRebuiltIndexIntoPlace`.
RebuildSetup setUpRebuild(const std::string& baseFolder) {
  ql::filesystem::create_directory(baseFolder);
  std::string oldBase = baseFolder + "/index";
  std::string tmpDir = baseFolder + "/rebuild.tmp";
  ql::filesystem::create_directory(tmpDir);
  std::string rebuiltBase = tmpDir + "/index";

  ad_utility::testing::makeTestIndex(oldBase, "<a> <b> <c> .");
  Index rebuilt = ad_utility::testing::makeTestIndex(
      rebuiltBase, "<a> <b> <c> . <d> <e> <f> .");
  // Remove the input files of the index build (which are not part of the index
  // itself), so that the temporary directory holds nothing but the rebuilt
  // index and can hence be removed once that index has been moved out of it
  // (see `moveRebuiltIndexIntoPlace`, and
  // `moveRebuiltIndexIntoPlaceWithNonRemovableBuildDirectory` for the opposite
  // case).
  for (auto suffix : {".ttl", ".ttl.settings.json"}) {
    ql::filesystem::remove(rebuiltBase + suffix);
  }
  auto indexAndViews = std::make_shared<qlever::Qlever::IndexAndViews>(
      std::move(rebuilt), MaterializedViewsManager{rebuiltBase});
  return {std::move(oldBase), std::move(rebuiltBase), std::move(indexAndViews)};
}
}  // namespace

// _____________________________________________________________________________
// `Qlever::moveRebuiltIndexIntoPlace` performs the on-disk swap (which is
// tested in detail in `test/index/IndexSwapTest.cpp`) and then re-anchors the
// path-derived in-memory state of the new index to its final base name. Only
// the latter is tested here.
TEST(Qlever, moveRebuiltIndexIntoPlace) {
  // The re-anchoring of the filenames for the persisted updates only happens
  // for an index that persists its updates, so run the test for both cases.
  for (bool persistUpdates : {false, true}) {
    std::string baseFolder =
        absl::StrCat(gtestCurrentTestName(), persistUpdates);
    absl::Cleanup removeFiles{
        [&baseFolder] { ql::filesystem::remove_all(baseFolder); }};
    auto setup = setUpRebuild(baseFolder);
    auto& index = setup.indexAndViews_->index_;

    if (persistUpdates) {
      index.getImpl().setFilenamesForPersistentUpdates(false);
    }
    ASSERT_EQ(index.deltaTriplesManager().persists(), persistUpdates);
    ASSERT_EQ(index.getOnDiskBase(), setup.rebuiltBase_);

    std::string newBase = baseFolder + "/index";
    qlever::IndexSwapConfig config{setup.oldBase_, setup.rebuiltBase_,
                                   baseFolder + "/previous/index", newBase};
    qlever::Qlever::moveRebuiltIndexIntoPlace(*setup.indexAndViews_, config);

    // The on-disk swap happened, and the in-memory state of the new index was
    // re-anchored to the final base name, with its persistence of updates
    // unchanged.
    EXPECT_TRUE(ql::filesystem::exists(newBase + CONFIGURATION_FILE));
    EXPECT_EQ(index.getOnDiskBase(), newBase);
    EXPECT_EQ(index.deltaTriplesManager().persists(), persistUpdates);
  }
}

// _____________________________________________________________________________
// Test `parseQuery` + `planQuery` + `PlannedQuery::cloneQetInPlace`, which
// together make it possible to plan a query once and then execute it
// repeatedly with varying values: copying the resulting `PlannedQuery` shares
// its `QueryExecutionTree`, and `cloneQetInPlace` gives the copy a tree of its
// own, which can then be modified without affecting the original.
TEST(LibQlever, planQueryOfParsedQueryAndCloneQetInPlace) {
  EngineConfig ec = buildTestIndex("<s1> <p> 1 . <s2> <p> 2 . <s3> <p> 3 .");
  // Caching must be disabled for externally specified values.
  ec.disableCaching_ = QueryExecutionContext::DisableCaching::True;
  Qlever engine{ec};

  std::string query = R"(
    SELECT ?x ?o WHERE {
      ?x <p> ?o .
      SERVICE <https://qlever.cs.uni-freiburg.de/external-values/> {
        [] <name> "myValues" .
        [] <variable> ?x .
      }
    } ORDER BY ?o
  )";

  // Parsing is instance-independent, planning is not, so the same
  // `ParsedQuery` can be planned more than once.
  // `parseQuery` returns the `ParsedQuery` together with the
  // `QueryExecutionContext` it was parsed against, and `planQuery` plans it
  // against exactly that context.
  ParsedQueryAndContext parsedQuery = engine.parseQuery(query);
  EXPECT_EQ(&parsedQuery.queryExecutionContext(),
            parsedQuery.sharedQueryExecutionContext().get());
  PlannedQuery plan = engine.planQuery(parsedQuery);
  EXPECT_EQ(&plan.queryExecutionContext(),
            &parsedQuery.queryExecutionContext());

  // Inject `iris` into the single `ExternalValues` placeholder of `plan`,
  // execute it, and return the objects that it yields (ordered by `?o`).
  auto runWith = [](PlannedQuery& plan, const std::vector<std::string>& iris) {
    std::vector<ExternalValues*> values;
    plan.queryExecutionTree().getRootOperation()->getExternalValues(values);
    AD_CONTRACT_CHECK(values.size() == 1);
    parsedQuery::SparqlValues newValues;
    newValues._variables = {Variable{"?x"}};
    for (const auto& iri : iris) {
      newValues._values.push_back({TripleComponent::Iri::fromIriref(iri)});
    }
    values.at(0)->updateValues(std::move(newValues));

    const auto& qet = plan.queryExecutionTree();
    auto result = qet.getResult();
    auto objectColumn = qet.getVariableColumn(Variable{"?o"});
    std::vector<int64_t> objects;
    for (size_t i = 0; i < result->idTableView().numRows(); ++i) {
      objects.push_back(result->idTableView()(i, objectColumn).getInt());
    }
    return objects;
  };

  // A copy of a `PlannedQuery` shares the `QueryExecutionTree`, so modifying
  // the copy would also modify `plan`.
  PlannedQuery firstCopy = plan;
  EXPECT_EQ(&firstCopy.queryExecutionTree(), &plan.queryExecutionTree());

  // `cloneQetInPlace` gives the copy its own tree, while the
  // `QueryExecutionContext` stays shared.
  firstCopy.cloneQetInPlace();
  EXPECT_NE(&firstCopy.queryExecutionTree(), &plan.queryExecutionTree());
  EXPECT_EQ(&firstCopy.queryExecutionContext(), &plan.queryExecutionContext());

  PlannedQuery secondCopy = plan;
  secondCopy.cloneQetInPlace();
  EXPECT_NE(&secondCopy.queryExecutionTree(), &firstCopy.queryExecutionTree());

  // The two copies can be given different values and executed independently.
  EXPECT_THAT(runWith(firstCopy, {"<s1>", "<s3>"}), ElementsAre(1, 3));
  EXPECT_THAT(runWith(secondCopy, {"<s2>"}), ElementsAre(2));

  // `firstCopy` still has its own tree, so it can be given new values again,
  // without `secondCopy` interfering.
  EXPECT_THAT(runWith(firstCopy, {"<s1>", "<s2>"}), ElementsAre(1, 2));
  EXPECT_THAT(runWith(secondCopy, {"<s3>"}), ElementsAre(3));
}

// _____________________________________________________________________________
// Test that `parseAndPlanQuery` is exactly `parseQuery` followed by
// `planQuery`, and that all the arguments of the former reach the two halves.
TEST(LibQlever, parseAndPlanQueryIsParseThenPlan) {
  Qlever engine{buildTestIndex("<s> <p> <o> . <s2> <p> <o2> .")};

  std::string query = "SELECT ?s WHERE { ?s <p> ?o }";

  // Both paths produce the same result.
  auto viaCombined =
      engine.query(engine.parseAndPlanQuery(query), ad_utility::MediaType::tsv);
  auto viaSplit = engine.query(engine.planQuery(engine.parseQuery(query)),
                               ad_utility::MediaType::tsv);
  EXPECT_EQ(viaCombined, viaSplit);
  EXPECT_EQ(viaCombined, "?s\n<s>\n<s2>\n");

  // `requestTimer` reaches the query planner through `planQuery` and ends up in
  // the runtime information, just as it does via `parseAndPlanQuery`.
  ad_utility::Timer requestTimer{ad_utility::Timer::Started};
  auto plan =
      engine.planQuery(engine.parseQuery(query),
                       std::make_shared<ad_utility::CancellationHandle<>>(),
                       std::nullopt, requestTimer);
  EXPECT_GT(plan.queryExecutionTree()
                .getRootOperation()
                ->getRuntimeInfoWholeQuery()
                .timeQueryPlanning.count(),
            -1);

  // A cancellation handle that is already cancelled makes planning fail, which
  // shows that the handle reaches the query planner as well.
  auto cancelledHandle = std::make_shared<ad_utility::CancellationHandle<>>();
  cancelledHandle->cancel(ad_utility::CancellationState::MANUAL);
  AD_EXPECT_THROW_WITH_MESSAGE(
      engine.planQuery(engine.parseQuery(query), cancelledHandle),
      HasSubstr("manually cancelled"));
}

// _____________________________________________________________________________
// Test `bindParsedQuery`: a query that was parsed once can be planned on a
// second `Qlever` instance, as long as that instance has an equivalent
// `EncodedIriManager` (see the note on reusing a parsed query in `parseQuery`).
TEST(LibQlever, bindParsedQueryReusesAParsedQuery) {
  // Two indexes over the same data and with the same configuration, so their
  // `EncodedIriManager`s are equivalent.
  std::string_view turtle = "<s> <p> <o> . <s2> <p> <o2> .";
  Qlever first{buildTestIndex(turtle, ".first")};
  Qlever second{buildTestIndex(turtle, ".second")};

  std::string query = "SELECT ?s WHERE { ?s <p> ?o }";
  std::string expected = "?s\n<s>\n<s2>\n";

  // Parse once on `first`, then plan on both instances.
  ParsedQueryAndContext parsedOnFirst = first.parseQuery(query);
  EXPECT_EQ(
      first.query(first.planQuery(parsedOnFirst), ad_utility::MediaType::tsv),
      expected);

  // `bindParsedQuery` pairs the parsed query with a context of `second`, so the
  // parsing is not repeated. The context of the plan is one of `second`, not
  // the one the query was parsed with.
  ParsedQueryAndContext boundToSecond =
      second.bindParsedQuery(parsedOnFirst.parsedQuery());
  EXPECT_NE(&boundToSecond.queryExecutionContext(),
            &parsedOnFirst.queryExecutionContext());
  PlannedQuery planOnSecond = second.planQuery(boundToSecond);
  EXPECT_EQ(&planOnSecond.queryExecutionContext(),
            &boundToSecond.queryExecutionContext());
  EXPECT_EQ(second.query(planOnSecond, ad_utility::MediaType::tsv), expected);
}

// _____________________________________________________________________________
// Test `EngineConfig::computeSortPerformanceEstimators_`: the (potentially
// expensive) estimates are computed by default, but not if the config disables
// them.
TEST(LibQlever, computeSortPerformanceEstimators) {
  EngineConfig ec = buildTestIndex("<s> <p> <o> .");
  ASSERT_TRUE(ec.computeSortPerformanceEstimators_);
  EXPECT_TRUE(Qlever{ec}.sortPerformanceEstimator().estimatesWereCalculated());

  ec.computeSortPerformanceEstimators_ = false;
  EXPECT_FALSE(Qlever{ec}.sortPerformanceEstimator().estimatesWereCalculated());
}

// _____________________________________________________________________________
// Test the trivial getters of `ParsedQueryAndContext`, both the `const` and the
// non-`const` overloads. All of them refer to the same objects.
TEST(LibQlever, parsedQueryAndContextGetters) {
  Qlever engine{buildTestIndex("<s> <p> <o> .")};

  std::string query = "SELECT ?s WHERE { ?s <p> ?o }";
  ParsedQueryAndContext parsedQuery = engine.parseQuery(query);
  const ParsedQueryAndContext& constParsedQuery = parsedQuery;

  // The non-`const` and the `const` getter yield the same `ParsedQuery`, which
  // is the one that was parsed from `query`.
  EXPECT_EQ(&parsedQuery.parsedQuery(), &constParsedQuery.parsedQuery());
  EXPECT_EQ(constParsedQuery.parsedQuery()._originalString, query);
  EXPECT_TRUE(constParsedQuery.parsedQuery().hasSelectClause());

  // The same holds for the `QueryExecutionContext`, which is also the one that
  // the (only `const`) getter for the `shared_ptr` yields.
  EXPECT_EQ(&parsedQuery.queryExecutionContext(),
            &constParsedQuery.queryExecutionContext());
  EXPECT_EQ(constParsedQuery.sharedQueryExecutionContext().get(),
            &constParsedQuery.queryExecutionContext());
}

// _____________________________________________________________________________
// Test `Qlever::clearCache`, and trivially the `const` getter for the named
// result cache.
TEST(LibQlever, clearCache) {
  Qlever engine{buildTestIndex("<s> <p> <o> . <s2> <p> <o2> .")};

  // The cache starts out empty.
  ASSERT_EQ(engine.cache().numPinnedEntries(), 0U);
  ASSERT_EQ(engine.cache().numNonPinnedEntries(), 0U);

  // Run a query with `pinResult`, so that its result is stored in the cache as
  // a pinned entry.
  PlannedQuery plan = engine.planQuery(engine.parseQuery(
      "SELECT ?s WHERE { ?s <p> ?o }", {}, ad_utility::noop, false, true));
  EXPECT_EQ(engine.query(plan, ad_utility::MediaType::tsv), "?s\n<s>\n<s2>\n");
  EXPECT_GT(engine.cache().numPinnedEntries(), 0U);

  // `clearCache` clears the pinned as well as the unpinned entries.
  engine.clearCache();
  EXPECT_EQ(engine.cache().numPinnedEntries(), 0U);
  EXPECT_EQ(engine.cache().numNonPinnedEntries(), 0U);

  // The named result cache is a separate cache, and its `const` getter yields
  // the same cache as the non-`const` one.
  const Qlever& constEngine = engine;
  EXPECT_EQ(&constEngine.namedResultCache(), &engine.namedResultCache());
}

// _____________________________________________________________________________
// Test that `Qlever::applyUpdate` is directly usable through `Qlever`,
// independent of the HTTP `Server` layer (which only wraps this in
// thread/timer/response-formatting concerns, see `Server::processUpdate`).
TEST(LibQlever, applyUpdate) {
  // Never persist updates to disk in this test (would leave files behind
  // after the test, and pollute a re-run of the same test that reuses the
  // same on-disk base name).
  auto config = buildTestIndex("<s> <p> <o> .");
  config.persistUpdates_ = false;
  Qlever engine{config};

  // Populate the cache with a pinned query result, so that clearing the
  // cache as a side effect of `applyUpdate` is actually observable below.
  PlannedQuery plan = engine.planQuery(engine.parseQuery(
      "SELECT ?s WHERE { ?s <p> ?o }", {}, ad_utility::noop, false, true));
  engine.query(plan, ad_utility::MediaType::tsv);
  ASSERT_GT(engine.cache().numPinnedEntries(), 0U);

  // `Qlever::parseQuery`/`parseAndPlanQuery` only accept SPARQL queries, not
  // updates (see `SparqlParser::parseQuery` vs. `parseUpdate`), so an update
  // has to be parsed separately and then planned via `bindParsedQuery`.
  ad_utility::BlankNodeManager bnm;
  auto parsedUpdates =
      SparqlParser::parseUpdate(&bnm, ad_utility::testing::encodedIriManager(),
                                "INSERT DATA { <a> <b> <c> }");
  ASSERT_THAT(parsedUpdates, SizeIs(1));
  auto plannedUpdate =
      engine.planQuery(engine.bindParsedQuery(std::move(parsedUpdates[0])));
  ASSERT_TRUE(plannedUpdate.parsedQuery().hasUpdateClause());

  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  // NOTE: This takes a fresh index snapshot, independent of the one
  // `plannedUpdate` was planned against a few lines above. In general this
  // is not thread-safe: if a concurrent index rebuild swapped in a new
  // `IndexAndViews` between the two calls, `deltaTriples` here would belong
  // to a different `Index` than the one `plannedUpdate` was planned
  // against, violating `applyUpdate`'s precondition. This test gets away
  // with it because it is single-threaded and nothing swaps the index in
  // between. `PlannedQuery`/`QueryExecutionContext` currently don't expose
  // a way to get back the exact snapshot a query was planned against (only
  // a `const Index&`), so there is no easy way to avoid the second
  // snapshot here yet. A future API that threads the `IndexAndViews`
  // snapshot explicitly through parsing/planning/execution would close
  // this gap, but that is a larger redesign, out of scope for now.
  auto snapshot = engine.indexAndViewsSnapshot();
  UpdateMetadata updateMetadata =
      snapshot->index_.deltaTriplesManager().modify<UpdateMetadata>(
          [&](DeltaTriples& deltaTriples) {
            return engine.applyUpdate(plannedUpdate, handle, deltaTriples);
          });

  EXPECT_THAT(updateMetadata.countBefore_,
              Optional(Eq(DeltaTriplesCount{0, 0})));
  EXPECT_THAT(updateMetadata.countAfter_,
              Optional(Eq(DeltaTriplesCount{1, 0})));

  // The query result cache is invalidated as a side effect of `applyUpdate`.
  EXPECT_EQ(engine.cache().numPinnedEntries(), 0U);
  EXPECT_EQ(engine.cache().numNonPinnedEntries(), 0U);
}

namespace {
// Parse and plan `update` and apply it to `engine` via `Qlever::applyUpdate`,
// returning the metadata. For why the update has to be parsed separately and
// for the thread-safety caveat of taking the snapshot only here, see the
// comments in `LibQlever.applyUpdate` above.
UpdateMetadata applyUpdateToEngine(Qlever& engine, const std::string& update) {
  ad_utility::BlankNodeManager bnm;
  auto parsedUpdates = SparqlParser::parseUpdate(
      &bnm, ad_utility::testing::encodedIriManager(), update);
  AD_CORRECTNESS_CHECK(parsedUpdates.size() == 1);
  auto plannedUpdate =
      engine.planQuery(engine.bindParsedQuery(std::move(parsedUpdates[0])));
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  auto snapshot = engine.indexAndViewsSnapshot();
  return snapshot->index_.deltaTriplesManager().modify<UpdateMetadata>(
      [&](DeltaTriples& deltaTriples) {
        return engine.applyUpdate(plannedUpdate, handle, deltaTriples);
      });
}
}  // namespace

// _____________________________________________________________________________
// Direct counterpart to `ServerTest.clearDeltaTriples`: populate the delta
// triples via `applyUpdate` and clear them directly through `Qlever`,
// independent of the HTTP `Server` layer.
TEST(LibQlever, clearDeltaTriples) {
  auto config = buildTestIndex("<s> <p> <o> .");
  config.persistUpdates_ = false;
  Qlever engine{config};

  auto metadata = applyUpdateToEngine(engine, "INSERT DATA { <a> <b> <c> }");
  EXPECT_THAT(metadata.countAfter_, Optional(Eq(DeltaTriplesCount{1, 0})));

  EXPECT_THAT(engine.clearDeltaTriples(), Eq(DeltaTriplesCount{0, 0}));
}

// _____________________________________________________________________________
// Direct counterpart to `ServerTest.vacuumDeltaTriples`: insert a triple that
// is already in the index (a redundant insertion that `vacuum` removes) and
// vacuum directly through `Qlever`, independent of the HTTP `Server` layer.
TEST(LibQlever, vacuumDeltaTriples) {
  auto config = buildTestIndex("<a> <b> <c> .");
  config.persistUpdates_ = false;
  Qlever engine{config};

  // Without this, the single block of the (tiny) test index doesn't meet the
  // minimum size for `vacuum` to process it.
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::vacuumMinimumBlockSize_>(
          size_t{0});

  auto metadata = applyUpdateToEngine(engine, "INSERT DATA { <a> <b> <c> }");
  EXPECT_THAT(metadata.countAfter_, Optional(Eq(DeltaTriplesCount{1, 0})));

  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  auto stats = engine.vacuumDeltaTriples(handle);
  EXPECT_EQ(stats["external"]["insertionsRemoved"], 1);
  EXPECT_THAT(engine.indexAndViewsSnapshot()
                  ->index_.deltaTriplesManager()
                  .getCurrentLocatedTriplesSharedState()
                  ->counts_,
              Optional(Eq(DeltaTriplesCount{0, 0})));
}

// _____________________________________________________________________________
// The non-`const` getter for the allocator yields a mutable reference to the
// allocator of the engine, so that memory allocated through it counts towards
// the engine's memory limit, which the `const` getter observes.
TEST(LibQlever, allocatorGetter) {
  Qlever engine{buildTestIndex("<s> <p> <o> .")};
  const Qlever& constEngine = engine;
  ASSERT_EQ(&engine.allocator(), &constEngine.allocator());

  auto& allocator = engine.allocator();
  const auto memoryLeftBefore = constEngine.allocator().amountMemoryLeft();
  constexpr size_t numIds = 16;
  Id* ptr = allocator.allocate(numIds);
  EXPECT_EQ(
      constEngine.allocator().amountMemoryLeft(),
      memoryLeftBefore - ad_utility::MemorySize::bytes(numIds * sizeof(Id)));
  allocator.deallocate(ptr, numIds);
  EXPECT_EQ(constEngine.allocator().amountMemoryLeft(), memoryLeftBefore);
}

// _____________________________________________________________________________
// The `clearOnAllocation` hook that `Qlever` installs in its allocator (see the
// constructor in `Qlever.cpp`) evicts entries from the query result cache when
// an allocation would otherwise exceed the memory limit.
TEST(LibQlever, allocatorMakesRoomByClearingTheCache) {
  EngineConfig config = buildTestIndex("<s> <p> <o> . <s2> <p> <o2> .");
  config.memoryLimit_ = ad_utility::MemorySize::megabytes(1);
  Qlever engine{config};

  // Run a query, the result of which is then stored in the cache as an unpinned
  // entry. The memory of that result is held by the engine's allocator.
  EXPECT_EQ(
      engine.query("SELECT ?s WHERE { ?s <p> ?o }", ad_utility::MediaType::tsv),
      "?s\n<s>\n<s2>\n");
  ASSERT_GT(engine.cache().numNonPinnedEntries(), 0U);

  // Request slightly more memory than is left. This exceeds the limit, so the
  // hook is called, which makes room by evicting the cache entry, after which
  // the allocation succeeds.
  auto& allocator = engine.allocator();
  const size_t numIds =
      allocator.amountMemoryLeft().getBytes() / sizeof(Id) + 1;
  Id* ptr = allocator.allocate(numIds);
  EXPECT_EQ(engine.cache().numNonPinnedEntries(), 0U);
  allocator.deallocate(ptr, numIds);
}

// _____________________________________________________________________________
// `Qlever::makeIndexRebuildConfig` is a thin wrapper around
// `qlever::makeIndexSwapConfig` (which is tested in detail in
// `test/index/IndexSwapTest.cpp`): it takes the base name of the current index
// from the `Index` and fills in the naming scheme of `qlever-server`'s index
// rebuild. Only that is tested here. The test runs in a fresh working
// directory, because the default directories are created relative to the
// directory of the current index.
TEST(Qlever, makeIndexRebuildConfig) {
  auto cleanup = ad_utility::testing::useFreshWorkingDirectory();
  Index index = ad_utility::testing::makeTestIndex("index", "<a> <b> <c> .");
  auto makeConfig = [&index](
                        std::optional<std::string> rebuildTmpDir,
                        std::optional<std::string> rebuildPreviousIndexDir) {
    return Qlever::makeIndexRebuildConfig(index, std::move(rebuildTmpDir),
                                          std::move(rebuildPreviousIndexDir));
  };

  // The base name of the current index is where the old index is taken from
  // and where the new index has to end up, the new index is staged in
  // `rebuild.<current datetime>.tmp`, and the old index is retired to
  // `previous.<datetime of the build of the current index>`.
  auto config = makeConfig(std::nullopt, std::nullopt);
  EXPECT_EQ(config.oldIndexSource(), index.getOnDiskBase());
  EXPECT_EQ(config.newIndexTarget(), index.getOnDiskBase());
  EXPECT_THAT(config.newIndexSource(),
              AllOf(StartsWith("rebuild."), EndsWith(".tmp/index")));
  EXPECT_EQ(
      config.oldIndexTarget(),
      absl::StrCat("previous.", index.getImpl().dateOfIndexBuild(), "/index"));

  // The two directories can also be given explicitly, in which case the file
  // name of the current index is used inside them.
  EXPECT_EQ(makeConfig("tmpForRebuild", "oldIndex").newIndexSource(),
            "tmpForRebuild/index");
  EXPECT_EQ(makeConfig("tmpForRebuild", "oldIndex").oldIndexTarget(),
            "oldIndex/index");

  // If the default directory for the old index and all of its uniquified
  // variants are taken, the error message points at the command parameter with
  // which the directory can be chosen explicitly.
  std::string defaultPreviousDir =
      absl::StrCat("previous.", index.getImpl().dateOfIndexBuild());
  ql::filesystem::create_directory(defaultPreviousDir);
  ad_utility::makeOfstream(defaultPreviousDir + "/index.meta-data.json")
      << "occupied";
  for (size_t i = 1; i <= 99; ++i) {
    ql::filesystem::create_directory(absl::StrCat(defaultPreviousDir, ".", i));
  }
  AD_EXPECT_THROW_WITH_MESSAGE(makeConfig(std::nullopt, std::nullopt),
                               AllOf(HasSubstr("all already exist"),
                                     HasSubstr("rebuild-previous-index-dir")));
}
