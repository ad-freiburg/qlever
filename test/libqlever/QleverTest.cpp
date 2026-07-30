// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include <memory>

#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "backports/filesystem.h"
#include "engine/ExternalValues.h"
#include "engine/MaterializedViews.h"
#include "global/FileSuffixConstants.h"
#include "index/IndexImpl.h"
#include "libqlever/Qlever.h"
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

// Create a fresh (empty) directory named after the currently running test and
// make it the working directory. The returned cleanup first restores the
// previous working directory and then removes that directory again, so both
// steps live in a single cleanup to fix their order. Needed by the tests that
// deal with base names without a directory component, as those are resolved
// against the working directory.
[[nodiscard]] auto useFreshWorkingDirectory() {
  auto oldCwd = ql::filesystem::current_path();
  std::string folder = gtestCurrentTestName();
  // Leftovers from a previous run would break the checks for directories that
  // must not exist yet.
  ql::filesystem::remove_all(folder);
  ql::filesystem::create_directory(folder);
  ql::filesystem::current_path(folder);
  return absl::Cleanup{
      [oldCwd = std::move(oldCwd), folder = std::move(folder)] {
        ql::filesystem::current_path(oldCwd);
        ql::filesystem::remove_all(folder);
      }};
}
}  // namespace

// _____________________________________________________________________________
// Build an "old" index and a freshly "rebuilt" index (in a temporary
// directory), then move the rebuilt index into the place of the old one and
// check the resulting on-disk layout and the re-anchored in-memory state.
TEST(Qlever, moveRebuiltIndexIntoPlace) {
  std::string baseFolder = gtestCurrentTestName();
  absl::Cleanup removeFiles{
      [&baseFolder] { ql::filesystem::remove_all(baseFolder); }};
  auto setup = setUpRebuild(baseFolder);

  // Use a base name for the old index that lives in a not-yet-existing
  // directory AND uses a different file-name prefix than the original index.
  // This exercises that the individual files are re-prefixed, not just moved.
  std::string oldIndexBackup = baseFolder + "/previous/old-index";
  std::string newBase = baseFolder + "/index";
  qlever::IndexRebuildConfig config{setup.oldBase_, setup.rebuiltBase_,
                                    oldIndexBackup, newBase};

  // The old index carries a build log, and the rebuilt index a rebuild log;
  // both must travel with their respective index (exercising the log-moving
  // branches).
  auto touch = [](const std::string& path) {
    ad_utility::makeOfstream(path) << "log";
  };
  touch(setup.oldBase_ + INDEX_LOG_SUFFIX);
  touch(setup.rebuiltBase_ + REBUILD_INDEX_LOG_SUFFIX);

  // Enable persistence of updates for the rebuilt index, so that the re-anchor
  // of the persisted-updates filenames is exercised.
  setup.indexAndViews_->index_.getImpl().setFilenamesForPersistentUpdates(
      false);
  ASSERT_TRUE(setup.indexAndViews_->index_.deltaTriplesManager().persists());

  qlever::Qlever::moveRebuiltIndexIntoPlace(*setup.indexAndViews_, config);

  // The old index's files were moved to the base name for the old index, with
  // their file-name prefix changed to match that base name. This includes the
  // build log.
  EXPECT_TRUE(ql::filesystem::exists(oldIndexBackup + CONFIGURATION_FILE));
  EXPECT_TRUE(ql::filesystem::exists(oldIndexBackup + ".index.pso"));
  EXPECT_TRUE(ql::filesystem::exists(oldIndexBackup + INDEX_LOG_SUFFIX));

  // The rebuilt index now lives at the final base name (the place of the old
  // index) and no longer in the temporary directory. Its rebuild log traveled
  // with it to the final base name.
  EXPECT_TRUE(ql::filesystem::exists(newBase + CONFIGURATION_FILE));
  EXPECT_TRUE(ql::filesystem::exists(newBase + ".index.pso"));
  EXPECT_TRUE(ql::filesystem::exists(newBase + REBUILD_INDEX_LOG_SUFFIX));
  // The temporary directory in which the new index was built became empty by
  // the move and was therefore removed (which also implies that no file of the
  // new index was left behind in it).
  EXPECT_FALSE(ql::filesystem::exists(baseFolder + "/rebuild.tmp"));

  // The in-memory state of the new index was re-anchored to the final base, and
  // it still persists its updates (now under the new base name).
  EXPECT_EQ(setup.indexAndViews_->index_.getOnDiskBase(), newBase);
  EXPECT_TRUE(setup.indexAndViews_->index_.deltaTriplesManager().persists());
}

// _____________________________________________________________________________
// The base name for the retired old index may also be a plain directory (given
// with a trailing separator). The directory does not exist yet and has to be
// created; the old index's files then live directly inside it, with an empty
// file-name prefix (e.g. `<dir>/.index.pso`).
TEST(Qlever, moveRebuiltIndexIntoPlaceWithDirectoryBasename) {
  std::string baseFolder = gtestCurrentTestName();
  absl::Cleanup removeFiles{
      [&baseFolder] { ql::filesystem::remove_all(baseFolder); }};
  auto setup = setUpRebuild(baseFolder);

  std::string oldDir = baseFolder + "/previous/";
  std::string newBase = baseFolder + "/index";
  qlever::IndexRebuildConfig config{setup.oldBase_, setup.rebuiltBase_, oldDir,
                                    newBase};

  // Complementary to `moveRebuiltIndexIntoPlace` above: here neither index has
  // a log file and the rebuilt index does not persist its updates, so this test
  // covers the "no log file to move" and "index does not persist" branches
  // (whereas the other test covers their counterparts). Do not add log files or
  // enable persistence here, or that negative coverage is lost.
  qlever::Qlever::moveRebuiltIndexIntoPlace(*setup.indexAndViews_, config);

  // The (previously non-existent) directory was created and the old index's
  // files now live inside it (with an empty base-name prefix).
  EXPECT_TRUE(ql::filesystem::is_directory(oldDir));
  EXPECT_TRUE(ql::filesystem::exists(oldDir + CONFIGURATION_FILE));
  EXPECT_TRUE(ql::filesystem::exists(oldDir + ".index.pso"));

  // The new index is installed at its final base name as usual, and no rebuild
  // log was created for it.
  EXPECT_TRUE(ql::filesystem::exists(newBase + CONFIGURATION_FILE));
  EXPECT_TRUE(ql::filesystem::exists(newBase + ".index.pso"));
  EXPECT_FALSE(ql::filesystem::exists(newBase + REBUILD_INDEX_LOG_SUFFIX));
  EXPECT_FALSE(setup.indexAndViews_->index_.deltaTriplesManager().persists());
}

// _____________________________________________________________________________
// The standard production layout: the index is served with a BARE base name
// (no directory component) from the current working directory, which is how
// `qlever-control` starts the server. The file enumeration must then return
// bare file names as well, otherwise the base-name prefix substitution of the
// move fails on the globbed files (vocabulary, views).
TEST(Qlever, moveRebuiltIndexIntoPlaceWithBareBasename) {
  auto cleanup = useFreshWorkingDirectory();
  ad_utility::testing::makeTestIndex("index", "<a> <b> <c> .");
  ql::filesystem::create_directory("rebuild.tmp");
  Index rebuilt = ad_utility::testing::makeTestIndex(
      "rebuild.tmp/index", "<a> <b> <c> . <d> <e> <f> .");
  auto indexAndViews = std::make_shared<qlever::Qlever::IndexAndViews>(
      std::move(rebuilt), MaterializedViewsManager{"rebuild.tmp/index"});

  qlever::IndexRebuildConfig config{"index", "rebuild.tmp/index",
                                    "previous/index", "index"};
  qlever::Qlever::moveRebuiltIndexIntoPlace(*indexAndViews, config);

  // The old index (including its vocabulary, which is enumerated via the glob)
  // was moved away completely, and the new index is installed in its place.
  EXPECT_TRUE(ql::filesystem::exists(std::string{"previous/index"} +
                                     std::string{CONFIGURATION_FILE}));
  EXPECT_FALSE(
      qlever::util::filesWithBaseNameAndSuffix("previous/index", VOCAB_SUFFIX)
          .empty());
  EXPECT_TRUE(ql::filesystem::exists(std::string{"index"} +
                                     std::string{CONFIGURATION_FILE}));
  EXPECT_FALSE(
      qlever::util::filesWithBaseNameAndSuffix("index", VOCAB_SUFFIX).empty());
  EXPECT_TRUE(IndexImpl::allIndexFiles("rebuild.tmp/index").empty());
  EXPECT_EQ(indexAndViews->index_.getOnDiskBase(), "index");
  // A bare base name has no directory component, so nothing has to be created
  // for it. In particular, the base name itself must not be mistaken for a
  // directory to create.
  EXPECT_FALSE(ql::filesystem::exists("index"));
}

// _____________________________________________________________________________
// The freshly rebuilt index may also live at a BARE base name (no directory
// component), i.e. directly in the working directory. There is then no
// temporary directory that the new index has to be taken out of, so the removal
// of that directory has to be skipped (in particular, the working directory
// itself must not be removed).
TEST(Qlever, moveRebuiltIndexIntoPlaceWithBareNewIndexSource) {
  auto cleanup = useFreshWorkingDirectory();
  ad_utility::testing::makeTestIndex("index", "<a> <b> <c> .");
  Index rebuilt = ad_utility::testing::makeTestIndex(
      "rebuilt", "<a> <b> <c> . <d> <e> <f> .");
  auto indexAndViews = std::make_shared<qlever::Qlever::IndexAndViews>(
      std::move(rebuilt), MaterializedViewsManager{"rebuilt"});

  qlever::IndexRebuildConfig config{"index", "rebuilt", "previous/index",
                                    "index"};
  auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
  qlever::Qlever::moveRebuiltIndexIntoPlace(*indexAndViews, config);

  // The swap happened as usual: the old index was moved away and the new index
  // took its place, with nothing left at the base name it was built under.
  EXPECT_TRUE(ql::filesystem::exists(std::string{"previous/index"} +
                                     std::string{CONFIGURATION_FILE}));
  EXPECT_TRUE(ql::filesystem::exists(std::string{"index"} +
                                     std::string{CONFIGURATION_FILE}));
  EXPECT_TRUE(IndexImpl::allIndexFiles("rebuilt").empty());
  EXPECT_EQ(indexAndViews->index_.getOnDiskBase(), "index");
  // No directory removal was attempted, hence also no warning about a failed
  // one, and the working directory is of course still there.
  EXPECT_THAT(logStream.str(),
              Not(HasSubstr("Could not remove the directory")));
  EXPECT_TRUE(ql::filesystem::is_directory(ql::filesystem::current_path()));
}

// _____________________________________________________________________________
// If the directory in which the new index was built cannot be removed after the
// new index has been moved out of it (here because it still holds a file that
// is not part of the index), the swap is still complete and only a warning is
// logged.
TEST(Qlever, moveRebuiltIndexIntoPlaceWithNonRemovableBuildDirectory) {
  SKIP_IF_LOGLEVEL_IS_LOWER(WARN);
  std::string baseFolder = gtestCurrentTestName();
  absl::Cleanup removeFiles{
      [&baseFolder] { ql::filesystem::remove_all(baseFolder); }};
  auto setup = setUpRebuild(baseFolder);

  // Put a file that is not part of the index into the temporary directory of
  // the rebuild: it is not moved along and hence keeps the directory from being
  // removed (unlike in `moveRebuiltIndexIntoPlace`, where the directory becomes
  // empty and is removed).
  std::string unrelatedFile = baseFolder + "/rebuild.tmp/unrelated.txt";
  ad_utility::makeOfstream(unrelatedFile) << "keep me";

  std::string oldIndexBackup = baseFolder + "/previous/index";
  std::string newBase = baseFolder + "/index";
  qlever::IndexRebuildConfig config{setup.oldBase_, setup.rebuiltBase_,
                                    oldIndexBackup, newBase};

  auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
  qlever::Qlever::moveRebuiltIndexIntoPlace(*setup.indexAndViews_, config);
  EXPECT_THAT(logStream.str(), HasSubstr("Could not remove the directory"));

  // Both indexes still ended up where they belong, and only the unrelated file
  // was left behind.
  EXPECT_TRUE(ql::filesystem::exists(oldIndexBackup + CONFIGURATION_FILE));
  EXPECT_TRUE(ql::filesystem::exists(newBase + CONFIGURATION_FILE));
  EXPECT_TRUE(IndexImpl::allIndexFiles(setup.rebuiltBase_).empty());
  EXPECT_TRUE(ql::filesystem::exists(unrelatedFile));
}

// _____________________________________________________________________________
// The `IndexRebuildConfig` constructor rejects base-name combinations that
// would collide destructively. Because the validation lives in the constructor,
// this needs no index on disk at all.
TEST(Qlever, indexRebuildConfigRejectsCollidingBaseNames) {
  using qlever::IndexRebuildConfig;
  // The four positional arguments are: current index, rebuilt index, retired
  // old index, new index. The common (valid) case has the new index served
  // from the place of the current index.
  EXPECT_NO_THROW(
      IndexRebuildConfig("index", "tmp/index", "previous/old", "index"));

  // The currently served index and the freshly rebuilt index must differ.
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexRebuildConfig("index", "index", "previous/old", "index"),
      ::testing::HasSubstr(
          "currently served index and the freshly rebuilt index"));

  // The retired-old-index base name must differ from the currently served
  // index, ...
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexRebuildConfig("index", "tmp/index", "index", "index"),
      ::testing::HasSubstr("differ from the currently served index"));

  // ... from the freshly rebuilt index, ...
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexRebuildConfig("index", "tmp/index", "tmp/index", "index"),
      ::testing::HasSubstr("differ from the freshly rebuilt index"));

  // ... and from the new index.
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexRebuildConfig("index", "tmp/index", "shared", "shared"),
      ::testing::HasSubstr("retired old index and the new index must differ"));

  // Collisions are detected up to lexical path normalization, so `abc/../index`
  // (which denotes `index`) collides with the currently served index `index`.
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexRebuildConfig("index", "tmp/index", "abc/../index", "index"),
      ::testing::HasSubstr("differ from the currently served index"));

  // Base names that merely share a string prefix (e.g. `index` and `indexdata`)
  // do NOT collide: the `.`-delimited file-name suffixes keep the two indexes'
  // files apart, so such a configuration is valid.
  EXPECT_NO_THROW(
      IndexRebuildConfig("index", "tmp/index", "indexdata", "index"));

  // But a base name that is another base name followed by a '.' DOES collide:
  // `index.view` sits inside the `index.view.*` glob that enumerates `index`'s
  // materialized views, so retiring the old index to `index.view` would sweep
  // up the current index's view files.
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexRebuildConfig("index", "tmp/index", "index.view", "index"),
      ::testing::HasSubstr("differ from the currently served index"));
  // The same holds regardless of which of the two base names is the longer one.
  AD_EXPECT_THROW_WITH_MESSAGE(
      IndexRebuildConfig("index", "index.view", "previous/old", "newidx"),
      ::testing::HasSubstr("currently served index and the freshly rebuilt"));
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
// `Qlever::makeIndexRebuildConfig` turns the two directories that a rebuild can
// be configured with into base names (by appending the file name of the current
// index) and validates them. The test runs in a fresh working directory,
// because the directories are resolved against the working directory and have
// to lie inside the directory of the current index (which is the working
// directory here as the index is served from the bare base name `index`).
TEST(Qlever, makeIndexRebuildConfig) {
  auto cleanup = useFreshWorkingDirectory();
  Index index = ad_utility::testing::makeTestIndex("index", "<a> <b> <c> .");
  auto makeConfig = [&index](
                        std::optional<std::string> rebuildTmpDir,
                        std::optional<std::string> rebuildPreviousIndexDir) {
    return Qlever::makeIndexRebuildConfig(index, std::move(rebuildTmpDir),
                                          std::move(rebuildPreviousIndexDir));
  };

  // Both directories default to a directory that does not exist yet, derived
  // from the current time resp. the build date of the current index. The new
  // index ends up at the base name the current index is served from.
  {
    auto config = makeConfig(std::nullopt, std::nullopt);
    EXPECT_EQ(config.oldIndexSource(), "index");
    EXPECT_EQ(config.newIndexTarget(), "index");
    EXPECT_THAT(config.newIndexSource(),
                AllOf(StartsWith("rebuild."), EndsWith(".tmp/index")));
    EXPECT_THAT(config.oldIndexTarget(),
                AllOf(StartsWith("previous."), EndsWith("/index")));
    // The success response reports the directory of the old index (which the
    // client cannot know in advance when it was defaulted), not its base name.
    auto response = config.successResponseAsJson();
    EXPECT_THAT(response["previous-index-dir"].get<std::string>(),
                AllOf(StartsWith("previous."), Not(EndsWith("/index"))));
  }

  // Explicitly given directories that do not exist yet are accepted as is (the
  // rebuild creates them), and the file name of the current index is used
  // inside them.
  {
    auto config = makeConfig("tmpForRebuild", "oldIndex");
    EXPECT_EQ(config.newIndexSource(), "tmpForRebuild/index");
    EXPECT_EQ(config.oldIndexTarget(), "oldIndex/index");
    EXPECT_EQ(config.successResponseAsJson()["previous-index-dir"], "oldIndex");
  }

  // Directories that already exist are fine as long as they are empty.
  ql::filesystem::create_directory("emptyTmpForRebuild");
  ql::filesystem::create_directory("emptyOldIndex");
  EXPECT_NO_THROW(makeConfig("emptyTmpForRebuild", "emptyOldIndex"));

  // But a directory that exists and is not empty is rejected, no matter which
  // of the two directories it is.
  ql::filesystem::create_directory("notEmpty");
  ad_utility::makeOfstream("notEmpty/someFile") << "not empty";
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeConfig("notEmpty", std::nullopt),
      ::testing::HasSubstr("already exists and is not empty"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeConfig(std::nullopt, "notEmpty"),
      ::testing::HasSubstr("already exists and is not empty"));

  // An empty directory string yields a base name without a directory component
  // (here simply `index`), which refers to the working directory itself. That
  // directory exists and is not empty (it holds the current index), so it is
  // rejected with the same message as above, and the message names the working
  // directory.
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeConfig("", std::nullopt),
      AllOf(HasSubstr(ql::filesystem::current_path().string()),
            HasSubstr("already exists and is not empty")));

  // The directories must be relative paths, ...
  AD_EXPECT_THROW_WITH_MESSAGE(makeConfig("/absolute", std::nullopt),
                               HasSubstr("must be a relative path"));
  AD_EXPECT_THROW_WITH_MESSAGE(makeConfig(std::nullopt, "/absolute"),
                               HasSubstr("must be a relative path"));

  // ... and must lie inside the directory of the current index.
  AD_EXPECT_THROW_WITH_MESSAGE(makeConfig("../outside", std::nullopt),
                               HasSubstr("not a subdirectory"));
}
