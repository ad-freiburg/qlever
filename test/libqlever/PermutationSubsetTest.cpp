// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests for indexes that only have a subset of the permutations (see
// `IndexImpl::permutationsToBuildOrLoad`).

#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "./QleverTestHelpers.h"
#include "index/IndexImpl.h"
#include "libqlever/Qlever.h"

using namespace qlever;
using ::testing::ElementsAreArray;
using ::testing::HasSubstr;
using ::testing::Not;

namespace {

// A small knowledge graph that contains repeated terms within a triple, so
// that the queries below also cover the handling of repeated variables.
constexpr std::string_view knowledgeGraph =
    "<a> <p> <b> . <a> <p> <c> . <a> <q> \"x\" . <b> <p> <a> . <c> <q> <c> . "
    "<a> <a> <a> . <b> <b> <d> . <d> <p> <d> . <c> <a> 42 .";

// Queries that cover all combinations of fixed positions and repeated
// variables. All variables are ordered, so that the results are
// deterministic.
const std::vector<std::string> defaultQueries{
    "SELECT * { ?s ?p ?o } ORDER BY ?s ?p ?o",
    "SELECT * { <a> ?p ?o } ORDER BY ?p ?o",
    "SELECT * { ?s <p> ?o } ORDER BY ?s ?o",
    "SELECT * { ?s ?p <c> } ORDER BY ?s ?p",
    "SELECT * { <a> <p> ?o } ORDER BY ?o",
    "SELECT * { <a> ?p <b> } ORDER BY ?p",
    "SELECT * { ?s <q> <c> } ORDER BY ?s",
    "SELECT * { <a> <p> <b> }",
    "SELECT * { ?s ?p 42 } ORDER BY ?s ?p",
    "SELECT * { ?x ?p ?x } ORDER BY ?x ?p",
    "SELECT * { ?x ?x ?o } ORDER BY ?x ?o",
    "SELECT * { ?s ?x ?x } ORDER BY ?s ?x",
    "SELECT * { ?x ?x ?x } ORDER BY ?x",
    "SELECT * { <a> ?x ?x } ORDER BY ?x",
    "SELECT * { ?x <p> ?x } ORDER BY ?x",
    "SELECT * { ?x ?x <d> } ORDER BY ?x",
    "SELECT * { ?s ?p ?o . ?o ?p2 ?o2 } ORDER BY ?s ?p ?o ?p2 ?o2",
    "SELECT ?s (COUNT(?o) AS ?c) { ?s ?p ?o } GROUP BY ?s ORDER BY ?s",
    "SELECT ?p (COUNT(?s) AS ?c) { ?s ?p ?o } GROUP BY ?p ORDER BY ?p",
    "SELECT ?o (COUNT(?s) AS ?c) { ?s ?p ?o } GROUP BY ?o ORDER BY ?o",
    "SELECT * { ?s <p>* ?o } ORDER BY ?s ?o",
    "SELECT * { ?s <p>* <a> } ORDER BY ?s",
    "SELECT DISTINCT ?g { GRAPH ?g { ?s ?p ?o } } ORDER BY ?g",
    "DESCRIBE <a>",
};

// Return the results of all the `queries` on the `engine` in TSV format. The
// lines of each result are sorted, because the order of the result of
// `DESCRIBE` depends on the permutation that is used.
std::vector<std::string> runQueries(
    Qlever& engine, const std::vector<std::string>& queries = defaultQueries) {
  std::vector<std::string> results;
  for (const auto& query : queries) {
    std::vector<std::string> lines =
        absl::StrSplit(engine.query(query, ad_utility::MediaType::tsv), '\n');
    ql::ranges::sort(lines);
    results.push_back(absl::StrJoin(lines, "\n"));
  }
  return results;
}

// Expect that all the `queries` have the `expected` results on the `engine`.
void expectResults(Qlever& engine, const std::vector<std::string>& expected,
                   const std::vector<std::string>& queries = defaultQueries,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto actual = runQueries(engine, queries);
  ASSERT_EQ(actual.size(), expected.size());
  for (size_t i = 0; i < actual.size(); ++i) {
    EXPECT_EQ(actual.at(i), expected.at(i)) << queries.at(i);
  }
}

// Build an index from the `input` (by default the `knowledgeGraph`) with the
// given `permutations` (by name, see `CommonConfig::permutations_`) and return
// an engine for it. The `suffix` makes the base name of the index unique within
// a test.
EngineConfig buildIndexWithPermutations(
    std::vector<std::string> permutations, std::string_view suffix,
    std::string_view input = knowledgeGraph,
    qlever::Filetype filetype = qlever::Filetype::Turtle) {
  std::string basename = absl::StrCat(gtestCurrentTestName(), suffix);
  ql::ranges::for_each(IndexImpl::allIndexFiles(basename),
                       [](const auto& f) { ad_utility::deleteFile(f); });
  std::string sourceFilename = absl::StrCat(basename, ".ttl");
  ad_utility::makeOfstream(sourceFilename) << input;
  absl::Cleanup cleanup = [&sourceFilename] {
    ad_utility::deleteFile(sourceFilename);
  };
  IndexBuilderConfig config;
  config.inputFiles_.push_back({sourceFilename, filetype, std::nullopt});
  config.baseName_ = std::move(basename);
  config.permutations_ = std::move(permutations);
  // Small blocks, such that the scans span several blocks.
  config.indexRowsPerBlock_ = 8;
  // The patterns can only be built with all six permutations.
  config.noPatterns_ = !config.permutations_.empty();
  Qlever::buildIndex(config);
  EngineConfig engineConfig{config};
  engineConfig.persistUpdates_ = false;
  return engineConfig;
}

}  // namespace

// Build indexes with different subsets of the permutations and check that all
// queries (also after updates) have the same results as with all six
// permutations, and that exactly the requested permutations exist.
TEST(PermutationSubset, sameResultsAsWithAllPermutations) {
  Qlever reference{buildIndexWithPermutations({}, "all")};
  auto expected = runQueries(reference);

  const std::vector<std::vector<std::string>> subsets{
      {"PSO", "POS"}, {"SPO"},        {"SOP"},        {"OSP"},
      {"OPS"},        {"SPO", "OPS"}, {"SOP", "OSP"}, {"spog", "OPSG"}};
  for (const auto& subset : subsets) {
    auto name = absl::StrJoin(subset, "_");
    SCOPED_TRACE(name);
    auto config = buildIndexWithPermutations(subset, name);
    Qlever engine{config};

    // The PSO and POS permutations are always built, in addition to the
    // requested ones.
    std::vector<Permutation::Enum> expectedPermutations;
    for (auto permutation : Permutation::ALL) {
      bool requested = ql::ranges::any_of(subset, [permutation](const auto& n) {
        return Permutation::fromString(n) == permutation;
      });
      if (requested ||
          ad_utility::contains(Permutation::INTERNAL, permutation)) {
        expectedPermutations.push_back(permutation);
      }
    }
    const auto& index = engine.indexAndViewsSnapshot()->index_;
    EXPECT_THAT(index.loadedPermutations(),
                ElementsAreArray(expectedPermutations));
    for (auto permutation : Permutation::ALL) {
      bool exists = ad_utility::contains(expectedPermutations, permutation);
      for (const auto& file :
           Permutation::fileNames(permutation, config.baseName_)) {
        EXPECT_EQ(ql::filesystem::exists(file), exists) << file;
      }
    }

    expectResults(engine, expected);
  }

  // Apply the same updates to an index with all permutations and an index
  // with only some permutations, and compare the results again.
  auto config = buildIndexWithPermutations({"SOP"}, "updates");
  Qlever engine{config};
  for (auto* e : {&reference, &engine}) {
    ad_utility::testing::applyUpdateToEngine(
        *e, "INSERT DATA { <e> <p> <a> . <e> <e> <e> . <a> <p> <new> }");
    ad_utility::testing::applyUpdateToEngine(
        *e, "DELETE DATA { <a> <p> <b> . <c> <q> <c> }");
  }
  expectResults(engine, runQueries(reference));
}

// Load an index that has all permutations, but only load some of them.
TEST(PermutationSubset, loadOnlySomePermutations) {
  auto config = buildIndexWithPermutations({}, "");
  std::vector<std::string> expected;
  {
    Qlever engine{config};
    EXPECT_TRUE(engine.indexAndViewsSnapshot()->index_.hasAllPermutations());
    expected = runQueries(engine);
  }
  config.permutations_ = {"OSP", "GSPO"};
  Qlever engine{config};
  const auto& index = engine.indexAndViewsSnapshot()->index_;
  EXPECT_THAT(
      index.loadedPermutations(),
      ElementsAreArray({Permutation::PSO, Permutation::POS, Permutation::OSP}));
  EXPECT_FALSE(index.hasAllPermutations());
  expectResults(engine, expected);

  // The two ways of restricting the permutations must not be combined.
  config.onlyPsoAndPos_ = true;
  AD_EXPECT_THROW_WITH_MESSAGE(Qlever{config},
                               HasSubstr("must not be combined"));
}

// A query for which none of the loaded permutations is suitable is answered
// with a warning.
TEST(PermutationSubset, warningForDegenerateScan) {
  Qlever engine{buildIndexWithPermutations({"OPS"}, "")};
  auto result =
      engine.query("SELECT * { <a> ?p ?o }", ad_utility::MediaType::qleverJson);
  EXPECT_THAT(result, HasSubstr("None of the loaded permutations is suitable "
                                "for the triple pattern <a> ?p ?o"));
  // For this query, the OPS permutation is suitable.
  result =
      engine.query("SELECT * { ?s ?p <a> }", ad_utility::MediaType::qleverJson);
  EXPECT_THAT(result, Not(HasSubstr("None of the loaded permutations")));
}

// _____________________________________________________________________________
TEST(PermutationSubset, invalidPermutations) {
  AD_EXPECT_THROW_WITH_MESSAGE(buildIndexWithPermutations({"SPX"}, ""),
                               HasSubstr("is not the name of a permutation"));
  // The patterns require all six permutations.
  auto basename = gtestCurrentTestName();
  IndexBuilderConfig config;
  config.baseName_ = basename;
  config.permutations_ = {"SPO"};
  config.noPatterns_ = false;
  config.inputFiles_.push_back(
      {absl::StrCat(basename, ".ttl"), qlever::Filetype::Turtle, std::nullopt});
  AD_EXPECT_THROW_WITH_MESSAGE(Qlever::buildIndex(config),
                               HasSubstr("all 6 permutations"));
}

namespace {
// A small dataset with named graphs, in N-Quads format.
constexpr std::string_view namedGraphs =
    "<a> <p> <b> <g1> .\n<a> <p> <c> <g1> .\n<a> <q> \"x\" <g1> .\n"
    "<b> <p> <a> <g1> .\n<a> <p> <b> <g2> .\n<c> <q> <c> <g2> .\n"
    "<d> <p> <a> <g2> .\n<a> <p> <d> .\n<b> <q> <b> <g3> .\n"
    "<a> <p> <b> <g3> .\n";

// Queries for the `namedGraphs`, in particular with fixed graphs and graph
// variables.
const std::vector<std::string> graphQueries{
    "SELECT * { GRAPH <g1> { ?s ?p ?o } } ORDER BY ?s ?p ?o",
    "SELECT * { GRAPH <g1> { <a> ?p ?o } } ORDER BY ?p ?o",
    "SELECT * { GRAPH <g2> { ?s <p> ?o } } ORDER BY ?s ?o",
    "SELECT * { GRAPH <g2> { ?s ?p <a> } } ORDER BY ?s ?p",
    "SELECT * { GRAPH <g1> { <a> <p> ?o } } ORDER BY ?o",
    "SELECT * { GRAPH <g3> { <a> <p> <b> } }",
    "SELECT * { GRAPH <g1> { ?s <p> ?o . ?o <p> ?o2 } } ORDER BY ?s ?o ?o2",
    "SELECT * { GRAPH <g4> { ?s ?p ?o } }",
    "SELECT * { GRAPH <g1> { ?x ?p ?x } } ORDER BY ?x ?p",
    "SELECT * FROM <g2> { ?s ?p ?o } ORDER BY ?s ?p ?o",
    "SELECT * FROM <g1> FROM <g2> { ?s ?p ?o } ORDER BY ?s ?p ?o",
    "SELECT * { GRAPH ?g { ?s ?p ?o } } ORDER BY ?g ?s ?p ?o",
    "SELECT * { GRAPH ?g { <a> <p> ?o } } ORDER BY ?g ?o",
    "SELECT * FROM NAMED <g1> FROM NAMED <g3> { GRAPH ?g { ?s ?p ?o } } "
    "ORDER BY ?g ?s ?p ?o",
    "SELECT * { ?s ?p ?o } ORDER BY ?s ?p ?o",
    "SELECT ?g (COUNT(*) AS ?c) { GRAPH ?g { ?s ?p ?o } } GROUP BY ?g "
    "ORDER BY ?g",
    "SELECT (COUNT(*) AS ?c) { GRAPH <g1> { ?s ?p ?o } }",
    "SELECT ?s (COUNT(*) AS ?c) { GRAPH <g1> { ?s ?p ?o } } GROUP BY ?s "
    "ORDER BY ?s",
};
}  // namespace

// Indexes with permutations that don't have the graph as their last column
// give the same results as an index with the default permutations, also after
// updates, and the scans with a fixed graph actually use them.
TEST(PermutationSubset, permutationsWithGraphAsKeyColumn) {
  auto build = [](std::vector<std::string> permutations,
                  std::string_view suffix) {
    return buildIndexWithPermutations(std::move(permutations), suffix,
                                      namedGraphs, qlever::Filetype::NQuad);
  };
  Qlever reference{build({}, "reference")};
  auto expected = runQueries(reference, graphQueries);

  const std::vector<std::vector<std::string>> subsets{
      {"GSPO"},
      {"GPOS", "GOSP"},
      {"SPO", "GSPO", "GSOP", "GPSO", "GPOS", "GOSP", "GOPS"},
      {"SGPO", "OPGS", "PSGO"},
      {"OSGP", "POGS", "SOGP"}};
  for (const auto& subset : subsets) {
    auto name = absl::StrJoin(subset, "_");
    SCOPED_TRACE(name);
    auto config = build(subset, name);
    Qlever engine{config};
    // The requested permutations and their twins are built, the twins are
    // deleted again if they were not requested.
    const auto& index = engine.indexAndViewsSnapshot()->index_;
    for (const auto& permutationName : subset) {
      auto permutation = Permutation::fromString(permutationName);
      EXPECT_TRUE(index.hasPermutation(permutation)) << permutationName;
      auto twin = Permutation::twin(permutation);
      bool twinRequested =
          ql::ranges::any_of(subset, [twin](const std::string& n) {
            return Permutation::fromString(n) == twin;
          });
      EXPECT_EQ(index.hasPermutation(twin), twinRequested);
      for (const auto& file : Permutation::fileNames(twin, config.baseName_)) {
        EXPECT_EQ(ql::filesystem::exists(file), twinRequested) << file;
      }
    }
    expectResults(engine, expected, graphQueries);

    // Updates in named graphs.
    for (auto* e : {&reference, &engine}) {
      ad_utility::testing::applyUpdateToEngine(
          *e,
          "INSERT DATA { GRAPH <g1> { <e> <p> <a> . <a> <p> <new> } "
          "GRAPH <g4> { <a> <p> <b> } }");
      ad_utility::testing::applyUpdateToEngine(
          *e,
          "DELETE DATA { GRAPH <g1> { <a> <p> <b> } GRAPH <g2> { <c> <q> "
          "<c> } }");
    }
    expectResults(engine, runQueries(reference, graphQueries), graphQueries);
    // Undo the updates on the reference for the next subset.
    ad_utility::testing::applyUpdateToEngine(
        reference,
        "DELETE DATA { GRAPH <g1> { <e> <p> <a> . <a> <p> <new> } "
        "GRAPH <g4> { <a> <p> <b> } }");
    ad_utility::testing::applyUpdateToEngine(
        reference,
        "INSERT DATA { GRAPH <g1> { <a> <p> <b> } GRAPH <g2> { <c> <q> <c> } "
        "}");
  }

  // A scan with a fixed graph uses a permutation that starts with the graph,
  // if the graph is small compared to the other graphs.
  std::string withLargeGraph{namedGraphs};
  for (size_t i = 0; i < 100; ++i) {
    absl::StrAppend(&withLargeGraph, "<s", i, "> <p> <o", i, "> <large> .\n");
  }
  Qlever engine{buildIndexWithPermutations(
      {"GSPO", "GPOS"}, "plan", withLargeGraph, qlever::Filetype::NQuad)};
  auto plan = engine.query("SELECT * { GRAPH <g1> { ?s <p> ?o } }",
                           ad_utility::MediaType::qleverJson);
  EXPECT_THAT(plan, HasSubstr("IndexScan GPOS"));
  plan = engine.query("SELECT * { GRAPH <g1> { <a> ?p ?o } }",
                      ad_utility::MediaType::qleverJson);
  EXPECT_THAT(plan, HasSubstr("IndexScan GSPO"));
  // For a few graphs in the `FROM` clause, the scans for the single graphs are
  // merged, and the duplicates are removed.
  plan = engine.query("SELECT * FROM <g1> FROM <g2> FROM <g3> { ?s <p> ?o }",
                      ad_utility::MediaType::qleverJson);
  EXPECT_THAT(plan, HasSubstr("IndexScan GPOS"));
  EXPECT_THAT(plan, HasSubstr("Union"));
  EXPECT_THAT(plan, HasSubstr("Distinct"));
  EXPECT_EQ(engine.query("SELECT * FROM <g1> FROM <g2> FROM <g3> { ?s <p> ?o "
                         "} ORDER BY ?s ?o",
                         ad_utility::MediaType::tsv),
            "?s\t?o\n<a>\t<b>\n<a>\t<c>\n<b>\t<a>\n<d>\t<a>\n");
}

namespace {
// Apply the `update`, which may consist of several operations (like `MOVE`),
// to the `engine`.
void applyUpdates(Qlever& engine, const std::string& update) {
  ad_utility::BlankNodeManager blankNodeManager;
  auto parsedUpdates = SparqlParser::parseUpdate(
      &blankNodeManager, ad_utility::testing::encodedIriManager(),
      engine.allocator(), update);
  for (auto& parsedUpdate : parsedUpdates) {
    auto plannedUpdate =
        engine.planQuery(engine.bindParsedQuery(std::move(parsedUpdate)));
    auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
    auto snapshot = engine.indexAndViewsSnapshot();
    snapshot->index_.deltaTriplesManager().modify<UpdateMetadata>(
        [&](DeltaTriples& deltaTriples) {
          return engine.applyUpdate(plannedUpdate, handle, deltaTriples);
        });
  }
}
}  // namespace

// Dropping a graph (see `DeltaTriples::dropGraph`) gives the same results as
// deleting its triples one by one, for various permutations, also when triples
// are inserted into the dropped graph again, and when the updates are
// persisted and read again.
TEST(DropGraph, sameResultsAsDeletingTheTriples) {
  const std::vector<std::vector<std::string>> subsets{
      {}, {"PSO", "POS"}, {"GSPO", "GPOS"}, {"SGPO", "OPGS"}};
  for (const auto& subset : subsets) {
    auto name = absl::StrJoin(subset, "_");
    SCOPED_TRACE(name);
    auto config = buildIndexWithPermutations(subset, name, namedGraphs,
                                             qlever::Filetype::NQuad);
    // The `reference` deletes the triples one by one.
    Qlever reference{config};
    config.persistUpdates_ = true;
    std::optional<Qlever> engine;
    engine.emplace(config);
    auto applyToBoth = [&](std::string_view fastUpdate,
                           std::string_view referenceUpdate) {
      applyUpdates(*engine, std::string{fastUpdate});
      applyUpdates(reference, std::string{referenceUpdate});
      expectResults(*engine, runQueries(reference, graphQueries), graphQueries);
    };
    // Return the number of dropped graphs of the `qlever` instance.
    auto numDroppedGraphs = [](Qlever& qlever) {
      size_t result = 0;
      qlever.indexAndViewsSnapshot()->index_.deltaTriplesManager().modify<void>(
          [&result](DeltaTriples& deltaTriples) {
            result = deltaTriples.droppedGraphs().size();
          },
          false, false);
      return result;
    };
    applyToBoth("DROP GRAPH <g1>", "DELETE WHERE { GRAPH <g1> { ?s ?p ?o } }");
    // The graph was dropped without deleting its triples one by one.
    EXPECT_EQ(numDroppedGraphs(*engine), 1);
    EXPECT_EQ(numDroppedGraphs(reference), 0);
    // Inserting into the dropped graph and dropping it again.
    applyToBoth("INSERT DATA { GRAPH <g1> { <a> <p> <b> . <new> <p> <a> } }",
                "INSERT DATA { GRAPH <g1> { <a> <p> <b> . <new> <p> <a> } }");
    applyToBoth("DELETE DATA { GRAPH <g1> { <new> <p> <a> } }",
                "DELETE DATA { GRAPH <g1> { <new> <p> <a> } }");
    // Delta triples of a graph are erased when it is dropped.
    applyToBoth("INSERT DATA { GRAPH <g2> { <x> <p> <y> } }",
                "INSERT DATA { GRAPH <g2> { <x> <p> <y> } }");
    applyToBoth("CLEAR GRAPH <g2>", "DELETE WHERE { GRAPH <g2> { ?s ?p ?o } }");
    applyToBoth("DROP DEFAULT",
                "DELETE WHERE { GRAPH "
                "<http://qlever.cs.uni-freiburg.de/builtin-functions/"
                "default-graph> { ?s ?p ?o } }");
    // A graph that doesn't exist and a graph that only has delta triples.
    applyToBoth("DROP SILENT GRAPH <doesNotExist>",
                "DELETE WHERE { GRAPH <doesNotExist> { ?s ?p ?o } }");
    applyToBoth("INSERT DATA { GRAPH <onlyDelta> { <a> <p> <b> } }",
                "INSERT DATA { GRAPH <onlyDelta> { <a> <p> <b> } }");
    applyToBoth("DROP GRAPH <onlyDelta>",
                "DELETE WHERE { GRAPH <onlyDelta> { ?s ?p ?o } }");
    applyToBoth("MOVE <g3> TO <g1>", "MOVE <g3> TO <g1>");

    // The dropped graphs are persisted together with the delta triples.
    auto expected = runQueries(*engine, graphQueries);
    auto numDropped = numDroppedGraphs(*engine);
    EXPECT_EQ(numDropped, 4);
    engine.reset();
    engine.emplace(config);
    EXPECT_EQ(numDroppedGraphs(*engine), numDropped);
    expectResults(*engine, expected, graphQueries);
    engine.reset();
    for (const auto& file : IndexImpl::allIndexFiles(config.baseName_)) {
      ad_utility::deleteFile(file, false);
    }
  }
}
