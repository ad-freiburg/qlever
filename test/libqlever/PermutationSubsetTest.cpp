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
const std::vector<std::string> queries{
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
std::vector<std::string> runQueries(Qlever& engine) {
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
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto actual = runQueries(engine);
  ASSERT_EQ(actual.size(), expected.size());
  for (size_t i = 0; i < actual.size(); ++i) {
    EXPECT_EQ(actual.at(i), expected.at(i)) << queries.at(i);
  }
}

// Build an index from the `knowledgeGraph` with the given `permutations` (by
// name, see `CommonConfig::permutations_`) and return an engine for it. The
// `suffix` makes the base name of the index unique within a test.
EngineConfig buildIndexWithPermutations(std::vector<std::string> permutations,
                                        std::string_view suffix) {
  std::string basename = absl::StrCat(gtestCurrentTestName(), suffix);
  ql::ranges::for_each(IndexImpl::allIndexFiles(basename),
                       [](const auto& f) { ad_utility::deleteFile(f); });
  std::string sourceFilename = absl::StrCat(basename, ".ttl");
  ad_utility::makeOfstream(sourceFilename) << knowledgeGraph;
  absl::Cleanup cleanup = [&sourceFilename] {
    ad_utility::deleteFile(sourceFilename);
  };
  IndexBuilderConfig config;
  config.inputFiles_.push_back(
      {sourceFilename, qlever::Filetype::Turtle, std::nullopt});
  config.baseName_ = std::move(basename);
  config.permutations_ = std::move(permutations);
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
  // A permutation that does not have the graph as its last column cannot be
  // built yet.
  AD_EXPECT_THROW_WITH_MESSAGE(buildIndexWithPermutations({"GSPO"}, ""),
                               HasSubstr("not yet supported"));
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
