// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Tests for indexes that only have a subset of the permutations (see
// `CommonConfig::onlyPsoAndPos_`).

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "./QleverTestHelpers.h"
#include "index/IndexImpl.h"
#include "libqlever/Qlever.h"

using namespace qlever;
using ::testing::ElementsAre;
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
    "SELECT ?p (COUNT(?s) AS ?c) { ?s ?p ?o } GROUP BY ?p ORDER BY ?p",
};

// Return the results of all the `queries` on the `engine` in TSV format.
std::vector<std::string> runQueries(Qlever& engine) {
  std::vector<std::string> results;
  for (const auto& query : queries) {
    results.push_back(engine.query(query, ad_utility::MediaType::tsv));
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

// Build an index from the `knowledgeGraph`, either with all six permutations
// or only with the PSO and POS permutations, and return the configuration of
// an engine for it. The `suffix` makes the base name of the index unique
// within a test.
EngineConfig buildIndex(bool onlyPsoAndPos, std::string_view suffix) {
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
  config.onlyPsoAndPos_ = onlyPsoAndPos;
  // The patterns can only be built with all six permutations.
  config.noPatterns_ = onlyPsoAndPos;
  Qlever::buildIndex(config);
  EngineConfig engineConfig{config};
  engineConfig.persistUpdates_ = false;
  return engineConfig;
}

}  // namespace

// Check that all queries (also after updates) have the same results with only
// the PSO and POS permutations as with all six permutations.
TEST(PermutationSubset, sameResultsAsWithAllPermutations) {
  Qlever reference{buildIndex(false, "all")};
  Qlever engine{buildIndex(true, "psoPos")};
  {
    const auto& index = engine.indexAndViewsSnapshot()->index_;
    EXPECT_THAT(index.loadedPermutations(),
                ElementsAre(Permutation::PSO, Permutation::POS));
    EXPECT_TRUE(index.hasPermutation(Permutation::PSO));
    EXPECT_FALSE(index.hasPermutation(Permutation::SPO));
    EXPECT_EQ(
        reference.indexAndViewsSnapshot()->index_.loadedPermutations().size(),
        6);
  }
  expectResults(engine, runQueries(reference));

  // Apply the same updates to both indexes and compare the results again.
  for (auto* e : {&reference, &engine}) {
    ad_utility::testing::applyUpdateToEngine(
        *e, "INSERT DATA { <e> <p> <a> . <e> <e> <e> . <a> <p> <new> }");
    ad_utility::testing::applyUpdateToEngine(
        *e, "DELETE DATA { <a> <p> <b> . <c> <q> <c> }");
  }
  expectResults(engine, runQueries(reference));
}

// A query for which none of the loaded permutations is suitable is answered
// with a warning.
TEST(PermutationSubset, warningForDegenerateScan) {
  Qlever engine{buildIndex(true, "")};
  auto result =
      engine.query("SELECT * { <a> ?p ?o }", ad_utility::MediaType::qleverJson);
  EXPECT_THAT(result, HasSubstr("None of the loaded permutations is suitable "
                                "for the triple pattern <a> ?p ?o"));
  // For this query, the PSO permutation is suitable.
  result = engine.query("SELECT * { <a> <p> ?o }",
                        ad_utility::MediaType::qleverJson);
  EXPECT_THAT(result, Not(HasSubstr("None of the loaded permutations")));
}

// _____________________________________________________________________________
TEST(PermutationSubset, firstLoadedPermutation) {
  Qlever engine{buildIndex(true, "")};
  const auto& index = engine.indexAndViewsSnapshot()->index_.getImpl();
  EXPECT_EQ(index.firstLoadedPermutation({Permutation::SPO, Permutation::OSP}),
            nullptr);
  EXPECT_EQ(index.firstLoadedPermutation({Permutation::SPO, Permutation::POS}),
            &index.getPermutation(Permutation::POS));
  EXPECT_EQ(
      &index.getFirstLoadedPermutation({Permutation::SPO, Permutation::PSO}),
      &index.getPermutation(Permutation::PSO));
  AD_EXPECT_THROW_WITH_MESSAGE(
      index.getFirstLoadedPermutation({Permutation::SPO, Permutation::OSP}),
      HasSubstr("None of the permutations SPO, OSP is loaded"));
  EXPECT_THAT(index.loadedPermutations<true>(),
              ElementsAre(Permutation::PSO, Permutation::POS));
}
