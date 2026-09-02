// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/functional/bind_front.h>
#include <gmock/gmock.h>

#include "./MaterializedViewsTestHelpers.h"
#include "./QueryPlannerTestHelpers.h"
#include "engine/MaterializedViews.h"
#include "engine/MaterializedViewsQueryAnalysis.h"
#include "libqlever/Qlever.h"
#include "util/File.h"

namespace {

using namespace materializedViewsTestHelpers;
using namespace ad_utility::testing;
using V = Variable;

}  // namespace

// Example queries for testing the rewriting of star-shaped patterns.
constexpr std::string_view simpleStar =
    "SELECT * { ?s <p1> ?o1 . ?s <p2> ?o2 }";
constexpr std::string_view simpleStarRenamed =
    "SELECT * { ?x <p2> ?b . ?x <p1> ?a }";
constexpr std::string_view simpleStarPlusJoin =
    "SELECT * { ?s <p1> ?o1 . ?s <p2> ?o2 . ?s <p3> ?o3 }";
constexpr std::string_view simpleStarFixedObject =
    "SELECT * { ?s <p1> ?o1 . ?s <p2> <o2a> }";
constexpr std::string_view simpleStarDifferentPredicate =
    "SELECT * { ?s <p1> ?o1 . ?s <p5> ?o2 }";
constexpr std::string_view simpleStarJoinPredicateTwice =
    "SELECT * { ?s <p1> ?o1 . ?s <p2> ?o2 . ?s <p2> ?o3 . }";
constexpr std::string_view singleTripleFromStar = "SELECT * { ?s <p1> ?o1 }";

// _____________________________________________________________________________
TEST_F(MaterializedViewsPatternRewriteTest, starRewrite) {
  // Test dataset: subjects with predicates p1, p2, p3.
  const std::string starTtl = R"(
      <s1> <p1> <o1a> .
      <s1> <p2> <o2a> .
      <s2> <p1> <o1b> .
      <s2> <p2> <o2b> .
      <s2> <p3> <o3a> .
  )";
  const std::string onDiskBase = gtestCurrentTestName();
  const std::string viewName = "testViewStar";

  materializedViewsTestHelpers::makeTestIndex(onDiskBase, starTtl);
  auto cleanUp = absl::Cleanup(
      [&]() { materializedViewsTestHelpers::removeTestIndex(onDiskBase); });
  qlever::EngineConfig config;
  config.baseName_ = onDiskBase;
  qlever::Qlever qlv{config};

  // Without the materialized view, a regular join is executed.
  h::expect(std::string{simpleStar},
            h::Join(h::IndexScanFromStrings("?s", "<p1>", "?o1"),
                    h::IndexScanFromStrings("?s", "<p2>", "?o2")));

  // Write a star structure to the materialized view.
  MaterializedViewsManager manager{onDiskBase};
  manager.writeViewToDisk(viewName,
                          qlv.parseAndPlanQuery(std::string{simpleStar}));
  qlv.loadMaterializedView(viewName);
  auto starView = std::bind_front(&viewScanSimple, viewName);

  // With the materialized view loaded, an index scan on the view is performed
  // instead of a regular join.
  qpExpect(qlv, simpleStar, starView("?s", "?o1", "?o2"));
  qpExpect(qlv, simpleStarRenamed, starView("?x", "?a", "?b"));

  // When the user's query has additional arms beyond the star, the extra
  // triples are joined normally.
  qpExpect(qlv, simpleStarPlusJoin,
           h::Join(starView("?s", "?o1", "?o2"),
                   h::IndexScanFromStrings("?s", "<p3>", "?o3")));

  // When the same predicate is used multiple times, the extra occurrences are
  // joined normally. Since both occurrences of `<p2>` are structurally and
  // semantically interchangeable here, either occurrence may end up being
  // rewritten.
  qpExpect(
      qlv, simpleStarJoinPredicateTwice,
      ::testing::AnyOf(h::Join(starView("?s", "?o1", "?o2"),
                               h::IndexScanFromStrings("?s", "<p2>", "?o3")),
                       h::Join(starView("?s", "?o1", "?o3"),
                               h::IndexScanFromStrings("?s", "<p2>", "?o2"))));

  // When an object is fixed, star rewriting is not applied.
  qpExpect(qlv, simpleStarFixedObject,
           h::Join(h::IndexScanFromStrings("?s", "<p1>", "?o1"),
                   h::IndexScanFromStrings("?s", "<p2>", "<o2a>")));

  // One of the predicates does not match the view.
  qpExpect(qlv, simpleStarDifferentPredicate,
           h::Join(h::IndexScanFromStrings("?s", "<p1>", "?o1"),
                   h::IndexScanFromStrings("?s", "<p5>", "?o2")));

  // View is not used for a single triple (arm) contained in the star.
  qpExpect(qlv, singleTripleFromStar,
           h::IndexScanFromStrings("?s", "<p1>", "?o1"));

  // Test cases where star rewriting cannot be applied.
  auto noStarRewrite = [&qlv, &manager](std::string query,
                                        source_location sourceLocation =
                                            AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(sourceLocation);
    expectNotSuitableForRewrite(qlv, manager, "noStarRewriteView", query,
                                "No supported query pattern for rewriting "
                                "joins was found");
  };

  // Disconnected: the fixed subject shares no variable with the other arm.
  noStarRewrite("SELECT * { <s1> <p1> ?o1 . ?s <p2> ?o2 }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . <s1> <p2> ?o2 }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 } ");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?o2 ^<p2> ?s }");
  noStarRewrite("SELECT * { ?s1 <p1> ?o1 . ?s2 <p2> ?o2 }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?s <p2>* ?o2 }");

  // A triple with neither side a variable cannot connect to the rest of the
  // pattern and is rejected.
  noStarRewrite("SELECT * { ?s <p1> ?o1 . <o2a> <p9> <o2b> }");

  // A predicate used twice in the query leaves the view's other predicate
  // uncovered, so the star as a whole is rejected.
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?s <p1> ?o2 }");

  // A self-loop (subject equals object) is rejected.
  noStarRewrite("SELECT * { ?s <p1> ?s . ?s <p2> ?o1 }");

  // Two star arms sharing the same object variable are rejected.
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?s <p2> ?o1 }");

  // A 3-arm star where one arm's object is fixed is rejected.
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?s <p2> ?o2 . ?s <p3> <o2a> }");
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsPatternRewriteTest,
       fullTextPseudoPredicateNotRewritten) {
  //  `ql:contains-word` and `ql:contains-entity` are not allowed for view
  //  rewriting.
  const std::string onDiskBase = gtestCurrentTestName();
  const std::string ttlFilename = absl::StrCat(onDiskBase, ".ttl");
  ad_utility::makeOfstream(ttlFilename)
      << " <s1> <p1> \"some text with several needle words\" . \n"
         " <s1> <p2> <o1> . \n";
  qlever::IndexBuilderConfig indexConfig;
  indexConfig.inputFiles_.emplace_back(ttlFilename, qlever::Filetype::Turtle);
  indexConfig.baseName_ = onDiskBase;
  indexConfig.addWordsFromLiterals_ = true;
  qlever::Qlever::buildIndex(indexConfig);
  auto cleanUp = absl::Cleanup(
      [&]() { materializedViewsTestHelpers::removeTestIndex(onDiskBase); });

  qlever::EngineConfig config;
  config.baseName_ = onDiskBase;
  config.loadTextIndex_ = true;
  qlever::Qlever qlv{config};
  MaterializedViewsManager manager{onDiskBase};

  expectNotSuitableForRewrite(
      qlv, manager, "fullTextView",
      "SELECT ?s ?o { ?s ql:contains-word \"needle\" . ?s <p2> ?o }",
      "No supported query pattern for rewriting joins was found");
}

// Regression test for #3193: an aggregate removes a pattern variable from the
// view's columns, so the view must not be registered for rewriting. Covers
// explicit (`GROUP BY`) and implicit (aggregate in `SELECT`) aggregation.
TEST_F(MaterializedViewsPatternMatchingTest, aggregatingPatternsNotRewritten) {
  constexpr std::string_view kAggregatingReason = "The view's query aggregates";

  expectNotSuitableForRewrite(
      qlv(), manager(), "aggregatingStarView",
      "SELECT ?s (COUNT(?o1) AS ?c) { ?s <p1> ?o1 . ?s <p2> ?o2 } "
      "GROUP BY ?s",
      kAggregatingReason);
  expectNotSuitableForRewrite(
      qlv(), manager(), "aggregatingChainView",
      "SELECT ?s (COUNT(?m) AS ?c) { ?s <p1> ?m . ?m <p2> ?o } "
      "GROUP BY ?s",
      kAggregatingReason);

  // Same as above, but the star's subject (rather than one of its arms) is
  // aggregated away.
  expectNotSuitableForRewrite(
      qlv(), manager(), "aggregatingStarSubjectView",
      "SELECT ?o1 ?o2 (COUNT(?s) AS ?c) { ?s <p1> ?o1 . ?s <p2> ?o2 } "
      "GROUP BY ?o1 ?o2",
      kAggregatingReason);

  // Same as `aggregatingChainView` above, but the chain's first (subject) or
  // last (object) variable is aggregated away instead of the middle one.
  expectNotSuitableForRewrite(
      qlv(), manager(), "aggregatingChainSubjectView",
      "SELECT ?m ?o (COUNT(?s) AS ?c) { ?s <p1> ?m . ?m <p2> ?o } "
      "GROUP BY ?m ?o",
      kAggregatingReason);
  expectNotSuitableForRewrite(
      qlv(), manager(), "aggregatingChainObjectView",
      "SELECT ?s ?m (COUNT(?o) AS ?c) { ?s <p1> ?m . ?m <p2> ?o } "
      "GROUP BY ?s ?m",
      kAggregatingReason);

  // Same as `aggregatingChainView` above, but the `GROUP BY` is implicit (no
  // explicit `GROUP BY` clause, just an aggregate in the `SELECT` clause).
  expectNotSuitableForRewrite(
      qlv(), manager(), "implicitlyAggregatingChainView",
      "SELECT (COUNT(?m) AS ?c) { ?s <p1> ?m . ?m <p2> ?o }",
      kAggregatingReason);
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsPatternMatchingTest,
       unprojectedVariablePatternsNotRewritten) {
  constexpr std::string_view kNoPatternReason =
      "No supported query pattern for rewriting joins was found";

  // Star: the subject is not selected.
  expectNotSuitableForRewrite(qlv(), manager(), "unprojectedStarSubjectView",
                              "SELECT ?o1 ?o2 { ?s <p1> ?o1 . ?s <p2> ?o2 }",
                              kNoPatternReason);
  // Star: one arm's object is not selected.
  expectNotSuitableForRewrite(qlv(), manager(), "unprojectedStarArmView",
                              "SELECT ?s ?o2 { ?s <p1> ?o1 . ?s <p2> ?o2 }",
                              kNoPatternReason);

  // Chain: the subject is not selected.
  expectNotSuitableForRewrite(qlv(), manager(), "unprojectedChainSubjectView",
                              "SELECT ?m ?o { ?s <p1> ?m . ?m <p2> ?o }",
                              kNoPatternReason);
  // Chain: the middle (chain) variable is not selected.
  expectNotSuitableForRewrite(qlv(), manager(), "unprojectedChainMiddleView",
                              "SELECT ?s ?o { ?s <p1> ?m . ?m <p2> ?o }",
                              kNoPatternReason);
  // Chain: the object is not selected.
  expectNotSuitableForRewrite(qlv(), manager(), "unprojectedChainObjectView",
                              "SELECT ?s ?m { ?s <p1> ?m . ?m <p2> ?o }",
                              kNoPatternReason);
}

// Regression test: FILTER, VALUES, DISTINCT/REDUCED, LIMIT/OFFSET and FROM
// restrict the rows written to disk, but (unlike aggregation) leave
// `variableToColumnMap()` intact. Without an explicit check, the view would be
// used for a query needing the full join.
TEST_F(MaterializedViewsPatternMatchingTest, restrictingModifiersNotRewritten) {
  // Star / chain with a top-level FILTER.
  expectNotSuitableForRewrite(
      qlv(), manager(), "filteredStarView",
      "SELECT ?s ?o1 ?o2 { ?s <p1> ?o1 . ?s <p2> ?o2 . FILTER(?s = <s1>) }",
      "top-level FILTER");
  expectNotSuitableForRewrite(
      qlv(), manager(), "filteredChainView",
      "SELECT ?s ?m ?o { ?s <p1> ?m . ?m <p2> ?o . FILTER(?s = <s1>) }",
      "top-level FILTER");

  // Star / chain with a trailing VALUES clause.
  expectNotSuitableForRewrite(
      qlv(), manager(), "valuesStarView",
      "SELECT ?s ?o1 ?o2 { ?s <p1> ?o1 . ?s <p2> ?o2 } VALUES ?s { <s1> }",
      "trailing VALUES clause");
  expectNotSuitableForRewrite(
      qlv(), manager(), "valuesChainView",
      "SELECT ?s ?m ?o { ?s <p1> ?m . ?m <p2> ?o } VALUES ?s { <s1> }",
      "trailing VALUES clause");

  // Star with DISTINCT, chain with REDUCED.
  expectNotSuitableForRewrite(
      qlv(), manager(), "distinctStarView",
      "SELECT DISTINCT ?s ?o1 ?o2 { ?s <p1> ?o1 . ?s <p2> ?o2 }",
      "DISTINCT or REDUCED");
  expectNotSuitableForRewrite(
      qlv(), manager(), "reducedChainView",
      "SELECT REDUCED ?s ?m ?o { ?s <p1> ?m . ?m <p2> ?o }",
      "DISTINCT or REDUCED");

  // Star with LIMIT, chain with OFFSET.
  auto expectIgnoredForPatternRewrite = [&](const std::string& query,
                                            std::string_view expectedReason) {
    auto plan = qlv().parseAndPlanQuery(query);
    EXPECT_THAT(materializedViewsQueryAnalysis::getTriplesForPatternRewrite(
                    plan.parsedQuery()),
                ::testing::VariantWith<
                    materializedViewsQueryAnalysis::RewriteIgnoreReason>(
                    ::testing::HasSubstr(expectedReason)));
  };
  expectIgnoredForPatternRewrite(
      "SELECT ?s ?o1 ?o2 { ?s <p1> ?o1 . ?s <p2> ?o2 } LIMIT 1",
      "LIMIT or OFFSET clause");
  expectIgnoredForPatternRewrite(
      "SELECT ?s ?m ?o { ?s <p1> ?m . ?m <p2> ?o } OFFSET 1",
      "LIMIT or OFFSET clause");

  // Star with FROM, chain with FROM NAMED.
  expectNotSuitableForRewrite(
      qlv(), manager(), "fromStarView",
      "SELECT ?s ?o1 ?o2 FROM <g> { ?s <p1> ?o1 . ?s <p2> ?o2 }",
      "FROM or FROM NAMED clause");
  expectNotSuitableForRewrite(
      qlv(), manager(), "fromNamedChainView",
      "SELECT ?s ?m ?o FROM NAMED <g> { ?s <p1> ?m . ?m <p2> ?o }",
      "FROM or FROM NAMED clause");
}

// _____________________________________________________________________________
TEST(MaterializedViewsPatternRewriteRejectionTest,
     emptyGraphPatternNotRewritten) {
  ParsedQuery parsed;
  parsed._rootGraphPattern._graphPatterns.emplace_back(
      parsedQuery::BasicGraphPattern{});

  auto result =
      materializedViewsQueryAnalysis::getTriplesForPatternRewrite(parsed);
  EXPECT_THAT(result, ::testing::VariantWith<
                          materializedViewsQueryAnalysis::RewriteIgnoreReason>(
                          ::testing::HasSubstr("query body is empty")));
}

// Example queries for testing the rewriting of chain-shaped patterns.
constexpr std::string_view simpleChain = "SELECT * { ?s <p1> ?m . ?m <p2> ?o }";
constexpr std::string_view simpleChainRenamed =
    "SELECT * { ?b <p2> ?c . ?a <p1> ?b }";
constexpr std::string_view simpleChainFixed =
    "SELECT * {  <s2> <p1>/<p2> ?c . }";
constexpr std::string_view simpleChainPlusJoin =
    "SELECT * { ?s <p1>/<p2> ?o . ?s <p3> ?o2 }";
constexpr std::string_view simpleChainRenamedPlusBind =
    "SELECT ?a ?b ?c ?x { ?b <p2> ?c . ?a <p1> ?b . BIND(5 AS ?x) }";
constexpr std::string_view simpleChainDifferentSort =
    "SELECT ?m ?s ?o { ?s <p1> ?m . ?m <p2> ?o }";
constexpr std::string_view overlappingChains =
    "SELECT * { ?s <p1> ?m . ?m <p2> ?o1 . ?m <p2> ?o2 }";

// _____________________________________________________________________________
TEST_P(MaterializedViewsPatternRewriteTestP, simpleChain) {
  const std::string& writeQuery = GetParam();

  // Test dataset and query.
  const std::string chainTtl = R"(
      <s1> <p1> <m2> .
      <m1> <p2> <o1> .
      <s2> <p1> <m2> .
      <m2> <p2> <http://example.com/> .
      <m2> <p3> "abc" .
      <s2> <p3> <o3> .
  )";
  const std::string onDiskBase = gtestCurrentTestName();
  const std::string viewName = "testViewChain";

  // Initialized libqlever.
  materializedViewsTestHelpers::makeTestIndex(onDiskBase, chainTtl);
  auto cleanUp = absl::Cleanup(
      [&]() { materializedViewsTestHelpers::removeTestIndex(onDiskBase); });
  qlever::EngineConfig config;
  config.baseName_ = onDiskBase;
  qlever::Qlever qlv{config};

  // Without the materialized view, a regular join is executed.
  h::expect(std::string{simpleChain},
            h::Join(h::IndexScanFromStrings("?s", "<p1>", "?m"),
                    h::IndexScanFromStrings("?m", "<p2>", "?o")));

  // Write a chain structure to the materialized view.
  qlv.writeMaterializedView(viewName, writeQuery);
  qlv.loadMaterializedView(viewName);
  auto chainView = std::bind_front(&viewScanSimple, viewName);

  // With the materialized view loaded, an index scan on the view is performed
  // instead of a regular join.
  qpExpect(qlv, simpleChain, chainView("?s", "?m", "?o"));
  qpExpect(qlv, simpleChainRenamed, chainView("?a", "?b", "?c"));
  qpExpect(qlv, simpleChainFixed,
           chainView("<s2>", "?_QLever_internal_variable_qp_0", "?c"));
  qpExpect(qlv, simpleChainPlusJoin,
           h::Join(chainView("?s", "?_QLever_internal_variable_qp_0", "?o"),
                   h::IndexScanFromStrings("?s", "<p3>", "?o2")));

  // If the view is sorted such that the subject of the chain is not the first
  // column, rewriting cannot be applied with a fixed subject.
  qlv.writeMaterializedView(viewName, std::string{simpleChainDifferentSort});
  qlv.loadMaterializedView(viewName);
  qpExpect(qlv, simpleChainFixed,
           h::Join(h::IndexScanFromStrings("<s2>", "<p1>",
                                           "?_QLever_internal_variable_qp_0"),
                   h::IndexScanFromStrings("?_QLever_internal_variable_qp_0",
                                           "<p2>", "?c")));

  // Test overlapping view plans: the rewriting can be applied but the remaining
  // triple must be joined normally.
  auto firstRewritten = h::Join(chainView("?m", "?s", "?o1"),
                                h::IndexScanFromStrings("?m", "<p2>", "?o2"));
  auto secondRewritten = h::Join(chainView("?m", "?s", "?o2"),
                                 h::IndexScanFromStrings("?m", "<p2>", "?o1"));
  qpExpect(qlv, overlappingChains,
           ::testing::AnyOf(firstRewritten, secondRewritten));
}

// _____________________________________________________________________________
INSTANTIATE_TEST_SUITE_P(
    ChainWriteQueries, MaterializedViewsPatternRewriteTestP,
    ::testing::Values(
        std::string{simpleChain},

        // An additional `BIND` is ignored and the view can still be used for
        // query rewriting. Also uses a different sorting.
        std::string{simpleChainRenamedPlusBind}));

// _____________________________________________________________________________
TEST_F(MaterializedViewsPatternRewriteContextTest,
       DegenerateChainsAndGraphClause) {
  qlv().writeMaterializedView("testViewChain", std::string{simpleChain});
  qlv().loadMaterializedView("testViewChain");

  // A degenerate chain (`?a <p1> ?b . ?b <p2> ?a`) must be rejected for
  // rewriting (thus planned normally).
  qpExpect(qlv(), "SELECT * { ?x <p1> ?v . ?v <p2> ?x }",
           h::MultiColumnJoin(h::IndexScanFromStrings("?x", "<p1>", "?v"),
                              h::IndexScanFromStrings("?v", "<p2>", "?x")));

  // The same holds for a degenerate chain where the middle and the end are
  // the same variable.
  EXPECT_EQ(qlv().query("SELECT ?x ?v { ?x <p1> ?v . ?v <p2> ?v }",
                        ad_utility::MediaType::tsv),
            "?x\t?v\n<x2>\t<v2>\n");

  // Outside of any `GRAPH` clause, rewriting is applied.
  auto chainView = std::bind_front(&viewScanSimple, "testViewChain");
  qpExpect(qlv(), simpleChain, chainView("?s", "?m", "?o"));

  // Inside `GRAPH <g1> {...}`, the triples are scanned restricted to graph
  // `<g1>` and not replaced by the view without graph constraint.
  qpExpect(
      qlv(), "SELECT * { GRAPH <g1> { ?s <p1> ?m . ?m <p2> ?o } }",
      h::Join(
          h::IndexScanFromStrings("?s", "<p1>", "?m", {},
                                  ad_utility::HashSet<std::string>{"<g1>"}),
          h::IndexScanFromStrings("?m", "<p2>", "?o", {},
                                  ad_utility::HashSet<std::string>{"<g1>"})));
}
