// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/functional/bind_front.h>
#include <absl/numeric/bits.h>
#include <gmock/gmock.h>

#include "./MaterializedViewsTestHelpers.h"
#include "./QueryPlannerTestHelpers.h"
#include "./util/RuntimeParametersTestHelpers.h"
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
  // A fixed object still shares `?s`, see `generalPatternRewrite`.
  noStarRewrite("SELECT * { <s1> <p1> ?o1 . ?s <p2> ?o2 }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . <s1> <p2> ?o2 }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 } ");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?o2 ^<p2> ?s }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?s <p2>* ?o2 }");

  // A triple with neither side a variable cannot connect to the rest of the
  // pattern and is rejected.
  noStarRewrite("SELECT * { ?s <p1> ?o1 . <o2a> <p9> <o2b> }");
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsPatternRewriteTest, generalPatternRewrite) {
  const std::string onDiskBase = gtestCurrentTestName();
  const std::string generalPatternTtl =
      " <gp2s> <p1> <gp2s> . \n"
      " <gp2s> <p2> <gp2a> . \n"
      " <gp2s> <p3> <gp2b> . \n"
      " <gp2s> <p9> <gp2c> . \n"
      " <gp3s> <p1> <gp3a> . \n"
      " <gp3s> <p2> <gp3a> . \n"
      " <gp3s> <p3> <gp3b> . \n"
      " <gp3s> <p9> <gp3c> . \n";
  materializedViewsTestHelpers::makeTestIndex(onDiskBase, generalPatternTtl);
  auto cleanUp = absl::Cleanup(
      [&]() { materializedViewsTestHelpers::removeTestIndex(onDiskBase); });
  qlever::EngineConfig config;
  config.baseName_ = onDiskBase;
  qlever::Qlever qlv{config};
  MaterializedViewsManager manager{onDiskBase};

  auto generalPatternView =
      std::bind_front(&viewScanSimple, "generalPatternView");
  auto extraArm = h::IndexScanFromStrings("?s", "<p9>", "?o9");

  // A self-loop.
  expectRewrite(
      qlv, "generalPatternView",
      "SELECT * { ?s <p1> ?s . ?s <p2> ?o1 . ?s <p3> ?o2 }",
      "SELECT * { ?s <p1> ?s . ?s <p2> ?o1 . ?s <p3> ?o2 . ?s <p9> ?o9 }",
      h::Join(generalPatternView("?s", "?o1", "?o2"), extraArm));

  // Two star arms with the same object variable.
  expectRewrite(
      qlv, "generalPatternView",
      "SELECT * { ?s <p1> ?o1 . ?s <p2> ?o1 . ?s <p3> ?o2 }",
      "SELECT * { ?s <p1> ?o1 . ?s <p2> ?o1 . ?s <p3> ?o2 . ?s <p9> ?o9 }",
      h::Join(generalPatternView("?s", "?o1", "?o2"), extraArm));

  // One star arm's object is fixed in the view's own definition (e.g. `?x
  // osmkey:railway "rail"`). This is allowed: the view is already filtered to
  // that value, so it matches a query asking for the same value.
  expectRewrite(
      qlv, "fixedArmView", "SELECT * { ?s <p2> <gp2a> . ?s <p3> ?o2 }",
      "SELECT * { ?s <p2> <gp2a> . ?s <p3> ?o2 . ?s <p9> ?o9 }",
      h::Join(
          viewScan("fixedArmView", "?s", "?o2", "?_ql_materialized_view_o", 2),
          extraArm));

  // Disconnected pattern (no shared variable) is rejected.
  expectNotSuitableForRewrite(
      qlv, manager, "disconnectedPatternView",
      "SELECT * { ?s1 <p1> ?o1 . ?s2 <p2> ?o2 }",
      "No supported query pattern for rewriting joins was found");

  // A `BIND` that is filtered out as invariant can be selected as the view's
  // first column. Such a view must be rejected.
  expectNotSuitableForRewrite(
      qlv, manager, "bindOccupiesColumnZeroView",
      "SELECT ?x ?s ?o1 ?o2 { ?s <p2> ?o1 . ?s <p3> ?o2 . BIND(1 AS ?x) }",
      "No supported query pattern for rewriting joins was found");
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

// An assignment limit too small for one full match (a 2-edge chain needs >= 2
// attempts) finds nothing and warns; the default limit finds the match.
TEST_F(MaterializedViewsPatternMatchingTest,
       PatternMatchNumAssignmentsIsRespected) {
  materializedViewsQueryAnalysis::QueryPatternCache qpc;
  registerView(qpc, "numAssignmentsChain",
               "SELECT * { ?s <bp1> ?m . ?m <bp2> ?o }");
  const std::string query = "SELECT * { ?s <bp1> ?m . ?m <bp2> ?o }";

  {
    auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
    auto cleanupNumAssignments = setRuntimeParameterForTest<
        &RuntimeParameters::materializedViewPatternMatchNumAssignments_>(1);
    EXPECT_TRUE(match(qpc, query).empty());
    EXPECT_THAT(logStream.str(),
                ::testing::HasSubstr(
                    "materialized-view-pattern-match-num-assignments"));
  }
  {
    auto cleanupNumAssignments = setRuntimeParameterForTest<
        &RuntimeParameters::materializedViewPatternMatchNumAssignments_>(
        100'000);
    EXPECT_FALSE(match(qpc, query).empty());
  }
}

// A limit of `0` assignments disables pattern-based rewriting: no match is
// found and no warning is logged.
TEST_F(MaterializedViewsPatternMatchingTest,
       PatternMatchNumAssignmentsZeroDisables) {
  materializedViewsQueryAnalysis::QueryPatternCache qpc;
  registerView(qpc, "numAssignmentsZeroChain",
               "SELECT * { ?s <bz1> ?m . ?m <bz2> ?o }");

  auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
  auto cleanupNumAssignments = setRuntimeParameterForTest<
      &RuntimeParameters::materializedViewPatternMatchNumAssignments_>(0);
  EXPECT_TRUE(match(qpc, "SELECT * { ?s <bz1> ?m . ?m <bz2> ?o }").empty());
  EXPECT_THAT(logStream.str(),
              ::testing::Not(::testing::HasSubstr(
                  "materialized-view-pattern-match-num-assignments")));
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsPatternMatchingTest, ReplacementCountIsCapped) {
  materializedViewsQueryAnalysis::QueryPatternCache qpc;
  registerView(qpc, "capView", "SELECT * { ?s <cp1> ?o1 . ?s <cp1> ?o2 }");

  // 40 triples sharing subject and predicate: assigning two of them to the
  // view's two interchangeable arms yields 40*39 = 1560 matches, well over the
  // cap.
  std::string hostQuery = "SELECT * { ";
  for (int i = 0; i < 40; ++i) {
    hostQuery += absl::StrCat("<s> <cp1> <o", i, "> . ");
  }
  hostQuery += "}";

  {
    auto cleanupNumReplacementPlans = setRuntimeParameterForTest<
        &RuntimeParameters::materializedViewPatternMatchNumReplacementPlans_>(
        20);
    auto replacements = match(qpc, hostQuery);
    EXPECT_EQ(replacements.size(), 20u);
  }
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsPatternMatchingTest, PatternMatchingEdgeCases) {
  materializedViewsQueryAnalysis::QueryPatternCache qpc;
  auto numReplacements = [&](const std::string& query) {
    return match(qpc, query).size();
  };

  // Binding two or all three of a star view's first, second, and third
  // columns to fixed values in a single match is legal.
  registerView(qpc, "fixedPrefixStarView",
               "SELECT * { ?s <fp1> ?o1 . ?s <fp2> ?o2 }");
  EXPECT_EQ(numReplacements("SELECT * { <s1> <fp1> <o1a> . <s1> <fp2> ?o2 }"),
            1u);
  EXPECT_EQ(numReplacements("SELECT * { <s1> <fp1> <o1a> . <s1> <fp2> <o2a> }"),
            1u);

  // The first column is `?m`, the chain's middle variable, so the first
  // edge's subject `?s` is a variable, but not the first column.
  registerView(qpc, "differentSortChainView",
               "SELECT ?m ?s ?o { ?s <cs1> ?m . ?m <cs2> ?o }");
  EXPECT_EQ(numReplacements("SELECT * { ?a <cs1> ?b . ?b <cs2> ?c }"), 1u);

  // The first column is `?o2`, so the first edge's fixed (not variable)
  // object is also checked, but can never match the first column.
  registerView(qpc, "reorderedFixedArmView",
               "SELECT ?o2 ?s { ?s <cs3> <fixedArm> . ?s <cs4> ?o2 }");
  EXPECT_EQ(numReplacements("SELECT * { ?a <cs3> <fixedArm> . ?a <cs4> ?b }"),
            1u);

  // A fixed value can never be bound to a payload column (after the third
  // column), even if binding it would otherwise be legal.
  registerView(qpc, "threeArmStarView",
               "SELECT * { ?s <p1> ?o1 . ?s <p2> ?o2 . ?s <p3> ?o3 }");
  EXPECT_EQ(
      numReplacements("SELECT * { ?s <p1> ?o1 . ?s <p2> ?o2 . ?s <p3> <o3a> }"),
      0u);
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsPatternMatchingTest, BookkeepingEdgeCases) {
  // Only a subset of candidates is found due to an assignments limit of 2.
  {
    materializedViewsQueryAnalysis::QueryPatternCache qpc;
    registerView(qpc, "multiCandidateStarView",
                 "SELECT * { ?s <mp1> ?o1 . ?s <mp2> ?o2 }");
    auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
    auto cleanupNumAssignments = setRuntimeParameterForTest<
        &RuntimeParameters::materializedViewPatternMatchNumAssignments_>(2);
    // Only one of the four possible (mp1-candidate, mp2-candidate)
    // combinations is found before the limit of `2` assignments is
    // exhausted.
    EXPECT_EQ(match(qpc,
                    "SELECT * { ?s <mp1> ?a . ?s <mp1> ?b . ?s <mp2> ?c . "
                    "?s <mp2> ?d }")
                  .size(),
              1u);
    EXPECT_THAT(logStream.str(),
                ::testing::HasSubstr(
                    "materialized-view-pattern-match-num-assignments"));
  }

  // More than 64 triples in the user's query disable pattern-based
  // rewriting, since a 64-bit bitmask is used to track covered triple
  // indices.
  {
    materializedViewsQueryAnalysis::QueryPatternCache qpc;
    registerView(qpc, "tooManyTriplesView",
                 "SELECT * { ?s <tp1> ?o1 . ?s <tp2> ?o2 }");
    std::string hostQuery = "SELECT * { ";
    for (int i = 0; i < 65; ++i) {
      hostQuery += absl::StrCat("<s", i, "> <tp1> <o", i, "> . ");
    }
    hostQuery += "}";
    EXPECT_TRUE(match(qpc, hostQuery).empty());
  }

  // A property-path predicate has no `getSimplePredicate()` value and is
  // skipped when grouping the user query's triples by predicate; the other
  // triples in the same query are still matched normally.
  {
    materializedViewsQueryAnalysis::QueryPatternCache qpc;
    registerView(qpc, "propertyPathStarView",
                 "SELECT * { ?s <p1> ?o1 . ?s <p2> ?o2 }");
    auto replacements =
        match(qpc, "SELECT * { ?s <p1> ?o1 . ?s <p2> ?o2 . ?s <p3>* ?o3 }");
    ASSERT_EQ(replacements.size(), 1u);
    EXPECT_EQ(absl::popcount(replacements.at(0).coveredTriples_), 2u);
    EXPECT_EQ(replacements.at(0).coveredTriples_ & (uint64_t{1} << 2), 0u);
  }

  // With two candidate views sharing predicates with the query, once the
  // first view exhausts the shared replacement-plan limit, the loop breaks
  // before even attempting the second view.
  {
    materializedViewsQueryAnalysis::QueryPatternCache qpc;
    registerView(qpc, "aFirstView", "SELECT * { ?s <cp1> ?o1 . ?s <cp2> ?o2 }");
    registerView(qpc, "bSecondView", "SELECT * { ?x <cp2> ?y . ?x <cp3> ?z }");
    auto cleanupNumReplacementPlans = setRuntimeParameterForTest<
        &RuntimeParameters::materializedViewPatternMatchNumReplacementPlans_>(
        1);
    EXPECT_EQ(match(qpc, "SELECT * { ?s <cp1> ?o1 . ?s <cp2> ?o2 }").size(),
              1u);
  }
}
