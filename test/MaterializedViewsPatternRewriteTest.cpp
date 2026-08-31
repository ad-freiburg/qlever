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
#include "./util/RuntimeParametersTestHelpers.h"
#include "engine/MaterializedViews.h"
#include "engine/MaterializedViewsQueryAnalysis.h"
#include "index/vocabulary/EncodedIriManager.h"
#include "libqlever/Qlever.h"
#include "parser/SparqlParser.h"

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
  const std::string starTtl =
      " <s1> <p1> <o1a> . \n"
      " <s1> <p2> <o2a> . \n"
      " <s2> <p1> <o1b> . \n"
      " <s2> <p2> <o2b> . \n"
      " <s2> <p3> <o3a> . \n";
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
    auto l = generateLocationTrace(sourceLocation);
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

  // Two view variables may be fixed to the same query value: `<gp2s> <p1>
  // <gp2s>` fixes both `?a` and `?b`. Unlike two query variables on one view
  // variable, this is no injectivity violation (`makeScanConfig` allows the
  // same fixed value on several columns).
  // Two view variables may be fixed to the same query value: `<gp2s> <p1>
  // <gp2s>` fixes both `?a` and `?b`. Unlike two query variables on one view
  // variable, this is no injectivity violation (`makeScanConfig` allows the
  // same fixed value on several columns). Checked directly: with every
  // triple's subject fixed to the same constant, the planner's cost estimate
  // on this toy dataset does not actually pick the rewrite (it prefers three
  // separate single-triple scans), even though the pattern matcher finds it.
  {
    auto plan = qlv.parseAndPlanQuery(
        "SELECT * { <gp2s> <p1> <gp2s> . <gp2s> <p2> ?c . <gp2s> <p9> ?o9 }");
    auto qec = qlv.createQueryExecutionContext(qlv.indexAndViewsSnapshot());
    manager.writeViewToDisk(
        "repeatedFixedValueView",
        qlv.parseAndPlanQuery("SELECT * { ?a <p1> ?b . ?b <p2> ?c }"));
    auto view = manager.getView("repeatedFixedValueView", qec.get());
    materializedViewsQueryAnalysis::QueryPatternCache qpc;
    qpc.analyzeView(view, qec.get());
    const auto& triples =
        plan.parsedQuery()._rootGraphPattern._graphPatterns.at(0).getBasic();
    auto replacements = qpc.makeJoinReplacementIndexScans(qec.get(), triples);
    using materializedViewsQueryAnalysis::MaterializedViewJoinReplacement;
    EXPECT_THAT(replacements, ::testing::ElementsAre(AD_FIELD(
                                  MaterializedViewJoinReplacement,
                                  coveredTriples_, ::testing::Eq(0b011u))));
  }

  // Disconnected pattern (no shared variable): rejected outright, since the
  // planner could never select a replacement spanning two components.
  expectNotSuitableForRewrite(
      qlv, manager, "disconnectedPatternView",
      "SELECT * { ?s1 <p1> ?o1 . ?s2 <p2> ?o2 }",
      "No supported query pattern for rewriting joins was found");

  // A `BIND` that is filtered out as invariant can still occupy the view's
  // column 0, which matching then never assigns. Such a view must be
  // rejected, else `makeIndexScan` throws. Checked directly, because the
  // view's query has two graph patterns (the triples and the `BIND`), which
  // `expectNotSuitableForRewrite` does not support.
  {
    auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
    auto plan = qlv.parseAndPlanQuery(
        "SELECT ?x ?s ?o1 ?o2 { ?s <p2> ?o1 . ?s <p3> ?o2 . BIND(1 AS ?x) }");
    auto qec = qlv.createQueryExecutionContext(qlv.indexAndViewsSnapshot());
    manager.writeViewToDisk("bindOccupiesColumnZeroView", plan);
    auto view = manager.getView("bindOccupiesColumnZeroView", qec.get());
    materializedViewsQueryAnalysis::QueryPatternCache qpc;
    qpc.analyzeView(view, qec.get());
    EXPECT_THAT(logStream.str(),
                ::testing::HasSubstr(
                    "No supported query pattern for rewriting joins was "
                    "found"));
    const auto& triples =
        plan.parsedQuery()._rootGraphPattern._graphPatterns.at(0).getBasic();
    EXPECT_TRUE(qpc.makeJoinReplacementIndexScans(qec.get(), triples).empty());
    manager.unloadViewIfLoaded("bindOccupiesColumnZeroView");
  }
}

// _____________________________________________________________________________
// `QueryPlanner` expands `ql:contains-word` into one planner node per word
// (and defers `ql:contains-entity`), which `coveredTriples_` cannot represent.
// Such pseudo-predicates must therefore be rejected as pattern edges.
TEST_F(MaterializedViewsPatternRewriteTest,
       fullTextPseudoPredicateNotRewritten) {
  const std::string onDiskBase = gtestCurrentTestName();
  const std::string ttlFilename = absl::StrCat(onDiskBase, ".ttl");
  {
    std::ofstream ttl{ttlFilename};
    ttl << " <s1> <p1> \"some text with several needle words\" . \n"
           " <s1> <p2> <o1> . \n";
  }
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

// _____________________________________________________________________________
// Regression test for #3193: an aggregate removes a pattern variable from the
// view's columns, so the view must not be registered for rewriting. Covers
// explicit (`GROUP BY`) and implicit (aggregate in `SELECT`) aggregation.
TEST(MaterializedViewsPatternRewriteRejectionTest,
     aggregatingPatternsNotRewritten) {
  const std::string onDiskBase = gtestCurrentTestName();
  const std::string starTtl =
      " <s1> <p1> <o1a> . \n"
      " <s1> <p2> <o2a> . \n"
      " <s2> <p1> <o1b> . \n"
      " <s2> <p2> <o2b> . \n";
  materializedViewsTestHelpers::makeTestIndex(onDiskBase, starTtl);
  auto cleanUp = absl::Cleanup(
      [&]() { materializedViewsTestHelpers::removeTestIndex(onDiskBase); });
  qlever::EngineConfig config;
  config.baseName_ = onDiskBase;
  qlever::Qlever qlv{config};
  MaterializedViewsManager manager{onDiskBase};

  constexpr std::string_view kAggregatingReason = "The view's query aggregates";

  expectNotSuitableForRewrite(
      qlv, manager, "aggregatingStarView",
      "SELECT ?s (COUNT(?o1) AS ?c) { ?s <p1> ?o1 . ?s <p2> ?o2 } "
      "GROUP BY ?s",
      kAggregatingReason);
  expectNotSuitableForRewrite(
      qlv, manager, "aggregatingChainView",
      "SELECT ?s (COUNT(?m) AS ?c) { ?s <p1> ?m . ?m <p2> ?o } "
      "GROUP BY ?s",
      kAggregatingReason);

  // Same as above, but the star's subject (rather than one of its arms) is
  // aggregated away.
  expectNotSuitableForRewrite(
      qlv, manager, "aggregatingStarSubjectView",
      "SELECT ?o1 ?o2 (COUNT(?s) AS ?c) { ?s <p1> ?o1 . ?s <p2> ?o2 } "
      "GROUP BY ?o1 ?o2",
      kAggregatingReason);

  // Same as `aggregatingChainView` above, but the chain's first (subject) or
  // last (object) variable is aggregated away instead of the middle one.
  expectNotSuitableForRewrite(
      qlv, manager, "aggregatingChainSubjectView",
      "SELECT ?m ?o (COUNT(?s) AS ?c) { ?s <p1> ?m . ?m <p2> ?o } "
      "GROUP BY ?m ?o",
      kAggregatingReason);
  expectNotSuitableForRewrite(
      qlv, manager, "aggregatingChainObjectView",
      "SELECT ?s ?m (COUNT(?o) AS ?c) { ?s <p1> ?m . ?m <p2> ?o } "
      "GROUP BY ?s ?m",
      kAggregatingReason);

  // Same as `aggregatingChainView` above, but the `GROUP BY` is implicit (no
  // explicit `GROUP BY` clause, just an aggregate in the `SELECT` clause).
  expectNotSuitableForRewrite(
      qlv, manager, "implicitlyAggregatingChainView",
      "SELECT (COUNT(?m) AS ?c) { ?s <p1> ?m . ?m <p2> ?o }",
      kAggregatingReason);
}

// _____________________________________________________________________________
TEST(MaterializedViewsPatternRewriteRejectionTest,
     unprojectedVariablePatternsNotRewritten) {
  const std::string onDiskBase = gtestCurrentTestName();
  const std::string starTtl =
      " <s1> <p1> <o1a> . \n"
      " <s1> <p2> <o2a> . \n"
      " <s2> <p1> <o1b> . \n"
      " <s2> <p2> <o2b> . \n";
  materializedViewsTestHelpers::makeTestIndex(onDiskBase, starTtl);
  auto cleanUp = absl::Cleanup(
      [&]() { materializedViewsTestHelpers::removeTestIndex(onDiskBase); });
  qlever::EngineConfig config;
  config.baseName_ = onDiskBase;
  qlever::Qlever qlv{config};
  MaterializedViewsManager manager{onDiskBase};

  constexpr std::string_view kNoPatternReason =
      "No supported query pattern for rewriting joins was found";

  // Star: the subject is not selected.
  expectNotSuitableForRewrite(qlv, manager, "unprojectedStarSubjectView",
                              "SELECT ?o1 ?o2 { ?s <p1> ?o1 . ?s <p2> ?o2 }",
                              kNoPatternReason);
  // Star: one arm's object is not selected.
  expectNotSuitableForRewrite(qlv, manager, "unprojectedStarArmView",
                              "SELECT ?s ?o2 { ?s <p1> ?o1 . ?s <p2> ?o2 }",
                              kNoPatternReason);

  // Chain: the subject is not selected.
  expectNotSuitableForRewrite(qlv, manager, "unprojectedChainSubjectView",
                              "SELECT ?m ?o { ?s <p1> ?m . ?m <p2> ?o }",
                              kNoPatternReason);
  // Chain: the middle (chain) variable is not selected.
  expectNotSuitableForRewrite(qlv, manager, "unprojectedChainMiddleView",
                              "SELECT ?s ?o { ?s <p1> ?m . ?m <p2> ?o }",
                              kNoPatternReason);
  // Chain: the object is not selected.
  expectNotSuitableForRewrite(qlv, manager, "unprojectedChainObjectView",
                              "SELECT ?s ?m { ?s <p1> ?m . ?m <p2> ?o }",
                              kNoPatternReason);
}

// _____________________________________________________________________________
// Regression test: FILTER, VALUES, DISTINCT/REDUCED, LIMIT/OFFSET and FROM
// restrict the rows written to disk, but (unlike aggregation) leave
// `variableToColumnMap()` intact. Without an explicit check, the view would be
// used for a query needing the full join.
TEST(MaterializedViewsPatternRewriteRejectionTest,
     restrictingModifiersNotRewritten) {
  const std::string onDiskBase = gtestCurrentTestName();
  const std::string starTtl =
      " <s1> <p1> <o1a> . \n"
      " <s1> <p2> <o2a> . \n"
      " <s2> <p1> <o1b> . \n"
      " <s2> <p2> <o2b> . \n";
  materializedViewsTestHelpers::makeTestIndex(onDiskBase, starTtl);
  auto cleanUp = absl::Cleanup(
      [&]() { materializedViewsTestHelpers::removeTestIndex(onDiskBase); });
  qlever::EngineConfig config;
  config.baseName_ = onDiskBase;
  qlever::Qlever qlv{config};
  MaterializedViewsManager manager{onDiskBase};

  // Star / chain with a top-level FILTER.
  expectNotSuitableForRewrite(
      qlv, manager, "filteredStarView",
      "SELECT ?s ?o1 ?o2 { ?s <p1> ?o1 . ?s <p2> ?o2 . FILTER(?s = <s1>) }",
      "top-level FILTER");
  expectNotSuitableForRewrite(
      qlv, manager, "filteredChainView",
      "SELECT ?s ?m ?o { ?s <p1> ?m . ?m <p2> ?o . FILTER(?s = <s1>) }",
      "top-level FILTER");

  // Star / chain with a trailing VALUES clause.
  expectNotSuitableForRewrite(
      qlv, manager, "valuesStarView",
      "SELECT ?s ?o1 ?o2 { ?s <p1> ?o1 . ?s <p2> ?o2 } VALUES ?s { <s1> }",
      "trailing VALUES clause");
  expectNotSuitableForRewrite(
      qlv, manager, "valuesChainView",
      "SELECT ?s ?m ?o { ?s <p1> ?m . ?m <p2> ?o } VALUES ?s { <s1> }",
      "trailing VALUES clause");

  // Star with DISTINCT, chain with REDUCED.
  expectNotSuitableForRewrite(
      qlv, manager, "distinctStarView",
      "SELECT DISTINCT ?s ?o1 ?o2 { ?s <p1> ?o1 . ?s <p2> ?o2 }",
      "DISTINCT or REDUCED");
  expectNotSuitableForRewrite(
      qlv, manager, "reducedChainView",
      "SELECT REDUCED ?s ?m ?o { ?s <p1> ?m . ?m <p2> ?o }",
      "DISTINCT or REDUCED");

  // Star with LIMIT, chain with OFFSET.
  auto expectIgnoredForPatternRewrite = [&](const std::string& query,
                                            std::string_view expectedReason) {
    auto plan = qlv.parseAndPlanQuery(query);
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
      qlv, manager, "fromStarView",
      "SELECT ?s ?o1 ?o2 FROM <g> { ?s <p1> ?o1 . ?s <p2> ?o2 }",
      "FROM or FROM NAMED clause");
  expectNotSuitableForRewrite(
      qlv, manager, "fromNamedChainView",
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
  const std::string chainTtl =
      " <s1> <p1> <m2> . \n"
      " <m1> <p2> <o1> . \n"
      " <s2> <p1> <m2> . \n"
      " <m2> <p2> <http://example.com/> . \n"
      " <m2> <p3> \"abc\" . \n"
      " <s2> <p3> <o3> . \n";
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
  // the same variable. Planning previously failed with an exception. The
  // winning plan is not fixed here (the repeated variable is planned as an
  // internal variable plus an equality filter, and the cache-key based
  // rewriting may then legitimately replace the join by a scan of the view),
  // so check that the query is planned and answered correctly instead of
  // checking the plan.
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

// _____________________________________________________________________________
// An assignment limit too small for one full match (a 2-edge chain needs >= 2
// attempts) finds nothing and warns; the default limit finds the match.
TEST(MaterializedViewsPatternMatchLimitsTest,
     PatternMatchNumAssignmentsIsRespected) {
  const std::string onDiskBase = gtestCurrentTestName();
  materializedViewsTestHelpers::makeTestIndex(onDiskBase,
                                              " <s1> <p0> <o1> .\n");
  auto cleanUp = absl::Cleanup(
      [&]() { materializedViewsTestHelpers::removeTestIndex(onDiskBase); });
  qlever::EngineConfig config;
  config.baseName_ = onDiskBase;
  qlever::Qlever qlv{config};
  MaterializedViewsManager manager{onDiskBase};
  auto qec = qlv.createQueryExecutionContext(qlv.indexAndViewsSnapshot());

  materializedViewsQueryAnalysis::QueryPatternCache qpc;
  manager.writeViewToDisk(
      "numAssignmentsChain",
      qlv.parseAndPlanQuery("SELECT * { ?s <bp1> ?m . ?m <bp2> ?o }"));
  auto view = manager.getView("numAssignmentsChain", qec.get());
  qpc.analyzeView(view, qec.get());

  auto plan = qlv.parseAndPlanQuery("SELECT * { ?s <bp1> ?m . ?m <bp2> ?o }");
  const auto& triples =
      plan.parsedQuery()._rootGraphPattern._graphPatterns.at(0).getBasic();

  {
    auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
    auto cleanupNumAssignments = setRuntimeParameterForTest<
        &RuntimeParameters::materializedViewPatternMatchNumAssignments_>(1);
    EXPECT_TRUE(qpc.makeJoinReplacementIndexScans(qec.get(), triples).empty());
    EXPECT_THAT(logStream.str(),
                ::testing::HasSubstr(
                    "materialized-view-pattern-match-num-assignments"));
  }
  {
    auto cleanupNumAssignments = setRuntimeParameterForTest<
        &RuntimeParameters::materializedViewPatternMatchNumAssignments_>(
        100'000);
    EXPECT_FALSE(qpc.makeJoinReplacementIndexScans(qec.get(), triples).empty());
  }
}

// _____________________________________________________________________________
// A limit of `0` assignments deliberately disables pattern-based rewriting: no
// match is found, and (unlike a too-small limit) no warning is logged.
TEST(MaterializedViewsPatternMatchLimitsTest,
     PatternMatchNumAssignmentsZeroDisables) {
  const std::string onDiskBase = gtestCurrentTestName();
  materializedViewsTestHelpers::makeTestIndex(onDiskBase,
                                              " <s1> <p0> <o1> .\n");
  auto cleanUp = absl::Cleanup(
      [&]() { materializedViewsTestHelpers::removeTestIndex(onDiskBase); });
  qlever::EngineConfig config;
  config.baseName_ = onDiskBase;
  qlever::Qlever qlv{config};
  MaterializedViewsManager manager{onDiskBase};
  auto qec = qlv.createQueryExecutionContext(qlv.indexAndViewsSnapshot());

  materializedViewsQueryAnalysis::QueryPatternCache qpc;
  manager.writeViewToDisk(
      "numAssignmentsZeroChain",
      qlv.parseAndPlanQuery("SELECT * { ?s <bz1> ?m . ?m <bz2> ?o }"));
  auto view = manager.getView("numAssignmentsZeroChain", qec.get());
  qpc.analyzeView(view, qec.get());

  auto plan = qlv.parseAndPlanQuery("SELECT * { ?s <bz1> ?m . ?m <bz2> ?o }");
  const auto& triples =
      plan.parsedQuery()._rootGraphPattern._graphPatterns.at(0).getBasic();

  auto [logCleanup, logStream] = setGlobalLoggingStreamToStringStream();
  auto cleanupNumAssignments = setRuntimeParameterForTest<
      &RuntimeParameters::materializedViewPatternMatchNumAssignments_>(0);
  EXPECT_TRUE(qpc.makeJoinReplacementIndexScans(qec.get(), triples).empty());
  EXPECT_THAT(logStream.str(),
              ::testing::Not(::testing::HasSubstr(
                  "materialized-view-pattern-match-num-assignments")));
}

// _____________________________________________________________________________
// Regression test: a cheap-to-complete match can complete far more often than
// the assignment limit suggests, so the number of replacement plans handed to
// the query planner needs an independent cap.
TEST(MaterializedViewsPatternMatchLimitsTest, ReplacementCountIsCapped) {
  const std::string onDiskBase = gtestCurrentTestName();
  materializedViewsTestHelpers::makeTestIndex(onDiskBase,
                                              " <s1> <p0> <o1> .\n");
  auto cleanUp = absl::Cleanup(
      [&]() { materializedViewsTestHelpers::removeTestIndex(onDiskBase); });
  qlever::EngineConfig config;
  config.baseName_ = onDiskBase;
  qlever::Qlever qlv{config};
  MaterializedViewsManager manager{onDiskBase};
  auto qec = qlv.createQueryExecutionContext(qlv.indexAndViewsSnapshot());

  materializedViewsQueryAnalysis::QueryPatternCache qpc;
  manager.writeViewToDisk(
      "capView",
      qlv.parseAndPlanQuery("SELECT * { ?s <cp1> ?o1 . ?s <cp1> ?o2 }"));
  auto view = manager.getView("capView", qec.get());
  qpc.analyzeView(view, qec.get());

  // 40 triples sharing subject and predicate: assigning two of them to the
  // view's two interchangeable arms yields 40*39 = 1560 matches, well over the
  // cap. Parsed directly, to sidestep the planner's 64-triple limit.
  std::string hostQuery = "SELECT * { ";
  for (int i = 0; i < 40; ++i) {
    hostQuery += absl::StrCat("<s> <cp1> <o", i, "> . ");
  }
  hostQuery += "}";
  EncodedIriManager encodedIriManager;
  auto parsed = SparqlParser::parseQuery(&encodedIriManager, hostQuery, {});
  const auto& triples =
      parsed._rootGraphPattern._graphPatterns.at(0).getBasic();

  auto replacements = qpc.makeJoinReplacementIndexScans(qec.get(), triples);
  EXPECT_GT(replacements.size(), 0u);
  EXPECT_LE(replacements.size(), 1000u);
}
