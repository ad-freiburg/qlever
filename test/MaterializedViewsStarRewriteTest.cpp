// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/functional/bind_front.h>
#include <gmock/gmock.h>

#include "./MaterializedViewsTestHelpers.h"
#include "./util/RuntimeParametersTestHelpers.h"

namespace {

using namespace materializedViewsTestHelpers;
using namespace ad_utility::testing;
using V = Variable;

}  // namespace

// Example queries for testing star query rewriting.
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
TEST_P(MaterializedViewsStarRewriteTest, starRewrite) {
  RewriteTestParams p = GetParam();
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::queryPlanningBudget_>(
          p.queryPlanningBudget_);

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
  manager.writeViewToDisk(viewName, qlv.parseAndPlanQuery(p.writeQuery_));
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

  noStarRewrite("SELECT * { <s1> <p1> ?o1 . ?s <p2> ?o2 }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . <s1> <p2> ?o2 }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 } ");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?s <p2> <o2a> }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?s <p2> ?o2 . ?s <p3> <o2a> }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?o2 ^<p2> ?s }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?s <p2>* ?o2 }");

  // The general pattern matcher (subgraph isomorphism between the view's
  // pattern graph and the query, see `MaterializedViewsQueryAnalysis`) is not
  // restricted to the "star" and "chain" shapes above: it also accepts
  // patterns the old special-cased star/chain code used to reject solely
  // because its predicate-keyed lookup could not represent them. Each case
  // checks (like the other rewrite tests above) that the query plans to
  // exactly the expected view scan, not merely that some rewrite was found.
  auto generalPatternView =
      std::bind_front(&viewScanSimple, "generalPatternView");

  // Two arms with the same predicate (rejected by the old star cache, which
  // needed distinct predicates as its lookup key). Since both arms are
  // structurally and semantically interchangeable here, either may end up
  // bound to which of the two (identically named, since view and query are
  // the same text) object variables -- same ambiguity as
  // `simpleStarJoinPredicateTwice` above, hence `AnyOf`.
  expectSuitableForRewrite(
      qlv, "generalPatternView", "SELECT * { ?s <p1> ?o1 . ?s <p1> ?o2 }",
      ::testing::AnyOf(generalPatternView("?s", "?o1", "?o2"),
                       generalPatternView("?s", "?o2", "?o1")));

  // A self-loop arm (subject and object of one triple are the same
  // variable), alongside a normal arm.
  expectSuitableForRewrite(
      qlv, "generalPatternView",
      "SELECT * { ?s <p1> ?s . ?s <p2> ?o1 . ?s <p3> ?o2 }",
      generalPatternView("?s", "?o1", "?o2"));

  // Two arms converging on the same object variable instead of distinct
  // ones, alongside a normal arm.
  expectSuitableForRewrite(
      qlv, "generalPatternView",
      "SELECT * { ?s <p1> ?o1 . ?s <p2> ?o1 . ?s <p3> ?o2 }",
      generalPatternView("?s", "?o1", "?o2"));

  // Two triples with no variable in common (a disconnected pattern); still a
  // valid embedding target since there is no shared-variable constraint
  // between them to violate. 4 distinct variables, so the 4th (?o2) is an
  // additional (non-SPO) column of the view.
  expectSuitableForRewrite(qlv, "generalPatternView",
                           "SELECT * { ?s1 <p1> ?o1 . ?s2 <p2> ?o2 }",
                           viewScan("generalPatternView", "?s1", "?o1", "?s2",
                                    std::nullopt, {{3, V{"?o2"}}}));
}

// _____________________________________________________________________________
INSTANTIATE_TEST_SUITE_P(MaterializedViewsTest,
                         MaterializedViewsStarRewriteTest,
                         ::testing::Values(
                             // Default case.
                             RewriteTestParams{std::string{simpleStar}, 1500},

                             // Forced greedy planning.
                             RewriteTestParams{std::string{simpleStar}, 1}));

// _____________________________________________________________________________
// Regression test for #3193: an aggregate in the view's query removes one of
// the pattern's variables from the view's columns (`?o1`/`?m` only occur inside
// `COUNT(...)`), so neither the star nor the chain must be registered for
// pattern-based rewriting. Covers both explicit (`GROUP BY`) and implicit
// (aggregate in `SELECT` without a `GROUP BY` clause) aggregation.
TEST(MaterializedViewsStarRewriteAggregationTest,
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
TEST(MaterializedViewsStarRewriteAggregationTest,
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
// Regression test: a top-level FILTER, a trailing VALUES clause, or
// DISTINCT/REDUCED in the view's defining query restrict which rows actually
// end up on disk, but (unlike aggregation) do not remove any variable from
// `variableToColumnMap()`. Without an explicit check for these, a query with
// the same star/chain pattern could be silently rewritten to read the view
// even though its content is only a restricted subset of the join.
TEST(MaterializedViewsStarRewriteAggregationTest,
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
  expectNotSuitableForRewrite(
      qlv, manager, "limitStarView",
      "SELECT ?s ?o1 ?o2 { ?s <p1> ?o1 . ?s <p2> ?o2 } LIMIT 1",
      "LIMIT or OFFSET clause");
  expectNotSuitableForRewrite(
      qlv, manager, "offsetChainView",
      "SELECT ?s ?m ?o { ?s <p1> ?m . ?m <p2> ?o } OFFSET 1",
      "LIMIT or OFFSET clause");
}
