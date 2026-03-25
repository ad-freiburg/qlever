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
  const std::string onDiskBase = "_materializedViewRewriteStar";
  const std::string viewName = "testViewStar";

  materializedViewsTestHelpers::makeTestIndex(onDiskBase, starTtl);
  auto cleanUp = absl::MakeCleanup(
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
  // joined normally.
  qpExpect(qlv, simpleStarJoinPredicateTwice,
           h::Join(starView("?s", "?o1", "?o2"),
                   h::IndexScanFromStrings("?s", "<p2>", "?o3")));

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
    expectNotSuitableForRewrite(qlv, manager, "noStarRewriteView", query);
  };

  noStarRewrite("SELECT * { <s1> <p1> ?o1 . ?s <p2> ?o2 }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?s <p1> ?o2 }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 } ");
  noStarRewrite("SELECT * { ?s <p1> ?s . ?s <p2> ?o1 }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?s <p2> ?o1 }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?s <p2> <o2a> }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?s <p2> ?o2 . ?s <p3> <o2a> }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?o2 ^<p2> ?s }");
  noStarRewrite("SELECT * { ?s1 <p1> ?o1 . ?s2 <p2> ?o2 }");
  noStarRewrite("SELECT * { ?s <p1> ?o1 . ?s <p2>* ?o2 }");
}

// _____________________________________________________________________________
INSTANTIATE_TEST_SUITE_P(MaterializedViewsTest,
                         MaterializedViewsStarRewriteTest,
                         ::testing::Values(
                             // Default case.
                             RewriteTestParams{std::string{simpleStar}, 1500},

                             // Forced greedy planning.
                             RewriteTestParams{std::string{simpleStar}, 1}));
