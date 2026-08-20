// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/functional/bind_front.h>
#include <absl/strings/match.h>
#include <gmock/gmock.h>

#include "./MaterializedViewsTestHelpers.h"
#include "./QueryPlannerTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/RuntimeParametersTestHelpers.h"

namespace {

using namespace materializedViewsTestHelpers;
using namespace ad_utility::testing;
using V = Variable;

}  // namespace

// _____________________________________________________________________________
TEST_F(MaterializedViewsCacheKeyRewriteTest, CacheKeyRewrite) {
  auto prepareView = [this](const auto& name, const auto& query,
                            size_t expectedRows, const auto& qpMatcher,
                            source_location sourceLocation =
                                AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(sourceLocation);
    EXPECT_EQ(getQueryResultAsIdTable(query).numRows(), expectedRows);
    qlv().writeMaterializedView(name, query);
    qlv().loadMaterializedView(name);
    // The view is always detected if the user query is exactly the view query.
    qpExpect(qlv(), query, qpMatcher);
  };

  // Neither a star nor a chain.
  const std::string writeQuery1 = R"(
    SELECT ?s ?m ?o1 ?o2 {
      ?s <p1> ?o1 .
      ?s <p3> ?m .
      ?m <p2> ?o2 .
    }
  )";
  prepareView("testView1", writeQuery1, 2,
              viewScan("testView1", "?s", "?m", "?o1", 4, {{3, V{"?o2"}}}));

  // User query is the view query only with an edge reversed.
  qpExpect(qlv(), R"(
      SELECT * {
        ?s <p1> ?o1 .
        ?s <p3> ?m .
        ?o2 ^<p2> ?m .
      }
    )",
           viewScan("testView1", "?s", "?m", "?o1", 4, {{3, V{"?o2"}}}));

  // User query is the view query only with a property path instead of a manual
  // join.
  qpExpect(qlv(), R"(
      SELECT * {
        ?s <p1> ?o1 .
        ?s <p3>/<p2> ?o2 .
      }
    )",
           viewScan("testView1", "?s", "?_QLever_internal_variable_qp_0", "?o1",
                    4, {{3, V{"?o2"}}}));

  // User query contains an additional filter.
  qpExpect(qlv(), R"(
      SELECT * {
        ?s <p1> ?o1 .
        ?s <p3>/<p2> ?o2 .
        FILTER (?o2 > 5)
      }
    )",
           h::Filter("?o2 > 5", viewScan("testView1", "?s",
                                         "?_QLever_internal_variable_qp_0",
                                         "?o1", 4, {{3, V{"?o2"}}})));

  // User query contains an additional join.
  qpExpect(qlv(), R"(
      SELECT * {
        ?s <p1> ?o1 .
        ?s <p3> ?m1 .
        ?m2 <p4> ?o2 .
        ?m1 <p2> ?m2 .
      }
    )",
           h::Join(h::Sort(viewScan("testView1", "?s", "?m1", "?o1", 4,
                                    {{3, V{"?m2"}}})),
                   h::IndexScanFromStrings("?m2", "<p4>", "?o2")));

  // User query contains an additional `OPTIONAL`.
  const std::string optionalQuery = R"(
      SELECT ?s ?o1 ?m1 ?m2 ?o2 {
        ?s <p1> ?o1 .
        ?s <p3> ?m1 .
        ?m1 <p2> ?m2 .
        OPTIONAL { ?m2 <p4> ?o2 }
      }
    )";
  qpExpect(qlv(), optionalQuery,
           h::OptionalJoin(h::Sort(viewScan("testView1", "?s", "?m1", "?o1", 4,
                                            {{3, V{"?m2"}}})),
                           h::IndexScanFromStrings("?m2", "<p4>", "?o2")));

  // Filter in write query.
  qlv().unloadMaterializedView("testView1");
  const std::string writeQuery2 = R"(
    SELECT ?s ?m ?o1 ?o2 {
      ?s <p1> ?o1 .
      ?s <p3> ?m .
      ?m <p2> ?o2 .
      FILTER (?o2 < 6)
    }
  )";
  prepareView("testView2", writeQuery2, 1,
              viewScan("testView2", "?s", "?m", "?o1", 4, {{3, V{"?o2"}}}));

  // Filter in write query and an additional filter in the user query.
  qpExpect(qlv(), R"(
        SELECT ?s ?m ?o1 ?o2 {
          ?s <p1> ?o1 .
          ?s <p3> ?m .
          ?m <p2> ?o2 .
          FILTER (?o2 < 6)
          FILTER (?m != <s1>)
        }
      )",
           h::Filter("?m != <s1>", viewScan("testView2", "?s", "?m", "?o1", 4,
                                            {{3, V{"?o2"}}})));

  // Optional in write query.
  qlv().unloadMaterializedView("testView2");
  prepareView("testView3", optionalQuery, 2,
              viewScan("testView3", "?s", "?o1", "?m1", 5,
                       {{3, V{"?m2"}}, {4, V{"?o2"}}}));

  // Write query containing a `BIND`.
  const std::string bindQuery = R"(
    SELECT ?s ?m ?o1 ?o2 ?b {
      ?s <p1> ?o1 .
      ?s <p3> ?m .
      ?m <p2> ?o2 .
      BIND (5 * ?o2 AS ?b)
    }
  )";
  qlv().unloadMaterializedView("testView3");
  prepareView("testView4", bindQuery, 2,
              viewScan("testView4", "?s", "?m", "?o1", 5,
                       {{3, V{"?o2"}}, {4, V{"?b"}}}));

  // User query is the same as the write query but the `BIND` is omitted.
  qpExpect(qlv(), writeQuery1,
           viewScan("testView4", "?s", "?m", "?o1", 4, {{3, V{"?o2"}}}));

  // User query is the same as the write query but it contains an additional
  // `BIND`.
  qpExpect(qlv(), R"(
      SELECT * {
        ?s <p1> ?o1 .
        ?s <p3> ?m .
        ?m <p2> ?o2 .
        BIND (5 * ?o2 AS ?b1)
        BIND (?o2 + 2 AS ?b2)
      }
    )",
           h::Bind(viewScan("testView4", "?s", "?m", "?o1", 5,
                            {{3, V{"?o2"}}, {4, V{"?b1"}}}),
                   "?o2 + 2", V{"?b2"}));
}

namespace {

// The plan for `query` must (not) contain a scan on a materialized view. View
// permutations live in files named `<index>.view.<name>...`, which is what an
// `IndexScan`'s cache key contains for them.
void expectViewScanUsed(
    qlever::Qlever& qlv, const std::string& query, bool expected,
    source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  qlv.clearQueryResultCache();
  auto plan = qlv.parseAndPlanQuery(query);
  EXPECT_EQ(
      absl::StrContains(plan.queryExecutionTree().getCacheKey(), ".view."),
      expected);
}

}  // namespace

// _____________________________________________________________________________
// Cache-key based rewriting when the view's first column does not appear as a
// variable in the user query, but is fixed to an IRI or literal. Note that the
// views below are neither a star nor a chain, so a rewrite of these queries can
// only come from the cache-key based matching, not from the pattern-based one.
TEST_F(MaterializedViewsCacheKeyRewriteTest, CacheKeyRewriteFixedFirstColumn) {
  // Compare the result of `query` to the result of the same query planned with
  // materialized view rewriting disabled. The view is sorted differently than
  // the join it replaces, so both results have to be sorted to be comparable.
  auto expectSameResultWithoutRewriting =
      [this](const std::string& query, const std::string& sortBy,
             source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
        auto l = generateLocationTrace(sourceLocation);
        auto sortedQuery = absl::StrCat(query, " INTERNAL SORT BY ", sortBy);
        auto rewritten = getQueryResultAsIdTable(sortedQuery);
        auto cleanup = setRuntimeParameterForTest<
            &RuntimeParameters::enableMaterializedViewQueryRewrite_>(false);
        qlv().clearQueryResultCache();
        EXPECT_THAT(rewritten,
                    matchesIdTable(getQueryResultAsIdTable(sortedQuery)));
      };

  // A view whose first column `?s` is the subject of two of its triples.
  const std::string subjectViewQuery = R"(
    SELECT ?s ?m ?o1 ?o2 {
      ?s <p1> ?o1 .
      ?s <p3> ?m .
      ?m <p2> ?o2 .
    }
  )";
  qlv().writeMaterializedView("subjectView", subjectViewQuery);
  qlv().loadMaterializedView("subjectView");
  auto subjectView =
      viewScan("subjectView", "?s", "?m", "?o1", 4, {{3, V{"?o2"}}});

  // The user query is the view query.
  qpExpect(qlv(), subjectViewQuery, subjectView);

  // The user query is the view query plus an unrelated triple. Under dynamic
  // programming a `QueryExecutionTree` is built for every subset of the query's
  // triples, so the subset that matches the view is found.
  qpExpect(qlv(), R"(
    SELECT * {
      ?s <p1> ?o1 .
      ?s <p3> ?m .
      ?m <p2> ?o2 .
      ?m <p4> ?x .
    }
  )",
           h::Join(h::Sort(subjectView),
                   h::IndexScanFromStrings("?m", "<p4>", "?x")));

  // The first column is fixed in the user query.
  const std::string fixedSubjectQuery = R"(
    SELECT * {
      <s2> <p1> ?o1 .
      <s2> <p3> ?m .
      ?m <p2> ?o2 .
    }
  )";
  qpExpect(qlv(), fixedSubjectQuery,
           viewScan("subjectView", "<s2>", "?m", "?o1", 3, {{3, V{"?o2"}}}));
  expectSameResultWithoutRewriting(fixedSubjectQuery, "?o1 ?m ?o2");

  // The same for a value for which the view holds no row at all.
  expectSameResultWithoutRewriting(R"(
    SELECT * {
      <s1> <p1> ?o1 .
      <s1> <p3> ?m .
      ?m <p2> ?o2 .
    }
  )",
                                   "?o1 ?m ?o2");

  // Fixing the subject of the view's two `?s` triples leaves them without a
  // shared variable, so the query's triple graph falls apart into two connected
  // components. The query planner optimizes each component separately, so the
  // view can then only replace the whole query (as above), not a part of it.
  // This is the same restriction that applies to pattern-based rewriting.
  expectViewScanUsed(qlv(), R"(
    SELECT * {
      <s2> <p1> ?o1 .
      <s2> <p3> ?m .
      ?m <p2> ?o2 .
      ?m <p4> ?x .
    }
  )",
                     false);

  // A fixed value on a column other than the first one cannot be expressed as a
  // scan on the view, which is sorted on its first column.
  expectViewScanUsed(qlv(), R"(
    SELECT * {
      ?s <p1> ?o1 .
      ?s <p3> <s3> .
      <s3> <p2> ?o2 .
    }
  )",
                     false);
  qlv().unloadMaterializedView("subjectView");

  // A view whose first column `?e` is the *object* of one of its triples, as in
  // a view from an OSM tag value to the geometries that have it. Fixing it
  // keeps the remaining triples connected, so the view can also replace part of
  // a larger query.
  qlv().writeMaterializedView("objectView", R"(
    SELECT ?e ?s ?m ?o2 {
      ?s <p3> ?m .
      ?m <p2> ?o2 .
      ?m <p4> ?e .
    }
  )");
  qlv().loadMaterializedView("objectView");
  auto fixedObjectView = viewScan("objectView", "<http://example.com/>", "?s",
                                  "?m", 3, {{3, V{"?o2"}}});
  const std::string fixedObjectQuery = R"(
    SELECT * {
      ?s <p3> ?m .
      ?m <p2> ?o2 .
      ?m <p4> <http://example.com/> .
    }
  )";
  qpExpect(qlv(), fixedObjectQuery, fixedObjectView);
  expectSameResultWithoutRewriting(fixedObjectQuery, "?s ?m ?o2");

  // The fixed first column plus an unrelated triple.
  // No `Sort` is needed for the join: with the first column fixed, the scan on
  // the view is sorted by its second column, which is `?s` here.
  qpExpect(
      qlv(), R"(
    SELECT * {
      ?s <p3> ?m .
      ?m <p2> ?o2 .
      ?m <p4> <http://example.com/> .
      ?s <p1> ?x .
    }
  )",
      h::Join(fixedObjectView, h::IndexScanFromStrings("?s", "<p1>", "?x")));
  qlv().unloadMaterializedView("objectView");

  // The fixed value may also be a literal.
  qlv().writeMaterializedView("literalView", R"(
    SELECT ?o1 ?s ?m ?o2 {
      ?s <p1> ?o1 .
      ?s <p3> ?m .
      ?m <p2> ?o2 .
    }
  )");
  qlv().loadMaterializedView("literalView");
  const std::string fixedLiteralQuery = R"(
    SELECT * {
      ?s <p1> "xyz" .
      ?s <p3> ?m .
      ?m <p2> ?o2 .
    }
  )";
  expectViewScanUsed(qlv(), fixedLiteralQuery, true);
  expectSameResultWithoutRewriting(fixedLiteralQuery, "?s ?m ?o2");
  qlv().unloadMaterializedView("literalView");
}

// _____________________________________________________________________________
// Views for which restricting the view's own query to one value of the first
// column is either not equivalent to a scan on the view with that column fixed,
// or is not supported.
TEST_F(MaterializedViewsCacheKeyRewriteTest,
       CacheKeyRewriteFixedFirstColumnUnsupported) {
  auto expectFixedFirstColumnUnsupported =
      [this](const std::string& viewQuery, const std::string& fixedQuery,
             source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
        auto l = generateLocationTrace(sourceLocation);
        qlv().writeMaterializedView("unsupportedView", viewQuery);
        qlv().loadMaterializedView("unsupportedView");
        // The view itself is still matched for the unmodified query; only
        // fixing its first column is unsupported.
        expectViewScanUsed(qlv(), viewQuery, true);
        expectViewScanUsed(qlv(), fixedQuery, false);
        qlv().unloadMaterializedView("unsupportedView");
      };

  // A `LIMIT` makes the view an arbitrary subset of its query's rows, so the
  // rows of that subset for one value of the first column are not the rows of
  // the restricted query.
  expectFixedFirstColumnUnsupported(
      R"(
        SELECT ?s ?m ?o1 ?o2 {
          ?s <p1> ?o1 .
          ?s <p3> ?m .
          ?m <p2> ?o2 .
        } LIMIT 1
      )",
      R"(
        SELECT * {
          <s2> <p1> ?o1 .
          <s2> <p3> ?m .
          ?m <p2> ?o2 .
        } LIMIT 1
      )");

  // The variable of the first column occurs outside of the view's triples, so
  // it cannot simply be substituted.
  expectFixedFirstColumnUnsupported(
      R"(
        SELECT ?s ?m ?o1 ?o2 {
          ?s <p1> ?o1 .
          ?s <p3> ?m .
          ?m <p2> ?o2 .
          FILTER (?s != <s1>)
        }
      )",
      R"(
        SELECT * {
          <s2> <p1> ?o1 .
          <s2> <p3> ?m .
          ?m <p2> ?o2 .
          FILTER (<s2> != <s1>)
        }
      )");

  // Dropping a `GROUP BY` on the first column is not equivalent: for a value
  // that does not occur in the view, an implicit `GROUP BY` returns one row
  // (with `COUNT` = 0), a scan on the view none.
  expectFixedFirstColumnUnsupported(
      R"(
        SELECT ?s (COUNT(?o1) AS ?c) {
          ?s <p1> ?o1 .
          ?s <p3> ?m .
        } GROUP BY ?s
      )",
      R"(
        SELECT (COUNT(?o1) AS ?c) {
          <s2> <p1> ?o1 .
          <s2> <p3> ?m .
        }
      )");
}

// _____________________________________________________________________________
// Regression test: the sampled placeholder value can coincidentally equal an
// unrelated, already-fixed literal elsewhere in the view's own query. Without
// the occurrence-count check in `QueryPatternCache::analyzeView`, substituting
// a real query value into the resulting cache-key template would also corrupt
// that unrelated literal, and could then wrongly match a real, unrelated
// query subtree that happens to fix both positions to the same value.
TEST(MaterializedViewsCacheKeyRewriteCoincidenceTest,
     coincidentalPlaceholderCollisionIsSafe) {
  const std::string onDiskBase = gtestCurrentTestName();
  const std::string ttl =
      " <a1> <hasLabel> \"abc\" . \n"
      " <a2> <hasLabel> \"zzz\" . \n"
      " <d1> <p2> \"abc\" . \n";
  materializedViewsTestHelpers::makeTestIndex(onDiskBase, ttl);
  auto cleanUp = absl::Cleanup(
      [&]() { materializedViewsTestHelpers::removeTestIndex(onDiskBase); });
  qlever::EngineConfig config;
  config.baseName_ = onDiskBase;
  qlever::Qlever qlv{config};

  // The view's first column `?lbl` is sorted ascending, so the sampled
  // placeholder is its smallest value, "abc". The unrelated second triple is
  // hard-coded to the exact same literal.
  qlv.writeMaterializedView("coincView", R"(
    SELECT ?lbl ?a ?dummy {
      ?a <hasLabel> ?lbl .
      ?dummy <p2> "abc" .
    }
  )");
  qlv.loadMaterializedView("coincView");

  // No `<... p2 "zzz">` triple exists, so the correct answer is 0 rows: the
  // second (unrelated) triple's cartesian factor is empty for this query.
  auto result = qlv.query(R"(
    SELECT ?a {
      ?a <hasLabel> "zzz" .
      ?d2 <p2> "zzz" .
    }
  )",
                          ad_utility::MediaType::tsv);
  EXPECT_EQ(result, "?a\n");
}
