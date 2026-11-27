// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock.h>

#include "./QueryPlannerTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/operation/Filter.h"
#include "engine/operation/SpatialJoin.h"

// Tests for the rewrite of FILTERs that are facts only with the definitions of
// their group (from `VALUES` and `BIND`), see `rewriteFiltersWithFacts`.

namespace {

namespace h = queryPlannerTestHelpers;
using ::testing::DoubleNear;
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using V = Variable;

constexpr std::string_view prefixes =
    "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
    "PREFIX unit: <http://qudt.org/vocab/unit/> ";

// Four points at increasing distances from the first one (about 0 m, 333 m,
// 667 m, and 2.2 km).
constexpr std::string_view turtle =
    "<a1> <p> \"POINT(7.8 48.0)\"^^"
    "<http://www.opengis.net/ont/geosparql#wktLiteral> . "
    "<a2> <p> \"POINT(7.8 48.003)\"^^"
    "<http://www.opengis.net/ont/geosparql#wktLiteral> . "
    "<a3> <p> \"POINT(7.8 48.006)\"^^"
    "<http://www.opengis.net/ont/geosparql#wktLiteral> . "
    "<a4> <p> \"POINT(7.8 48.02)\"^^"
    "<http://www.opengis.net/ont/geosparql#wktLiteral> . ";

constexpr std::string_view geoms = "?a <p> ?ga . ?b <p> ?gb . ";
constexpr std::string_view dist = "geof:distance(?ga, ?gb, unit:M)";
// The geometry of `<a1>` as a fixed value.
constexpr std::string_view point =
    "\"POINT(7.8 48.0)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";

// `SELECT * { <body> }` with the prefixes from above.
std::string query(std::string_view body) {
  return absl::StrCat(prefixes, "SELECT * { ", body, " }");
}

// Collect all operations of type `T` in `qet`.
template <typename T>
void collect(const QueryExecutionTree& qet, std::vector<const T*>& result) {
  const auto& op = *qet.getRootOperation();
  if (const auto* t = dynamic_cast<const T*>(&op)) {
    result.push_back(t);
  }
  for (const auto* child : op.getChildren()) {
    collect(*child, result);
  }
}

// The maximum distances of all `SpatialJoin`s and the descriptors of all
// `Filter`s of the plan for `q`.
struct PlanSummary {
  std::vector<double> spatialJoinMaxDists_;
  std::vector<std::string> filters_;
};
PlanSummary plan(const std::string& q) {
  auto qet =
      h::parseAndPlan(q, ad_utility::testing::getQec(std::string{turtle}));
  std::vector<const SpatialJoin*> spatialJoins;
  collect(*qet, spatialJoins);
  std::vector<const Filter*> filters;
  collect(*qet, filters);
  PlanSummary result;
  for (const auto* sj : spatialJoins) {
    EXPECT_TRUE(sj->isConstructed());
    result.spatialJoinMaxDists_.push_back(sj->getMaxDist().value_or(-1));
  }
  for (const auto* filter : filters) {
    result.filters_.push_back(filter->getDescriptor());
  }
  return result;
}

// Test that a distance FILTER with the maximum distance or the distance itself
// from a `VALUES` or `BIND` clause becomes a `SpatialJoin`, which replaces the
// FILTER unless the maximum distance differs between rows.
TEST(QueryPlannerFilterFacts, DistanceWithDefinitions) {
  auto expect = [](std::string body, double maxDist, std::string filter,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    auto summary = plan(query(absl::StrCat(geoms, body)));
    // The tolerance is for the rounded factors of the conversion of units.
    EXPECT_THAT(summary.spatialJoinMaxDists_,
                ElementsAre(DoubleNear(maxDist, 1e-3)));
    if (filter.empty()) {
      EXPECT_THAT(summary.filters_, IsEmpty());
    } else {
      EXPECT_THAT(summary.filters_,
                  ElementsAre(absl::StrCat("Filter ", filter)));
    }
  };

  // The maximum distance or the distance from `VALUES` or `BIND`.
  expect(absl::StrCat("VALUES ?r { 500 } FILTER(", dist, " <= ?r)"), 500, "");
  expect(absl::StrCat("BIND(0.5 AS ?r) FILTER(geof:distance(?ga, ?gb) < ?r)"),
         std::nextafter(500.0, 0.0), "");
  expect(absl::StrCat("BIND(", dist, " AS ?d) VALUES ?r { 500 } ",
                      "FILTER(?d <= ?r && ?a != ?b)"),
         500, "?a != ?b");

  // Different maximum distances in different rows: the `SpatialJoin` uses
  // the largest one, and the FILTER stays.
  expect(absl::StrCat("VALUES ?r { 500 1000 } FILTER(", dist, " <= ?r)"), 1000,
         absl::StrCat("(", dist, " <= ?r)"));
}

// Test that the `SpatialJoin` also replaces a FILTER on the target of a
// `BIND` of the distance, and the `BIND` itself (by providing `?d`).
TEST(QueryPlannerFilterFacts, BindOfDistance) {
  auto scan = h::IndexScanFromStrings;
  h::expect(
      query(absl::StrCat(geoms, "BIND(", dist, " AS ?d) FILTER(?d <= 500)")),
      h::spatialJoinFilterSubstitute(
          500, -1, V{"?ga"}, V{"?gb"}, V{"?d"}, PayloadVariables::all(),
          SpatialJoinAlgorithm::LIBSPATIALJOIN, SpatialJoinType::WITHIN_DIST,
          std::nullopt, scan("?a", "<p>", "?ga"), scan("?b", "<p>", "?gb")));
}

// Test that a FILTER on the target of a `BIND` of the distance to a fixed
// geometry becomes a `SpatialJoin` with this geometry in a `VALUES` child. The
// `BIND` stays above the `SpatialJoin`, which only provides the distance
// between two variables as a column.
TEST(QueryPlannerFilterFacts, BindOfDistanceToFixedGeometry) {
  auto scan = h::IndexScanFromStrings;
  V fixed{absl::StrCat(QLEVER_INTERNAL_VARIABLE_QUERY_PLANNER_PREFIX, 0)};
  auto values = h::ValuesClause(absl::StrCat(
      "VALUES (", fixed.name(),
      ") { (G:", GeoPoint{48.0, 7.8}.toStringRepresentation(), ") }"));
  auto bind = absl::StrCat("geof:distance(?ga, ", point, ")");
  h::expect(
      query(absl::StrCat("?a <p> ?ga . BIND(", bind,
                         " AS ?d) FILTER(?d <= 0.5)")),
      h::Bind(h::spatialJoinFilterSubstitute(
                  500, -1, V{"?ga"}, fixed, std::nullopt,
                  PayloadVariables::all(), SpatialJoinAlgorithm::LIBSPATIALJOIN,
                  SpatialJoinType::WITHIN_DIST, std::nullopt,
                  scan("?a", "<p>", "?ga"), values),
              bind, V{"?d"}));
}

// Test that definitions which are not equivalent to their variable in the
// whole group are not used.
TEST(QueryPlannerFilterFacts, NoRewrite) {
  for (std::string body : {
           // `?gb` is not bound when the `BIND` is evaluated.
           absl::StrCat("?a <p> ?ga . BIND(", dist, " AS ?d) ?b <p> ?gb . ",
                        "FILTER(?d <= 500)"),
           // The target of the `BIND` is bound again later.
           absl::StrCat(geoms, "BIND(", dist, " AS ?d) ?a <p> ?d ",
                        "FILTER(?d <= 500)"),
           // `UNDEF` in `VALUES`.
           absl::StrCat(geoms, "VALUES ?r { 500 UNDEF } FILTER(", dist,
                        " <= ?r)"),
           // Non-deterministic `BIND`s.
           absl::StrCat(geoms, "BIND(", dist, " + RAND() AS ?d) ",
                        "FILTER(?d <= 500)"),
           absl::StrCat(geoms, "BIND(500 + YEAR(NOW()) * 0 AS ?r) FILTER(",
                        dist, " <= ?r)"),
           // Definitions in another group.
           absl::StrCat("{ ", geoms, "BIND(", dist, " AS ?d) } ",
                        "FILTER(?d <= 500)"),
           absl::StrCat(geoms, "OPTIONAL { VALUES ?r { 500 } } FILTER(", dist,
                        " <= ?r)"),
           absl::StrCat(geoms, "{ SELECT ?r { VALUES ?r { 500 } } } FILTER(",
                        dist, " <= ?r)"),
           // A string instead of a number.
           absl::StrCat(geoms, "VALUES ?r { \"500\" } FILTER(", dist,
                        " <= ?r)"),
       }) {
    EXPECT_THAT(plan(query(body)).spatialJoinMaxDists_, IsEmpty()) << body;
  }

  // A trailing `VALUES` clause is joined with the result of the complete
  // `WHERE` clause, so its values are not visible in the FILTER.
  EXPECT_THAT(plan(absl::StrCat(prefixes, "SELECT * { ", geoms, "FILTER(", dist,
                                " <= ?r) } VALUES ?r { 500 }"))
                  .spatialJoinMaxDists_,
              IsEmpty());
}

// Run `q` on a small index with the given `kg` and return the rows of the
// result in TSV format, sorted.
std::vector<std::string> run(const std::string& q, std::string_view kg) {
  auto* qec = ad_utility::testing::getQec(std::string{kg});
  qec->clearCacheUnpinnedOnly();
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  auto parsedQuery = ad_utility::testing::parseQuery(q);
  QueryPlanner qp{qec, cancellationHandle};
  auto qet = qp.createExecutionTree(parsedQuery);
  ad_utility::Timer timer{ad_utility::Timer::Started};
  std::string tsv;
  for (const auto& block : ExportQueryExecutionTrees::computeResult(
           parsedQuery, *qet, ad_utility::MediaType::tsv, timer,
           cancellationHandle)) {
    tsv += block;
  }
  std::vector<std::string> rows = absl::StrSplit(tsv, '\n');
  ql::ranges::sort(rows);
  return rows;
}

// Test that a rewritten query has the same result as the query with the
// FILTER `(f) || false` instead of `f`, which is never rewritten.
TEST(QueryPlannerFilterFacts, ResultsAreEqual) {
  // The body of the query with `$` for the expression of the FILTER, this
  // expression, and the number of rows.
  auto filter = absl::StrCat(dist, " <= ?r");
  std::vector<std::tuple<std::string, std::string, size_t>> queries{
      // Pairs within 500 m: 4 with itself, a1-a2, a2-a3 (in both directions).
      {absl::StrCat(geoms, "VALUES ?r { 500 } FILTER($)"), filter, 8},
      {absl::StrCat(geoms, "BIND(0.5 AS ?r) FILTER($)"),
       "geof:distance(?ga, ?gb) < ?r", 8},
      // Without `?d`, which the `SpatialJoin` computes with `libspatialjoin`
      // (the `BIND` is pushed down), with a slightly different value than
      // `geof:distance`.
      {absl::StrCat("{ SELECT ?a ?b { ", geoms, "BIND(", dist,
                    " AS ?d) FILTER($) } }"),
       "?d <= 500", 8},
      // The points within 500 m of the fixed point `<a1>`: `<a1>` and `<a2>`.
      {absl::StrCat("?a <p> ?ga . BIND(geof:distance(?ga, ", point,
                    ") AS ?d) FILTER($)"),
       "?d <= 0.5", 2},
      // The conjunct `?a != ?b` stays, and removes the four pairs of a point
      // with itself.
      {absl::StrCat(geoms, "VALUES ?r { 500 } FILTER($)"),
       absl::StrCat(filter, " && ?a != ?b"), 4},
      // Two radii: the `SpatialJoin` uses 1000 m, the FILTER does the rest.
      {absl::StrCat(geoms, "VALUES ?r { 500 1000 } FILTER($)"), filter, 8 + 10},
  };
  for (const auto& [body, expression, numRows] : queries) {
    auto rewritten =
        run(query(absl::StrReplaceAll(body, {{"$", expression}})), turtle);
    auto opaque =
        run(query(absl::StrReplaceAll(
                body, {{"$", absl::StrCat("(", expression, ") || false")}})),
            turtle);
    EXPECT_EQ(rewritten, opaque) << body;
    // The header and the empty line after the last row.
    EXPECT_EQ(rewritten.size(), numRows + 2) << body;
  }
}

// Test that a `BIND` whose target is bound again by a later pattern is not
// used (where the expression is undefined, the later pattern binds the target
// freely).
TEST(QueryPlannerFilterFacts, BindTargetBoundLater) {
  // A point, a literal that is not a geometry, and the number 3.
  constexpr std::string_view kg =
      "<a1> <p> \"POINT(7.8 48.0)\"^^"
      "<http://www.opengis.net/ont/geosparql#wktLiteral> . "
      "<a2> <p> \"no geometry\" . <c> <q> 3 .";

  // The three pairs with `<a2>` have no distance, so `?d` is bound to 3 by the
  // triple after the `BIND`, which passes the FILTER.
  auto before = absl::StrCat(geoms, "BIND(", dist, " AS ?d) ?c <q> ?d");
  auto rewritten = run(query(absl::StrCat(before, " FILTER(?d <= 500)")), kg);
  EXPECT_EQ(
      rewritten,
      run(query(absl::StrCat(before, " FILTER((?d <= 500) || false)")), kg));
  EXPECT_EQ(rewritten.size(), 3 + 2);
}

}  // namespace
