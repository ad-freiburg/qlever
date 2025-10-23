// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "./printers/PayloadVariablePrinters.h"
#include "QueryPlannerTestHelpers.h"
#include "engine/SpatialJoin.h"
#include "engine/SpatialJoinConfig.h"
#include "parser/MagicServiceQuery.h"
#include "parser/PayloadVariables.h"
#include "parser/SpatialQuery.h"
#include "util/TripleComponentTestHelpers.h"

namespace h = queryPlannerTestHelpers;
namespace {
using Var = Variable;
constexpr auto iri = ad_utility::testing::iri;
using queryPlannerTestHelpers::NamedTag;
}  // namespace
using ::testing::HasSubstr;

// _____________________________________________________________________________
TEST(QueryPlanner, SpatialJoinService) {
  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  auto S2 = SpatialJoinAlgorithm::S2_GEOMETRY;
  auto Basel = SpatialJoinAlgorithm::BASELINE;
  auto BBox = SpatialJoinAlgorithm::BOUNDING_BOX;
  auto SJ = SpatialJoinAlgorithm::LIBSPATIALJOIN;
  PayloadVariables emptyPayload{};

  // Simple base cases
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 1 . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, S2,
                     std::nullopt, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 1 . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, S2,
                     std::nullopt, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:baseline ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 1 . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, Basel,
                     std::nullopt, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:boundingBox ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 100 . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(100, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload,
                     BBox, std::nullopt, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:libspatialjoin ;"
      "spatialSearch:joinType spatialSearch:within-dist ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 100 . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(100, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, SJ,
                     SpatialJoinType::WITHIN_DIST, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:libspatialjoin ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ."
      "{ ?a <p> ?b } }}",
      h::spatialJoin(-1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, SJ,
                     SpatialJoinType::INTERSECTS, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:libspatialjoin ;"
      "spatialSearch:joinType spatialSearch:intersects ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b  . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(-1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, SJ,
                     SpatialJoinType::INTERSECTS, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:libspatialjoin ;"
      "spatialSearch:joinType spatialSearch:covers ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(-1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, SJ,
                     SpatialJoinType::COVERS, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:libspatialjoin ;"
      "spatialSearch:joinType spatialSearch:contains ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(-1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, SJ,
                     SpatialJoinType::CONTAINS, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:libspatialjoin ;"
      "spatialSearch:joinType spatialSearch:touches ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(-1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, SJ,
                     SpatialJoinType::TOUCHES, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:libspatialjoin ;"
      "spatialSearch:joinType spatialSearch:crosses ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(-1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, SJ,
                     SpatialJoinType::CROSSES, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:libspatialjoin ;"
      "spatialSearch:joinType spatialSearch:overlaps ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(-1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, SJ,
                     SpatialJoinType::OVERLAPS, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:libspatialjoin ;"
      "spatialSearch:joinType spatialSearch:within ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(-1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, SJ,
                     SpatialJoinType::WITHIN, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:libspatialjoin ;"
      "spatialSearch:joinType spatialSearch:equals ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b  . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(-1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, SJ,
                     SpatialJoinType::EQUALS, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:libspatialjoin ;"
      "spatialSearch:joinType spatialSearch:within ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(-1, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, SJ,
                     SpatialJoinType::WITHIN, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 100 ;"
      "spatialSearch:numNearestNeighbors 2 ;"
      "spatialSearch:bindDistance ?dist ."
      "{ ?a <p> ?b } }}",
      h::spatialJoin(100, 2, V{"?y"}, V{"?b"}, V{"?dist"}, emptyPayload, S2,
                     std::nullopt, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "{ ?a <p> ?b } }}",
      h::spatialJoin(-1, 5, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, S2,
                     std::nullopt, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));

  // Floating point as maximum distance
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 0.5 . "
      "{ ?a <p> ?b } }}",
      h::spatialJoin(0.5, -1, V{"?y"}, V{"?b"}, std::nullopt, emptyPayload, S2,
                     std::nullopt, scan("?x", "<p>", "?y"),
                     scan("?a", "<p>", "?b")));
}

// _____________________________________________________________________________
TEST(QueryPlanner, SpatialJoinServicePayloadVars) {
  // Test the <payload> option which allows selecting columns from the graph
  // pattern inside the service.

  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  auto S2 = SpatialJoinAlgorithm::S2_GEOMETRY;
  using PV = PayloadVariables;

  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:bindDistance ?dist ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "_:config spatialSearch:payload ?a ."
      "{ ?a <p> ?b } }}",
      h::spatialJoin(-1, 5, V{"?y"}, V{"?b"}, V{"?dist"},
                     PV{std::vector<V>{V{"?a"}}}, S2, std::nullopt,
                     scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:bindDistance ?dist ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "_:config spatialSearch:payload ?a , ?a2 ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::spatialJoin(
          -1, 5, V{"?y"}, V{"?b"}, V{"?dist"},
          PV{std::vector<V>{V{"?a"}, V{"?a2"}}}, S2, std::nullopt,
          scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));

  // Right variable and duplicates are possible (silently deduplicated during
  // query result computation)
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:bindDistance ?dist ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "_:config spatialSearch:payload ?a, ?a, ?b, ?a2 ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::spatialJoin(
          -1, 5, V{"?y"}, V{"?b"}, V{"?dist"},
          PV{std::vector<V>{V{"?a"}, V{"?a"}, V{"?b"}, V{"?a2"}}}, S2,
          std::nullopt, scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));

  // Selecting all payload variables using "all"
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:bindDistance ?dist ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "_:config spatialSearch:payload <all> ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::spatialJoin(
          -1, 5, V{"?y"}, V{"?b"}, V{"?dist"}, PayloadVariables::all(), S2,
          std::nullopt, scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:bindDistance ?dist ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "_:config spatialSearch:payload spatialSearch:all ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::spatialJoin(
          -1, 5, V{"?y"}, V{"?b"}, V{"?dist"}, PayloadVariables::all(), S2,
          std::nullopt, scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));

  // All and explicitly named ones just select all
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:bindDistance ?dist ;"
      "spatialSearch:numNearestNeighbors 5 . "
      "_:config spatialSearch:left ?y ."
      "_:config spatialSearch:payload <all> ."
      "_:config spatialSearch:payload ?a ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::spatialJoin(
          -1, 5, V{"?y"}, V{"?b"}, V{"?dist"}, PayloadVariables::all(), S2,
          std::nullopt, scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));
}

// _____________________________________________________________________________
TEST(QueryPlanner, SpatialJoinServiceMaxDistOutside) {
  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  auto S2 = SpatialJoinAlgorithm::S2_GEOMETRY;

  // If only maxDistance is used but not numNearestNeighbors, the right variable
  // must not come from inside the SERVICE
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 1 . "
      " } }",
      h::spatialJoin(1, -1, V{"?y"}, V{"?b"}, std::nullopt,
                     // Payload variables have the default all instead of empty
                     // in this case
                     PayloadVariables::all(), S2, std::nullopt,
                     scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));

  // If the user explicitly states that they want all payload variables (which
  // is enforced and the default anyway), this should also work
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "SERVICE spatialSearch: {"
      "_:config spatialSearch:algorithm spatialSearch:s2 ;"
      "spatialSearch:left ?y ;"
      "spatialSearch:right ?b ;"
      "spatialSearch:maxDistance 1 ; "
      "spatialSearch:payload spatialSearch:all ."
      " } }",
      h::spatialJoin(1, -1, V{"?y"}, V{"?b"}, std::nullopt,
                     PayloadVariables::all(), S2, std::nullopt,
                     scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));

  // Nearest neighbors search requires the right child to be defined inside the
  // service
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?a <p> ?b ."
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:algorithm spatialSearch:s2 ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:right ?b ;"
                "spatialSearch:maxDistance 1 ; "
                "spatialSearch:numNearestNeighbors 5 ."
                "}}",
                ::testing::_),
      ::testing::ContainsRegex(
          "must have its right "
          "variable declared inside the service using a graph pattern"));

  // The user may not select specific payload variables if the right join table
  // is declared outside because this would mess up the query semantics and may
  // not have deterministic results on different inputs because of query planner
  // decisions
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?a <p> ?b ."
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:algorithm spatialSearch:s2 ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:right ?b ;"
                "spatialSearch:maxDistance 1 ; "
                "spatialSearch:payload ?a ."
                "}}",
                ::testing::_),
      ::testing::ContainsRegex(
          "right variable for the spatial search is declared outside the "
          "SERVICE, but the <payload> parameter was set"));
}

// _____________________________________________________________________________
TEST(QueryPlanner, SpatialJoinMultipleServiceSharedLeft) {
  // Test two spatial join SERVICEs that share a common ?left variable

  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  auto S2 = SpatialJoinAlgorithm::S2_GEOMETRY;
  using PV = PayloadVariables;

  h::expect(
      "SELECT * WHERE {"
      "?x <p> ?y ."
      "?y <max-distance-in-meters:100> ?b ."
      "?ab <p1> ?b ."
      "?y <max-distance-in-meters:500> ?c ."
      "?ac <p2> ?c ."
      "}",
      // Use two matchers using AnyOf here because the query planner may add the
      // children one way or the other depending on cost estimates. Both
      // versions are semantically correct.
      ::testing::AnyOf(
          h::spatialJoin(
              100, -1, V{"?y"}, V{"?b"}, std::nullopt, PV::all(), S2,
              std::nullopt,
              h::spatialJoin(500, -1, V{"?y"}, V{"?c"}, std::nullopt, PV::all(),
                             S2, std::nullopt, scan("?x", "<p>", "?y"),
                             scan("?ac", "<p2>", "?c")),
              scan("?ab", "<p1>", "?b")),
          h::spatialJoin(
              500, -1, V{"?y"}, V{"?c"}, std::nullopt, PV::all(), S2,
              std::nullopt,
              h::spatialJoin(100, -1, V{"?y"}, V{"?b"}, std::nullopt, PV::all(),
                             S2, std::nullopt, scan("?x", "<p>", "?y"),
                             scan("?ab", "<p1>", "?b")),
              scan("?ac", "<p2>", "?c"))));
  h::expect(
      "PREFIX spatialSearch: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y ."
      "SERVICE spatialSearch: {"
      "  _:config spatialSearch:algorithm spatialSearch:s2 ;"
      "    spatialSearch:left ?y ;"
      "    spatialSearch:right ?b ;"
      "    spatialSearch:numNearestNeighbors 5 ; "
      "    spatialSearch:bindDistance ?db ."
      "  { ?ab <p1> ?b } "
      "}"
      "SERVICE spatialSearch: {"
      "  _:config spatialSearch:algorithm spatialSearch:s2 ;"
      "    spatialSearch:left ?y ;"
      "    spatialSearch:right ?c ;"
      "    spatialSearch:numNearestNeighbors 5 ; "
      "    spatialSearch:maxDistance 500 ; "
      "    spatialSearch:payload ?ac ; "
      "    spatialSearch:bindDistance ?dc ."
      "  { ?ac <p2> ?c }"
      " }"
      "}",
      // Use two matchers using AnyOf here because the query planner may add the
      // children one way or the other depending on cost estimates. Both
      // versions are semantically correct.
      ::testing::AnyOf(
          h::spatialJoin(
              500, 5, V{"?y"}, V{"?c"}, V{"?dc"}, PV{std::vector<V>{V{"?ac"}}},
              S2, std::nullopt,
              h::spatialJoin(-1, 5, V{"?y"}, V{"?b"}, V{"?db"}, PV{}, S2,
                             std::nullopt, scan("?x", "<p>", "?y"),
                             scan("?ab", "<p1>", "?b")),
              scan("?ac", "<p2>", "?c")),
          h::spatialJoin(-1, 5, V{"?y"}, V{"?b"}, V{"?db"}, PV{}, S2,
                         std::nullopt,
                         h::spatialJoin(500, 5, V{"?y"}, V{"?c"}, V{"?dc"},
                                        PV{std::vector<V>{V{"?ac"}}}, S2,
                                        std::nullopt, scan("?x", "<p>", "?y"),
                                        scan("?ac", "<p2>", "?c")),
                         scan("?ab", "<p1>", "?b"))));
}

// _____________________________________________________________________________
TEST(QueryPlanner, SpatialJoinMissingConfig) {
  // Tests with incomplete config
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:maxDistance 5 . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Missing parameter `<left>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:numNearestNeighbors 5 . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Missing parameter `<left>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Missing parameter `<right>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:left ?y ;"
                "spatialSearch:numNearestNeighbors 5 . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Missing parameter `<right>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:left ?y ;"
                " spatialSearch:right ?b ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Neither `<numNearestNeighbors>` nor "
                               "`<maxDistance>` were provided"));
}

// _____________________________________________________________________________
TEST(QueryPlanner, SpatialJoinInvalidOperationsInService) {
  // Test that unallowed operations inside the SERVICE statement throw
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:algorithm spatialSearch:s2 ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:right ?b ;"
                "spatialSearch:maxDistance 1 . "
                "{ ?a <p> ?b }"
                "SERVICE <http://example.com/> { ?a <something> <else> }"
                " }}",
                ::testing::_),
      ::testing::ContainsRegex("Unsupported element in a magic service query "
                               "of type `spatial join`"));
}

// _____________________________________________________________________________
TEST(QueryPlanner, SpatialJoinServiceMultipleGraphPatterns) {
  // Test that the SERVICE statement may only contain at most one graph pattern
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:algorithm spatialSearch:s2 ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:right ?b ;"
                "spatialSearch:maxDistance 1 . "
                "{ ?a <p> ?b }"
                "{ ?a <p2> ?c } }}",
                ::testing::_),
      ::testing::ContainsRegex("A magic SERVICE query must not contain more "
                               "than one nested group graph pattern"));
}

// _____________________________________________________________________________
TEST(QueryPlanner, SpatialJoinIncorrectConfigValues) {
  // Tests with mistakes in the config
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance \"5\" . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<maxDistance>` expects an integer"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:numNearestNeighbors \"1\" ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<numNearestNeighbors>` expects an integer"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:algorithm \"1\" ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("parameter `<algorithm>` needs an IRI"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:algorithm <http://example.com/some-nonsense> ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<algorithm>` does not refer to a supported "
                               "spatial search algorithm"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "<http://example.com/some-nonsense> 123 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Unsupported argument"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:bindDistance 123 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<bindDistance>` has to be a variable"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:payload 123 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex(
          "`<payload>` parameter must be either a variable "
          "to be selected or `<all>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:payload <http://some.iri.that.is.not.all> ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex(
          "`<payload>` parameter must be either a variable "
          "to be selected or `<all>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:bindDistance ?dist_a ;"
                "spatialSearch:bindDistance ?dist_b ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<bindDistance>` has already been set"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right 123 ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<right>` has to be a variable"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left \"abc\" ;"
                "spatialSearch:maxDistance 5 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<left>` has to be a variable"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:algorithm spatialSearch:libspatialjoin ;"
                "spatialSearch:joinType 5 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("parameter `<joinType>` needs an IRI"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:algorithm spatialSearch:libspatialjoin ;"
                "spatialSearch:joinType <http://example.com/some-nonsense> ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("parameter `<joinType>` does not refer to"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:algorithm spatialSearch:libspatialjoin ;"
                "spatialSearch:joinType <intersects> ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::HasSubstr(
          "The algorithm `<libspatialjoin>` supports the "
          "`<maxDistance>` option only if `<joinType>` is set to "
          "`<within-dist>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:numNearestNeighbors 5 ;"
                "spatialSearch:algorithm spatialSearch:libspatialjoin ;"
                "spatialSearch:joinType <within-dist> ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::HasSubstr(
          "The algorithm `<libspatialjoin>` does not support the option "
          "`<numNearestNeighbors>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:numNearestNeighbors 5 ;"
                "spatialSearch:algorithm spatialSearch:s2 ;"
                "spatialSearch:joinType <within-dist> ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::HasSubstr(
          "The selected algorithm does not support the `<joinType>` option"));

  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX spatialSearch: "
                "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE spatialSearch: {"
                "_:config spatialSearch:right ?b ;"
                "spatialSearch:left ?y ;"
                "spatialSearch:maxDistance 5 ;"
                "spatialSearch:algorithm spatialSearch:s2 ;"
                "spatialSearch:experimentalRightCacheName \"dummy\" . "
                "}}",
                ::testing::_),
      ::testing::HasSubstr(
          "`<experimentalRightCacheName>` is only supported by the "
          "`<experimentalPointPolyline>` algorithm"));

  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect(
          "PREFIX spatialSearch: "
          "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
          "SELECT * WHERE {"
          "?x <p> ?y ."
          "SERVICE spatialSearch: {"
          "_:config spatialSearch:right ?b ;"
          "spatialSearch:left ?y ;"
          "spatialSearch:maxDistance 5 ;"
          "spatialSearch:algorithm spatialSearch:experimentalPointPolyline ;"
          "spatialSearch:experimentalRightCacheName <http://example.com> . "
          "}}",
          ::testing::_),
      ::testing::HasSubstr(
          "must be the name of a pinned cache entry as a string literal"));

  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect(
          "PREFIX spatialSearch: "
          "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
          "SELECT * WHERE {"
          "?x <p> ?y ."
          "SERVICE spatialSearch: {"
          "_:config spatialSearch:right ?b ;"
          "spatialSearch:left ?y ;"
          "spatialSearch:maxDistance 5 ;"
          "spatialSearch:algorithm spatialSearch:experimentalPointPolyline ."
          "}}",
          ::testing::_),
      ::testing::HasSubstr(
          "parameter `<experimentalRightCacheName>` is mandatory"));

  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect(
          "PREFIX spatialSearch: "
          "<https://qlever.cs.uni-freiburg.de/spatialSearch/>"
          "SELECT * WHERE {"
          "?x <p> ?y ."
          "SERVICE spatialSearch: {"
          "_:config spatialSearch:right ?b ;"
          "spatialSearch:left ?y ;"
          "spatialSearch:maxDistance 5 ;"
          "spatialSearch:algorithm spatialSearch:experimentalPointPolyline ;"
          "spatialSearch:experimentalRightCacheName \"dummy\" . "
          " { ?a <p> ?b . }"
          "}}",
          ::testing::_),
      ::testing::HasSubstr(
          "a group graph pattern for the right side may not be specified"));
}

// _____________________________________________________________________________
TEST(QueryPlanner, SpatialJoinS2PointPolylineAndCachedIndex) {
  using V = Variable;
  using PV = PayloadVariables;
  auto scan = h::IndexScanFromStrings;
  using enum SpatialJoinAlgorithm;

  std::string kb =
      "<s> <p> \"LINESTRING(1.5 2.5, 1.55 2.5)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> . "
      "<s> <p> \"LINESTRING(15.5 2.5, 16.0 3.0)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> . "
      "<s2> <p> \"LINESTRING(11.5 21.5, 11.5 22.0)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral> . "
      "<s3> <p2> <o2>.";
  size_t numLineStrings = 3;
  std::string pinned = "SELECT * { ?s <p> ?o }";

  std::string testQuery =
      "PREFIX qlss: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y ."
      "SERVICE qlss: {"
      "_:config qlss:right ?o ;"
      "qlss:left ?y ;"
      "qlss:maxDistance 500 ;"
      "qlss:algorithm qlss:experimentalPointPolyline ;"
      "qlss:experimentalRightCacheName \"dummy\" ."
      "} }";

  // Requested query for right child not pinned
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect(testQuery, ::testing::_),
      ::testing::HasSubstr(
          "\"dummy\" is not contained in the named result cache"));

  // Requested query for right child pinned but without the cached geometry
  // index
  {
    auto qec = ad_utility::testing::getQec(kb);
    qec->pinResultWithName() = {"dummy", std::nullopt};
    auto plan = h::parseAndPlan(pinned, qec);
    [[maybe_unused]] auto pinResult = plan.getResult();

    AD_EXPECT_THROW_WITH_MESSAGE(
        h::expect(testQuery, ::testing::_, qec),
        ::testing::HasSubstr("no cached geometry index was found"));
  }

  // Requested query for right child correctly pinned
  {
    auto qec = ad_utility::testing::getQec(kb);
    qec->pinResultWithName() = {"dummy", V{"?o"}};
    auto plan = h::parseAndPlan(pinned, qec);
    [[maybe_unused]] auto pinResult = plan.getResult();

    h::expect(
        "PREFIX qlss: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
        "SELECT * WHERE {"
        "?x <p> ?y ."
        "SERVICE qlss: {"
        "_:config qlss:right ?o ;"
        "qlss:left ?y ;"
        "qlss:maxDistance 500 ;"
        "qlss:algorithm qlss:experimentalPointPolyline ;"
        "qlss:experimentalRightCacheName \"dummy\" ."
        "} }",
        h::spatialJoin(500, -1, V{"?y"}, V{"?o"}, std::nullopt, PV::all(),
                       S2_POINT_POLYLINE, std::nullopt, scan("?x", "<p>", "?y"),
                       h::ExplicitIdTableOperation(numLineStrings)),
        qec);

    // Payload variables from the cached right side are allowed
    h::expect(
        "PREFIX qlss: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
        "SELECT * WHERE {"
        "?x <p> ?y ."
        "SERVICE qlss: {"
        "_:config qlss:right ?o ;"
        "qlss:left ?y ;"
        "qlss:maxDistance 500 ;"
        "qlss:algorithm qlss:experimentalPointPolyline ;"
        "qlss:experimentalRightCacheName \"dummy\" ;"
        "qlss:payload ?s ."
        "} }",
        h::spatialJoin(500, -1, V{"?y"}, V{"?o"}, std::nullopt, PV::all(),
                       S2_POINT_POLYLINE, std::nullopt, scan("?x", "<p>", "?y"),
                       h::ExplicitIdTableOperation(numLineStrings)),
        qec);
  }

  // Query is pinned correctly with geometry index, but the user does not
  // request the correct column to be used
  {
    auto qec = ad_utility::testing::getQec(kb);
    qec->pinResultWithName() = {"dummy", V{"?o"}};
    auto plan = h::parseAndPlan(pinned, qec);
    [[maybe_unused]] auto pinResult = plan.getResult();

    AD_EXPECT_THROW_WITH_MESSAGE(
        h::expect(
            "PREFIX qlss: <https://qlever.cs.uni-freiburg.de/spatialSearch/>"
            "SELECT * WHERE {"
            "?x <p> ?y ."
            "SERVICE qlss: {"
            "_:config qlss:right ?wrongVariableHere ;"
            "qlss:left ?y ;"
            "qlss:maxDistance 500 ;"
            "qlss:algorithm qlss:experimentalPointPolyline ;"
            "qlss:experimentalRightCacheName \"dummy\" ."
            "} }",
            ::testing::_, qec),
        ::testing::HasSubstr(
            "built on the column \"?o\" but this query requests "
            "\"?wrongVariableHere\" as the right join variable"));
  }
}

// _____________________________________________________________________________
TEST(QueryPlanner, SpatialJoinFromGeofDistanceFilter) {
  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  auto algo = SpatialJoinAlgorithm::LIBSPATIALJOIN;
  auto type = SpatialJoinType::WITHIN_DIST;

  // Basic test with 2-argument geof:distance
  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(?y, ?b) <= 0.5)"
      " }",
      h::spatialJoinFilterSubstitute(
          500, -1, V{"?y"}, V{"?b"}, std::nullopt, PayloadVariables::all(),
          algo, type, scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));

  // Metric distance function
  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:metricDistance(?y, ?b) <= 500)"
      " }",
      h::spatialJoinFilterSubstitute(
          500, -1, V{"?y"}, V{"?b"}, std::nullopt, PayloadVariables::all(),
          algo, type, scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));

  // Distance function with unit
  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(?y, ?b, <http://qudt.org/vocab/unit/M>) <= 500)"
      " }",
      h::spatialJoinFilterSubstitute(
          500, -1, V{"?y"}, V{"?b"}, std::nullopt, PayloadVariables::all(),
          algo, type, scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(?y, ?b, <http://qudt.org/vocab/unit/MI>) <= 1)"
      " }",
      h::spatialJoinFilterSubstitute(
          1609.344, -1, V{"?y"}, V{"?b"}, std::nullopt, PayloadVariables::all(),
          algo, type, scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(?y, ?b, <http://qudt.org/vocab/unit/KiloM>) <= 0.5)"
      " }",
      h::spatialJoinFilterSubstitute(
          500, -1, V{"?y"}, V{"?b"}, std::nullopt, PayloadVariables::all(),
          algo, type, scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(?y, ?b, "
      "\"http://qudt.org/vocab/unit/M\"^^<http://www.w3.org/2001/"
      "XMLSchema#anyURI>) <= 500)"
      " }",
      h::spatialJoinFilterSubstitute(
          500, -1, V{"?y"}, V{"?b"}, std::nullopt, PayloadVariables::all(),
          algo, type, scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(?y, ?b, "
      "\"http://qudt.org/vocab/unit/MI\"^^<http://www.w3.org/2001/"
      "XMLSchema#anyURI>) <= 1)"
      " }",
      h::spatialJoinFilterSubstitute(
          1609.344, -1, V{"?y"}, V{"?b"}, std::nullopt, PayloadVariables::all(),
          algo, type, scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(?y, ?b, "
      "\"http://qudt.org/vocab/unit/KiloM\"^^<http://www.w3.org/2001/"
      "XMLSchema#anyURI>) <= 0.5)"
      " }",
      h::spatialJoinFilterSubstitute(
          500, -1, V{"?y"}, V{"?b"}, std::nullopt, PayloadVariables::all(),
          algo, type, scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));

  // Two distance filters
  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(?y, ?b) <= 0.5)"
      "?m <p> ?n ."
      "FILTER(geof:distance(?y, ?n) <= 1)"
      " }",
      ::testing::AnyOf(
          h::spatialJoinFilterSubstitute(
              1000, -1, V{"?y"}, V{"?n"}, std::nullopt, PayloadVariables::all(),
              algo, type,
              h::spatialJoinFilterSubstitute(
                  500, -1, V{"?y"}, V{"?b"}, std::nullopt,
                  PayloadVariables::all(), algo, type, scan("?x", "<p>", "?y"),
                  scan("?a", "<p>", "?b")),
              scan("?m", "<p>", "?n")),
          h::spatialJoinFilterSubstitute(
              500, -1, V{"?y"}, V{"?b"}, std::nullopt, PayloadVariables::all(),
              algo, type,
              h::spatialJoinFilterSubstitute(
                  1000, -1, V{"?y"}, V{"?n"}, std::nullopt,
                  PayloadVariables::all(), algo, type, scan("?x", "<p>", "?y"),
                  scan("?m", "<p>", "?n")),
              scan("?a", "<p>", "?b"))));

  // Regression test: two distance filters and unrelated bind operation
  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "BIND(1 AS ?unrelated)"
      "FILTER(geof:distance(?y, ?b) <= 0.5)"
      "?m <p> ?n ."
      "FILTER(geof:distance(?y, ?n) <= 1)"
      " }",
      ::testing::AnyOf(
          h::Bind(h::spatialJoinFilterSubstitute(
                      1000, -1, V{"?y"}, V{"?n"}, std::nullopt,
                      PayloadVariables::all(), algo, type,
                      h::spatialJoinFilterSubstitute(
                          500, -1, V{"?y"}, V{"?b"}, std::nullopt,
                          PayloadVariables::all(), algo, type,
                          scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")),
                      scan("?m", "<p>", "?n")),
                  "1", Variable{"?unrelated"}),
          h::spatialJoinFilterSubstitute(
              1000, -1, V{"?y"}, V{"?n"}, std::nullopt, PayloadVariables::all(),
              algo, type,
              h::Bind(h::spatialJoinFilterSubstitute(
                          500, -1, V{"?y"}, V{"?b"}, std::nullopt,
                          PayloadVariables::all(), algo, type,
                          scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")),
                      "1", Variable{"?unrelated"}),
              scan("?m", "<p>", "?n")),
          h::spatialJoinFilterSubstitute(
              500, -1, V{"?y"}, V{"?b"},
              std::nullopt, PayloadVariables::all(), algo, type,
              h::Bind(h::spatialJoinFilterSubstitute(
                          1000, -1, V{"?y"}, V{"?n"}, std::nullopt,
                          PayloadVariables::all(), algo, type,
                          scan("?x", "<p>", "?y"), scan("?m", "<p>", "?n")),
                      "1", Variable{"?unrelated"}),
              scan("?a", "<p>", "?b")),
          h::Bind(h::spatialJoinFilterSubstitute(
                      500, -1, V{"?y"}, V{"?b"},
                      std::nullopt, PayloadVariables::all(), algo, type,
                      h::spatialJoinFilterSubstitute(
                          1000, -1, V{"?y"}, V{"?n"}, std::nullopt,
                          PayloadVariables::all(), algo, type,
                          scan("?x", "<p>", "?y"), scan("?m", "<p>", "?n")),
                      scan("?a", "<p>", "?b")),
                  "1", Variable{"?unrelated"})));
}

// _____________________________________________________________________________
TEST(QueryPlanner, FilterIsNotRewritten) {
  auto scan = h::IndexScanFromStrings;

  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(?y, ?b) > 0.5)"
      " }",
      h::Filter("geof:distance(?y, ?b) > 0.5",
                h::CartesianProductJoin(scan("?x", "<p>", "?y"),
                                        scan("?a", "<p>", "?b"))));

  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(\"POINT(50. "
      "50.0)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>, ?b) <= 0.5)"
      " }",
      ::testing::AnyOf(
          h::Filter(
              "geof:distance(\"POINT(50. "
              "50.0)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>, "
              "?b) <= 0.5",
              h::CartesianProductJoin(scan("?x", "<p>", "?y"),
                                      scan("?a", "<p>", "?b"))),
          h::CartesianProductJoin(
              scan("?x", "<p>", "?y"),
              h::Filter(
                  "geof:distance(\"POINT(50. "
                  "50.0)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>, "
                  "?b) <= 0.5",
                  scan("?a", "<p>", "?b")))));

  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(?b, \"POINT(50. "
      "50.0)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>) <= 0.5)"
      " }",
      ::testing::AnyOf(
          h::Filter("geof:distance(?b, \"POINT(50. "
                    "50.0)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"
                    ") <= 0.5",
                    h::CartesianProductJoin(scan("?x", "<p>", "?y"),
                                            scan("?a", "<p>", "?b"))),
          h::CartesianProductJoin(
              scan("?x", "<p>", "?y"),
              h::Filter(
                  "geof:distance(?b, \"POINT(50. "
                  "50.0)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>"
                  ") <= 0.5",
                  scan("?a", "<p>", "?b")))));

  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(?b, ?y, ?a) <= 0.5)"
      " }",
      h::Filter("geof:distance(?b, ?y, ?a) <= 0.5",
                h::CartesianProductJoin(scan("?x", "<p>", "?y"),
                                        scan("?a", "<p>", "?b"))));

  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER((?b + ?y) <= 0.5)"
      " }",
      h::Filter("(?b + ?y) <= 0.5",
                h::CartesianProductJoin(scan("?x", "<p>", "?y"),
                                        scan("?a", "<p>", "?b"))));

  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(?y, ?b) <= ?a)"
      " }",
      h::Filter("geof:distance(?y, ?b) <= ?a",
                h::CartesianProductJoin(scan("?x", "<p>", "?y"),
                                        scan("?a", "<p>", "?b"))));

  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "FILTER(geof:distance(?y, ?b) <= \"abc\")"
      " }",
      h::Filter("geof:distance(?y, ?b) <= \"abc\"",
                h::CartesianProductJoin(scan("?x", "<p>", "?y"),
                                        scan("?a", "<p>", "?b"))));

  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "FILTER(geof:sfContains(?b, \"POINT(50.0 50.0)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>)) . }",
      h::Filter("geof:sfContains(?b, \"POINT(50.0 50.0)\""
                "^^<http://www.opengis.net/ont/geosparql#wktLiteral>)",
                scan("?a", "<p>", "?b")));

  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "FILTER(geof:sfContains(\"POINT(50.0 50.0)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>, ?b)) . }",
      h::Filter("geof:sfContains(\"POINT(50.0 50.0)\""
                "^^<http://www.opengis.net/ont/geosparql#wktLiteral>, ?b)",
                scan("?a", "<p>", "?b")));
}

// _____________________________________________________________________________
TEST(QueryPlanner, SpatialJoinFromGeofRelationFilter) {
  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  auto algo = SpatialJoinAlgorithm::LIBSPATIALJOIN;
  using enum SpatialJoinType;

  std::vector<std::pair<std::string, SpatialJoinType>>
      geofFunctionNameAndSJType{
          {"sfIntersects", INTERSECTS}, {"sfContains", CONTAINS},
          {"sfCovers", COVERS},         {"sfCrosses", CROSSES},
          {"sfTouches", TOUCHES},       {"sfEquals", EQUALS},
          {"sfOverlaps", OVERLAPS},     {"sfWithin", WITHIN}};

  // Run basic query planner test for each of the geo relation functions
  for (const auto& [funcName, sjType] : geofFunctionNameAndSJType) {
    std::string query = absl::StrCat(
        "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
        "SELECT * WHERE {"
        "?a <p> ?b ."
        "?x <p> ?y ."
        "FILTER(geof:",
        funcName, "(?y, ?b))  }");
    h::expect(query, h::spatialJoinFilterSubstitute(
                         -1, -1, V{"?y"}, V{"?b"}, std::nullopt,
                         PayloadVariables::all(), algo, sjType,
                         scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  }

  // Combination of two geo relation filters
  for (const auto& [a, b] : ::ranges::views::cartesian_product(
           geofFunctionNameAndSJType, geofFunctionNameAndSJType)) {
    const auto& [funcName1, sjType1] = a;
    const auto& [funcName2, sjType2] = b;
    std::string query = absl::StrCat(
        "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
        "SELECT * WHERE {"
        "?a <p> ?b ."
        "?x <p> ?y ."
        "FILTER geof:",
        funcName1,
        "(?y, ?b)  ."
        "?m <p> ?n ."
        "FILTER geof:",
        funcName2, "(?y, ?n) .  }");
    h::expect(query,
              ::testing::AnyOf(
                  h::spatialJoinFilterSubstitute(
                      -1, -1, V{"?y"}, V{"?n"}, std::nullopt,
                      PayloadVariables::all(), algo, sjType2,
                      h::spatialJoinFilterSubstitute(
                          -1, -1, V{"?y"}, V{"?b"}, std::nullopt,
                          PayloadVariables::all(), algo, sjType1,
                          scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")),
                      scan("?m", "<p>", "?n")),
                  h::spatialJoinFilterSubstitute(
                      -1, -1, V{"?y"}, V{"?b"}, std::nullopt,
                      PayloadVariables::all(), algo, sjType1,
                      h::spatialJoinFilterSubstitute(
                          -1, -1, V{"?y"}, V{"?n"}, std::nullopt,
                          PayloadVariables::all(), algo, sjType2,
                          scan("?x", "<p>", "?y"), scan("?m", "<p>", "?n")),
                      scan("?a", "<p>", "?b"))));
  }

  // Two geo relation filters on the same variables: The second one may not be
  // substituted by a spatial join as this spatial join would be incomplete
  // (that is: have only one child).
  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?m <p> ?n ."
      "FILTER geof:sfCovers(?n, ?b) ."
      "FILTER geof:sfContains(?n, ?b) .  }",
      ::testing::AnyOf(
          h::Filter("geof:sfCovers(?n, ?b)",
                    h::spatialJoinFilterSubstitute(
                        -1, -1, V{"?n"}, V{"?b"}, std::nullopt,
                        PayloadVariables::all(), algo, CONTAINS,
                        scan("?m", "<p>", "?n"), scan("?a", "<p>", "?b"))),
          h::Filter("geof:sfContains(?n, ?b)",
                    h::spatialJoinFilterSubstitute(
                        -1, -1, V{"?n"}, V{"?b"}, std::nullopt,
                        PayloadVariables::all(), algo, COVERS,
                        scan("?m", "<p>", "?n"), scan("?a", "<p>", "?b")))));

  // Combination of geo relation filter and geo distance filter
  h::expect(
      "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?x <p> ?y ."
      "?m <p> ?n ."
      "FILTER(geof:metricDistance(?b, ?y) <= 1000) ."
      "FILTER geof:sfContains(?n, ?b) .  }",
      ::testing::AnyOf(
          h::spatialJoinFilterSubstitute(
              1000, -1, V{"?b"}, V{"?y"}, std::nullopt, PayloadVariables::all(),
              algo, WITHIN_DIST,
              h::spatialJoinFilterSubstitute(
                  -1, -1, V{"?n"}, V{"?b"}, std::nullopt,
                  PayloadVariables::all(), algo, CONTAINS,
                  scan("?m", "<p>", "?n"), scan("?a", "<p>", "?b")),
              scan("?x", "<p>", "?y")),
          h::spatialJoinFilterSubstitute(
              -1, -1, V{"?n"}, V{"?b"}, std::nullopt, PayloadVariables::all(),
              algo, CONTAINS, scan("?m", "<p>", "?n"),
              h::spatialJoinFilterSubstitute(
                  1000, -1, V{"?b"}, V{"?y"}, std::nullopt,
                  PayloadVariables::all(), algo, WITHIN_DIST,
                  scan("?a", "<p>", "?b"), scan("?x", "<p>", "?y")))));

  // Geo relation filter with the same variable twice is not allowed
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "
                "SELECT * WHERE {"
                "?a <p> ?b ."
                "FILTER geof:sfContains(?b, ?b) . }",
                ::testing::_),
      ::testing::HasSubstr("Variable ?b on both sides"));
}

// _____________________________________________________________________________
TEST(QueryPlanner, SpatialJoinLegacyPredicateSupport) {
  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  auto S2 = SpatialJoinAlgorithm::S2_GEOMETRY;

  // For maxDistance the special predicate remains supported
  h::expect(
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?y <max-distance-in-meters:1> ?b ."
      "?x <p> ?y ."
      " }",
      h::spatialJoin(1, -1, V{"?y"}, V{"?b"}, std::nullopt,
                     PayloadVariables::all(), S2, std::nullopt,
                     scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "SELECT * WHERE {"
      "?a <p> ?b ."
      "?y <max-distance-in-meters:5000> ?b ."
      "?x <p> ?y ."
      " }",
      h::spatialJoin(5000, -1, V{"?y"}, V{"?b"}, std::nullopt,
                     PayloadVariables::all(), S2, std::nullopt,
                     scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));

  // Test that invalid triples throw an error
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <max-distance-in-meters:1> ?b ."
                "?y <a> ?b}",
                ::testing::_),
      ::testing::ContainsRegex(
          "Currently, if both sides of a SpatialJoin are variables, then the"
          "SpatialJoin must be the only connection between these variables"));

  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("SELECT ?x ?y WHERE {"
                "?y <p> ?b."
                "?y <max-distance-in-meters:1> ?b }",
                ::testing::_),
      ::testing::ContainsRegex(
          "Currently, if both sides of a SpatialJoin are variables, then the"
          "SpatialJoin must be the only connection between these variables"));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?y <max-distance-in-meters:1> <a> }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "<a> <max-distance-in-meters:1> ?y }",
                ::testing::_));

  AD_EXPECT_THROW_WITH_MESSAGE(h::expect("SELECT ?x ?y WHERE {"
                                         "?x <p> ?y."
                                         "?a <p> ?b."
                                         "?y <max-distance-in-meters:-1> ?b }",
                                         ::testing::_),
                               ::testing::ContainsRegex("unknown triple"));

  // Test that the nearest neighbors special predicate is still accepted but
  // produces a warning
  h::expect(
      "SELECT ?x ?y WHERE {"
      "?x <p> ?y."
      "?a <p> ?b."
      "?y <nearest-neighbors:2:500> ?b }",
      h::QetWithWarnings(
          {"special predicate <nearest-neighbors:...> is deprecated"},
          h::spatialJoin(500, 2, V{"?y"}, V{"?b"}, std::nullopt,
                         PayloadVariables::all(), S2, std::nullopt,
                         scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b"))));
  h::expect(
      "SELECT ?x ?y WHERE {"
      "?x <p> ?y."
      "?a <p> ?b."
      "?y <nearest-neighbors:20> ?b }",
      h::QetWithWarnings(
          {"special predicate <nearest-neighbors:...> is deprecated"},
          h::spatialJoin(-1, 20, V{"?y"}, V{"?b"}, std::nullopt,
                         PayloadVariables::all(), S2, std::nullopt,
                         scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b"))));

  AD_EXPECT_THROW_WITH_MESSAGE(h::expect("SELECT ?x ?y WHERE {"
                                         "?x <p> ?y."
                                         "?a <p> ?b."
                                         "?y <nearest-neighbors:1:-200> ?b }",
                                         ::testing::_),
                               ::testing::ContainsRegex("unknown triple"));

  AD_EXPECT_THROW_WITH_MESSAGE(h::expect("SELECT ?x ?y WHERE {"
                                         "?x <p> ?y."
                                         "?a <p> ?b."
                                         "?y <nearest-neighbors:0:-1> ?b }",
                                         ::testing::_),
                               ::testing::ContainsRegex("unknown triple"));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <nearest-neighbors:2:500> ?b ."
                "?y <a> ?b}",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?y <p> ?b."
                "?y <nearest-neighbors:1> ?b }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?y <nearest-neighbors:2:500> <a> }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "<a> <nearest-neighbors:2:500> ?y }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <nearest-neighbors:> ?b }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <nearest-neighbors:-50:500> ?b }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <nearest-neighbors:1:-200> ?b }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <nearest-neighbors:0:-1> ?b }",
                ::testing::_));
}

// _____________________________________________________________________________
TEST(QueryPlanner, SpatialJoinLegacyMaxDistanceParsing) {
  // test if the SpatialJoin operation parses the maximum distance correctly
  auto testMaxDistance = [](std::string distanceIRI, long long distance,
                            bool shouldThrow) {
    auto qec = ad_utility::testing::getQec();
    TripleComponent subject{Variable{"?subject"}};
    TripleComponent object{Variable{"?object"}};
    if (shouldThrow) {
      ASSERT_ANY_THROW((parsedQuery::SpatialQuery{
                            SparqlTriple{subject, iri(distanceIRI), object}})
                           .toSpatialJoinConfiguration());
    } else {
      auto config = parsedQuery::SpatialQuery{
          SparqlTriple{
              subject, iri(distanceIRI),
              object}}.toSpatialJoinConfiguration();
      std::shared_ptr<QueryExecutionTree> spatialJoinOperation =
          ad_utility::makeExecutionTree<SpatialJoin>(qec, config, std::nullopt,
                                                     std::nullopt);
      std::shared_ptr<Operation> op = spatialJoinOperation->getRootOperation();
      SpatialJoin* spatialJoin = static_cast<SpatialJoin*>(op.get());
      ASSERT_TRUE(spatialJoin->getMaxDist().has_value());
      ASSERT_EQ(spatialJoin->getMaxDist(), distance);
      ASSERT_FALSE(spatialJoin->getMaxResults().has_value());
    }
  };

  testMaxDistance("<max-distance-in-meters:1000>", 1000, false);

  testMaxDistance("<max-distance-in-meters:0>", 0, false);

  testMaxDistance("<max-distance-in-meters:20000000>", 20000000, false);

  testMaxDistance("<max-distance-in-meters:123456789>", 123456789, false);

  // the following distance is slightly bigger than earths circumference.
  // This distance should still be representable
  testMaxDistance("<max-distance-in-meters:45000000000>", 45000000000, false);

  // distance must be positive
  testMaxDistance("<max-distance-in-meters:-10>", -10, true);

  // some words start with an upper case
  testMaxDistance("<max-Distance-In-Meters:1000>", 1000, true);

  // wrong keyword for the spatialJoin operation
  testMaxDistance("<maxDistanceInMeters:1000>", 1000, true);

  // "M" in meters is upper case
  testMaxDistance("<max-distance-in-Meters:1000>", 1000, true);

  // two > at the end
  testMaxDistance("<maxDistanceInMeters:1000>>", 1000, true);

  // distance must be given as integer
  testMaxDistance("<maxDistanceInMeters:oneThousand>", 1000, true);

  // distance must be given as integer
  testMaxDistance("<maxDistanceInMeters:1000.54>>", 1000, true);

  // missing > at the end
  testMaxDistance("<maxDistanceInMeters:1000", 1000, true);

  // prefix before correct iri
  testMaxDistance("<asdfmax-distance-in-meters:1000>", 1000, true);

  // suffix after correct iri
  testMaxDistance("<max-distance-in-metersjklö:1000>", 1000, true);

  // suffix after correct iri
  testMaxDistance("<max-distance-in-meters:qwer1000>", 1000, true);

  // suffix after number.
  // Note that the usual stoll function would return
  // 1000 instead of throwing an exception. To fix this mistake, a for loop
  // has been added to the parsing, which checks, if each character (which
  // should be converted to a number) is a digit
  testMaxDistance("<max-distance-in-meters:1000asff>", 1000, true);

  // prefix before <
  testMaxDistance("yxcv<max-distance-in-metersjklö:1000>", 1000, true);

  // suffix after >
  testMaxDistance("<max-distance-in-metersjklö:1000>dfgh", 1000, true);
}
