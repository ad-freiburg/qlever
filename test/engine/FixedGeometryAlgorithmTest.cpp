// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/IndexTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "./SpatialJoinTestHelpers.h"
#include "engine/SpatialJoin.h"
#include "engine/Values.h"
#include "engine/spatialJoinAlgorithms/FixedGeometryAlgorithm.h"
#include "global/RuntimeParameters.h"
#include "index/LocalVocab.h"
#include "index/LocalVocabEntry.h"

namespace {

using namespace SpatialJoinTestHelpers;
using ::testing::UnorderedElementsAreArray;
using enum SpatialJoinType::Enum;

constexpr std::string_view wktDatatype =
    "^^<http://www.opengis.net/ont/geosparql#wktLiteral>";

// A polygon that contains the two Freiburg points of the small dataset, the
// Minster point (7.85298, 47.99557) only by a meter.
const std::string polygonFreiburg = absl::StrCat(
    "\"POLYGON((7.7 47.9, 7.85299 47.9, 7.85299 48.1, 7.7 48.1, 7.7 47.9))\"",
    wktDatatype);
// The same polygon with the Minster point exactly on its right edge.
const std::string polygonMinsterOnEdge = absl::StrCat(
    "\"POLYGON((7.7 47.9, 7.85298 47.9, 7.85298 48.1, 7.7 48.1, 7.7 47.9))\"",
    wktDatatype);
// A polygon that contains none of the points.
const std::string polygonAtlantic = absl::StrCat(
    "\"POLYGON((-30 40, -20 40, -20 50, -30 50, -30 40))\"", wktDatatype);
const std::string multiPolygon = absl::StrCat(
    "\"MULTIPOLYGON(((7.7 47.9, 8.0 47.9, 8.0 48.1, 7.7 48.1, 7.7 47.9)))\"",
    wktDatatype);
const std::string lineString =
    absl::StrCat("\"LINESTRING(7.7 47.9, 8.0 48.1)\"", wktDatatype);
// A point not in the dataset, 1.6 km from the Uni Freiburg point.
const std::string pointNearUni =
    absl::StrCat("\"POINT(7.85 48.02)\"", wktDatatype);
// The Uni Freiburg point of the dataset (`pointUniFreiburg` of the helpers
// has a leading space, which a `VALUES` literal must not have).
const std::string pointUni =
    absl::StrCat("\"POINT(7.83505 48.01267)\"", wktDatatype);

// A child with one row that binds `?fixed` to the given WKT literal.
std::shared_ptr<QueryExecutionTree> oneRow(QueryExecutionContext* qec,
                                           const std::string& wktLiteral) {
  parsedQuery::SparqlValues values;
  values._variables = {Variable{"?fixed"}};
  values._values.push_back({TripleComponent{
      TripleComponent::Literal::fromStringRepresentation(wktLiteral)}});
  return ad_utility::makeExecutionTree<Values>(qec, std::move(values));
}

// All geometries of the dataset, bound to `?geom`.
std::shared_ptr<QueryExecutionTree> allGeometries(QueryExecutionContext* qec) {
  return buildIndexScan(qec, {"?node", "<asWKT>", "?geom"});
}

// Run a spatial join with the `libspatialjoin` algorithm and the given join
// type and children, with or without the fixed geometry algorithm, and return
// the result rows as strings and whether the fixed geometry algorithm was
// used.
struct Outcome {
  std::vector<std::string> rows_;
  bool usedFixedGeometry_;
};
Outcome runJoin(QueryExecutionContext* qec, SpatialJoinType joinType,
                std::shared_ptr<QueryExecutionTree> left, Variable leftVar,
                std::shared_ptr<QueryExecutionTree> right, Variable rightVar,
                bool useFixedGeometryAlgorithm,
                std::optional<double> maxDist = std::nullopt,
                std::optional<Variable> distanceVariable = std::nullopt) {
  auto parameter = setRuntimeParameterForTest<
      &RuntimeParameters::spatialJoinFixedGeometryAlgorithm_>(
      useFixedGeometryAlgorithm);
  LibSpatialJoinConfig task{joinType, maxDist, std::nullopt};
  SpatialJoinConfiguration config{task,
                                  std::move(leftVar),
                                  std::move(rightVar),
                                  distanceVariable,
                                  PayloadVariables::all(),
                                  SpatialJoinAlgorithm::LIBSPATIALJOIN};
  if (distanceVariable.has_value()) {
    config.distanceUnit_ = UnitOfMeasurement::METERS;
  }
  qec->clearCacheUnpinnedOnly();
  auto tree = ad_utility::makeExecutionTree<SpatialJoin>(
      qec, std::move(config), std::move(left), std::move(right));
  auto result = tree->getRootOperation()->getResult();
  const auto& details = tree->getRootOperation()->runtimeInfo().details_;
  bool used =
      details.contains("algorithm") && details["algorithm"] == "fixed-geometry";
  return {printTable(qec, result.get()), used};
}

// Test the fixed geometry algorithm against `libspatialjoin` for the given
// join type and fixed geometry, with the fixed geometry on the left and on
// the right side. `expectedNumResults` is the number of rows both must
// produce.
void expectSameAsLibspatialjoin(QueryExecutionContext* qec,
                                SpatialJoinType joinType,
                                const std::string& fixedWkt, bool fixedIsLeft,
                                size_t expectedNumResults,
                                std::optional<double> maxDist = std::nullopt) {
  auto fixed = oneRow(qec, fixedWkt);
  auto points = allGeometries(qec);
  auto left = fixedIsLeft ? fixed : points;
  auto right = fixedIsLeft ? points : fixed;
  Variable leftVar{fixedIsLeft ? "?fixed" : "?geom"};
  Variable rightVar{fixedIsLeft ? "?geom" : "?fixed"};
  auto withAlgorithm =
      runJoin(qec, joinType, left, leftVar, right, rightVar, true, maxDist);
  auto reference =
      runJoin(qec, joinType, left, leftVar, right, rightVar, false, maxDist);
  EXPECT_TRUE(withAlgorithm.usedFixedGeometry_) << fixedWkt;
  EXPECT_FALSE(reference.usedFixedGeometry_) << fixedWkt;
  EXPECT_THAT(withAlgorithm.rows_, UnorderedElementsAreArray(reference.rows_))
      << fixedWkt;
  EXPECT_EQ(withAlgorithm.rows_.size(), expectedNumResults) << fixedWkt;
}

// Run a spatial join of the fixed geometry with all points and return the
// number of result rows and whether the fixed geometry algorithm was used.
std::pair<size_t, bool> numResults(QueryExecutionContext* qec,
                                   SpatialJoinType joinType,
                                   const std::string& fixedWkt,
                                   bool fixedIsLeft) {
  auto fixed = oneRow(qec, fixedWkt);
  auto points = allGeometries(qec);
  auto outcome = fixedIsLeft ? runJoin(qec, joinType, fixed, Variable{"?fixed"},
                                       points, Variable{"?geom"}, true)
                             : runJoin(qec, joinType, points, Variable{"?geom"},
                                       fixed, Variable{"?fixed"}, true);
  return {outcome.rows_.size(), outcome.usedFixedGeometry_};
}

// The relations of points to a fixed polygon that contains two of them. The
// results are the same as those of `libspatialjoin`.
TEST(FixedGeometryAlgorithm, pointsAndPolygon) {
  auto* qec = buildTestQEC();
  for (bool fixedIsLeft : {true, false}) {
    expectSameAsLibspatialjoin(qec, INTERSECTS, polygonFreiburg, fixedIsLeft,
                               2);
    expectSameAsLibspatialjoin(qec, COVERS, polygonFreiburg, fixedIsLeft,
                               fixedIsLeft ? 2 : 0);
    expectSameAsLibspatialjoin(qec, CONTAINS, polygonFreiburg, fixedIsLeft,
                               fixedIsLeft ? 2 : 0);
    expectSameAsLibspatialjoin(qec, WITHIN, polygonFreiburg, fixedIsLeft,
                               fixedIsLeft ? 0 : 2);
    // A polygon that contains no point.
    expectSameAsLibspatialjoin(qec, INTERSECTS, polygonAtlantic, fixedIsLeft,
                               0);
  }
}

// A point on the boundary of the fixed polygon intersects it and is covered
// by it, but not contained in it.
//
// NOTE: This case is not compared with `libspatialjoin`, whose sweep misses
// a pair whose events have exactly the same coordinate (here, the point and
// the right edge of the polygon), so that it does not report the boundary
// point at all.
TEST(FixedGeometryAlgorithm, pointOnBoundary) {
  auto* qec = buildTestQEC();
  for (bool fixedIsLeft : {true, false}) {
    EXPECT_EQ(numResults(qec, INTERSECTS, polygonMinsterOnEdge, fixedIsLeft),
              std::pair(2ul, true));
    EXPECT_EQ(numResults(qec, COVERS, polygonMinsterOnEdge, fixedIsLeft),
              std::pair(fixedIsLeft ? 2ul : 0ul, true));
    EXPECT_EQ(numResults(qec, CONTAINS, polygonMinsterOnEdge, fixedIsLeft),
              std::pair(fixedIsLeft ? 1ul : 0ul, true));
    EXPECT_EQ(numResults(qec, WITHIN, polygonMinsterOnEdge, fixedIsLeft),
              std::pair(fixedIsLeft ? 0ul : 1ul, true));
  }
}

// The relations of points to a fixed point: equal points intersect, contain
// and cover each other, other points do not.
//
// NOTE: Like the boundary case above, the equal points are not compared with
// `libspatialjoin`, which does not report them (see there).
TEST(FixedGeometryAlgorithm, pointsAndPoint) {
  auto* qec = buildTestQEC();
  for (bool fixedIsLeft : {true, false}) {
    for (auto joinType : {INTERSECTS, CONTAINS, COVERS, WITHIN}) {
      EXPECT_EQ(numResults(qec, joinType, pointUni, fixedIsLeft),
                std::pair(1ul, true));
      expectSameAsLibspatialjoin(qec, joinType, pointNearUni, fixedIsLeft, 0);
    }
  }
}

// A distance join with a fixed point, with and without a distance variable.
TEST(FixedGeometryAlgorithm, pointsWithinDistanceOfPoint) {
  auto* qec = buildTestQEC();
  for (bool fixedIsLeft : {true, false}) {
    // The Uni Freiburg point is 1.6 km from the fixed point, the Minster
    // point 2.7 km, all others far away.
    expectSameAsLibspatialjoin(qec, WITHIN_DIST, pointNearUni, fixedIsLeft, 1,
                               2000);
    expectSameAsLibspatialjoin(qec, WITHIN_DIST, pointNearUni, fixedIsLeft, 2,
                               3000);
    expectSameAsLibspatialjoin(qec, WITHIN_DIST, pointNearUni, fixedIsLeft, 0,
                               500);
  }

  // With a distance variable, the distances are those of `libspatialjoin`
  // (which reports them through a string, so compare them as numbers).
  auto fixed = oneRow(qec, pointNearUni);
  auto points = allGeometries(qec);
  Variable distance{"?distance"};
  auto withAlgorithm = runJoin(qec, WITHIN_DIST, fixed, Variable{"?fixed"},
                               points, Variable{"?geom"}, true, 3000, distance);
  auto reference = runJoin(qec, WITHIN_DIST, fixed, Variable{"?fixed"}, points,
                           Variable{"?geom"}, false, 3000, distance);
  ASSERT_TRUE(withAlgorithm.usedFixedGeometry_);
  ASSERT_EQ(withAlgorithm.rows_.size(), 2u);
  ASSERT_EQ(reference.rows_.size(), 2u);
  std::sort(withAlgorithm.rows_.begin(), withAlgorithm.rows_.end());
  std::sort(reference.rows_.begin(), reference.rows_.end());
  for (size_t i = 0; i < 2; ++i) {
    // The distance is the last value of the row.
    auto lastValue = [](const std::string& row) {
      return std::stod(row.substr(row.rfind(' ') + 1));
    };
    EXPECT_EQ(
        withAlgorithm.rows_[i].substr(0, withAlgorithm.rows_[i].rfind(' ')),
        reference.rows_[i].substr(0, reference.rows_[i].rfind(' ')));
    EXPECT_NEAR(lastValue(withAlgorithm.rows_[i]),
                lastValue(reference.rows_[i]), 1.0);
    EXPECT_GT(lastValue(withAlgorithm.rows_[i]), 1000.0);
    EXPECT_LT(lastValue(withAlgorithm.rows_[i]), 3000.0);
  }
}

// The cases in which the algorithm does not apply and `libspatialjoin` is
// used: unsupported join types and fixed geometries, a distance join with a
// fixed polygon, an other side with WKT literals, and no side with one row.
TEST(FixedGeometryAlgorithm, notApplicable) {
  auto* qec = buildTestQEC();
  auto points = allGeometries(qec);
  auto expectNotUsed = [&](std::shared_ptr<QueryExecutionTree> left,
                           Variable leftVar,
                           std::shared_ptr<QueryExecutionTree> right,
                           Variable rightVar, SpatialJoinType joinType,
                           std::optional<double> maxDist = std::nullopt) {
    auto outcome =
        runJoin(qec, joinType, left, leftVar, right, rightVar, true, maxDist);
    EXPECT_FALSE(outcome.usedFixedGeometry_);
  };
  Variable fixedVar{"?fixed"};
  Variable geomVar{"?geom"};
  // Unsupported join types.
  for (auto joinType : {TOUCHES, EQUALS, OVERLAPS, CROSSES}) {
    expectNotUsed(oneRow(qec, polygonFreiburg), fixedVar, points, geomVar,
                  joinType);
  }
  // Unsupported fixed geometries.
  expectNotUsed(oneRow(qec, multiPolygon), fixedVar, points, geomVar,
                INTERSECTS);
  expectNotUsed(oneRow(qec, lineString), fixedVar, points, geomVar, INTERSECTS);
  // A distance join with a fixed polygon.
  expectNotUsed(oneRow(qec, polygonFreiburg), fixedVar, points, geomVar,
                WITHIN_DIST, 1000);
  // No side with exactly one row.
  expectNotUsed(points, geomVar,
                buildIndexScan(qec, {"?node2", "<asWKT>", "?geom2"}),
                Variable{"?geom2"}, INTERSECTS);
  // The other side contains WKT literals.
  parsedQuery::SparqlValues polygons;
  polygons._variables = {geomVar};
  for (const auto& wkt : {polygonFreiburg, polygonAtlantic}) {
    polygons._values.push_back({TripleComponent{
        TripleComponent::Literal::fromStringRepresentation(wkt)}});
  }
  expectNotUsed(oneRow(qec, polygonFreiburg), fixedVar,
                ad_utility::makeExecutionTree<Values>(qec, std::move(polygons)),
                geomVar, INTERSECTS);
}

// The parsing of the fixed geometry from a `ValueId`.
TEST(FixedGeometryAlgorithm, parseFixedGeometry) {
  auto* qec = buildTestQEC();
  const auto& index = qec->getIndex();
  LocalVocab localVocab;
  auto literalId = [&localVocab, qec](const std::string& literal) {
    return Id::makeFromLocalVocabIndex(
        localVocab.getIndexAndAddIfNotContained(LocalVocabEntry{
            ad_utility::triple_component::Literal::fromStringRepresentation(
                literal),
            qec->getLocalVocabContext()}));
  };
  using PreparedPolygon = FixedGeometryAlgorithm::PreparedPolygon;

  // A `GeoPoint` and a point literal give the same point.
  auto fromGeoPoint = FixedGeometryAlgorithm::parseFixedGeometry(
      Id::makeFromGeoPoint(GeoPoint{48.02, 7.85}), index);
  auto fromLiteral = FixedGeometryAlgorithm::parseFixedGeometry(
      literalId(pointNearUni), index);
  ASSERT_TRUE(fromGeoPoint.has_value());
  ASSERT_TRUE(fromLiteral.has_value());
  ASSERT_TRUE(std::holds_alternative<util::geo::I32Point>(*fromGeoPoint));
  ASSERT_TRUE(std::holds_alternative<util::geo::I32Point>(*fromLiteral));
  // The `GeoPoint` stores its coordinates with a limited precision, so the
  // two points agree up to one unit (a tenth of a meter).
  EXPECT_NEAR(std::get<util::geo::I32Point>(*fromGeoPoint).getX(),
              std::get<util::geo::I32Point>(*fromLiteral).getX(), 1);
  EXPECT_NEAR(std::get<util::geo::I32Point>(*fromGeoPoint).getY(),
              std::get<util::geo::I32Point>(*fromLiteral).getY(), 1);

  // A polygon literal gives a prepared polygon, with box ids.
  auto polygon = FixedGeometryAlgorithm::parseFixedGeometry(
      literalId(polygonFreiburg), index);
  ASSERT_TRUE(polygon.has_value());
  ASSERT_TRUE(std::holds_alternative<PreparedPolygon>(*polygon));
  const auto& prepared = std::get<PreparedPolygon>(*polygon);
  EXPECT_FALSE(prepared.polygon_.empty());
  EXPECT_FALSE(prepared.boxIds_.empty());

  // Everything else gives nothing.
  EXPECT_FALSE(
      FixedGeometryAlgorithm::parseFixedGeometry(literalId(multiPolygon), index)
          .has_value());
  EXPECT_FALSE(
      FixedGeometryAlgorithm::parseFixedGeometry(literalId(lineString), index)
          .has_value());
  EXPECT_FALSE(
      FixedGeometryAlgorithm::parseFixedGeometry(
          literalId(absl::StrCat("\"POLYGON((7.7\"", wktDatatype)), index)
          .has_value());
  EXPECT_FALSE(FixedGeometryAlgorithm::parseFixedGeometry(
                   literalId("\"not a geometry\""), index)
                   .has_value());
  EXPECT_FALSE(
      FixedGeometryAlgorithm::parseFixedGeometry(Id::makeFromInt(42), index)
          .has_value());
  EXPECT_FALSE(
      FixedGeometryAlgorithm::parseFixedGeometry(Id::makeUndefined(), index)
          .has_value());
}

// The relation of a point to a prepared polygon, with the shortcuts via the
// bounding box and the box ids, agrees with the exact check.
TEST(FixedGeometryAlgorithm, relate) {
  auto* qec = buildTestQEC();
  LocalVocab localVocab;
  auto polygon = FixedGeometryAlgorithm::parseFixedGeometry(
      Id::makeFromLocalVocabIndex(
          localVocab.getIndexAndAddIfNotContained(LocalVocabEntry{
              ad_utility::triple_component::Literal::fromStringRepresentation(
                  polygonMinsterOnEdge),
              qec->getLocalVocabContext()})),
      qec->getIndex());
  ASSERT_TRUE(polygon.has_value());
  const auto& prepared =
      std::get<FixedGeometryAlgorithm::PreparedPolygon>(*polygon);
  auto relate = [&prepared](double lat, double lng) {
    return FixedGeometryAlgorithm::relate(
        FixedGeometryAlgorithm::toWebMercator(GeoPoint{lat, lng}), prepared);
  };
  // Inside.
  EXPECT_TRUE(relate(48.0, 7.8).within());
  EXPECT_TRUE(relate(48.0, 7.8).coveredBy());
  // On the boundary (the right edge of the polygon).
  EXPECT_FALSE(relate(47.99557, 7.85298).within());
  EXPECT_TRUE(relate(47.99557, 7.85298).coveredBy());
  // Outside, inside and outside the bounding box.
  EXPECT_FALSE(relate(48.0, 7.9).coveredBy());
  EXPECT_FALSE(relate(0.0, 0.0).coveredBy());
  // The shortcuts agree with the exact check for all these points.
  for (auto [lat, lng] : std::vector<std::pair<double, double>>{
           {48.0, 7.8}, {47.99557, 7.85298}, {48.0, 7.9}, {0.0, 0.0}}) {
    auto point = FixedGeometryAlgorithm::toWebMercator(GeoPoint{lat, lng});
    auto exact = util::geo::DE9IM(point, prepared.polygon_);
    EXPECT_EQ(relate(lat, lng).within(), exact.within());
    EXPECT_EQ(relate(lat, lng).coveredBy(), exact.coveredBy());
  }
}

// The distance between two points, with the early return for far apart
// points.
TEST(FixedGeometryAlgorithm, meterDist) {
  auto point = [](double lat, double lng) {
    return FixedGeometryAlgorithm::toWebMercator(GeoPoint{lat, lng});
  };
  // Uni Freiburg to Minster: about 2.3 km.
  auto uni = point(48.01267, 7.83505);
  auto minster = point(47.99557, 7.85298);
  auto distance = FixedGeometryAlgorithm::meterDist(uni, minster, 5000);
  EXPECT_NEAR(distance, 2300, 100);
  EXPECT_EQ(FixedGeometryAlgorithm::meterDist(uni, minster, 1000),
            std::numeric_limits<double>::max());
  EXPECT_NEAR(FixedGeometryAlgorithm::meterDist(uni, uni, 1), 0, 0.01);
}

}  // namespace
