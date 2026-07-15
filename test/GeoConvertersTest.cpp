// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>

#include "absl/types/span.h"
#include "util/GTestHelpers.h"
#include "util/GeoConverters.h"

namespace {

using geometryConverters::simplifyPolygon;
using geometryConverters::simplifyPolyline;

// Helper: build an `S2Polyline` from a list of (lng, lat) degree pairs.
S2Polyline makePolyline(
    std::initializer_list<std::pair<double, double>> coords) {
  std::vector<S2LatLng> latlngs;
  latlngs.reserve(coords.size());
  for (auto [lng, lat] : coords) {
    latlngs.push_back(S2LatLng::FromDegrees(lat, lng));
  }
  return S2Polyline{absl::MakeConstSpan(latlngs)};
}

// Helper: build a triangular `S2Polygon` from a list of (lng, lat) degree
// pairs.
S2Polygon makeTrianglePolygon(
    std::initializer_list<std::pair<double, double>> coords) {
  AD_CORRECTNESS_CHECK(coords.size() == 3);
  std::vector<S2Point> points;
  points.reserve(coords.size());
  for (auto [lng, lat] : coords) {
    points.push_back(S2LatLng::FromDegrees(lat, lng).ToPoint());
  }
  auto loop = std::make_unique<S2Loop>(std::move(points));
  loop->Normalize();
  std::vector<std::unique_ptr<S2Loop>> loops;
  loops.push_back(std::move(loop));
  return S2Polygon{std::move(loops)};
}

// _____________________________________________________________________________
TEST(SimplifyPolylineTest, NonPositiveErrorThrows) {
  S2Polyline polyline = makePolyline(
      {{7.84, 47.99}, {7.841, 47.991}, {7.842, 47.992}, {7.85, 47.99}});
  AD_EXPECT_THROW_WITH_MESSAGE(simplifyPolyline(polyline, 0.0),
                               ::testing::HasSubstr("metersError > 0.0"));
  AD_EXPECT_THROW_WITH_MESSAGE(simplifyPolyline(polyline, -5.0),
                               ::testing::HasSubstr("metersError > 0.0"));
}

// _____________________________________________________________________________
TEST(SimplifyPolylineTest, SimplificationReducesVertices) {
  // Build a polyline where the middle vertex is only ~6 m off the direct
  // path. A 10 m tolerance should simplify it away; a 0.001 m tolerance
  // should not.
  S2Polyline polyline =
      makePolyline({{7.840000, 47.999000},
                    {7.840045, 47.999050},  // ~6 m off the direct line
                    {7.841000, 47.999900}});
  EXPECT_EQ(polyline.num_vertices(), 3);

  auto simplified = simplifyPolyline(polyline, 10.0);
  EXPECT_EQ(simplified.num_vertices(), 2);

  auto notSimplified = simplifyPolyline(polyline, 0.001);
  EXPECT_EQ(notSimplified.num_vertices(), 3);
}

// _____________________________________________________________________________
TEST(SimplifyPolylineTest, AlwaysKeepsFirstAndLastVertex) {
  S2Polyline polyline = makePolyline(
      {{7.84, 47.99}, {7.841, 47.991}, {7.842, 47.992}, {7.85, 47.99}});
  auto simplified = simplifyPolyline(polyline, 100000.0);
  ASSERT_GE(simplified.num_vertices(), 2);
  EXPECT_EQ(simplified.vertex(0), polyline.vertex(0));
  EXPECT_EQ(simplified.vertex(simplified.num_vertices() - 1),
            polyline.vertex(polyline.num_vertices() - 1));
}

// _____________________________________________________________________________
TEST(SimplifyPolygonTest, NonPositiveErrorThrows) {
  S2Polygon polygon =
      makeTrianglePolygon({{7.84, 47.99}, {7.841, 47.99}, {7.8405, 47.991}});
  AD_EXPECT_THROW_WITH_MESSAGE(simplifyPolygon(polygon, 0.0),
                               ::testing::HasSubstr("metersError > 0.0"));
  AD_EXPECT_THROW_WITH_MESSAGE(simplifyPolygon(polygon, -5.0),
                               ::testing::HasSubstr("metersError > 0.0"));
}

// _____________________________________________________________________________
TEST(SimplifyPolygonTest, SimplificationReducesVertices) {
  // A quadrilateral where one vertex is only ~1 m off the edge between its
  // neighbors, so a 10 m tolerance simplifies it away, leaving a triangle; a
  // 0.001 m tolerance keeps all vertices.
  S2Polygon polygon = [] {
    std::vector<S2Point> points{
        S2LatLng::FromDegrees(47.999000, 7.840000).ToPoint(),
        S2LatLng::FromDegrees(47.999050, 7.840045).ToPoint(),  // ~6 m off
        S2LatLng::FromDegrees(47.999900, 7.841000).ToPoint(),
        S2LatLng::FromDegrees(47.999900, 7.839000).ToPoint()};
    auto loop = std::make_unique<S2Loop>(std::move(points));
    loop->Normalize();
    std::vector<std::unique_ptr<S2Loop>> loops;
    loops.push_back(std::move(loop));
    return S2Polygon{std::move(loops)};
  }();
  ASSERT_EQ(polygon.num_loops(), 1);
  ASSERT_EQ(polygon.loop(0)->num_vertices(), 4);

  auto simplified = simplifyPolygon(polygon, 10.0);
  ASSERT_EQ(simplified.num_loops(), 1);
  EXPECT_EQ(simplified.loop(0)->num_vertices(), 3);

  auto notSimplified = simplifyPolygon(polygon, 0.001);
  ASSERT_EQ(notSimplified.num_loops(), 1);
  EXPECT_EQ(notSimplified.loop(0)->num_vertices(), 4);
}

// _____________________________________________________________________________
TEST(SimplifyPolygonTest, FallsBackToOriginalWhenBelowThreeVertices) {
  // A triangle where two vertices are close enough together that an
  // aggressive tolerance would reduce the loop below three vertices; the
  // function must then fall back to the original vertices.
  S2Polygon polygon = makeTrianglePolygon(
      {{7.840000, 47.999000}, {7.840001, 47.999001}, {7.850000, 47.999000}});
  ASSERT_EQ(polygon.loop(0)->num_vertices(), 3);

  auto simplified = simplifyPolygon(polygon, 1000000.0);
  EXPECT_EQ(simplified.loop(0)->num_vertices(), 3);
}

}  // namespace
