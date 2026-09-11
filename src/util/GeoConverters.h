// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_UTIL_GEOCONVERTERS_H
#define QLEVER_SRC_UTIL_GEOCONVERTERS_H

#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2point.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>
#include <util/geo/Geo.h>

#include <memory>
#include <vector>

#include "backports/algorithm.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"

// Several helpers to convert geometry representations between `S2` and
// `libspatialjoin`.
namespace geometryConverters {

using CoordType = double;

// Helper function to convert `GeoPoint` objects to `S2Point`.
inline S2Point toS2Point(const GeoPoint& p) {
  return S2LatLng::FromDegrees(p.getLat(), p.getLng()).ToPoint();
}

// Helper function to convert `libspatialjoin` `DPoint` objects to `S2LatLng`.
inline S2LatLng toS2LatLng(const util::geo::DPoint& point) {
  return S2LatLng::FromDegrees(point.getY(), point.getX());
}

// Helper function to convert `libspatialjoin` `DLine` objects to `S2Polyline`.
inline S2Polyline toS2Polyline(const util::geo::DLine& line) {
  AD_CORRECTNESS_CHECK(!line.empty());
  return S2Polyline{
      ::ranges::to_vector(line | ql::views::transform(toS2LatLng))};
}

// Helper function to convert `libspatialjoin` `DPoint` objects to `S2Point`.
inline S2Point utilPointToS2Point(const util::geo::DPoint& p) {
  return S2LatLng::FromDegrees(p.getY(), p.getX()).ToPoint();
}

// Helper to convert a `libspatialjoin` `Ring` to an `S2Loop`
inline std::unique_ptr<S2Loop> makeS2Loop(
    const util::geo::Ring<CoordType>& ring) {
  std::vector<S2Point> points = ::ranges::to<std::vector>(
      ring | ::ranges::views::transform(&utilPointToS2Point));

  // Ensure that there are no zero-length edges (that is edges with twice the
  // same point), as this will lead to an exception from `S2Loop`.
  points.erase(::ranges::unique(points), points.end());
  if (points.front() == points.back()) {
    points.pop_back();
  }

  // We have to check validity of the polygon loop in a separate step because s2
  // throws an uncatchable fatal error otherwise if the loop is invalid.
  auto loop = std::make_unique<S2Loop>(std::move(points), S2Debug::DISABLE);
  loop->Normalize();
  if (!loop->IsValid()) {
    throw ad_utility::InvalidPolygonError();
  }
  return loop;
}

// Helper to convert a `libspatialjoin` `Polygon` to an `S2Polygon`
inline std::unique_ptr<S2Polygon> makeS2Polygon(
    const util::geo::Polygon<CoordType>& polygon) {
  std::vector<std::unique_ptr<S2Loop>> loops;

  // Outer boundary
  loops.push_back(makeS2Loop(polygon.getOuter()));

  // Holes
  for (const auto& hole : polygon.getInners()) {
    loops.push_back(makeS2Loop(hole));
  }

  // We have to check validity of the polygon in a separate step because s2
  // throws an uncatchable fatal error otherwise if the polygon is invalid.
  S2Polygon s2polygon{std::move(loops), S2Debug::DISABLE};
  if (!s2polygon.IsValid()) {
    throw ad_utility::InvalidPolygonError();
  }
  return std::make_unique<S2Polygon>(std::move(s2polygon));
}

// Simplify an `S2Polyline` using the Douglas-Peucker algorithm via
// `S2Polyline::SubsampleVertices`. The first and last vertices are always kept.
// The `metersError` parameter is the maximum allowed deviation from the
// original polyline in meters and must be positive.
inline S2Polyline simplifyPolyline(const S2Polyline& polyline,
                                   double metersError) {
  AD_CONTRACT_CHECK(metersError > 0.0);
  auto tolerance = S2Earth::MetersToAngle(metersError);
  std::vector<int> indices;
  polyline.SubsampleVertices(tolerance, &indices);
  std::vector<S2Point> selected;
  selected.reserve(indices.size());
  for (int idx : indices) {
    selected.push_back(polyline.vertex(idx));
  }
  return S2Polyline{absl::MakeSpan(selected)};
}

// Simplify each loop of an `S2Polygon` by applying the Douglas-Peucker
// algorithm (via `S2Polyline::SubsampleVertices`) to each loop's vertices.
// The `metersError` parameter is the maximum allowed deviation in meters and
// must be positive.
// Falls back to the original loop vertices if simplification would reduce a
// loop below three vertices.
//
// NOTE: This function is not yet used in production (the cached geo index
// currently only supports line strings). Unlike for polylines, per-loop
// Douglas-Peucker can make a loop self-intersecting (which the
// `S2Debug::DISABLE` below deliberately does not check) and the nesting of
// the loops is re-inferred from the simplified loops. Before polygons are
// simplified in production, consider a topology-preserving simplification,
// e.g. via `S2Builder`.
inline S2Polygon simplifyPolygon(const S2Polygon& polygon, double metersError) {
  AD_CONTRACT_CHECK(metersError > 0.0);
  auto tolerance = S2Earth::MetersToAngle(metersError);
  std::vector<std::unique_ptr<S2Loop>> simplifiedLoops;
  simplifiedLoops.reserve(polygon.num_loops());
  for (int i = 0; i < polygon.num_loops(); ++i) {
    const S2Loop* loop = polygon.loop(i);
    int n = loop->num_vertices();
    std::vector<S2Point> points;
    points.reserve(n);
    for (int j = 0; j < n; ++j) {
      points.push_back(loop->vertex(j));
    }
    // Treat the loop as an open polyline to apply `SubsampleVertices`.
    // The first and last vertices are always preserved, so the ring remains
    // closable.
    S2Polyline polyline{absl::MakeConstSpan(points)};
    std::vector<int> indices;
    polyline.SubsampleVertices(tolerance, &indices);
    std::vector<S2Point> simplified;
    simplified.reserve(indices.size());
    for (int idx : indices) {
      simplified.push_back(points[idx]);
    }
    // A valid S2Loop requires at least three distinct vertices.
    if (simplified.size() < 3) {
      simplified = std::move(points);
    }
    auto newLoop =
        std::make_unique<S2Loop>(std::move(simplified), S2Debug::DISABLE);
    newLoop->Normalize();
    simplifiedLoops.push_back(std::move(newLoop));
  }
  return S2Polygon{std::move(simplifiedLoops), S2Debug::DISABLE};
}

}  // namespace geometryConverters

#endif  // QLEVER_SRC_UTIL_GEOCONVERTERS_H
