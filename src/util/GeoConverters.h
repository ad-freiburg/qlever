// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_UTIL_GEOCONVERTERS_H
#define QLEVER_SRC_UTIL_GEOCONVERTERS_H

#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2point.h>
#include <s2/s2polygon.h>
#include <s2/s2polyline.h>
#include <util/geo/Geo.h>

#include "backports/algorithm.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"

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
inline S2Polygon makeS2Polygon(const util::geo::Polygon<CoordType>& polygon) {
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
  return s2polygon;
}

}  // namespace geometryConverters

#endif  // QLEVER_SRC_UTIL_GEOCONVERTERS_H
