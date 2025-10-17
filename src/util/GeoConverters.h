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

#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"

// Several helpers to convert geometry representations between `S2` and
// `libspatialjoin`.
namespace geometryConverters {

using CoordType = double;
using namespace util::geo;

// Helper function to convert `GeoPoint` objects to `S2Point`.
inline S2Point toS2Point(const DPoint& p) {
  return S2LatLng::FromDegrees(p.getY(), p.getX()).ToPoint();
}

// Helper to convert a `libspatialjoin` `Ring` to an `S2Loop`
inline std::unique_ptr<S2Loop> makeS2Loop(const Ring<CoordType>& ring) {
  std::vector<S2Point> points =
      ::ranges::to<std::vector>(ring | ::ranges::views::transform(&toS2Point));

  // Ensure that there are no zero-length edges (that is edges with twice the
  // same point), as this will lead to an exception from `S2Loop`.
  auto new_end = ql::ranges::unique(points);
  points.erase(new_end.begin(), points.end());
  if (points.front() == points.back()) {
    points.pop_back();
  }

  auto loop = std::make_unique<S2Loop>(std::move(points));
  loop->Normalize();
  if (!loop->IsValid()) {
    throw ad_utility::InvalidPolygonError();
  }
  return loop;
}

// Helper to convert a `libspatialjoin` `Polygon` to an `S2Polygon`
inline S2Polygon makeS2Polygon(const Polygon<CoordType>& polygon) {
  std::vector<std::unique_ptr<S2Loop>> loops;

  // Outer boundary
  loops.push_back(makeS2Loop(polygon.getOuter()));

  // Holes
  for (const auto& hole : polygon.getInners()) {
    loops.push_back(makeS2Loop(hole));
  }

  S2Polygon s2polygon;
  s2polygon.InitNested(std::move(loops));
  return s2polygon;
}

}  // namespace geometryConverters

#endif  // QLEVER_SRC_UTIL_GEOCONVERTERS_H
