// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_GEOCONVERTERS_H
#define QLEVER_GEOCONVERTERS_H

#include <s2/s2point.h>
#include <s2/s2polyline.h>
#include <util/geo/Geo.h>

#include "backports/algorithm.h"
#include "rdfTypes/GeoPoint.h"

// Several helpers to convert geometry representations between `S2` and
// `libspatialjoin`.
namespace geometryConverters {

// Helper function to convert `GeoPoint` objects to `S2Point`.
inline S2Point toS2Point(const GeoPoint& p) {
  return S2LatLng::FromDegrees(p.getLat(), p.getLng()).ToPoint();
}

// Helper function to convert `libspatialjoin` `DPoint` objects to `S2LatLng`.
inline S2LatLng toS2LatLng(const util::geo::DPoint& point) {
  return S2LatLng::FromDegrees(point.getY(), point.getX());
}

inline S2Polyline toS2Polyline(const util::geo::Line<double>& line) {
  AD_CORRECTNESS_CHECK(!line.empty());
  return S2Polyline{
      ::ranges::to_vector(line | ql::views::transform(toS2LatLng))};
}

}  // namespace geometryConverters

#endif  // QLEVER_GEOCONVERTERS_H
