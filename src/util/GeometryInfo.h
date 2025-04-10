// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_GEOMETRYINFO_H
#define QLEVER_SRC_UTIL_GEOMETRYINFO_H

#include <cstdint>
#include <cstdio>
#include <string>

#include "parser/GeoPoint.h"

namespace ad_utility {

// A geometry info object holds precomputed details on WKT literals.
// IMPORTANT: Every change to the attributes of this struct is an index-breaking
// change. Please update the index version accordingly.
struct GeometryInfo {
  // TODO<ullingerc> Computation of GeometryInfo is a separate PR
  uint64_t dummy_;

  // Parse a WKT literal and compute all attributes.
  static GeometryInfo fromWktLiteral(const std::string_view& wkt);

  // Create geometry info from a GeoPoint object.
  static GeometryInfo fromGeoPoint(const GeoPoint& point);
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_GEOMETRYINFO_H
