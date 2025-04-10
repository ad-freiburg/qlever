// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_GEOMETRYINFO_H
#define QLEVER_SRC_UTIL_GEOMETRYINFO_H

#include <cstdint>
#include <cstdio>
#include <string>

namespace ad_utility {

// A geometry info object holds precomputed details on WKT literals.
struct GeometryInfo {
  // TODO<ullingerc> Computation of GeometryInfo is a separate PR
  uint64_t dummy_;

  // Parse a WKT literal and precompute all attributes.
  static GeometryInfo fromWktLiteral(const std::string_view& wkt);
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_GEOMETRYINFO_H
