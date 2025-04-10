// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_GEOMETRYINFO_H
#define QLEVER_SRC_UTIL_GEOMETRYINFO_H

#include <cstdint>
#include <cstdio>
#include <string>

// A geometry info object holds precomputed details on WKT literals.
struct GeometryInfo {
  uint8_t geometryType_;

  // TODO<ullingerc> Implement actual precomputation, f.ex.
  // WKTType type_;
  // std::pair<uint64_t, uint64_t> boundingBox_;
  // uint64_t centroid_;
  // double metricLength_;
  // double metricArea_;
  // uint64_t libspatialjoinOffset_;

  // Parse a WKT literal and precompute all attributes.
  static GeometryInfo fromWktLiteral(const std::string_view& wkt);
};

#endif  // QLEVER_SRC_UTIL_GEOMETRYINFO_H
