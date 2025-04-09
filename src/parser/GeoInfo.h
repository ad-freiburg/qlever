// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_GEOINFO_H
#define QLEVER_SRC_PARSER_GEOINFO_H

#include <cstdint>
#include <string>
#include <utility>

// TODO<ullingerc> Taken from libspatialjoin - reuse from there?
enum WKTType : uint8_t {
  NONE = 0,
  POINT = 1,
  LINESTRING = 2,
  POLYGON = 3,
  MULTIPOINT = 4,
  MULTILINESTRING = 5,
  MULTIPOLYGON = 6,
  COLLECTION = 7
};

// A geometry info object holds precomputed details on WKT literals.
struct GeometryInfo {
  uint64_t dummyAttribute_;

  // TODO<ullingerc> Implement actual precomputation, f.ex.
  // WKTType type_;
  // std::pair<uint64_t, uint64_t> boundingBox_;
  // uint64_t centroid_;
  // double metricLength_;
  // double metricArea_;
  // uint64_t libspatialjoinOffset_;

  // Parse a WKT literal and precompute all attributes.
  static GeometryInfo fromWktLiteral(
      [[maybe_unused]] const std::string_view& wkt) {
    // TODO<ullingerc> Replace this dummy with computation
    // TODO<ullingerc> How can we retrieve libspatialjoin offsets?
    return GeometryInfo{1};
  };
};

#endif  // QLEVER_SRC_PARSER_GEOINFO_H
