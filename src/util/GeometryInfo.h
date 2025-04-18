// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_GEOMETRYINFO_H
#define QLEVER_SRC_UTIL_GEOMETRYINFO_H

#include <cstdint>
#include <cstdio>
#include <string>

#include "global/ValueId.h"
#include "parser/GeoPoint.h"
#include "util/BitUtils.h"

namespace ad_utility {

// A geometry info object holds precomputed details on WKT literals.
// IMPORTANT: Every modification of the attributes of this class will be an
// index-breaking change. Please update the index version accordingly.
class GeometryInfo {
 private:
  std::pair<uint64_t, uint64_t> boundingBox_;  // TODO: Has 8 unused bits...
  uint64_t geometryTypeAndCentroid_;

  // double metricSize_;  // TODO: calc ; we only need length for lines and area
  // for polygons
  // std::optional<uint64_t> parsedVocabOffset_ = std::nullopt;

  static constexpr uint64_t bitMaskGeometryType =
      bitMaskForHigherBits(ValueId::numDatatypeBits);
  static constexpr uint64_t bitMaskCentroid =
      bitMaskForLowerBits(ValueId::numDataBits);

 public:
  GeometryInfo(uint8_t wktType, std::pair<GeoPoint, GeoPoint> boundingBox,
               GeoPoint centroid);

  // Parse an arbitrary WKT literal and compute all attributes.
  static GeometryInfo fromWktLiteral(const std::string_view& wkt);

  // Create geometry info for a GeoPoint object.
  static GeometryInfo fromGeoPoint(const GeoPoint& point);

  // Extract centroid from geometryTypeAndCentroid_ and convert it to GeoPoint.
  GeoPoint getCentroid() const;

  // Extract the WKT geometry type from geometryTypeAndCentroid_.
  uint8_t getWktType() const;

  // Convert the bounding box to GeoPoints.
  std::pair<GeoPoint, GeoPoint> getBoundingBox() const;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_GEOMETRYINFO_H
