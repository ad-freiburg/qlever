// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "rdfTypes/GeoRectangle.h"

#include <algorithm>
#include <cmath>

#include "rdfTypes/GeoPoint.h"
#include "util/Exception.h"

namespace ad_utility {

// ____________________________________________________________________________
GeoRectangle padGeoRectangle(const GeoRectangle& rectangle,
                             double distanceMeters) {
  AD_CONTRACT_CHECK(distanceMeters >= 0);
  // One degree of latitude is at least 110'567 m everywhere, so dividing by
  // 110'000 overestimates the padding.
  double dLat = distanceMeters / 110'000.0;
  double minLat = std::max(rectangle.minLat_ - dLat, -90.0);
  double maxLat = std::min(rectangle.maxLat_ + dLat, 90.0);
  // One degree of longitude at latitude x is at least 110'000 m * cos(x), so
  // using the largest latitude of the padded band overestimates the padding.
  double maxAbsLat = std::max(std::abs(minLat), std::abs(maxLat));
  double minLng = -180.0;
  double maxLng = 180.0;
  if (maxAbsLat < 89.0) {
    double dLng =
        distanceMeters / (110'000.0 * std::cos(maxAbsLat * M_PI / 180.0));
    minLng = rectangle.minLng_ - dLng;
    maxLng = rectangle.maxLng_ + dLng;
    if (minLng < -180.0 || maxLng > 180.0) {
      // The padded rectangle wraps around the antimeridian, which a
      // rectangle cannot represent; degrade to the full longitude range.
      minLng = -180.0;
      maxLng = 180.0;
    }
  }
  return {minLng, minLat, maxLng, maxLat};
}

// ____________________________________________________________________________
double geoRectangleSelectivity(const GeoRectangle& rectangle) {
  return std::clamp((rectangle.maxLng_ - rectangle.minLng_) / 360.0, 0.0, 1.0);
}

// ____________________________________________________________________________
bool GeoRectangleIdPrefilter::canBeSkipped(ValueId id) const {
  if (id.getDatatype() != Datatype::GeoPoint) {
    return false;
  }
  // The coordinates are quantized when encoded into the ID (see
  // `GeoPoint::toBitRepresentation`), so allow one quantization step of
  // slack in each direction.
  constexpr double eps = 360.0 / GeoPoint::maxCoordinateEncoded;
  auto point = id.getGeoPoint();
  return point.getLat() < rectangle_.minLat_ - eps ||
         point.getLat() > rectangle_.maxLat_ + eps ||
         point.getLng() < rectangle_.minLng_ - eps ||
         point.getLng() > rectangle_.maxLng_ + eps;
}

}  // namespace ad_utility
