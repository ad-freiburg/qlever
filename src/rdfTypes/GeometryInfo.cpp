// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "rdfTypes/GeometryInfo.h"

#include <cstdint>
#include <type_traits>

#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/Exception.h"

namespace ad_utility {

// ____________________________________________________________________________
GeometryInfo::GeometryInfo(uint8_t wktType, const BoundingBox& boundingBox,
                           Centroid centroid, MetricLength metricLength)
    : boundingBox_{boundingBox.lowerLeft().toBitRepresentation(),
                   boundingBox.upperRight().toBitRepresentation()},
      metricLength_{metricLength.length()} {
  // The WktType only has 8 different values and we have 4 unused bits for the
  // ValueId datatype of the centroid (it is always a point). Therefore we fold
  // the attributes together. On OSM planet this will save approx. 1 GiB in
  // index size.
  AD_CORRECTNESS_CHECK(
      wktType <= 7 && wktType < (1 << ValueId::numDatatypeBits) - 1,
      "WKT Type out of range");
  uint64_t typeBits = static_cast<uint64_t>(wktType) << ValueId::numDataBits;
  uint64_t centroidBits = centroid.centroid().toBitRepresentation();
  AD_CORRECTNESS_CHECK((centroidBits & bitMaskGeometryType) == 0,
                       "Centroid bit representation exceeds available bits.");
  geometryTypeAndCentroid_ = typeBits | centroidBits;
};

// ____________________________________________________________________________
GeometryInfo GeometryInfo::fromWktLiteral(const std::string_view& wkt) {
  // Parse WKT and compute info
  using namespace detail;
  auto [type, parsed] = parseWkt(wkt);
  AD_CORRECTNESS_CHECK(parsed.has_value());
  return {type, boundingBoxAsGeoPoints(parsed.value()),
          centroidAsGeoPoint(parsed.value()),
          computeMetricLength(parsed.value())};
}

// ____________________________________________________________________________
GeometryType::GeometryType(uint8_t type) : type_{type} {};

// ____________________________________________________________________________
MetricLength::MetricLength(double length) : length_{length} {
  AD_CORRECTNESS_CHECK(length_ >= 0, "Metric length must be positive");
};

// ____________________________________________________________________________
GeometryType GeometryInfo::getWktType(const std::string_view& wkt) {
  return static_cast<uint8_t>(detail::getWKTType(detail::removeDatatype(wkt)));
};

// ____________________________________________________________________________
GeometryInfo GeometryInfo::fromGeoPoint(const GeoPoint& point) {
  return {util::geo::WKTType::POINT, {point, point}, point, {0.0}};
}

// ____________________________________________________________________________
GeometryType GeometryInfo::getWktType() const {
  return static_cast<uint8_t>(
      (geometryTypeAndCentroid_ & bitMaskGeometryType) >> ValueId::numDataBits);
}

// ____________________________________________________________________________
std::optional<std::string_view> GeometryType::asIri() const {
  return detail::wktTypeToIri(type_);
}

// ____________________________________________________________________________
Centroid GeometryInfo::getCentroid() const {
  return {GeoPoint::fromBitRepresentation(geometryTypeAndCentroid_ &
                                          bitMaskCentroid)};
}

// ____________________________________________________________________________
Centroid GeometryInfo::getCentroid(const std::string_view& wkt) {
  auto [type, parsed] = detail::parseWkt(wkt);
  AD_CORRECTNESS_CHECK(parsed.has_value());
  return detail::centroidAsGeoPoint(parsed.value());
}

// ____________________________________________________________________________
BoundingBox GeometryInfo::getBoundingBox() const {
  return {GeoPoint::fromBitRepresentation(boundingBox_.first),
          GeoPoint::fromBitRepresentation(boundingBox_.second)};
}

// ____________________________________________________________________________
BoundingBox GeometryInfo::getBoundingBox(const std::string_view& wkt) {
  auto [type, parsed] = detail::parseWkt(wkt);
  AD_CORRECTNESS_CHECK(parsed.has_value());
  return detail::boundingBoxAsGeoPoints(parsed.value());
}

// ____________________________________________________________________________
BoundingBox::BoundingBox(GeoPoint lowerLeft, GeoPoint upperRight)
    : lowerLeft_{lowerLeft}, upperRight_{upperRight} {
  AD_CORRECTNESS_CHECK(
      lowerLeft.getLat() <= upperRight.getLat() &&
          lowerLeft.getLng() <= upperRight.getLng(),
      "Bounding box coordinates invalid: first point must be lower "
      "left and second point must be upper right of a rectangle.");
};

// ____________________________________________________________________________
std::string BoundingBox::asWkt() const {
  return detail::boundingBoxAsWkt(lowerLeft_, upperRight_);
}

// ____________________________________________________________________________
MetricLength GeometryInfo::getMetricLength() const { return {metricLength_}; };

// ____________________________________________________________________________
MetricLength GeometryInfo::getMetricLength(const std::string_view& wkt) {
  auto [type, parsed] = detail::parseWkt(wkt);
  AD_CORRECTNESS_CHECK(parsed.has_value());
  return {detail::computeMetricLength(parsed.value())};
};

// ____________________________________________________________________________
template <typename RequestedInfo>
requires RequestedInfoT<RequestedInfo>
RequestedInfo GeometryInfo::getRequestedInfo() const {
  if constexpr (std::is_same_v<RequestedInfo, GeometryInfo>) {
    return *this;
  } else if constexpr (std::is_same_v<RequestedInfo, Centroid>) {
    return getCentroid();
  } else if constexpr (std::is_same_v<RequestedInfo, BoundingBox>) {
    return getBoundingBox();
  } else if constexpr (std::is_same_v<RequestedInfo, GeometryType>) {
    return getWktType();
  } else if constexpr (std::is_same_v<RequestedInfo, MetricLength>) {
    return getMetricLength();
  } else {
    static_assert(ad_utility::alwaysFalse<RequestedInfo>);
  }
};

// Explicit instantiations
template GeometryInfo GeometryInfo::getRequestedInfo<GeometryInfo>() const;
template Centroid GeometryInfo::getRequestedInfo<Centroid>() const;
template BoundingBox GeometryInfo::getRequestedInfo<BoundingBox>() const;
template GeometryType GeometryInfo::getRequestedInfo<GeometryType>() const;
template MetricLength GeometryInfo::getRequestedInfo<MetricLength>() const;

// ____________________________________________________________________________
template <typename RequestedInfo>
requires RequestedInfoT<RequestedInfo>
RequestedInfo GeometryInfo::getRequestedInfo(const std::string_view& wkt) {
  if constexpr (std::is_same_v<RequestedInfo, GeometryInfo>) {
    return GeometryInfo::fromWktLiteral(wkt);
  } else if constexpr (std::is_same_v<RequestedInfo, Centroid>) {
    return GeometryInfo::getCentroid(wkt);
  } else if constexpr (std::is_same_v<RequestedInfo, BoundingBox>) {
    return GeometryInfo::getBoundingBox(wkt);
  } else if constexpr (std::is_same_v<RequestedInfo, GeometryType>) {
    return GeometryInfo::getWktType(wkt);
  } else if constexpr (std::is_same_v<RequestedInfo, MetricLength>) {
    return GeometryInfo::getMetricLength(wkt);
  } else {
    static_assert(ad_utility::alwaysFalse<RequestedInfo>);
  }
};

// Explicit instantiations
template GeometryInfo GeometryInfo::getRequestedInfo<GeometryInfo>(
    const std::string_view& wkt);
template Centroid GeometryInfo::getRequestedInfo<Centroid>(
    const std::string_view& wkt);
template BoundingBox GeometryInfo::getRequestedInfo<BoundingBox>(
    const std::string_view& wkt);
template GeometryType GeometryInfo::getRequestedInfo<GeometryType>(
    const std::string_view& wkt);
template MetricLength GeometryInfo::getRequestedInfo<MetricLength>(
    const std::string_view& wkt);

}  // namespace ad_utility
