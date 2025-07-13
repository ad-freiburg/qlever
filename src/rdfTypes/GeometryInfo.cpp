// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "rdfTypes/GeometryInfo.h"

#include <cstdint>

#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/Exception.h"

namespace ad_utility {

// ____________________________________________________________________________
GeometryInfo::GeometryInfo(uint8_t wktType, const BoundingBox& boundingBox,
                           Centroid centroid)
    : boundingBox_{boundingBox.lowerLeft_.toBitRepresentation(),
                   boundingBox.upperRight_.toBitRepresentation()} {
  // The WktType only has 8 different values and we have 4 unused bits for the
  // ValueId datatype of the centroid (it is always a point). Therefore we fold
  // the attributes together. On OSM planet this will save approx. 1 GiB in
  // index size.
  AD_CORRECTNESS_CHECK(wktType < (1 << ValueId::numDatatypeBits) - 1,
                       "WKT Type out of range");
  AD_CORRECTNESS_CHECK(wktType > 0, "WKT Type indicates invalid geometry");
  uint64_t typeBits = static_cast<uint64_t>(wktType) << ValueId::numDataBits;
  uint64_t centroidBits = centroid.centroid_.toBitRepresentation();
  AD_CORRECTNESS_CHECK((centroidBits & bitMaskGeometryType) == 0,
                       "Centroid bit representation exceeds available bits.");
  geometryTypeAndCentroid_ = typeBits | centroidBits;

  AD_CORRECTNESS_CHECK(
      boundingBox.lowerLeft_.getLat() <= boundingBox.upperRight_.getLat() &&
          boundingBox.lowerLeft_.getLng() <= boundingBox.upperRight_.getLng(),
      "Bounding box coordinates invalid: first point must be lower "
      "left and second point must be upper right of a rectangle.");
};

// ____________________________________________________________________________
std::optional<GeometryInfo> GeometryInfo::fromWktLiteral(std::string_view wkt) {
  // Parse WKT and compute info
  using namespace detail;
  auto [type, parsed] = parseWkt(wkt);
  if (!parsed.has_value()) {
    return std::nullopt;
  }
  return GeometryInfo{type, boundingBoxAsGeoPoints(parsed.value()),
                      centroidAsGeoPoint(parsed.value())};
}

// ____________________________________________________________________________
GeometryType GeometryInfo::getWktType(std::string_view wkt) {
  return static_cast<uint8_t>(detail::getWKTType(detail::removeDatatype(wkt)));
};

// ____________________________________________________________________________
GeometryInfo GeometryInfo::fromGeoPoint(const GeoPoint& point) {
  return {util::geo::WKTType::POINT, {point, point}, point};
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
Centroid GeometryInfo::getCentroid(std::string_view wkt) {
  auto [type, parsed] = detail::parseWkt(wkt);
  AD_CORRECTNESS_CHECK(parsed.has_value());
  return detail::centroidAsGeoPoint(parsed.value());
}

// ____________________________________________________________________________
BoundingBox GeometryInfo::getBoundingBox() const {
  return {GeoPoint::fromBitRepresentation(boundingBox_.lowerLeftEncoded_),
          GeoPoint::fromBitRepresentation(boundingBox_.upperRightEncoded_)};
}

// ____________________________________________________________________________
BoundingBox GeometryInfo::getBoundingBox(std::string_view wkt) {
  auto [type, parsed] = detail::parseWkt(wkt);
  AD_CORRECTNESS_CHECK(parsed.has_value());
  return detail::boundingBoxAsGeoPoints(parsed.value());
}

// ____________________________________________________________________________
std::string BoundingBox::asWkt() const {
  return detail::boundingBoxAsWkt(lowerLeft_, upperRight_);
}

// ____________________________________________________________________________
template <BoundingCoordinate RequestedCoordinate>
double BoundingBox::getBoundingCoordinate() const {
  using enum BoundingCoordinate;
  if constexpr (RequestedCoordinate == MIN_X) {
    return lowerLeft_.getLng();
  } else if constexpr (RequestedCoordinate == MIN_Y) {
    return lowerLeft_.getLat();
  } else if constexpr (RequestedCoordinate == MAX_X) {
    return upperRight_.getLng();
  } else if constexpr (RequestedCoordinate == MAX_Y) {
    return upperRight_.getLat();
  } else {
    // Unfortunately, we cannot use a `static_assert` here because some compiler
    // versions don't like it.
    AD_FAIL();
  }
};

// Explicit instantiations
template double BoundingBox::getBoundingCoordinate<BoundingCoordinate::MIN_X>()
    const;
template double BoundingBox::getBoundingCoordinate<BoundingCoordinate::MIN_Y>()
    const;
template double BoundingBox::getBoundingCoordinate<BoundingCoordinate::MAX_X>()
    const;
template double BoundingBox::getBoundingCoordinate<BoundingCoordinate::MAX_Y>()
    const;

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
  } else {
    static_assert(ad_utility::alwaysFalse<RequestedInfo>);
  }
};

// Explicit instantiations
template GeometryInfo GeometryInfo::getRequestedInfo<GeometryInfo>() const;
template Centroid GeometryInfo::getRequestedInfo<Centroid>() const;
template BoundingBox GeometryInfo::getRequestedInfo<BoundingBox>() const;
template GeometryType GeometryInfo::getRequestedInfo<GeometryType>() const;

// ____________________________________________________________________________
template <typename RequestedInfo>
requires RequestedInfoT<RequestedInfo>
RequestedInfo GeometryInfo::getRequestedInfo(std::string_view wkt) {
  if constexpr (std::is_same_v<RequestedInfo, GeometryInfo>) {
    auto optionalGeoInfo = GeometryInfo::fromWktLiteral(wkt);
    AD_CORRECTNESS_CHECK(optionalGeoInfo.has_value());
    return optionalGeoInfo.value();
  } else if constexpr (std::is_same_v<RequestedInfo, Centroid>) {
    return GeometryInfo::getCentroid(wkt);
  } else if constexpr (std::is_same_v<RequestedInfo, BoundingBox>) {
    return GeometryInfo::getBoundingBox(wkt);
  } else if constexpr (std::is_same_v<RequestedInfo, GeometryType>) {
    return GeometryInfo::getWktType(wkt);
  } else {
    static_assert(ad_utility::alwaysFalse<RequestedInfo>);
  }
};

// Explicit instantiations
template GeometryInfo GeometryInfo::getRequestedInfo<GeometryInfo>(
    std::string_view wkt);
template Centroid GeometryInfo::getRequestedInfo<Centroid>(
    std::string_view wkt);
template BoundingBox GeometryInfo::getRequestedInfo<BoundingBox>(
    std::string_view wkt);
template GeometryType GeometryInfo::getRequestedInfo<GeometryType>(
    std::string_view wkt);

}  // namespace ad_utility
