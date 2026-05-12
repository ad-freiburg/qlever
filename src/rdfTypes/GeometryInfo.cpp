// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "rdfTypes/GeometryInfo.h"

#include <cmath>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/Exception.h"
#include "util/Log.h"

namespace ad_utility {

// ____________________________________________________________________________
GeometryInfo::GeometryInfo(uint8_t wktType, const BoundingBox& boundingBox,
                           Centroid centroid, NumGeometries numGeometries,
                           MetricLength metricLength, MetricArea metricArea)
    : boundingBox_{boundingBox.lowerLeft().toBitRepresentation(),
                   boundingBox.upperRight().toBitRepresentation()},
      numGeometries_{numGeometries.numGeometries()},
      metricLength_{metricLength},
      metricArea_{metricArea} {
  // The WktType only has 8 different values and we have 4 unused bits for the
  // ValueId datatype of the centroid (it is always a point). Therefore we fold
  // the attributes together. On OSM planet this will save approx. 1 GiB in
  // index size.
  AD_CORRECTNESS_CHECK(
      wktType <= 7 && wktType < (1 << ValueId::numDatatypeBits) - 1,
      "WKT Type out of range");
  AD_CORRECTNESS_CHECK(wktType > 0, "WKT Type indicates invalid geometry");
  uint64_t typeBits = static_cast<uint64_t>(wktType) << ValueId::numDataBits;
  uint64_t centroidBits = centroid.centroid().toBitRepresentation();
  AD_CORRECTNESS_CHECK((centroidBits & bitMaskGeometryType) == 0,
                       "Centroid bit representation exceeds available bits.");
  geometryTypeAndCentroid_ = typeBits | centroidBits;

  AD_CORRECTNESS_CHECK(
      boundingBox.lowerLeft().getLat() <= boundingBox.upperRight().getLat() &&
          boundingBox.lowerLeft().getLng() <= boundingBox.upperRight().getLng(),
      "Bounding box coordinates invalid: first point must be lower "
      "left and second point must be upper right of a rectangle.");

  AD_CORRECTNESS_CHECK(numGeometries_ > 0,
                       "Number of geometries must be strictly positive.");
};

// ____________________________________________________________________________
std::optional<GeometryInfo> GeometryInfo::fromWktLiteral(std::string_view wkt) {
  // Parse WKT and compute info
  using namespace detail;
  auto [type, parsed] = parseWkt(wkt);
  if (!parsed.has_value()) {
    return std::nullopt;
  }

  auto boundingBox = boundingBoxAsGeoPoints(parsed.value());
  auto centroid = centroidAsGeoPoint(parsed.value());
  auto numGeom = countChildGeometries(parsed.value());
  auto metricLength = computeMetricLength(parsed.value());
  if (!boundingBox.has_value() || !centroid.has_value()) {
    AD_LOG_DEBUG << "The WKT string `" << wkt
                 << "` would lead to an invalid centroid or bounding box. It "
                    "will thus be treated as an invalid WKT literal."
                 << std::endl;
    return std::nullopt;
  }

  double area = std::numeric_limits<double>::quiet_NaN();
  try {
    area = computeMetricArea(parsed.value());
  } catch (const InvalidPolygonError&) {
    AD_LOG_DEBUG << "Could not compute area of WKT literal `" << wkt << "`."
                 << std::endl;
  }

  return GeometryInfo{type,      boundingBox.value(), centroid.value(),
                      {numGeom}, metricLength,        MetricArea{area}};
}

// ____________________________________________________________________________
GeometryType::GeometryType(uint8_t type) : type_{type} {};

// ____________________________________________________________________________
MetricLength::MetricLength(double length) : length_{length} {
  AD_CORRECTNESS_CHECK(length_ >= 0, "Metric length must be positive");
};

// ____________________________________________________________________________
std::optional<GeometryType> GeometryInfo::getWktType(std::string_view wkt) {
  auto wktType =
      static_cast<uint8_t>(detail::getWKTType(detail::removeDatatype(wkt)));
  if (wktType == 0) {
    // Type 0 represents invalid type
    return std::nullopt;
  }
  return GeometryType{wktType};
};

// ____________________________________________________________________________
GeometryInfo GeometryInfo::fromGeoPoint(const GeoPoint& point) {
  return {
      util::geo::WKTType::POINT, {point, point},  Centroid{point}, {1},
      MetricLength{0.0},         MetricArea{0.0},
  };
}

// ____________________________________________________________________________
GeometryType GeometryInfo::getWktType() const {
  return GeometryType{
      static_cast<uint8_t>((geometryTypeAndCentroid_ & bitMaskGeometryType) >>
                           ValueId::numDataBits)};
}

// ____________________________________________________________________________
std::optional<std::string_view> GeometryType::asIri() const {
  return detail::wktTypeToIri(type_);
}

// ____________________________________________________________________________
Centroid GeometryInfo::getCentroid() const {
  return Centroid{GeoPoint::fromBitRepresentation(geometryTypeAndCentroid_ &
                                                  bitMaskCentroid)};
}

// ____________________________________________________________________________
std::optional<Centroid> GeometryInfo::getCentroid(std::string_view wkt) {
  auto [type, parsed] = detail::parseWkt(wkt);
  if (!parsed.has_value()) {
    return std::nullopt;
  }
  return detail::centroidAsGeoPoint(parsed.value());
}

// ____________________________________________________________________________
BoundingBox GeometryInfo::getBoundingBox() const {
  return {GeoPoint::fromBitRepresentation(boundingBox_.lowerLeftEncoded_),
          GeoPoint::fromBitRepresentation(boundingBox_.upperRightEncoded_)};
}

// ____________________________________________________________________________
std::optional<BoundingBox> GeometryInfo::getBoundingBox(std::string_view wkt) {
  auto [type, parsed] = detail::parseWkt(wkt);
  if (!parsed.has_value()) {
    return std::nullopt;
  }
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
MetricArea GeometryInfo::getMetricArea() const { return metricArea_; }

// ____________________________________________________________________________
std::optional<MetricArea> GeometryInfo::getMetricArea(std::string_view wkt) {
  auto [type, parsed] = detail::parseWkt(wkt);
  if (!parsed.has_value()) {
    return std::nullopt;
  }
  try {
    return MetricArea{detail::computeMetricArea(parsed.value())};
  } catch (const InvalidPolygonError&) {
    return std::nullopt;
  }
}

// ____________________________________________________________________________
std::string BoundingBox::asWkt() const {
  return detail::boundingBoxAsWkt(lowerLeft_, upperRight_);
}

// ____________________________________________________________________________
MetricLength GeometryInfo::getMetricLength() const { return metricLength_; };

// ____________________________________________________________________________
std::optional<MetricLength> GeometryInfo::getMetricLength(
    const std::string_view& wkt) {
  auto [type, parsed] = detail::parseWkt(wkt);
  if (!parsed.has_value()) {
    return std::nullopt;
  }
  return {detail::computeMetricLength(parsed.value())};
};

// ____________________________________________________________________________
MetricArea::MetricArea(double area) : area_{area} {
  AD_CORRECTNESS_CHECK(area >= 0 || std::isnan(area),
                       "Metric area must be positive");
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
NumGeometries GeometryInfo::getNumGeometries() const {
  return {numGeometries_};
}

// ____________________________________________________________________________
std::optional<NumGeometries> GeometryInfo::getNumGeometries(
    std::string_view wkt) {
  auto [type, parsed] = detail::parseWkt(wkt);
  if (!parsed.has_value()) {
    return std::nullopt;
  }
  return NumGeometries{detail::countChildGeometries(parsed.value())};
}

// ____________________________________________________________________________
CPP_template_def(typename RequestedInfo)(requires RequestedInfoT<RequestedInfo>)
    RequestedInfo GeometryInfo::getRequestedInfo() const {
  if constexpr (std::is_same_v<RequestedInfo, GeometryInfo>) {
    return *this;
  } else if constexpr (std::is_same_v<RequestedInfo, Centroid>) {
    return getCentroid();
  } else if constexpr (std::is_same_v<RequestedInfo, BoundingBox>) {
    return getBoundingBox();
  } else if constexpr (std::is_same_v<RequestedInfo, GeometryType>) {
    return getWktType();
  } else if constexpr (std::is_same_v<RequestedInfo, NumGeometries>) {
    return getNumGeometries();
  } else if constexpr (std::is_same_v<RequestedInfo, MetricLength>) {
    return getMetricLength();
  } else if constexpr (std::is_same_v<RequestedInfo, MetricArea>) {
    return getMetricArea();
  } else {
    static_assert(ad_utility::alwaysFalse<RequestedInfo>);
  }
};

// Explicit instantiations
template GeometryInfo GeometryInfo::getRequestedInfo<GeometryInfo>() const;
template Centroid GeometryInfo::getRequestedInfo<Centroid>() const;
template BoundingBox GeometryInfo::getRequestedInfo<BoundingBox>() const;
template GeometryType GeometryInfo::getRequestedInfo<GeometryType>() const;
template NumGeometries GeometryInfo::getRequestedInfo<NumGeometries>() const;
template MetricLength GeometryInfo::getRequestedInfo<MetricLength>() const;
template MetricArea GeometryInfo::getRequestedInfo<MetricArea>() const;

// ____________________________________________________________________________
CPP_template_def(typename RequestedInfo)(requires RequestedInfoT<RequestedInfo>)
    std::optional<RequestedInfo> GeometryInfo::getRequestedInfo(
        std::string_view wkt) {
  if constexpr (std::is_same_v<RequestedInfo, GeometryInfo>) {
    return GeometryInfo::fromWktLiteral(wkt);
  } else if constexpr (std::is_same_v<RequestedInfo, Centroid>) {
    return GeometryInfo::getCentroid(wkt);
  } else if constexpr (std::is_same_v<RequestedInfo, BoundingBox>) {
    return GeometryInfo::getBoundingBox(wkt);
  } else if constexpr (std::is_same_v<RequestedInfo, GeometryType>) {
    return GeometryInfo::getWktType(wkt);
  } else if constexpr (std::is_same_v<RequestedInfo, NumGeometries>) {
    return GeometryInfo::getNumGeometries(wkt);
  } else if constexpr (std::is_same_v<RequestedInfo, MetricLength>) {
    return GeometryInfo::getMetricLength(wkt);
  } else if constexpr (std::is_same_v<RequestedInfo, MetricArea>) {
    return GeometryInfo::getMetricArea(wkt);
  } else {
    static_assert(ad_utility::alwaysFalse<RequestedInfo>);
  }
};

// Explicit instantiations
template std::optional<GeometryInfo>
GeometryInfo::getRequestedInfo<GeometryInfo>(std::string_view wkt);
template std::optional<Centroid> GeometryInfo::getRequestedInfo<Centroid>(
    std::string_view wkt);
template std::optional<BoundingBox> GeometryInfo::getRequestedInfo<BoundingBox>(
    std::string_view wkt);
template std::optional<GeometryType>
GeometryInfo::getRequestedInfo<GeometryType>(std::string_view wkt);
template std::optional<NumGeometries>
GeometryInfo::getRequestedInfo<NumGeometries>(std::string_view wkt);
template std::optional<MetricLength>
GeometryInfo::getRequestedInfo<MetricLength>(std::string_view wkt);
template std::optional<MetricArea> GeometryInfo::getRequestedInfo<MetricArea>(
    std::string_view wkt);

}  // namespace ad_utility
