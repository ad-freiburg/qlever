// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>,
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_GEOSPARQLHELPERS_H
#define QLEVER_GEOSPARQLHELPERS_H

#include <absl/strings/str_cat.h>

#include <limits>
#include <optional>
#include <string_view>

#include "global/Constants.h"
#include "parser/GeoPoint.h"
#include "parser/NormalizedString.h"

namespace ad_utility {

namespace detail {

static constexpr double invalidCoordinate =
    std::numeric_limits<double>::quiet_NaN();

static constexpr double kilometerToMile = 0.62137119;

// TODO: Make the SPARQL expressions work for function pointers or
// std::function.

// Extract coordinates from a well-known text literal.
std::pair<double, double> parseWktPoint(const std::string_view point);

// Calculate geographic distance between points in kilometers using s2geometry.
double wktDistImpl(GeoPoint point1, GeoPoint point2);

// Convert kilometers to other supported units.
double kilometerToUnit(double kilometers,
                       std::optional<UnitOfMeasurement> unit);

// Convert a unit IRI string (without quotes or brackets) to unit.
UnitOfMeasurement iriToUnitOfMeasurement(const std::string_view& uri);

}  // namespace detail

// Return the longitude coordinate from a WKT point.
class WktLongitude {
 public:
  double operator()(const std::optional<GeoPoint>& point) const {
    if (!point.has_value()) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    return point.value().getLng();
  }
};

// Return the latitude coordinate from a WKT point.
class WktLatitude {
 public:
  double operator()(const std::optional<GeoPoint>& point) const {
    if (!point.has_value()) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    return point.value().getLat();
  }
};

// Compute the distance between two WKT points.
class WktDistGeoPoints {
 public:
  double operator()(
      const std::optional<GeoPoint>& point1,
      const std::optional<GeoPoint>& point2,
      const std::optional<UnitOfMeasurement>& unit = std::nullopt) const {
    if (!point1.has_value() || !point2.has_value()) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    return detail::kilometerToUnit(
        detail::wktDistImpl(point1.value(), point2.value()), unit);
  }
};

// Compute the distance between two WKT points in meters.
class WktMetricDistGeoPoints {
 public:
  double operator()(const std::optional<GeoPoint>& point1,
                    const std::optional<GeoPoint>& point2) const {
    return WktDistGeoPoints{}(point1, point2, UnitOfMeasurement::METERS);
  }
};

}  // namespace ad_utility

#endif  // QLEVER_GEOSPARQLHELPERS_H
