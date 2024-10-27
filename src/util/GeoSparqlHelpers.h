// Copyright 2022, University of Freiburg,
// Author: Hannah Bast <bast@cs.uni-freiburg.de>
// Chair of Algorithms and Data Structures

#ifndef QLEVER_GEOSPARQLHELPERS_H
#define QLEVER_GEOSPARQLHELPERS_H

#include <ctre-unicode.hpp>
#include <limits>
#include <optional>
#include <string>

#include "parser/GeoPoint.h"

namespace ad_utility {

namespace detail {

static constexpr double invalidCoordinate =
    std::numeric_limits<double>::quiet_NaN();

// Implementations of the lambdas below + two helper functions. Note: our SPARQL
// expression code currently needs lambdas, and we can't define the lambdas in
// the .cpp file, hence this detour.
// TODO: Make the SPARQL expressions work for function pointers or
// std::function.
std::pair<double, double> parseWktPoint(const std::string_view point);

double wktDistImpl(GeoPoint point1, GeoPoint point2);

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

// Compute the distance in km between two WKT points.
class WktDistGeoPoints {
 public:
  double operator()(const std::optional<GeoPoint>& point1,
                    const std::optional<GeoPoint>& point2) const {
    if (!point1.has_value() || !point2.has_value()) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    return detail::wktDistImpl(point1.value(), point2.value());
  }
};

}  // namespace ad_utility

#endif  // QLEVER_GEOSPARQLHELPERS_H
