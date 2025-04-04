// Copyright 2022, University of Freiburg,
// Author: Hannah Bast <bast@cs.uni-freiburg.de>
// Chair of Algorithms and Data Structures

#ifndef QLEVER_GEOSPARQLHELPERS_H
#define QLEVER_GEOSPARQLHELPERS_H

#include <absl/strings/str_cat.h>

#include <limits>
#include <optional>
#include <string_view>

#include "parser/GeoPoint.h"
#include "parser/NormalizedString.h"

namespace ad_utility {

static constexpr std::string_view UNIT_METER_IRI =
    "http://qudt.org/vocab/unit/M";

static constexpr std::string_view UNIT_KILOMETER_IRI =
    "http://qudt.org/vocab/unit/KiloM";

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

// Compute the distance between two WKT points.
class WktDistGeoPoints {
 public:
  double operator()(const std::optional<GeoPoint>& point1,
                    const std::optional<GeoPoint>& point2,
                    const std::optional<ad_utility::triple_component::Iri>&
                        unit = std::nullopt) const {
    if (!point1.has_value() || !point2.has_value()) {
      return std::numeric_limits<double>::quiet_NaN();
    }

    double multiplicator = 1;
    if (unit.has_value()) {
      auto unitIri = asStringViewUnsafe(unit.value().getContent());
      if (unitIri == UNIT_METER_IRI) {
        multiplicator = 1000;
      } else if (unitIri == UNIT_KILOMETER_IRI) {
        multiplicator = 1;
      } else {
        AD_THROW(absl::StrCat("Unsupported distance unit: ", unitIri));
      }
    }

    return multiplicator * detail::wktDistImpl(point1.value(), point2.value());
  }
};

// Compute the distance between two WKT points in meters.
class WktMetricDistGeoPoints {
 public:
  double operator()(const std::optional<GeoPoint>& point1,
                    const std::optional<GeoPoint>& point2) const {
    return WktDistGeoPoints{}(
        point1, point2,
        ad_utility::triple_component::Iri::fromIrirefWithoutBrackets(
            UNIT_METER_IRI));
  }
};

}  // namespace ad_utility

#endif  // QLEVER_GEOSPARQLHELPERS_H
