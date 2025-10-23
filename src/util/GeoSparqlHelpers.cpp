// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>,
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "./GeoSparqlHelpers.h"

#include <absl/strings/charconv.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>
#include <s2/util/units/length-units.h>

#include <cmath>
#include <ctre-unicode.hpp>
#include <limits>
#include <numbers>
#include <string_view>

#include "global/Constants.h"
#include "rdfTypes/GeoPoint.h"
#include "util/Exception.h"

namespace ad_utility {

namespace detail {

static constexpr auto wktPointRegex = ctll::fixed_string(
    "^\\s*[Pp][Oo][Ii][Nn][Tt]\\s*\\(\\s*"
    "(-?[0-9]+|-?[0-9]+\\.[0-9]+)"
    "\\s+"
    "(-?[0-9]+|-?[0-9]+\\.[0-9]+)"
    "\\s*\\)\\s*$");

// Parse a single WKT point and returns a pair of longitude and latitude. If
// the given string does not parse as a WKT point, a pair of `invalidCoordinate`
// is returned.
std::pair<double, double> parseWktPoint(const std::string_view point) {
  double lng = invalidCoordinate, lat = invalidCoordinate;
  if (auto match = ctre::search<wktPointRegex>(point)) {
    std::string_view lng_sv = match.get<1>();
    std::string_view lat_sv = match.get<2>();
    absl::from_chars(lng_sv.data(), lng_sv.data() + lng_sv.size(), lng);
    absl::from_chars(lat_sv.data(), lat_sv.data() + lat_sv.size(), lat);
    // This should never happen: if the regex matches, then each of the two
    // coordinate strings should also parse to a double.
    AD_CORRECTNESS_CHECK(!std::isnan(lng));
    AD_CORRECTNESS_CHECK(!std::isnan(lat));
  }
  return std::pair(lng, lat);
}

// Compute distance (in km) between two WKT points.
double wktDistImpl(GeoPoint point1, GeoPoint point2) {
  auto p1 = S2Point{S2LatLng::FromDegrees(point1.getLat(), point1.getLng())};
  auto p2 = S2Point{S2LatLng::FromDegrees(point2.getLat(), point2.getLng())};
  return S2Earth::ToKm(S1Angle(p1, p2));
}

// ____________________________________________________________________________
double kilometerToUnit(double kilometers,
                       std::optional<UnitOfMeasurement> unit) {
  using enum UnitOfMeasurement;
  auto multiplicator = [&unit]() {
    if (!unit.has_value()) {
      return 1.0;
    }
    switch (unit.value()) {
      case METERS:
        return 1000.0;
      case KILOMETERS:
        return 1.0;
      case MILES:
        return kilometerToMile;
      default:
        AD_CORRECTNESS_CHECK(!isLengthUnit(unit.value()));
        AD_THROW("Unsupported unit of measurement for distance.");
    }
  };
  return multiplicator() * kilometers;
}

// ____________________________________________________________________________
double valueInUnitToKilometer(double valueInUnit,
                              std::optional<UnitOfMeasurement> unit) {
  return valueInUnit / kilometerToUnit(1.0, unit);
}

// ____________________________________________________________________________
double squareMeterToUnit(double squareMeters,
                         std::optional<UnitOfMeasurement> unit) {
  using enum UnitOfMeasurement;
  auto multiplicator = [&unit]() {
    if (!unit.has_value()) {
      return 1.0;
    }
    switch (unit.value()) {
      case SQUARE_METERS:
        return 1.0;
      case SQUARE_KILOMETERS:
        return 1.0E-6;
      case SQUARE_MILES:
        return squareMeterToSquareMile;
      default:
        AD_CORRECTNESS_CHECK(!isAreaUnit(unit.value()));
        AD_THROW("Unsupported unit of measurement for area.");
    }
  };
  return multiplicator() * squareMeters;
}

// ____________________________________________________________________________
UnitOfMeasurement iriToUnitOfMeasurement(const std::string_view& iri) {
  using enum UnitOfMeasurement;
  if (iri == UNIT_METER_IRI) {
    return METERS;
  } else if (iri == UNIT_KILOMETER_IRI) {
    return KILOMETERS;
  } else if (iri == UNIT_MILE_IRI) {
    return MILES;
  } else if (iri == UNIT_SQUARE_METER_IRI) {
    return SQUARE_METERS;
  } else if (iri == UNIT_SQUARE_KILOMETER_IRI) {
    return SQUARE_KILOMETERS;
  } else if (iri == UNIT_SQUARE_MILE_IRI) {
    return SQUARE_MILES;
  }
  return UNKNOWN;
}

// ____________________________________________________________________________
bool isLengthUnit(UnitOfMeasurement unit) {
  using enum UnitOfMeasurement;
  return unit == METERS || unit == KILOMETERS || unit == MILES;
}

// ____________________________________________________________________________
bool isAreaUnit(UnitOfMeasurement unit) {
  using enum UnitOfMeasurement;
  return unit == SQUARE_METERS || unit == SQUARE_KILOMETERS ||
         unit == SQUARE_MILES;
}

}  // namespace detail

}  // namespace ad_utility
