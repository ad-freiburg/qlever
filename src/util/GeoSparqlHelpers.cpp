// Copyright 2022, University of Freiburg,
// Author: Hannah Bast <bast@cs.uni-freiburg.de>
// Chair of Algorithms and Data Structures

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

#include "parser/GeoPoint.h"
#include "util/Exception.h"

namespace ad_utility {

namespace detail {

static constexpr auto wktPointRegex = ctll::fixed_string(
    "^\\s*[Pp][Oo][Ii][Nn][Tt]\\s*\\(\\s*"
    "(-?[0-9]+|-?[0-9]+\\.[0-9]+)"
    "\\s+"
    "(-?[0-9+]|-?[0-9]+\\.[0-9]+)"
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

}  // namespace detail

}  // namespace ad_utility
