// Copyright 2022, University of Freiburg,
// Author: Hannah Bast <bast@cs.uni-freiburg.de>
// Chair of Algorithms and Data Structures

#include "./GeoSparqlHelpers.h"

#include <absl/strings/charconv.h>
#include <ctre/ctre.h>

#include <cmath>
#include <limits>
#include <numbers>
#include <optional>
#include <string>
#include <string_view>

#include "./Exception.h"

namespace ad_utility {

namespace detail {

static constexpr auto wktPointRegex = ctll::fixed_string(
    "\"[Pp][Oo][Ii][Nn][Tt] *\\( *"
    "(-?[0-9]+|-?[0-9]+\\.[0-9]+) +(-?[0-9+]|-?[0-9]+\\.[0-9]+)"
    " *\\)\"");

static constexpr double invalidCoordinate =
    std::numeric_limits<double>::quiet_NaN();

// Parse a single WKT point and returns a pair of latitude and longitude (note
// that in the WKT point the order is reverse). If the given string does not
// parse as a WKT point, a pair of `invalidCoordinate` is returned.
std::pair<double, double> parseWktPoint(const std::string& wktPoint) {
  double lng = invalidCoordinate, lat = invalidCoordinate;
  if (auto match = ctre::match<wktPointRegex>(wktPoint)) {
    std::string_view lng_sv = match.get<1>();
    std::string_view lat_sv = match.get<2>();
    absl::from_chars(lng_sv.begin(), lng_sv.end() + lng_sv.size(), lng);
    absl::from_chars(lat_sv.begin(), lat_sv.end() + lat_sv.size(), lat);
    // This should never happen: if the regex matches, then each of the two
    // coordinate strings should also parse to a double.
    AD_CHECK(lng != invalidCoordinate);
    AD_CHECK(lat != invalidCoordinate);
  }
  return std::pair(lat, lng);
}

// Parse longitude from WKT point.
double wktLongitudeImpl(const std::string& wktPoint) {
  double lng = invalidCoordinate;
  if (auto match = ctre::match<wktPointRegex>(wktPoint)) {
    std::string_view lng_sv = match.get<1>();
    absl::from_chars(lng_sv.begin(), lng_sv.end() + lng_sv.size(), lng);
    AD_CHECK(lng != invalidCoordinate);
  }
  return lng;
}

// Parse latitude from WKI point.
double wktLatitudeImpl(const std::string& wktPoint) {
  double lat = invalidCoordinate;
  if (auto match = ctre::match<wktPointRegex>(wktPoint)) {
    std::string_view lat_sv = match.get<2>();
    absl::from_chars(lat_sv.begin(), lat_sv.end() + lat_sv.size(), lat);
    AD_CHECK(lat != invalidCoordinate);
  }
  return lat;
}

// Compute distance (in km) between two WKT points.
double wktDistImpl(const std::string& p1, const std::string& p2) {
  auto [lat1, lng1] = parseWktPoint(p1);
  auto [lat2, lng2] = parseWktPoint(p2);
  auto sqr = [](double x) { return x * x; };
  auto m = std::numbers::pi / 180.0 * (lat1 + lat2) / 2.0;
  auto k1 = 111.13209 - 0.56605 * cos(2 * m) + 0.00120 * cos(4 * m);
  auto k2 = 111.41513 * cos(m) - 0.09455 * cos(3 * m) + 0.00012 * cos(5 * m);
  return sqrt(sqr(k1 * (lat1 - lat2)) + sqr(k2 * (lng1 - lng2)));
}

}  // namespace detail

}  // namespace ad_utility
