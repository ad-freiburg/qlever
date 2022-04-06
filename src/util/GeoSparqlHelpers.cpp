// Copyright 2022, University of Freiburg,
// Author: Hannah Bast <bast@cs.uni-freiburg.de>
// Chair of Algorithms and Data Structures

#include "./GeoSparqlHelpers.h"

#include <absl/strings/charconv.h>

#include <cmath>
#include <limits>
#include <numbers>
#include <string>

namespace ad_utility {

namespace detail {

// Helper function that checks whether the given null-terminated string is a WKT
// point according to the official grammar. If yes, return pointer to first
// character after opening bracket, otherwise nullptr. Anything after the final
// quote is ignored for now.
const char* checkWktPoint(const char* s) {
  bool isPrefixOk =
      s[0] == '"' && (s[1] == 'P' || s[1] == 'p') &&
      (s[2] == 'O' || s[2] == 'o') && (s[3] == 'I' || s[3] == 'i') &&
      (s[4] == 'N' || s[4] == 'n') && (s[5] == 'T' || s[5] == 't');
  s += 6;
  if (!isPrefixOk) return nullptr;
  while (*s == ' ') ++s;
  if (*s++ != '(') return nullptr;
  while (*s == ' ') ++s;
  const char* beg = s;
  for (int i = 1; i <= 2; ++i) {
    while (*s == ' ') ++s;
    if (*s == '-') ++s;
    if (!(isdigit(*s++))) return nullptr;
    while (isdigit(*s)) ++s;
    if (*s == '.') {
      ++s;
      if (!(isdigit(*s++))) return nullptr;
      while (isdigit(*s)) ++s;
    }
  }
  while (*s == ' ') ++s;
  if (*s++ != ')') return nullptr;
  if (*s++ != '"') return nullptr;
  return beg;
}

// Helper function that parses a single WKT Point. If the parsing succeeds,
// returns a pair of latitude and longitude (note that it's the other way round
// in WKT). Otherwise, return a pair of `quiet_NaN`.
std::pair<double, double> parseWktPoint(const std::string& p) {
  auto error_result = std::pair(std::numeric_limits<double>::quiet_NaN(),
                                std::numeric_limits<double>::quiet_NaN());
  const char* beg = checkWktPoint(p.data());
  if (beg == nullptr) return error_result;
  const char* end = p.data() + p.size();
  double lng, lat;
  auto ret = absl::from_chars(beg, end, lng);
  if (ret.ec == std::errc::invalid_argument) return error_result;
  beg = ret.ptr + 1;
  while (*beg == ' ') ++beg;
  ret = absl::from_chars(beg, end, lat);
  if (ret.ec == std::errc::invalid_argument) return error_result;
  return std::pair(lat, lng);
}

// Parse longitude from WKT point.
double wktLongitudeImpl(const std::string& p) {
  auto nan = std::numeric_limits<double>::quiet_NaN();
  const char* beg = checkWktPoint(p.data());
  if (beg == nullptr) return nan;
  const char* end = p.data() + p.size();
  double lng;
  auto result = absl::from_chars(beg, end, lng);
  if (result.ec == std::errc::invalid_argument) return nan;
  return lng;
}

// Parse latitude from WKI point.
double wktLatitudeImpl(const std::string& p) {
  auto nan = std::numeric_limits<double>::quiet_NaN();
  const char* beg = checkWktPoint(p.data());
  if (beg == nullptr) return nan;
  const char* end = p.data() + p.size();
  while (*beg == ' ') ++beg;
  while (*beg != ' ') ++beg;
  while (*beg == ' ') ++beg;
  double lat;
  auto result = absl::from_chars(beg, end, lat);
  if (result.ec == std::errc::invalid_argument) return nan;
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
