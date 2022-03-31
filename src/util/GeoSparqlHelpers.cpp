// Copyright 2022, University of Freiburg,
// Author: Hannah Bast <bast@cs.uni-freiburg.de>
// Chair of Algorithms and Data Structures

#include "./GeoSparqlHelpers.h"

#include <cmath>
#include <limits>
#include <numbers>
#include <string>

namespace {

// Helper function that checks whether a string looks like a WKT pointC (just
// checks the beginning and that it has the minimal length).
bool is_wkt_point(const std::string& p) {
  return p.size() >= 12 && p[0] == '"' && (p[1] == 'P' || p[1] == 'p') &&
         (p[2] == 'O' || p[2] == 'o') && (p[3] == 'I' || p[3] == 'i') &&
         (p[4] == 'N' || p[4] == 'n') && (p[5] == 'T' || p[5] == 't') &&
         p[6] == '(';
}

// Parse a single WKT Point of the form "POINT(<double> <double>)", where the
// quote are included, the POINT must be uppercase, there is an arbitrary
// amount of whitespace after the comma, and arbitrary contents after the
// final quote. If the string does not match this format, a pair of
// `quiet_NaN` is returned
std::pair<double, double> parse_wkt_point(const std::string& p) {
  auto error_result = std::pair(std::numeric_limits<double>::quiet_NaN(),
                                std::numeric_limits<double>::quiet_NaN());
  if (!is_wkt_point(p)) {
    return error_result;
  }
  const char* beg = p.c_str() + 7;
  char* end;
  double lng = std::strtod(beg, &end);
  if (end == nullptr) return error_result;
  beg = end + 1;
  double lat = std::strtod(beg, &end);
  // Note: When the first condition is false, `end` points to a part of the
  // string (possibly the terminating 0), so that `*end` is OK. When the second
  // condition is false, `end` points to `)`, so that end + 1 points to a part
  // of the string (possibly the terminating 0), so that `*(end + 1)` is OK.
  if (end == nullptr || *end != ')' || *(end + 1) != '"') return error_result;
  return std::pair(lat, lng);
}

}  // namespace

namespace detail {

// ___________________________________________________________________________
double wktLongitudeImpl(const std::string& p) {
  auto nan = std::numeric_limits<double>::quiet_NaN();
  if (!is_wkt_point(p)) return nan;
  const char* beg = p.c_str() + 7;
  char* end;
  double lng = std::strtod(beg, &end);
  return end != nullptr || *(end + 1) == ' ' ? lng : nan;
}

// ___________________________________________________________________________
double wktLatitudeImpl(const std::string& p) {
  auto nan = std::numeric_limits<double>::quiet_NaN();
  if (!is_wkt_point(p)) return nan;
  const char* beg = p.c_str() + 7;
  while (*beg == ' ') ++beg;
  if (*beg == '-') ++beg;
  while (isdigit(*beg) || *beg == '.') ++beg;
  char* end;
  double lat = std::strtod(beg, &end);
  return end != nullptr || *(end + 1) == ')' ? lat : nan;
}

// ___________________________________________________________________________
double wktDistImpl(const std::string& p1, const std::string& p2) {
  auto latlng1 = parse_wkt_point(p1);
  auto latlng2 = parse_wkt_point(p2);
  auto sqr = [](double x) { return x * x; };
  auto m = std::numbers::pi / 180.0 * (latlng1.first + latlng2.first) / 2.0;
  // std::cout << "p1 = " << latlng1.first << " " << latlng1.second <<
  // std::endl; std::cout << "p2 = " << latlng2.first << " " << latlng2.second
  // << std::endl; std::cout << "m = " << m << std::endl;
  auto k1 = 111.13209 - 0.56605 * cos(2 * m) + 0.00120 * cos(4 * m);
  auto k2 = 111.41513 * cos(m) - 0.09455 * cos(3 * m) + 0.00012 * cos(5 * m);
  return sqrt(sqr(k1 * (latlng1.first - latlng2.first)) +
              sqr(k2 * (latlng1.second - latlng2.second)));
}

}  // namespace detail
