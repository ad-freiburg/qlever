// Copyright 2021, University of Freiburg,
// Author: Hannah Bast <bast@cs.uni-freiburg.de>
// Chair of Algorithms and Data Structures

#ifndef QLEVER_GEOSPARQLHELPERS_H
#define QLEVER_GEOSPARQLHELPERS_H

#include <string>

// TODO: How can I declare the lambdas in the .h file and implement them in the
// .cpp file? The best I managed so far is define them with inline in the .f
// file. If I define them without inline in the .cpp file, I get a multiple
// definition error.
//
// namespace ad_utility {
//
// // Longitude of a WKT point (it's the first coordinate).
// std::function<double(const std::string& p)> wktLongitude;
//
// // Latitude of a WKT point (it's the second coordinate).
// std::function<double(const std::string& p)> wktLatitude;
//
// // Distance between two WKT points.
// std::function<double(const std::string& p1, const std::string& p2)> wktDist;
//
// }  // namespace ad_utility

// Helper function that checks whether a string looks like a WKT pointC (just
// checks the beginning and that it has the minimal length).
namespace {
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
  if (end == nullptr || *end != ')' || *(end + 1) != '"') return error_result;
  return std::pair(lat, lng);
}

}  // namespace

namespace ad_utility {

// _________________________________________________________________________
inline auto wktLongitude = [](const std::string& p) -> double {
  auto nan = std::numeric_limits<double>::quiet_NaN();
  if (!is_wkt_point(p)) return nan;
  const char* beg = p.c_str() + 7;
  char* end;
  double lng = std::strtod(beg, &end);
  return end != nullptr || *(end + 1) == ' ' ? lng : nan;
};

// _________________________________________________________________________
inline auto wktLatitude = [](const std::string& p) -> double {
  auto nan = std::numeric_limits<double>::quiet_NaN();
  if (!is_wkt_point(p)) return nan;
  const char* beg = p.c_str() + 7;
  while (*beg == ' ') ++beg;
  while (isdigit(*beg) || *beg == '.') ++beg;
  char* end;
  double lat = std::strtod(beg, &end);
  return end != nullptr || *(end + 1) == ')' ? lat : nan;
};

// _________________________________________________________________________
inline auto wktDist = [](const std::string& p1,
                         const std::string& p2) -> double {
  auto latlng1 = parse_wkt_point(p1);
  auto latlng2 = parse_wkt_point(p2);
  auto sqr = [](double x) { return x * x; };
  // Using the "ellipsoidal earth projected to a plane" formula from
  // https://en.wikipedia.org/wiki/Geographical_distance
  // A more precise way is the Haversine formula, which we save for when we
  // compute this at indexing time.
  auto m = M_PI / 180.0 * (latlng1.first + latlng2.first) / 2.0;
  // std::cout << "p1 = " << latlng1.first << " " << latlng1.second <<
  // std::endl; std::cout << "p2 = " << latlng2.first << " " << latlng2.second
  // << std::endl; std::cout << "m = " << m << std::endl;
  auto k1 = 111.13209 - 0.56605 * cos(2 * m) + 0.00120 * cos(4 * m);
  auto k2 = 111.41513 * cos(m) - 0.09455 * cos(3 * m) + 0.00012 * cos(5 * m);
  return sqrt(sqr(k1 * (latlng1.first - latlng2.first)) +
              sqr(k2 * (latlng1.second - latlng2.second)));
};

}  // namespace ad_utility

#endif  // QLEVER_GEOSPARQLHELPERS_H
