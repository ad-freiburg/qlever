// Copyright 2022, University of Freiburg,
// Author: Hannah Bast <bast@cs.uni-freiburg.de>
// Chair of Algorithms and Data Structures

#ifndef QLEVER_GEOSPARQLHELPERS_H
#define QLEVER_GEOSPARQLHELPERS_H

#include <string>

namespace ad_utility {

namespace detail {

// Implementations of the lambdas below + two helper functions. Note: our SPARQL
// expression code currently needs lambdas, and we can't define the lambdas in
// the .cpp file, hence this detour.
// TODO: Make the SPARQL expressions work for function pointers or
// std::function.
const char* checkWktPoint(const char* s);
std::pair<double, double> parseWktPoint(const std::string& p);
double wktLongitudeImpl(const std::string& p);
double wktLatitudeImpl(const std::string& p);
double wktDistImpl(const std::string& p1, const std::string& p2);

}  // namespace detail

// Parse the longitude coordinate from a WKT point (it's the first coordinate).
inline auto wktLongitude = [](const std::string& p) -> double {
  return detail::wktLongitudeImpl(p);
};

// Parse the latitute coordinate from a WKT point (it's the second coordinate).
inline auto wktLatitude = [](const std::string& p) -> double {
  return detail::wktLatitudeImpl(p);
};

// Compute the distance in km between two WKT points according to the formula in
// https://en.wikipedia.org/wiki/Geographical_distance ("ellipsoidal earth
// projected to a plane"). A more precise way is the Haversine formula, which we
// save for when we compute this at indexing time.
inline auto wktDist = [](const std::string& p1,
                         const std::string& p2) -> double {
  return detail::wktDistImpl(p1, p2);
};

}  // namespace ad_utility

#endif  // QLEVER_GEOSPARQLHELPERS_H
