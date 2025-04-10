// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "util/GeometryInfo.h"

#include <util/geo/Geo.h>

#include <cstdint>

#include "parser/GeoPoint.h"
#include "parser/Literal.h"
#include "parser/NormalizedString.h"
#include "util/GeoSparqlHelpers.h"
#include "util/geo/Point.h"

namespace ad_utility {
namespace detail {

using namespace ::util::geo;

// TODO<ullingerc> Computation of GeometryInfo is a separate PR

}  // namespace detail

// ____________________________________________________________________________
GeometryInfo GeometryInfo::fromWktLiteral(
    [[maybe_unused]] const std::string_view& wkt) {
  // TODO<ullingerc> Computation of GeometryInfo is a separate PR
  return GeometryInfo{1};
}

// ____________________________________________________________________________
GeometryInfo GeometryInfo::fromGeoPoint(
    [[maybe_unused]] const GeoPoint& point) {
  // TODO<ullingerc> Computation of GeometryInfo is a separate PR
  return GeometryInfo{1};
}

}  // namespace ad_utility
