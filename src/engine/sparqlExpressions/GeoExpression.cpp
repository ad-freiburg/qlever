// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2025 Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>, UFR
// 2021 - 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/SpatialJoinConfig.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "rdfTypes/GeoSparqlHelpers.h"

// This file contains geometry utility expressions (`geof:latitude`,
// `geof:geometryN`, etc.). The distance expressions (`geof:distance`, etc.) can
// be found in `GeoDistanceExpression.cpp`, the geometric relation expressions
// (`geof:sfIntersects`, etc.) in `GeoRelationExpression.cpp`, and expressions
// extracting information from a `GeometryInfo` such as `geof:centroid` in
// `GeometryInfoExpression.cpp`.

namespace sparqlExpression {
namespace detail {

NARY_EXPRESSION(
    LongitudeExpression, 1,
    FV<NumericIdWrapper<ad_utility::WktLongitude, true>, GeoPointValueGetter>);
NARY_EXPRESSION(
    LatitudeExpression, 1,
    FV<NumericIdWrapper<ad_utility::WktLatitude, true>, GeoPointValueGetter>);

NARY_EXPRESSION(
    GeometryNExpression, 2,
    FV<ad_utility::WktGeometryN, GeoPointOrWktValueGetter, IntValueGetter>);

NARY_EXPRESSION(
    SimplifyGeometryExpression, 2,
    FV<ad_utility::WktSimplify, GeoPointOrWktValueGetter, NumericValueGetter>);

NARY_EXPRESSION(AsGeoJsonExpression, 1,
                FV<ad_utility::WktAsGeoJson, GeoPointOrWktValueGetter>);

}  // namespace detail

using namespace detail;

// _____________________________________________________________________________
SparqlExpression::Ptr makeLatitudeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<LatitudeExpression>(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeLongitudeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<LongitudeExpression>(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeGeometryNExpression(SparqlExpression::Ptr child1,
                                              SparqlExpression::Ptr child2) {
  return std::make_unique<GeometryNExpression>(std::move(child1),
                                               std::move(child2));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeSimplifyGeometryExpression(
    SparqlExpression::Ptr child1, SparqlExpression::Ptr child2) {
  return std::make_unique<SimplifyGeometryExpression>(std::move(child1),
                                                      std::move(child2));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeAsGeoJsonExpression(SparqlExpression::Ptr child) {
  return std::make_unique<AsGeoJsonExpression>(std::move(child));
}

}  // namespace sparqlExpression
