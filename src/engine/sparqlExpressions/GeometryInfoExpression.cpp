// Copyright 2021 - 2025
// University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "rdfTypes/GeoSparqlHelpers.h"
#include "rdfTypes/GeometryInfo.h"

namespace sparqlExpression {
namespace detail {

NARY_EXPRESSION(
    CentroidExpression, 1,
    FV<ad_utility::WktCentroid, GeometryInfoValueGetter<ad_utility::Centroid>>);

NARY_EXPRESSION(
    AreaExpression, 2,
    FV<ad_utility::WktArea, GeometryInfoValueGetter<ad_utility::MetricArea>,
       UnitOfMeasurementValueGetter>);
NARY_EXPRESSION(MetricAreaExpression, 1,
                FV<ad_utility::WktMetricArea,
                   GeometryInfoValueGetter<ad_utility::MetricArea>>);

NARY_EXPRESSION(EnvelopeExpression, 1,
                FV<ad_utility::WktEnvelope,
                   GeometryInfoValueGetter<ad_utility::BoundingBox>>);
NARY_EXPRESSION(
    EnvelopeLowerLeftExpression, 1,
    FV<ad_utility::WktEnvelopeCorner<ad_utility::BoundingBoxCorner::LOWER_LEFT>,
       GeometryInfoValueGetter<ad_utility::BoundingBox>>);
NARY_EXPRESSION(EnvelopeUpperRightExpression, 1,
                FV<ad_utility::WktEnvelopeCorner<
                       ad_utility::BoundingBoxCorner::UPPER_RIGHT>,
                   GeometryInfoValueGetter<ad_utility::BoundingBox>>);

NARY_EXPRESSION(GeometryTypeExpression, 1,
                FV<ad_utility::WktGeometryType,
                   GeometryInfoValueGetter<ad_utility::GeometryType>>);

NARY_EXPRESSION(
    LengthExpression, 2,
    FV<ad_utility::WktLength, GeometryInfoValueGetter<ad_utility::MetricLength>,
       UnitOfMeasurementValueGetter>);
NARY_EXPRESSION(MetricLengthExpression, 1,
                FV<ad_utility::WktMetricLength,
                   GeometryInfoValueGetter<ad_utility::MetricLength>>);

template <ad_utility::BoundingCoordinate RequestedCoordinate>
NARY_EXPRESSION(BoundingCoordinateExpression, 1,
                FV<ad_utility::WktBoundingCoordinate<RequestedCoordinate>,
                   GeometryInfoValueGetter<ad_utility::BoundingBox>>);

NARY_EXPRESSION(NumGeometriesExpression, 1,
                FV<ad_utility::WktNumGeometries,
                   GeometryInfoValueGetter<ad_utility::NumGeometries>>);

}  // namespace detail

using namespace detail;

// _____________________________________________________________________________
SparqlExpression::Ptr makeAreaExpression(SparqlExpression::Ptr child1,
                                         SparqlExpression::Ptr child2) {
  return std::make_unique<AreaExpression>(std::move(child1), std::move(child2));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeMetricAreaExpression(SparqlExpression::Ptr child1) {
  return std::make_unique<MetricAreaExpression>(std::move(child1));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeCentroidExpression(SparqlExpression::Ptr child) {
  return std::make_unique<CentroidExpression>(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeEnvelopeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<EnvelopeExpression>(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeGeometryTypeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<GeometryTypeExpression>(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeLengthExpression(SparqlExpression::Ptr child1,
                                           SparqlExpression::Ptr child2) {
  return std::make_unique<LengthExpression>(std::move(child1),
                                            std::move(child2));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeMetricLengthExpression(SparqlExpression::Ptr child1) {
  return std::make_unique<MetricLengthExpression>(std::move(child1));
}

// _____________________________________________________________________________
template <ad_utility::BoundingCoordinate RequestedCoordinate>
SparqlExpression::Ptr makeBoundingCoordinateExpression(
    SparqlExpression::Ptr child) {
  return std::make_unique<BoundingCoordinateExpression<RequestedCoordinate>>(
      std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeNumGeometriesExpression(SparqlExpression::Ptr child) {
  return std::make_unique<NumGeometriesExpression>(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeEnvelopeLowerLeftExpression(
    SparqlExpression::Ptr child) {
  return std::make_unique<EnvelopeLowerLeftExpression>(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeEnvelopeUpperRightExpression(
    SparqlExpression::Ptr child) {
  return std::make_unique<EnvelopeUpperRightExpression>(std::move(child));
}

}  // namespace sparqlExpression

// Explicit instantiations for the bounding coordinate expressions
using Ptr = sparqlExpression::SparqlExpression::Ptr;

#ifdef QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR
#error "Macro QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR already defined"
#endif
#define QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR(RequestedCoordinate) \
  template Ptr sparqlExpression::makeBoundingCoordinateExpression<   \
      ad_utility::BoundingCoordinate::RequestedCoordinate>(Ptr)

QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR(MIN_X);
QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR(MIN_Y);
QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR(MAX_X);
QL_INSTANTIATE_BOUNDING_COORDINATE_EXPR(MAX_Y);
