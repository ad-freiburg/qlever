// Copyright 2021 - 2024
// University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpression.h"

#include "engine/SpatialJoin.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/GeoSparqlHelpers.h"

namespace sparqlExpression {
namespace detail {
NARY_EXPRESSION(
    LongitudeExpression, 1,
    FV<NumericIdWrapper<ad_utility::WktLongitude, true>, GeoPointValueGetter>);
NARY_EXPRESSION(
    LatitudeExpression, 1,
    FV<NumericIdWrapper<ad_utility::WktLatitude, true>, GeoPointValueGetter>);
NARY_EXPRESSION(DistExpression, 2,
                FV<NumericIdWrapper<ad_utility::WktDistGeoPoints, true>,
                   GeoPointValueGetter>);
NARY_EXPRESSION(MetricDistExpression, 2,
                FV<NumericIdWrapper<ad_utility::WktMetricDistGeoPoints, true>,
                   GeoPointValueGetter>);
NARY_EXPRESSION(
    DistWithUnitExpression, 3,
    FV<NumericIdWrapper<ad_utility::WktDistGeoPoints, true>,
       GeoPointValueGetter, GeoPointValueGetter, UnitOfMeasurementValueGetter>);

template <SpatialJoinType Relation>
NARY_EXPRESSION(
    GeoRelationExpression, 2,
    FV<ad_utility::WktGeometricRelation<Relation>, GeoPointValueGetter>);

}  // namespace detail

using namespace detail;
SparqlExpression::Ptr makeDistExpression(SparqlExpression::Ptr child1,
                                         SparqlExpression::Ptr child2) {
  return std::make_unique<DistExpression>(std::move(child1), std::move(child2));
}
SparqlExpression::Ptr makeMetricDistExpression(SparqlExpression::Ptr child1,
                                               SparqlExpression::Ptr child2) {
  return std::make_unique<MetricDistExpression>(std::move(child1),
                                                std::move(child2));
}
SparqlExpression::Ptr makeDistWithUnitExpression(
    SparqlExpression::Ptr child1, SparqlExpression::Ptr child2,
    std::optional<SparqlExpression::Ptr> child3) {
  // Unit is optional
  if (child3.has_value()) {
    return std::make_unique<DistWithUnitExpression>(
        std::move(child1), std::move(child2), std::move(child3.value()));
  } else {
    return std::make_unique<DistExpression>(std::move(child1),
                                            std::move(child2));
  }
}

template <SpatialJoinType Relation>
SparqlExpression::Ptr makeGeoRelationExpression(SparqlExpression::Ptr child1,
                                                SparqlExpression::Ptr child2) {
  return std::make_unique<GeoRelationExpression<Relation>>(std::move(child1),
                                                           std::move(child2));
}

SparqlExpression::Ptr makeLatitudeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<LatitudeExpression>(std::move(child));
}
SparqlExpression::Ptr makeLongitudeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<LongitudeExpression>(std::move(child));
}

}  // namespace sparqlExpression

// Explicit instantiations for the different geometric relations to avoid linker
// problems
template sparqlExpression::SparqlExpression::Ptr
    sparqlExpression::makeGeoRelationExpression<SpatialJoinType::INTERSECTS>(
        sparqlExpression::SparqlExpression::Ptr,
        sparqlExpression::SparqlExpression::Ptr);

template sparqlExpression::SparqlExpression::Ptr
    sparqlExpression::makeGeoRelationExpression<SpatialJoinType::CONTAINS>(
        sparqlExpression::SparqlExpression::Ptr,
        sparqlExpression::SparqlExpression::Ptr);

template sparqlExpression::SparqlExpression::Ptr
    sparqlExpression::makeGeoRelationExpression<SpatialJoinType::CROSSES>(
        sparqlExpression::SparqlExpression::Ptr,
        sparqlExpression::SparqlExpression::Ptr);

template sparqlExpression::SparqlExpression::Ptr
    sparqlExpression::makeGeoRelationExpression<SpatialJoinType::TOUCHES>(
        sparqlExpression::SparqlExpression::Ptr,
        sparqlExpression::SparqlExpression::Ptr);

template sparqlExpression::SparqlExpression::Ptr
    sparqlExpression::makeGeoRelationExpression<SpatialJoinType::EQUALS>(
        sparqlExpression::SparqlExpression::Ptr,
        sparqlExpression::SparqlExpression::Ptr);

template sparqlExpression::SparqlExpression::Ptr
    sparqlExpression::makeGeoRelationExpression<SpatialJoinType::OVERLAPS>(
        sparqlExpression::SparqlExpression::Ptr,
        sparqlExpression::SparqlExpression::Ptr);
