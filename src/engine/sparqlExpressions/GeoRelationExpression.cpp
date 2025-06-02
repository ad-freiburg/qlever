// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include "engine/SpatialJoin.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "util/GeoSparqlHelpers.h"

namespace sparqlExpression {
namespace detail {

template <SpatialJoinType Relation>
NARY_EXPRESSION(
    GeoRelationExpression, 2,
    FV<ad_utility::WktGeometricRelation<Relation>, GeoPointValueGetter>);

}

using namespace detail;

template <SpatialJoinType Relation>
SparqlExpression::Ptr makeGeoRelationExpression(SparqlExpression::Ptr child1,
                                                SparqlExpression::Ptr child2) {
  return std::make_unique<GeoRelationExpression<Relation>>(std::move(child1),
                                                           std::move(child2));
}

}  // namespace sparqlExpression

// Explicit instantiations for the different geometric relations to avoid linker
// problems
using Ptr = sparqlExpression::SparqlExpression::Ptr;

#ifndef QL_INSTANTIATE_GEO_RELATION_EXPR
#define QL_INSTANTIATE_GEO_RELATION_EXPR(joinType)                            \
  template Ptr                                                                \
      sparqlExpression::makeGeoRelationExpression<SpatialJoinType::joinType>( \
          Ptr, Ptr);
#endif  // QL_INSTANTIATE_GEO_RELATION_EXPR

QL_INSTANTIATE_GEO_RELATION_EXPR(INTERSECTS);
QL_INSTANTIATE_GEO_RELATION_EXPR(CONTAINS);
QL_INSTANTIATE_GEO_RELATION_EXPR(CROSSES);
QL_INSTANTIATE_GEO_RELATION_EXPR(TOUCHES);
QL_INSTANTIATE_GEO_RELATION_EXPR(EQUALS);
QL_INSTANTIATE_GEO_RELATION_EXPR(OVERLAPS);
