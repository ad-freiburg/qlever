// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/QueryRewriteUtils.h"

#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"

// _____________________________________________________________________________
std::optional<SpatialJoinConfiguration> rewriteFilterToSpatialJoinConfig(
    const SparqlFilter& filter) {
  const auto& filterBody = *filter.expression_.getPimpl();

  // Currently, we can only optimize GeoSPARQL filters:
  // Analyze the expression: Check if the body of the filter directly is an
  // optimizable geof: function
  auto geoFuncCall = getGeoFunctionExpressionParameters(filterBody);
  std::optional<double> maxDist = std::nullopt;

  if (!geoFuncCall.has_value()) {
    // If the filter body is no geof: function, it can still be a maximum
    // distance spatial search (direct body of filter is comparison).
    auto distFilterRes = getGeoDistanceFilter(filterBody);
    if (!distFilterRes.has_value()) {
      return std::nullopt;
    }
    geoFuncCall = distFilterRes.value().first;
    maxDist = distFilterRes.value().second;
  }

  // Construct spatial join
  auto [type, left, right] = geoFuncCall.value();
  return SpatialJoinConfiguration{SpatialJoinConfig{type, maxDist},
                                  std::move(left),
                                  std::move(right),
                                  std::nullopt,
                                  PayloadVariables::all(),
                                  SpatialJoinAlgorithm::LIBSPATIALJOIN,
                                  type};
}
