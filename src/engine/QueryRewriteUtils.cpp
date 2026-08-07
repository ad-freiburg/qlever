// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/QueryRewriteUtils.h"

#include <stdexcept>

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
  std::optional<De9imFilterString> de9imFilter = std::nullopt;

  if (!geoFuncCall.has_value()) {
    // If the filter body is no geof:sf... function, it can still be a
    // `geof:relate` call with an explicit DE-9IM filter pattern.
    auto de9imCall = getDe9imRelationExpressionParameters(filterBody);
    if (de9imCall.has_value()) {
      de9imFilter = de9imCall.value().pattern_;
      geoFuncCall = std::move(de9imCall.value());
    } else {
      // If the filter body is neither, it can still be a maximum distance
      // spatial search (direct body of filter is comparison).
      auto distFilterRes = getGeoDistanceFilter(filterBody);
      if (!distFilterRes.has_value()) {
        return std::nullopt;
      }
      geoFuncCall = distFilterRes.value().first;
      maxDist = distFilterRes.value().second;
    }
  }

  // Construct spatial join
  auto [type, left, right] = geoFuncCall.value();
  if (left == right) {
    // TODO<ullingerc> As soon as we have a baseline implementation of
    // `WktGeometricRelation`, replace this `throw` by `return std::nullopt;`.
    throw std::runtime_error(
        absl::StrCat("Unsupported GeoSPARQL filter: Variable ", left.name(),
                     " on both sides. Is this what you intended?"));
  }
  return SpatialJoinConfiguration{
      LibSpatialJoinConfig{type, maxDist, de9imFilter},
      std::move(left),
      std::move(right),
      std::nullopt,
      PayloadVariables::all(),
      SpatialJoinAlgorithm::LIBSPATIALJOIN,
      type,
      std::nullopt};
}
