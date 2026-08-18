// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/QueryRewriteUtils.h"

#include <stdexcept>

#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"

// Try the three supported filter patterns in turn and directly build the
// resulting `LibSpatialJoinConfig` together with the joined variables. Kept
// as a separate function (as opposed to three loose `optional`s in the
// caller that have to be kept in sync manually) so that the join type,
// maximum distance, and DE-9IM filter pattern -- which depend on each other
// -- can only leave this function bundled together in a single consistent
// `LibSpatialJoinConfig`.
static std::optional<
    std::pair<LibSpatialJoinConfig, sparqlExpression::GeoFunctionCall>>
getSpatialJoinConfigForFilter(
    const sparqlExpression::SparqlExpression& filterBody) {
  // The filter body directly is an optimizable `geof:sf...` function.
  if (auto call = getGeoFunctionExpressionParameters(filterBody)) {
    return std::pair{LibSpatialJoinConfig{call.value().function_},
                     std::move(call).value()};
  }
  // The filter body is a `geof:relate` call with an explicit DE-9IM filter
  // pattern.
  if (auto call = getDe9imRelationExpressionParameters(filterBody)) {
    LibSpatialJoinConfig config{call.value().function_, std::nullopt,
                                call.value().pattern_};
    return std::pair{std::move(config), std::move(call).value()};
  }
  // The filter body is a maximum distance spatial search (direct body of
  // filter is a comparison).
  if (auto call = getGeoDistanceFilter(filterBody)) {
    LibSpatialJoinConfig config{call.value().first.function_,
                                call.value().second, std::nullopt};
    return std::pair{std::move(config), std::move(call).value().first};
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<SpatialJoinConfiguration> rewriteFilterToSpatialJoinConfig(
    const SparqlFilter& filter) {
  const auto& filterBody = *filter.expression_.getPimpl();

  // Currently, we can only optimize GeoSPARQL filters.
  auto configAndCall = getSpatialJoinConfigForFilter(filterBody);
  if (!configAndCall.has_value()) {
    return std::nullopt;
  }
  auto& [config, call] = configAndCall.value();

  if (call.left_ == call.right_) {
    // TODO<ullingerc> As soon as we have a baseline implementation of
    // `WktGeometricRelation`, replace this `throw` by `return std::nullopt;`.
    throw std::runtime_error(absl::StrCat(
        "Unsupported GeoSPARQL filter: Variable ", call.left_.name(),
        " on both sides. Is this what you intended?"));
  }
  auto joinType = call.function_;
  return SpatialJoinConfiguration{std::move(config),
                                  std::move(call.left_),
                                  std::move(call.right_),
                                  std::nullopt,
                                  PayloadVariables::all(),
                                  SpatialJoinAlgorithm::LIBSPATIALJOIN,
                                  joinType,
                                  std::nullopt};
}
