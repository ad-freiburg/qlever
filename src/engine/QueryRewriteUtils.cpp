// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/QueryRewriteUtils.h"

#include <stdexcept>

#include "engine/QueryExecutionTree.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"

namespace {

// A `TripleComponent` geo function operand resolved to a `Variable` (for
// `SpatialJoinConfiguration`) together with, for a fixed value, the one-row
// `VALUES` tree that binds it to a fresh internal variable.
struct ResolvedGeoOperand {
  Variable variable_;
  std::optional<std::shared_ptr<QueryExecutionTree>> child_ = std::nullopt;
};

// _____________________________________________________________________________
template <typename VarGenerator>
ResolvedGeoOperand resolveGeoOperand(
    const TripleComponent& operand, QueryExecutionContext* qec,
    const VarGenerator& generateUniqueVarName) {
  if (operand.isVariable()) {
    return {operand.getVariable()};
  }
  Variable var = generateUniqueVarName();
  auto tree = ad_utility::makeExecutionTree<Values>(
      qec, parsedQuery::SparqlValues{{var}, {{operand}}});
  return {std::move(var), std::move(tree)};
}

}  // namespace

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
std::optional<SpatialJoinRewriteResult> rewriteFilterToSpatialJoinConfig(
    const SparqlFilter& filter, QueryExecutionContext* qec,
    absl::FunctionRef<Variable()> generateUniqueVarName) {
  const auto& filterBody = *filter.expression_.getPimpl();

  // Currently, we can only optimize GeoSPARQL filters.
  auto configAndCall = getSpatialJoinConfigForFilter(filterBody);
  if (!configAndCall.has_value()) {
    return std::nullopt;
  }
  auto& [config, call] = configAndCall.value();

  // If neither side is a variable, rewriting is not possible.
  bool leftIsVar = call.left_.isVariable();
  bool rightIsVar = call.right_.isVariable();
  if (!leftIsVar && !rightIsVar) {
    return std::nullopt;
  }

  if (leftIsVar && rightIsVar &&
      call.left_.getVariable() == call.right_.getVariable()) {
    // TODO<ullingerc> As soon as we have a baseline implementation of
    // `WktGeometricRelation`, replace this `throw` by `return std::nullopt;`.
    throw std::runtime_error(
        absl::StrCat("Unsupported GeoSPARQL filter: Variable ",
                     call.left_.getVariable().name(),
                     " on both sides. Is this what you intended?"));
  }

  auto joinType = call.function_;
  auto left = resolveGeoOperand(call.left_, qec, generateUniqueVarName);
  auto right = resolveGeoOperand(call.right_, qec, generateUniqueVarName);
  return SpatialJoinRewriteResult{
      SpatialJoinConfiguration{
          std::move(config), std::move(left.variable_),
          std::move(right.variable_), std::nullopt, PayloadVariables::all(),
          SpatialJoinAlgorithm::LIBSPATIALJOIN, joinType, std::nullopt},
      std::move(left.child_), std::move(right.child_)};
}
