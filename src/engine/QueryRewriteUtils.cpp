// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/QueryRewriteUtils.h"

#include <stdexcept>

#include "engine/QueryExecutionTree.h"
#include "engine/SpatialJoin.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "parser/GraphPattern.h"
#include "parser/GraphPatternOperation.h"
#include "util/HashMap.h"

namespace {

// A `TripleComponent` geo function operand resolved to a `Variable` (for
// `SpatialJoinConfiguration`) together with, for a fixed value, the one-row
// `VALUES` tree that binds it to a fresh internal variable.
struct ResolvedGeoOperand {
  Variable variable_;
  std::optional<std::shared_ptr<QueryExecutionTree>> child_ = std::nullopt;
};

// _____________________________________________________________________________
ResolvedGeoOperand resolveGeoOperand(
    const TripleComponent& operand, QueryExecutionContext* qec,
    absl::FunctionRef<Variable()> generateUniqueVarName) {
  if (operand.isVariable()) {
    return {operand.getVariable()};
  }
  Variable var = generateUniqueVarName();
  auto tree = makeValuesForSingleValue(qec, var, operand);
  return {std::move(var), std::move(tree)};
}

// Add the variables that have the same numeric value in every result row of
// `pattern` to `constants`. See `substituteConstantMaxDistances` for details.
void collectConstantBindings(const parsedQuery::GraphPattern& pattern,
                             ad_utility::HashMap<Variable, Id>& constants) {
  for (const auto& op : pattern._graphPatterns) {
    if (const auto* values = std::get_if<parsedQuery::Values>(&op)) {
      const auto& [variables, rows] = values->_inlineValues;
      if (rows.empty()) {
        continue;
      }
      for (size_t col = 0; col < variables.size(); ++col) {
        const auto& value = rows[0][col];
        if (!ql::ranges::all_of(
                rows, [&](const auto& row) { return row[col] == value; })) {
          continue;
        }
        if (value.isInt()) {
          constants.insert_or_assign(variables[col],
                                     Id::makeFromInt(value.getInt()));
        } else if (value.isDouble()) {
          constants.insert_or_assign(variables[col],
                                     Id::makeFromDouble(value.getDouble()));
        }
      }
    } else if (const auto* bind = std::get_if<parsedQuery::Bind>(&op)) {
      if (const auto* literal =
              dynamic_cast<const sparqlExpression::IdExpression*>(
                  bind->_expression.getPimpl())) {
        constants.insert_or_assign(bind->_target, literal->value());
      }
    } else if (const auto* group =
                   std::get_if<parsedQuery::GroupGraphPattern>(&op)) {
      collectConstantBindings(group->_child, constants);
    }
  }
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
std::shared_ptr<SpatialJoin> rewriteFilterToSpatialJoin(
    const SparqlFilter& filter, QueryExecutionContext* qec,
    absl::FunctionRef<Variable()> generateUniqueVarName) {
  const auto& filterBody = *filter.expression_.getPimpl();

  // Currently, we can only optimize GeoSPARQL filters.
  auto configAndCall = getSpatialJoinConfigForFilter(filterBody);
  if (!configAndCall.has_value()) {
    return nullptr;
  }
  auto& [config, call] = configAndCall.value();

  // If neither side is a variable, rewriting is not possible.
  bool leftIsVar = call.left_.isVariable();
  bool rightIsVar = call.right_.isVariable();
  if (!leftIsVar && !rightIsVar) {
    return nullptr;
  }

  if (leftIsVar && rightIsVar &&
      call.left_.getVariable() == call.right_.getVariable()) {
    // TODO<ullingerc> As soon as we have a baseline implementation of
    // `WktGeometricRelation`, replace this `throw` by `return nullptr;`.
    throw std::runtime_error(
        absl::StrCat("Unsupported GeoSPARQL filter: Variable ",
                     call.left_.getVariable().name(),
                     " on both sides. Is this what you intended?"));
  }

  auto left = resolveGeoOperand(call.left_, qec, generateUniqueVarName);
  auto right = resolveGeoOperand(call.right_, qec, generateUniqueVarName);
  return qec->makeShared<SpatialJoin>(
      qec,
      SpatialJoinConfiguration{
          std::move(config), std::move(left.variable_),
          std::move(right.variable_), std::nullopt, PayloadVariables::all(),
          SpatialJoinAlgorithm::LIBSPATIALJOIN, std::nullopt},
      std::move(left.child_), std::move(right.child_), true);
}

// _____________________________________________________________________________
void substituteConstantMaxDistances(parsedQuery::GraphPattern& pattern) {
  // First find the filter children that are a variable compared to a distance
  // call, such that the bindings are only collected if there are any.
  struct Candidate {
    sparqlExpression::SparqlExpression* expression_;
    size_t childIndex_;
    Variable variable_;
  };
  std::vector<Candidate> candidates;
  for (auto& filter : pattern._filters) {
    auto* expression = filter.expression_.getPimpl();
    auto children = expression->children();
    if (children.size() != 2) {
      continue;
    }
    for (size_t i : {0, 1}) {
      auto variable = children[i]->getVariableOrNullopt();
      if (variable.has_value() &&
          getGeoDistanceExpressionParameters(*children[1 - i]).has_value()) {
        candidates.push_back({expression, i, std::move(variable).value()});
      }
    }
  }
  if (candidates.empty()) {
    return;
  }

  ad_utility::HashMap<Variable, Id> constants;
  collectConstantBindings(pattern, constants);
  for (const auto& [expression, i, variable] : candidates) {
    if (auto it = constants.find(variable); it != constants.end()) {
      expression->replaceChild(
          i, std::make_unique<sparqlExpression::IdExpression>(it->second));
    }
  }
}
