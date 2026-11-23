// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "engine/QueryRewriteUtils.h"

#include <stdexcept>

#include "engine/QueryExecutionTree.h"
#include "engine/SpatialJoin.h"
#include "engine/Values.h"
#include "engine/filterFacts/Recognizers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "parser/GraphPattern.h"

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
    const sparqlExpression::SparqlExpressionPimpl& filter) {
  const auto& filterBody = *filter.getPimpl();

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
  // The filter body bounds the distance between two geometries (see
  // `filterFacts::recognizeDistanceBound`).
  auto conjuncts = filterFacts::canonicalize(filter);
  if (conjuncts.size() != 1) {
    return std::nullopt;
  }
  if (auto bound = filterFacts::recognizeDistanceBound(conjuncts[0])) {
    using enum SpatialJoinType::Enum;
    return std::pair{
        LibSpatialJoinConfig{WITHIN_DIST, bound->maxMeters_, std::nullopt},
        sparqlExpression::GeoFunctionCall{WITHIN_DIST,
                                          std::move(bound->distance_.a_),
                                          std::move(bound->distance_.b_)}};
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::shared_ptr<SpatialJoin> rewriteFilterToSpatialJoin(
    const SparqlFilter& filter, QueryExecutionContext* qec,
    absl::FunctionRef<Variable()> generateUniqueVarName) {
  // Currently, we can only optimize GeoSPARQL filters.
  auto configAndCall = getSpatialJoinConfigForFilter(filter.expression_);
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

namespace {

// Return a FILTER that is equivalent to `bound`, namely
// `geof:metricDistance(a, b) <= maxMeters`.
SparqlFilter distanceBoundToFilter(const filterFacts::DistanceBound& bound) {
  using namespace sparqlExpression;
  // The operands of a `DistanceBound` are variables or constants (see
  // `getGeoDistanceExpressionParameters`).
  auto toExpression =
      [](const TripleComponent& operand) -> SparqlExpression::Ptr {
    if (operand.isVariable()) {
      return std::make_unique<VariableExpression>(operand.getVariable());
    } else if (operand.isLiteral()) {
      return std::make_unique<StringLiteralExpression>(operand.getLiteral());
    }
    AD_CORRECTNESS_CHECK(operand.isId());
    return std::make_unique<IdExpression>(operand.getId());
  };
  auto expression =
      std::make_unique<LessEqualExpression>(LessEqualExpression::Children{
          makeMetricDistExpression(toExpression(bound.distance_.a_),
                                   toExpression(bound.distance_.b_)),
          std::make_unique<IdExpression>(
              Id::makeFromDouble(bound.maxMeters_))});
  auto descriptor =
      absl::StrCat("geof:metricDistance(", bound.distance_.a_.toString(), ", ",
                   bound.distance_.b_.toString(), ") <= ", bound.maxMeters_);
  return SparqlFilter{
      SparqlExpressionPimpl{std::move(expression), std::move(descriptor)}};
}

}  // namespace

// _____________________________________________________________________________
void rewriteFiltersWithFacts(parsedQuery::GraphPattern& group) {
  auto defs = filterFacts::collectDefinitions(group._graphPatterns);
  qlm::vector<SparqlFilter> result{group._filters.get_allocator()};
  for (auto& filter : group._filters) {
    // Recognize the `DistanceBound` of each conjunct of `filter`.
    auto conjuncts = filterFacts::canonicalize(filter.expression_, defs);
    auto bounds = ::ranges::to<std::vector>(
        conjuncts | ql::views::transform(&filterFacts::recognizeDistanceBound));

    // Apply the `LangIn` fact of each conjunct to the triples of `group`. The
    // conjunct is replaced iff this succeeds and the fact is exact.
    std::vector<bool> isReplaced;
    for (const auto& conjunct : conjuncts) {
      auto langIn = filterFacts::recognizeLangIn(conjunct);
      bool isApplied = langIn.has_value() &&
                       group.addLanguageFilter(langIn->var_, langIn->langs_);
      isReplaced.push_back(isApplied && langIn->exact_);
    }

    // Keep a FILTER without a `DistanceBound` or a replaced conjunct as it is.
    //
    // NOTE: Keeping such a FILTER unsplit is for efficiency, not for
    // correctness. Splitting every FILTER would also be correct (a FILTER
    // holds iff all its conjuncts hold), but each conjunct would then become
    // its own `Filter` operation and its own filter in the query planning,
    // without any rewrite.
    if (ql::ranges::none_of(bounds,
                            [](const auto& b) { return b.has_value(); }) &&
        ql::ranges::none_of(isReplaced, std::identity{})) {
      result.push_back(std::move(filter));
      continue;
    }

    // Split `filter` into its conjuncts. Drop the replaced conjuncts. Replace a
    // conjunct whose fact needs the definitions by an equivalent FILTER without
    // them, which can be rewritten on its own.
    for (auto&& [conjunct, bound, replaced] :
         ::ranges::views::zip(conjuncts, bounds, isReplaced)) {
      if (replaced) {
        continue;
      }
      bool needsDefinitions =
          bound.has_value() &&
          !filterFacts::recognizeDistanceBound(
              filterFacts::canonicalize(conjunct.expression_)[0]);
      if (needsDefinitions) {
        result.push_back(distanceBoundToFilter(bound.value()));
      } else {
        result.push_back(SparqlFilter{std::move(conjunct.expression_)});
      }
    }
  }
  group._filters = std::move(result);
}
