// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/sparqlExpressions/ArgMinMaxExpression.h"

#include <absl/strings/str_cat.h>

#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"

namespace sparqlExpression {

// ____________________________________________________________________________
template <valueIdComparators::Comparison comparison>
ExpressionResult ArgMinMaxExpression<comparison>::evaluate(
    EvaluationContext* context) const {
  // Find the index of the first row with the minimal resp. maximal key, return
  // `std::nullopt` if no key is bound (in particular, for an empty group).
  //
  // NOTE: Unlike for `MIN` and `MAX`, an UNDEF key is skipped instead of being
  // smaller than any other key, because `?v` typically comes from an
  // `OPTIONAL` and the `?x` without a `?v` is not the one the user asks for.
  size_t size = context->size();
  auto findBestIndex = [context, size](auto&& keys) {
    using MinMax = detail::MinMaxForAllTypes<comparison>;
    auto generator = detail::makeGenerator(AD_FWD(keys), size, context);
    std::optional<ql::ranges::range_value_t<decltype(generator)>> bestKey;
    std::optional<size_t> bestIndex;
    size_t index = 0;
    for (auto&& key : generator) {
      context->cancellationHandle_->throwIfCancelled();
      if (!makeValueId(key, context).isUndefined() &&
          (!bestKey.has_value() ||
           MinMax::comesFirst(key, *bestKey, context))) {
        bestKey = AD_FWD(key);
        bestIndex = index;
      }
      ++index;
    }
    return bestIndex;
  };
  std::optional<size_t> bestIndex =
      std::visit(findBestIndex, children_[1]->evaluate(context));
  if (!bestIndex.has_value()) {
    return Id::makeUndefined();
  }

  // Return the value of the row with that index.
  auto getValueAt = [context, size, index = *bestIndex](auto&& values) {
    auto generator = detail::makeGenerator(AD_FWD(values), size, context);
    auto it = generator.begin();
    for (size_t i = 0; i < index; ++i) {
      ++it;
    }
    return ExpressionResult{*it};
  };
  return std::visit(getValueAt, children_[0]->evaluate(context));
}

// ____________________________________________________________________________
template <valueIdComparators::Comparison comparison>
std::string ArgMinMaxExpression<comparison>::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  return absl::StrCat(
      comparison == valueIdComparators::Comparison::LT ? "ARGMIN(" : "ARGMAX(",
      children_[0]->getCacheKey(varColMap), ", ",
      children_[1]->getCacheKey(varColMap), ")");
}

template class ArgMinMaxExpression<valueIdComparators::Comparison::LT>;
template class ArgMinMaxExpression<valueIdComparators::Comparison::GT>;

}  // namespace sparqlExpression
