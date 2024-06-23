//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./LangExpression.h"

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

namespace sparqlExpression {

namespace {
Variable getVariableOrThrow(const SparqlExpression::Ptr& ptr) {
  if (auto stringPtr = dynamic_cast<const VariableExpression*>(ptr.get())) {
    return stringPtr->value();
  } else {
    throw std::runtime_error{
        "The argument to the LANG function must be a single variable"};
  }
}
}  // namespace

// _____________________________________________________________________________
LangExpression::LangExpression(SparqlExpression::Ptr child)
    : variable_{getVariableOrThrow(child)}, child_{std::move(child)} {}

// _____________________________________________________________________________
ExpressionResult LangExpression::evaluate(EvaluationContext* context) const {
  auto resSize = context->size();
  VectorWithMemoryLimit<IdOrLiteralOrIri> result{context->_allocator};
  result.reserve(resSize);

  using LitOrIri = ad_utility::triple_component::LiteralOrIri;
  constexpr auto lit = [](const std::string& s) -> LitOrIri {
    return LitOrIri::fromStringRepresentation(absl::StrCat("\""sv, s, "\""sv));
  };

  using ValueGetter = sparqlExpression::detail::LanguageTagValueGetter;
  ValueGetter langTagGetter{};
  // use detail::makeGenerator from SparqlEpressionGenerators to
  // retrieve the Id's from the context (w.r.t. variable_).
  for (Id id : detail::makeGenerator(variable_, resSize, context)) {
    std::optional<std::string> optStr = langTagGetter(id, context);
    if (optStr.has_value()) {
      result.push_back(lit(std::move(optStr.value())));
    } else {
      result.push_back(Id::makeUndefined());
    }
    checkCancellation(context);
  }
  return result;
}

// _____________________________________________________________________________
std::string LangExpression::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  return absl::StrCat("LANG "sv, child_->getCacheKey(varColMap));
}

// ___________________________________________________________________________
std::span<SparqlExpression::Ptr> LangExpression::childrenImpl() {
  return {&child_, 1};
}

// ____________________________________________________________________________
void LangExpression::checkCancellation(
    const sparqlExpression::EvaluationContext* context,
    ad_utility::source_location location) {
  context->cancellationHandle_->throwIfCancelled(location);
}

}  // namespace sparqlExpression
