//  Copyright 2022-2024 University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de> (2022)
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de> (2024)

// Unit tests for the functionality from this file can be
// found in LanguageExpressionsTest.h

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"

namespace sparqlExpression {
namespace detail::langImpl {

using Lit = ad_utility::triple_component::Literal;
using OptValue = std::optional<std::string>;

//______________________________________________________________________________
struct GetLanguageTag {
  IdOrLiteralOrIri operator()(OptValue optLangTag) const {
    if (!optLangTag.has_value()) {
      return Id::makeUndefined();
    } else {
      return LiteralOrIri{Lit::literalWithNormalizedContent(
          asNormalizedStringViewUnsafe(std::move(optLangTag.value())))};
    }
  }
};

//______________________________________________________________________________
NARY_EXPRESSION(LangExpression, 1, FV<GetLanguageTag, LanguageTagValueGetter>);

}  //  namespace detail::langImpl

//______________________________________________________________________________
// This function is a stand-alone helper used in `RelationalExpression.cpp`,
// if `getVariableFromLangExpression()` returns std::nullopt, no language filter
// will be created within. This happens when the provided pointer doesn't
// point to a `LangExpression`, or the given respective `LangExpression` doesn't
// hold a `Variable` as a child expression.
std::optional<Variable> getVariableFromLangExpression(
    const SparqlExpression* expPtr) {
  const auto* langExpr =
      dynamic_cast<const detail::langImpl::LangExpression*>(expPtr);
  if (!langExpr) {
    return std::nullopt;
  }

  auto children = langExpr->children();
  AD_CORRECTNESS_CHECK(children.size() == 1);
  return children[0]->getVariableOrNullopt();
}

//______________________________________________________________________________
SparqlExpression::Ptr makeLangExpression(SparqlExpression::Ptr child) {
  return std::make_unique<detail::langImpl::LangExpression>(std::move(child));
}

}  // namespace sparqlExpression
