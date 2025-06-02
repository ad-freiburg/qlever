//  Copyright 2022-2024 University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de> (2022)
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de> (2024)

// Unit tests for the functionality from this file can be
// found in LanguageExpressionsTest.h

#include "./engine/sparqlExpressions/LiteralExpression.h"
#include "./engine/sparqlExpressions/NaryExpressionImpl.h"
#include "./engine/sparqlExpressions/SparqlExpressionTypes.h"

namespace sparqlExpression {
namespace detail::langImpl {

using Lit = ad_utility::triple_component::Literal;
using OptValue = std::optional<std::string>;

//______________________________________________________________________________
// `LangExpressionImpl` extends the `NaryExpression` class with the method
// `containsLangExpression()`, which is required for the
// usage within the `Filter()`. In addition, `Filter()` requires access to the
// optional underlying variable, this access is solved over the stand alone
// function `getVariableFromLangExpression()`.
CPP_template(typename NaryOperation)(
    requires isOperation<NaryOperation>) class LangExpressionImpl
    : public NaryExpression<NaryOperation> {
 public:
  using NaryExpression<NaryOperation>::NaryExpression;

  bool containsLangExpression() const override { return true; }

  std::optional<Variable> variable() const {
    auto children = this->children();
    AD_CORRECTNESS_CHECK(children.size() == 1);
    return children[0]->getVariableOrNullopt();
  }
};

//______________________________________________________________________________
inline auto getLanguageTag = [](OptValue optLangTag) -> IdOrLiteralOrIri {
  if (!optLangTag.has_value()) {
    return Id::makeUndefined();
  } else {
    return LiteralOrIri{Lit::literalWithNormalizedContent(
        asNormalizedStringViewUnsafe(std::move(optLangTag.value())))};
  }
};

//______________________________________________________________________________
using LangExpression = detail::langImpl::LangExpressionImpl<
    detail::Operation<1, detail::FV<decltype(detail::langImpl::getLanguageTag),
                                    detail::LanguageTagValueGetter>>>;

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
  return langExpr->variable();
}

//______________________________________________________________________________
SparqlExpression::Ptr makeLangExpression(SparqlExpression::Ptr child) {
  return std::make_unique<detail::langImpl::LangExpression>(std::move(child));
}

}  // namespace sparqlExpression
