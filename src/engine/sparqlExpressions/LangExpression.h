//  Copyright 2022-2024 University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de> (2022)
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de> (2024)

// Unit tests for the functionality from this file can be
// found in LanguageExpressionsTest.h

#pragma once

#include "./engine/sparqlExpressions/LiteralExpression.h"
#include "./engine/sparqlExpressions/NaryExpressionImpl.h"
#include "./engine/sparqlExpressions/SparqlExpressionTypes.h"

namespace sparqlExpression {
namespace detail::langImpl {

using Lit = ad_utility::triple_component::Literal;
using OptValue = std::optional<std::string>;

//______________________________________________________________________________
// `LangExpressionImpl` extends the `NaryExpression` class with the methods
// `containsLangExpression()` and `variable()`, which are required for the
// usage within the `Filter()`.
template <typename NaryOperation>
requires(isOperation<NaryOperation>)
class LangExpressionImpl : public NaryExpression<NaryOperation> {
 public:
  using NaryExpression<NaryOperation>::NaryExpression;

  bool containsLangExpression() const override { return true; }

  const Variable& variable() const {
    std::optional<SparqlExpression*> optChild = this->getNthChild(0);
    AD_CORRECTNESS_CHECK(optChild.has_value());
    if (auto stringPtr =
            dynamic_cast<const VariableExpression*>(optChild.value())) {
      return stringPtr->value();
    } else {
      throw std::runtime_error{
          "Use the LANG() function within a FILTER() expression only with a "
          "variable as its argument. Valid example: "
          "FILTER(LANG(?example_var) "
          "= \"en\")"};
    }
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

}  //  namespace detail::langImpl

// TODO: Directly implement this in a .cpp file and make it
// available as makeLangExpression.
// Expression that implements the `LANG(...)` function.
using LangExpression = detail::langImpl::LangExpressionImpl<
    detail::Operation<1, detail::FV<decltype(detail::langImpl::getLanguageTag),
                                    detail::LanguageTagValueGetter>>>;

}  // namespace sparqlExpression
