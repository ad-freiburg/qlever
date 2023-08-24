// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "NaryExpression.h"
#include "NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail::conditional_expressions {
using namespace sparqlExpression::detail;
  [[maybe_unused]] auto ifImpl = [](EffectiveBooleanValueGetter::Result condition, auto&& i, auto&& e) -> IdOrString {
  if (condition == EffectiveBooleanValueGetter::Result::True) {
    return AD_FWD(i);
  } else {
    return AD_FWD(e);
  }
};
    NARY_EXPRESSION(IfExpression, 3, FV<decltype(ifImpl), EffectiveBooleanValueGetter, ActualValueGetter, ActualValueGetter>);
}

class CoalesceExpression:

using namespace detail::conditional_expressions;
SparqlExpression::Ptr makeIfExpression(SparqlExpression::Ptr child1,
                                       SparqlExpression::Ptr child2, SparqlExpression::Ptr child3) {
  return std::make_unique<IfExpression>(std::move(child1), std::move(child2), std::move(child3));
}
SparqlExpression::Ptr makeCoalesceExpression(std::vector<SparqlExpression::Ptr> children);

}
