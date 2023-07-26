//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
namespace sparqlExpression {
namespace detail {
// String functions.
NARY_EXPRESSION(StrExpressionImpl, 1, FV<std::identity, StringValueGetter>);

class StrExpression : public StrExpressionImpl {
  using StrExpressionImpl::StrExpressionImpl;
  bool isStrExpression() const override { return true; }
};

// Compute string length.
inline auto strlen = [](std::string_view s) {
  return Id::makeFromInt(s.size());
};
NARY_EXPRESSION(StrlenExpression, 1, FV<decltype(strlen), StringValueGetter>);

}  // namespace detail
using namespace detail;
SparqlExpression::Ptr makeStrExpression(SparqlExpression::Ptr child) {
  return std::make_unique<StrExpression>(std::move(child));
}
SparqlExpression::Ptr makeStrlenExpression(SparqlExpression::Ptr child) {
  return std::make_unique<StrlenExpression>(std::move(child));
}
}  // namespace sparqlExpression
