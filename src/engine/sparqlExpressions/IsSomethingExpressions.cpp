// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

namespace sparqlExpression {
namespace detail {

// Quick recap of how defining n-ary functions works in QLever.
//
// 1. Define a subclass of `SparqlExpression` (see `SparqlExpression.h`),
//    called `...Expression`. For n-ary functions, make your life easier by
//    using the generic `NARY` (defined in `NaryExpressionImpl.h`).
//
// 2. NARY takes two arguments: the number of arguments (n) and a template
//    with the value getters for the arguments (one if all arguments are of
//    the same type, otherwise exactly one for each argument) and a function
//    to be applied to the results of the value getters.
//
// 3. Implement a function `make...Expression` that takes n arguments of type
//    `SparqlExpression::Ptr` and returns a `unique_ptr` to a new instance of
//    `...Expression` (`std::move` the arguments into the constructor). The
//    function should be declared in `NaryExpression.h`.

// Expressions for `isIRI`, `isBlank`, `isLiteral`, and `isNumeric`. Note that
// the value getters already return the correct `Id`, hence `std::identity`.
using isIriExpression = NARY<1, FV<std::identity, IsIriValueGetter>>;
using isBlankExpression = NARY<1, FV<std::identity, IsBlankNodeValueGetter>>;
using isLiteralExpression = NARY<1, FV<std::identity, IsLiteralValueGetter>>;
using isNumericExpression = NARY<1, FV<std::identity, IsNumericValueGetter>>;
// The expression for `bound` is slightly different because
// `IsValidValueGetter` returns a `bool` and not an `Id`.
inline auto boolToId = [](bool b) { return Id::makeFromBool(b); };
using boundExpression = NARY<1, FV<decltype(boolToId), IsValidValueGetter>>;

}  // namespace detail

SparqlExpression::Ptr makeIsIriExpression(SparqlExpression::Ptr arg) {
  return std::make_unique<detail::isIriExpression>(std::move(arg));
}
SparqlExpression::Ptr makeIsBlankExpression(SparqlExpression::Ptr arg) {
  return std::make_unique<detail::isBlankExpression>(std::move(arg));
}
SparqlExpression::Ptr makeIsLiteralExpression(SparqlExpression::Ptr arg) {
  return std::make_unique<detail::isLiteralExpression>(std::move(arg));
}
SparqlExpression::Ptr makeIsNumericExpression(SparqlExpression::Ptr arg) {
  return std::make_unique<detail::isNumericExpression>(std::move(arg));
}
SparqlExpression::Ptr makeBoundExpression(SparqlExpression::Ptr arg) {
  return std::make_unique<detail::boundExpression>(std::move(arg));
}

}  // namespace sparqlExpression
