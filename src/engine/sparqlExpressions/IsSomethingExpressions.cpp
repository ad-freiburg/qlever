// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

namespace sparqlExpression {
namespace detail {

// Quick recap of how defining n-ary functions works in QLever.
//
// 1. Define the implementation of the function, as a lambda called `...Impl`.
//
// 2. Define a subclass of `SparqlExpression` (see `SparqlExpression.h`),
//    called `...Expression`. For n-ary functions, use `NARY` (defind in
//    `NaryExpressionImpl.h`), which takes two template arguments:
//
//    2.1 The number of arguments (that is, the n)
//    2.2 An instatiation of `FunctionAndValueGetters` or `FV` for short
//        (defined in `SparqlExpression.h`), which takes two arguments:
//        2.2.1 The `decltype` of the implementation lambda
//        2.2.2 The `...ValueGetter` that should be applied to the arguments
//              (one if all arguments are of the same type, otherwise exactly
//              one for each argument)
//
// 3. Implement a function `make...Expression` that takes n arguments of type
//    `SparqlExpression::Ptr` and returns a `unique_ptr` to a new instance of
//    `...Expression` (`std::move` the arguments into the constructor). The
//    function should be declared in `NaryExpression.h`.

// isIRI
inline const auto isIriImpl = [](std::optional<std::string> arg) {
  LOG(DEBUG) << "isIriImpl called with "
             << (arg.has_value() ? arg.value() : "std::nullopt") << std::endl;
  return Id::makeFromBool(arg.has_value() && arg.value().size() > 1 &&
                          arg.value().front() == '<');
};
using isIriExpression = NARY<1, FV<decltype(isIriImpl), StringValueGetterRaw>>;

// isBlank
inline const auto isBlankImpl = [](bool isBlankNode) {
  return Id::makeFromBool(isBlankNode);
};
using isBlankExpression =
    NARY<1, FV<decltype(isBlankImpl), IsBlankNodeValueGetter>>;
// inline const auto isBlankImpl = [](std::optional<std::string> arg) {
//   LOG(DEBUG) << "isBlankImpl called with "
//              << (arg.has_value() ? arg.value() : "std::nullopt") <<
//              std::endl;
//   return Id::makeFromBool(arg.has_value() && arg.value().size() > 1 &&
//                           arg.value().front() == '_');
// };
// using isBlankExpression =
//     NARY<1, FV<decltype(isBlankImpl), StringValueGetterRaw>>;

// isLiteral
inline const auto isLiteralImpl = [](std::optional<std::string> arg) {
  LOG(DEBUG) << "isLiteralImpl called with "
             << (arg.has_value() ? arg.value() : "std::nullopt") << std::endl;
  return Id::makeFromBool(arg.has_value() && arg.value().size() > 1 &&
                          arg.value().front() == '"');
};
using isLiteralExpression =
    NARY<1, FV<decltype(isLiteralImpl), StringValueGetterRaw>>;

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

}  // namespace sparqlExpression
