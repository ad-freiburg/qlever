//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_STRINGEXPRESSIONSHELPER_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_STRINGEXPRESSIONSHELPER_H

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

namespace sparqlExpression::detail::string_expressions {

// Template for an expression that works on string literals. The arguments are
// the same as those to `NaryExpression` with the difference that the value
// getter is deduced automatically. If the child of the expression is the
// `STR()` expression, then the `StringValueGetter` will be used (which also
// returns string values for IRIs, numeric literals, etc.), otherwise the
// `LiteralFromIdGetter` is used (which returns `std::nullopt` for these cases).
template <typename ValueGetterWithStr, typename ValueGetterWithoutStr, size_t N,
          typename Function, typename... AdditionalNonStringValueGetters>
class StringExpressionImplImpl : public SparqlExpression {
 private:
  using ExpressionWithStr = NARY<
      N, FV<Function, ValueGetterWithStr, AdditionalNonStringValueGetters...>>;
  using ExpressionWithoutStr = NARY<N, FV<Function, ValueGetterWithoutStr,
                                          AdditionalNonStringValueGetters...>>;

  Ptr impl_;

 public:
  CPP_template(typename... C)(
      requires(concepts::same_as<C, SparqlExpression::Ptr>&&...)
          CPP_and(sizeof...(C) + 1 ==
                  N)) explicit StringExpressionImplImpl(Ptr child,
                                                        C... children) {
    AD_CORRECTNESS_CHECK(child != nullptr);
    if (child->isStrExpression()) {
      auto childrenOfStr = std::move(*child).moveChildrenOut();
      AD_CORRECTNESS_CHECK(childrenOfStr.size() == 1);
      impl_ = std::make_unique<ExpressionWithStr>(
          std::move(childrenOfStr.at(0)), std::move(children)...);
    } else {
      impl_ = std::make_unique<ExpressionWithoutStr>(std::move(child),
                                                     std::move(children)...);
    }
  }

  ExpressionResult evaluate(EvaluationContext* context) const override {
    return impl_->evaluate(context);
  }
  std::string getCacheKey(const VariableToColumnMap& varColMap) const override {
    return impl_->getCacheKey(varColMap);
  }

 private:
  std::span<Ptr> childrenImpl() override { return impl_->children(); }
};

// Impl class for expressions that work on plain strings.
template <size_t N, typename Function,
          typename... AdditionalNonStringValueGetters>
using StringExpressionImpl =
    StringExpressionImplImpl<StringValueGetter, LiteralFromIdGetter, N,
                             Function, AdditionalNonStringValueGetters...>;

// Impl class for expressions that work on literals with datatypes and language
// tags.
template <size_t N, typename Function,
          typename... AdditionalNonStringValueGetters>
using LiteralExpressionImpl =
    StringExpressionImplImpl<LiteralValueGetterWithStrFunction,
                             LiteralValueGetterWithoutStrFunction, N, Function,
                             AdditionalNonStringValueGetters...>;
}  // namespace sparqlExpression::detail::string_expressions

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_STRINGEXPRESSIONSHELPER_H
