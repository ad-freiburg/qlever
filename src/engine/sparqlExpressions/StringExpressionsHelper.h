//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_STRINGEXPRESSIONSHELPER_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_STRINGEXPRESSIONSHELPER_H

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

namespace sparqlExpression::detail::string_expressions {

using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;

// Convert a `string_view` to a `LiteralOrIri` that stores a `Literal`.
//
// NOTE: This currently copies the string, because the `Literal` class has to
// add the quotation marks.
struct ToLiteral {
  LiteralOrIri operator()(std::string_view normalizedContent,
                          const std::optional<std::variant<Iri, std::string>>&
                              descriptor = std::nullopt) const {
    return LiteralOrIri{
        ad_utility::triple_component::Literal::literalWithNormalizedContent(
            normalizedContent, descriptor)};
  }
};
inline constexpr ToLiteral toLiteral{};

// Lift a `Function` that takes one or multiple `std::string`s (possibly via
// references) and returns an `Id` or a `LiteralOrIri` to a function that takes
// the same number of `std::optional<std::string>` and returns an `Id` or an
// `IdOrLocalVocabEntry`. If any of the optionals is `std::nullopt`, the result
// is UNDEF, otherwise it is the result of the `Function` applied to the values
// of the optionals.
template <typename Function>
struct LiftStringFunction {
  CPP_template(typename... Arguments)(
      requires(concepts::same_as<Arguments, std::optional<std::string>>&&...))
  auto operator()(Arguments... arguments) const {
    using ResultOfFunction =
        decltype(std::invoke(Function{}, std::move(arguments.value())...));
    static_assert(concepts::same_as<ResultOfFunction, Id> ||
                      concepts::same_as<ResultOfFunction, LiteralOrIri>,
                  "Template argument of `LiftStringFunction` must return `Id` "
                  "or `LiteralOrIri`");
    using Result =
        std::conditional_t<ad_utility::isSimilar<ResultOfFunction, Id>, Id,
                           IdOrLocalVocabEntry>;
    if ((... || !arguments.has_value())) {
      return Result{Id::makeUndefined()};
    }
    return Result{std::invoke(Function{}, std::move(arguments.value())...)};
  }
};

// Create an expression of type `T` from the given `children`, which are moved
// into it.
CPP_template(typename T, typename... C)(
    requires(concepts::same_as<SparqlExpression::Ptr, C>&&...))
SparqlExpression::Ptr make(C&... children) {
  return std::make_unique<T>(std::move(children)...);
}

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
      requires(concepts::same_as<C, SparqlExpression::Ptr>&&...) &&
      (sizeof...(C) + 1 == N))
  explicit StringExpressionImplImpl(Ptr child, C... children) {
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
  [[nodiscard]] bool isDeterministic() const override {
    return impl_->isDeterministic();
  }

 private:
  ql::span<Ptr> childrenImpl() override { return impl_->children(); }
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
