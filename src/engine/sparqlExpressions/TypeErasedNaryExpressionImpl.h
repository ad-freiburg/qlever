// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_TYPEERASEDNARYEXPRESSIONIMPL_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_TYPEERASEDNARYEXPRESSIONIMPL_H

#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"

namespace sparqlExpression::detail {

// Base declaration.
template <typename T>
class TypeErasedNaryExpression;

inline bool isConstantExpressionResult(const ExpressionResult& res) {
  return std::visit(
      [](const auto& el) {
        return isConstantResult<std::decay_t<decltype(el)>>;
      },
      res);
}

// Type erased version of the `NaryExpression` class. Much cheaper to compile,
// but also slower in the execution. It is only template on the signature of its
// core implementation function, all other implementation (the actual function,
// as well as the value getters used to create the inputs) are type-erased.
template <typename Ret, typename... Args>
class TypeErasedNaryExpression<Ret(Args...)> : public SparqlExpression {
 public:
  static constexpr size_t N = sizeof...(Args);
  using Children = std::array<SparqlExpression::Ptr, N>;
  using Function = std::function<Ret(Args...)>;

  // type-erased `std::function` that converts the `ExpressionResult` variant
  // into a type-erased range of `Args`s.
  template <typename Arg>
  using Getter = std::function<ad_utility::InputRangeTypeErased<Arg>(
      ExpressionResult, EvaluationContext*, size_t)>;

  // Tuple of type-erased value-getters, one for each argument of this function.
  using Getters = std::tuple<Getter<Args>...>;

 private:
  Children children_;
  Function function_;
  Getters getters_;

 public:
  // Construct from an array of `N` child expressions, as well as the `function`
  // and `getters`.
  explicit TypeErasedNaryExpression(Function function, Getters getters,
                                    Children&& children)
      : children_{std::move(children)},
        function_{std::move(function)},
        getters_{std::move(getters)} {}

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
    return std::apply(
        [&](auto&&... child) {
          return evaluateOnChildrenOperands(context,
                                            child->evaluate(context)...);
        },
        children_);
  }

  // _________________________________________________________________________
  [[nodiscard]] std::string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    const auto& signatureId = typeid(*this);
    const auto& functionId = function_.target_type();
    std::string key =
        absl::StrCat(signatureId.name(), "_", signatureId.hash_code(), "_",
                     functionId.name(), "_", functionId.hash_code(), "_");
    for (const auto& child : children_) {
      key += child->getCacheKey(varColMap);
    }
    return key;
  }

 private:
  // _________________________________________________________________________
  ql::span<SparqlExpression::Ptr> childrenImpl() override { return children_; }

  // Evaluate the `naryOperation` on the `operands` using the `context`.
  CPP_variadic_template(typename... Operands)(requires(
      ...&& std::is_same_v<ExpressionResult, Operands>)) ExpressionResult
      evaluateOnChildrenOperands(EvaluationContext* context,
                                 Operands... operands) const {
    // We have to first determine the number of results the expression will
    // produce.
    auto targetSize = context->size();

    if ((... && isConstantExpressionResult(operands))) {
      targetSize = 1;
    }

    // A `zip_view` of the result of all the value getters applied to their
    // respective child result.
    auto zipper = std::apply(
        [&](const auto&... getters) {
          return ::ranges::views::zip(ad_utility::OwningView{
              getters(std::move(operands), context, targetSize)}...);
        },
        getters_);

    // Apply the `function_` on a tuple of arguments (the `zipper` above has
    // tuples as value and reference type).
    auto onTuple = [&](auto&& tuple) {
      return std::apply(
          [&](auto&&... args) { return function_(AD_FWD(args)...); },
          AD_FWD(tuple));
    };
    auto resultGenerator = ::ranges::views::transform(
        ad_utility::OwningView{std::move(zipper)}, onTuple);
    // Compute the result.
    VectorWithMemoryLimit<Ret> result{context->_allocator};
    result.reserve(targetSize);
    ql::ranges::move(resultGenerator, std::back_inserter(result));

    if (result.size() == 1) {
      return std::move(result.at(0));
    } else {
      return result;
    }
  }
};

// A struct that converts one of the overloaded `value getters` from
// `SparqlExpressionValueGetters.h` into a callable that takes an
// `ExpressionResult` variant, and returns a `TypeErasedInputRange`. This is
// exactly the signature that the `TypeErasedNaryExpression` above requires.
template <typename ValueGetter>
struct TypeErasedValueGetter {
  ad_utility::InputRangeTypeErased<typename ValueGetter::Value> operator()(
      ExpressionResult res, EvaluationContext* context, size_t size) const {
    /// Generate `numItems` many values from the `input` and apply the
    /// `valueGetter` to each of the values.
    return std::visit(
        [&](auto&& input)
            -> ad_utility::InputRangeTypeErased<typename ValueGetter::Value> {
          return ad_utility::InputRangeTypeErased{valueGetterGenerator(
              size, context, std::move(input), ValueGetter{})};
        },
        std::move(res));
  }
};

// `NaryExpression`  is either a `TypeErasedNaryExpression` or a
// `StronglyTypedNaryExpression`, depending on the flag
// `_QLEVER_TYPE_ERASED_EXPRESSIONS`.
#ifdef _QLEVER_TYPE_ERASED_EXPRESSIONS
template <typename Operation, typename... ValueGetters>
using NaryExpression = TypeErasedNaryExpression<
    std::invoke_result_t<Operation, typename ValueGetters::Value...>(
        typename ValueGetters::Value...)>;
#else
template <typename Operation, typename... ValueGetters>
using NaryExpression = NaryExpressionStronglyTyped<
    detail::Operation<sizeof...(ValueGetters), FV<Operation, ValueGetters...>>>;
#endif

// Create a lambda that takes the children of an expression (as
// `SparqlExpression::Ptr` arguments), and returns a
// `std::unique_ptr<SubClass>`. `SubClass` must be derived from
// `NaryExpression<Operation, ValueGetters...>` . The result is type-erased or
// strongly typed depending on the value of `_QLEVER_TYPE_ERASED_EXPRESSIONS`.
template <typename SubClass, typename Operation, typename... ValueGetters>
static constexpr auto namedExpressionFactory() {
  using Impl = NaryExpression<Operation, ValueGetters...>;
  static_assert(std::is_base_of_v<Impl, SubClass>);
#ifdef _QLEVER_TYPE_ERASED_EXPRESSIONS
  return [](auto... childPtrs) -> SparqlExpression::Ptr {
    return std::make_unique<SubClass>(
        Operation{}, std::tuple<TypeErasedValueGetter<ValueGetters>...>{},
        std::array{std::move(childPtrs)...});
  };
#else
  return [](auto... childPtrs) {
    return std::make_unique<SubClass>(std::array{std::move(childPtrs)...});
  };
#endif
}

// Same as `namedExpressionFactory` above, but doesn't explicitly specify a base
// class, but create `NaryOperation<Operation, ValueGetters>` itself.
template <typename Operation, typename... ValueGetters>
static constexpr auto expressionFactory() {
  using Impl = NaryExpression<Operation, ValueGetters...>;
  return namedExpressionFactory<Impl, Operation, ValueGetters...>();
}
}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_SPARQLEXPRESSIONS_TYPEERASEDNARYEXPRESSIONIMPL_H
