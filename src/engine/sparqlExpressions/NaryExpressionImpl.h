//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NARYEXPRESSIONIMPL_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NARYEXPRESSIONIMPL_H

#include <absl/functional/bind_front.h>

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/CryptographicHashUtils.h"

namespace sparqlExpression::detail {

template <typename NaryOperation>
class NaryExpressionStronglyTyped : public SparqlExpression {
  CPP_assert(isOperation<NaryOperation>);

 public:
  static constexpr size_t N = NaryOperation::N;
  using Children = std::array<SparqlExpression::Ptr, N>;

 private:
  Children children_;

 public:
  // Construct from an array of `N` child expressions.
  explicit NaryExpressionStronglyTyped(Children&& children);

  // Construct from `N` child expressions. Each of the children must have a type
  // `std::unique_ptr<SubclassOfSparqlExpression>`.
  CPP_template(typename... C)(
      requires(concepts::convertible_to<C, SparqlExpression::Ptr>&&...)
          CPP_and(sizeof...(C) ==
                  N)) explicit NaryExpressionStronglyTyped(C... children)
      : NaryExpressionStronglyTyped{Children{std::move(children)...}} {}

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override;

  // _________________________________________________________________________
  [[nodiscard]] std::string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

 private:
  // _________________________________________________________________________
  ql::span<SparqlExpression::Ptr> childrenImpl() override;

  // Evaluate the `naryOperation` on the `operands` using the `context`.
  CPP_template(typename... Operands)(
      requires(SingleExpressionResult<Operands>&&...)) static ExpressionResult
      evaluateOnChildrenOperands(NaryOperation naryOperation,
                                 EvaluationContext* context,
                                 Operands&&... operands) {
    // Perform a more efficient calculation if a specialized function exists
    // that matches all operands.
    if (isAnySpecializedFunctionPossible(naryOperation._specializedFunctions,
                                         operands...)) {
      auto optionalResult = evaluateOnSpecializedFunctionsIfPossible(
          naryOperation._specializedFunctions,
          std::forward<Operands>(operands)...);
      AD_CORRECTNESS_CHECK(optionalResult);
      return std::move(optionalResult.value());
    }

    // We have to first determine the number of results we will produce.
    auto targetSize = getResultSize(*context, operands...);

    // The result is a constant iff all the results are constants.
    constexpr static bool resultIsConstant =
        (... && isConstantResult<Operands>);

    // The generator for the result of the operation.
    auto resultGenerator =
        applyOperation(targetSize, naryOperation, context, AD_FWD(operands)...);

    // Compute the result.
    using ResultType = ql::ranges::range_value_t<decltype(resultGenerator)>;
    VectorWithMemoryLimit<ResultType> result{context->_allocator};
    result.reserve(targetSize);
    ql::ranges::move(resultGenerator, std::back_inserter(result));

    if constexpr (resultIsConstant) {
      AD_CORRECTNESS_CHECK(result.size() == 1);
      return std::move(result[0]);
    } else {
      return result;
    }
  }
};

// Takes a `Function` that returns a numeric value (integral or floating point)
// and converts it to a function, that takes the same arguments and returns the
// same result, but the return type is the `NumericValue` variant.
template <typename Function, bool nanToUndef = false>
struct NumericIdWrapper {
  // Note: Sonarcloud suggests `[[no_unique_address]]` for the following member,
  // but adding it causes an internal compiler error in Clang 16.
  Function function_{};
  template <typename... Args>
  Id operator()(Args&&... args) const {
    return makeNumericId<nanToUndef>(function_(AD_FWD(args)...));
  }
};

// Takes a `Function` that takes and returns numeric values (integral or
// floating point) and converts it to a function, that takes the same arguments
// and returns the same result, but the arguments and the return type are the
// `NumericValue` variant.
template <typename Function, bool NanOrInfToUndef = false>
struct MakeNumericExpression {
  template <typename... Args>
  Id operator()(const Args&... args) const {
    CPP_assert((concepts::same_as<std::decay_t<Args>, NumericValue> && ...));
    auto visitor = [](const auto&... t) {
      if constexpr ((... ||
                     std::is_same_v<NotNumeric, std::decay_t<decltype(t)>>)) {
        return Id::makeUndefined();
      } else {
        return makeNumericId<NanOrInfToUndef>(Function{}(t...));
      }
    };
    return std::visit(visitor, args...);
  }
};

// Two short aliases to make the instantiations more readable.
template <typename... T>
using FV = FunctionAndValueGetters<T...>;

template <size_t N, typename X, typename... T>
using NARY = NaryExpressionStronglyTyped<Operation<N, X, T...>>;

// True iff all types `Ts` are `SetOfIntervals`.
struct AreAllSetOfIntervals {
  template <typename... Ts>
  constexpr bool operator()(const Ts&... t) const {
    return (... && ad_utility::isSimilar<std::decay_t<decltype(t)>,
                                         ad_utility::SetOfIntervals>);
  }
};

template <typename F>
using SET = SpecializedFunction<F, AreAllSetOfIntervals>;

using ad_utility::SetOfIntervals;

// The types for the concrete MultiBinaryExpressions and UnaryExpressions.
using TernaryBool = EffectiveBooleanValueGetter::Result;

// _____________________________________________________________________________
template <typename Op>
NaryExpressionStronglyTyped<Op>::NaryExpressionStronglyTyped(
    Children&& children)
    : children_{std::move(children)} {}

// _____________________________________________________________________________

template <typename NaryOperation>
ExpressionResult NaryExpressionStronglyTyped<NaryOperation>::evaluate(
    EvaluationContext* context) const {
  auto resultsOfChildren = ad_utility::applyFunctionToEachElementOfTuple(
      [context](const auto& child) { return child->evaluate(context); },
      children_);

  // Bind the `evaluateOnChildrenOperands` to a lambda.
  auto evaluateOnChildOperandsAsLambda = [](auto&&... args) {
    return evaluateOnChildrenOperands(AD_FWD(args)...);
  };

  // A function that only takes several `ExpressionResult`s,
  // and evaluates the expression.
  auto evaluateOnChildrenResults = absl::bind_front(
      ad_utility::visitWithVariantsAndParameters,
      evaluateOnChildOperandsAsLambda, NaryOperation{}, context);

  return std::apply(evaluateOnChildrenResults, std::move(resultsOfChildren));
}

// _____________________________________________________________________________
template <typename Op>
ql::span<SparqlExpression::Ptr>
NaryExpressionStronglyTyped<Op>::childrenImpl() {
  return {children_.data(), children_.size()};
}

// __________________________________________________________________________
template <typename Op>
[[nodiscard]] std::string NaryExpressionStronglyTyped<Op>::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  std::string key = typeid(*this).name();
  key += ad_utility::lazyStrJoin(
      children_ | ql::views::transform([&varColMap](const auto& child) {
        return child->getCacheKey(varColMap);
      }),
      "");
  return key;
}

// ============================================================================
// Type-erased expression support.
// ============================================================================

// Type-erased version of the `NaryExpression` class. Much cheaper to compile,
// but also slower in the execution. It is only templated on the signature of
// its core implementation function; all other implementation details (the
// actual function, as well as the value getters used to create the inputs)
// are type-erased.
template <typename Ret, typename... Args>
class NaryExpressionTypeErasedImpl : public SparqlExpression {
 public:
  static constexpr size_t N = sizeof...(Args);
  using Children = std::array<SparqlExpression::Ptr, N>;
  using Function = std::function<Ret(Args...)>;

  // Type-erased `std::function` that converts the `ExpressionResult` variant
  // into a type-erased range of `Args`s.
  template <typename Arg>
  using TypeErasedGetter = std::function<ad_utility::InputRangeTypeErased<Arg>(
      ExpressionResult, EvaluationContext*, size_t)>;

  // Tuple of type-erased value-getters, one for each argument of this
  // function.
  using Getters = std::tuple<TypeErasedGetter<Args>...>;

 private:
  Children children_;
  Function function_;
  Getters getters_;

 public:
  // Construct from an array of `N` child expressions, as well as the
  // `function` and `getters`.
  explicit NaryExpressionTypeErasedImpl(Function function, Getters getters,
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
          [this](auto&&... args) { return function_(AD_FWD(args)...); },
          AD_FWD(tuple));
    };
    auto resultGenerator =
        ql::views::transform(ql::ranges::ref_view(zipper), onTuple);
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

// ============================================================================
// Implementation of `NaryExpressionTypeErased` using the
// `NaryExpressionTypeErasedImpl` from above: It has the same template argument
// as `NaryExpression`, but uses type erasure for the function and value
// getters.
// ============================================================================

// Helper: given a Function type and a tuple of (strongly typed) function
// getters, compute the corresponding `NaryExpressionTypeErasedImpl` type and
// provide a helper function to create the tuple of type-erased value getters.
template <typename Func, typename VGTuple>
struct TypeErasedNaryHelper;

template <typename Func, typename... VGs>
struct TypeErasedNaryHelper<Func, std::tuple<VGs...>> {
  using Res = std::invoke_result_t<Func, typename VGs::Value...>;
  using BaseType = NaryExpressionTypeErasedImpl<Res, typename VGs::Value...>;
  static auto makeGetters() {
    return typename BaseType::Getters{TypeErasedValueGetter<VGs>{}...};
  }
};

// Forward declaration, because we implement pattern matching using partial
// specializtion below.
template <typename NaryOperation>
class NaryExpressionTypeErased;

// Partial specialization for `Operation<N, FV<Function, ValueGetters...>,
// SpecializedFunctions...>` As that is exactly the pattern the strongly typed
// `NaryExpression` uses. Note: The `SpezializedFunctions` (which implement more
// efficient evaluation in some circumstances, but are not required for
// correctness) are ignored in the type-erased case, which is only used during
// development for cheaper compilation.
template <size_t N, typename Function, typename... ValueGetters,
          typename... SFs>
class NaryExpressionTypeErased<
    Operation<N, FunctionAndValueGetters<Function, ValueGetters...>, SFs...>>
    : public TypeErasedNaryHelper<
          Function, ValueGetterPack<N, std::tuple<ValueGetters...>>>::BaseType {
  using Helper =
      TypeErasedNaryHelper<Function,
                           ValueGetterPack<N, std::tuple<ValueGetters...>>>;
  using Base = Helper::BaseType;
  using Children = std::array<SparqlExpression::Ptr, N>;

 public:
  // Construct from an array of `N` child expressions.
  explicit NaryExpressionTypeErased(Children&& children)
      : Base(Function{}, Helper::makeGetters(), std::move(children)) {}

  // Construct from `N` child expressions.
  CPP_template(typename... C)(
      requires(concepts::convertible_to<C, SparqlExpression::Ptr>&&...)
          CPP_and(sizeof...(C) ==
                  N)) explicit NaryExpressionTypeErased(C... children)
      : NaryExpressionTypeErased{Children{std::move(children)...}} {}
};

// A unified alias for either the type-erased or the strongly-typed expressions.
#ifdef _QLEVER_TYPE_ERASED_EXPRESSIONS
template <typename... Args>
using NaryExpression = NaryExpressionTypeErased<Args...>;
#else
template <typename... Args>
using NaryExpression = NaryExpressionStronglyTyped<Args...>;
#endif

// Define a class `Name` that is a strong typedef (via inheritance) from
// `NaryExpression<N, X, ...>`.
// The strong typedef (vs. a simple `using` declaration) is used to improve
// compiler messages as the resulting class has a short and descriptive name.
#define NARY_EXPRESSION(Name, N, X, ...)                                     \
  class Name : public NaryExpression<detail::Operation<N, X, __VA_ARGS__>> { \
    using Base = NaryExpression<Operation<N, X, __VA_ARGS__>>;               \
    using Base::Base;                                                        \
  };
}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NARYEXPRESSIONIMPL_H
