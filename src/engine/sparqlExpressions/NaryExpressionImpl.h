//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NARYEXPRESSIONIMPL_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NARYEXPRESSIONIMPL_H

#include <absl/functional/bind_front.h>
#include <absl/strings/str_join.h>

#include "engine/sparqlExpressions/ExpressionResultHelpers.h"
#include "engine/sparqlExpressions/HomogeneousNumericExpressionHelpers.h"
#include "engine/sparqlExpressions/NumericExpressionWrappers.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/CryptographicHashUtils.h"

namespace sparqlExpression::detail {

// Common storage and metadata handling for expressions with a fixed number of
// child expressions.
template <size_t N>
class NaryExpressionBase : public SparqlExpression {
 protected:
  using Children = std::array<SparqlExpression::Ptr, N>;
  Children children_;

  explicit NaryExpressionBase(Children&& children)
      : children_{std::move(children)} {}

  [[nodiscard]] std::string getCacheKeyForChildren(
      const VariableToColumnMap& varColMap) const {
    return absl::StrJoin(
        children_ | ql::views::transform([&varColMap](const auto& child) {
          return child->getCacheKey(varColMap);
        }),
        "");
  }

 public:
  [[nodiscard]] std::string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    return std::string{typeid(*this).name()} +
           getCacheKeyForChildren(varColMap);
  }

  // Deterministic iff all children are deterministic.
  [[nodiscard]] bool isDeterministic() const override {
    return areChildrenDeterministic();
  }

 private:
  ql::span<SparqlExpression::Ptr> childrenImpl() override {
    return {children_.data(), children_.size()};
  }
};

// Create an indexed accessor for an N-ary operand. Vector operands are read at
// the requested row, while constant operands always return the same converted
// value.
template <typename ValueGetter, typename Operand>
auto makeIndexedNaryValueGetter(Operand&& operand, EvaluationContext* context) {
  using OperandType = std::decay_t<Operand>;

  if constexpr (isVectorResult<OperandType>) {
    AD_CORRECTNESS_CHECK(operand.size() == context->size());

    return [&operand, context](size_t i) {
      return ValueGetter{}(operand[i], context);
    };
  } else {
    return [value = ValueGetter{}(AD_FWD(operand), context)](size_t) mutable {
      return value;
    };
  }
}

// Return whether all value getters and converted operands support the numeric
// fast path. The index sequence matches each value getter with the
// corresponding converted operand type.
template <typename ValueGetters, typename ConvertedOperands, size_t... I>
constexpr bool canUseNumericFastPathImpl(std::index_sequence<I...>) {
  using namespace homogeneousNumeric;

  return (... &&
          (supportsNumericFastPath<std::tuple_element_t<I, ValueGetters>> &&
           supportsNumericFastPathOperand<
               std::tuple_element_t<I, ConvertedOperands>>()));
}

// Convenience wrapper that checks all entries of `ConvertedOperands`.
template <typename ValueGetters, typename ConvertedOperands>
constexpr bool canUseNumericFastPath() {
  return canUseNumericFastPathImpl<ValueGetters, ConvertedOperands>(
      std::make_index_sequence<std::tuple_size_v<ConvertedOperands>>{});
}

template <typename NaryOperation>
class NaryExpressionStronglyTyped
    : public NaryExpressionBase<NaryOperation::N> {
  CPP_assert(isOperation<NaryOperation>);

 public:
  static constexpr size_t N = NaryOperation::N;
  using Base = NaryExpressionBase<N>;
  using Children = typename Base::Children;

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

 private:
  // Evaluate the `naryOperation` on the `operands` using the `context`.
  // Is deliberately a functor, s.t. we can pass it to `bind_front` etc,
  // although the call operator is overloaded.
  struct EvaluateOnChildOperands {
    CPP_template(typename... Operands)(
        requires(SingleExpressionResult<Operands>&&...)) ExpressionResult
    operator()(NaryOperation naryOperation, EvaluationContext* context,
               Operands&&... operands) const {
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

      using ValueGetters = typename NaryOperation::ValueGetters;

      auto convertedOperands =
          std::tuple{convertToVectorOrConstant(AD_FWD(operands), context)...};

      using ConvertedOperands = decltype(convertedOperands);

      constexpr bool useNumericFastPath =
          canUseNumericFastPath<ValueGetters, ConvertedOperands>();

      // For numeric N-ary expressions, first try homogeneous execution, then a
      // speculative majority-type path. Fall back to the generic indexed
      // evaluation if neither optimization applies.
      if constexpr (useNumericFastPath && !resultIsConstant) {
        auto classifications = std::apply(
            [context](const auto&... values) {
              return homogeneousNumeric::classifyNumericOperands(context,
                                                                 values...);
            },
            convertedOperands);

        auto homogeneousTypes =
            homogeneousNumeric::getHomogeneousNumericTypes(classifications);

        if (homogeneousTypes.has_value()) {
          return homogeneousNumeric::dispatchNumericTypes(
              homogeneousTypes.value(),
              [&convertedOperands,
               context]<typename... NumericTypes>(NumericTypes...) {
                return homogeneousNumeric::evaluateHomogeneousNumericOperation<
                    typename NaryOperation::Function,
                    typename NumericTypes::type...>(convertedOperands, context);
              });
        }

        auto majorityTypes =
            homogeneousNumeric::getMajorityNumericTypes(classifications);

        if (majorityTypes.has_value()) {
          return homogeneousNumeric::dispatchNumericTypes(
              majorityTypes.value(),
              [&convertedOperands,
               context]<typename... NumericTypes>(NumericTypes...) {
                return homogeneousNumeric::
                    evaluateSpeculativeNaryNumericOperation<
                        typename NaryOperation::Function, ValueGetters,
                        typename NumericTypes::type...>(convertedOperands,
                                                        context);
              });
        }
      }

      // Create one indexed getter per operand, matching each operand with its
      // corresponding value getter from `ValueGetters`.
      auto indexedGetters = [&]<size_t... I>(std::index_sequence<I...>) {
        return std::tuple{
            makeIndexedNaryValueGetter<std::tuple_element_t<I, ValueGetters>>(
                std::get<I>(convertedOperands), context)...};
      }(std::index_sequence_for<Operands...>{});

      // Evaluate one result row by invoking every indexed getter at `i` and
      // passing the resulting values to the N-ary function.
      auto computeValue = [&](size_t i) {
        return std::apply(
            [&](auto&... getters) {
              return naryOperation._function(getters(i)...);
            },
            indexedGetters);
      };

      using ResultType =
          PromoteToLocalVocabEntry<std::decay_t<decltype(computeValue(0))>>;

      VectorWithMemoryLimit<ResultType> result{context->_allocator};
      result.reserve(targetSize);

      // Keep the named temporary to work around a Clang 16/17 code-generation
      // crash when `computeValue(i)` is passed directly to this function.
      for (size_t i = 0; i < targetSize; ++i) {
        auto value = computeValue(i);
        result.push_back(promoteToLocalVocabEntry(
            std::move(value), context->getLocalVocabContext()));
      }

      if constexpr (resultIsConstant) {
        AD_CORRECTNESS_CHECK(result.size() == 1);
        return std::move(result[0]);
      } else {
        return result;
      }
    }
  };
};

// _____________________________________________________________________________
template <typename Op>
NaryExpressionStronglyTyped<Op>::NaryExpressionStronglyTyped(
    Children&& children)
    : Base{std::move(children)} {}

// _____________________________________________________________________________

template <typename NaryOperation>
ExpressionResult NaryExpressionStronglyTyped<NaryOperation>::evaluate(
    EvaluationContext* context) const {
  auto resultsOfChildren = ad_utility::applyFunctionToEachElementOfTuple(
      [context](const auto& child) { return child->evaluate(context); },
      this->children_);

  // A function that only takes several `ExpressionResult`s,
  // and evaluates the expression.
  // Avoid an inline NaryOperation{} temporary here: Clang 16/17 can crash
  // during
  // IR generation for numeric operation wrappers (for example, RoundImpl).
  NaryOperation naryOperation;
  auto evaluateOnChildrenResults = absl::bind_front(
      ad_utility::visitWithVariantsAndParameters, EvaluateOnChildOperands{},
      std::move(naryOperation), context);

  return std::apply(evaluateOnChildrenResults, std::move(resultsOfChildren));
}

// ============================================================================
// Type-erased expression support.
// ============================================================================

// Type-erased version of the `NaryExpression` class. Much cheaper to compile,
// but also slower in the execution. It is only templated on the signature of
// its core implementation function; all other implementation details (the
// actual function, as well as the value getters used to create the inputs)
// are type-erased.
// Note: The `getCacheKey` function of this class itself will not work
// correctly, as it doesn't account for the "same function, different value
// getter" case. But when used as an implementation detail of the
// `NaryExpressionTypeErased` class below, then this works, because the value
// getters become part of the classes name/typeid.
template <typename Ret, typename... Args>
class NaryExpressionTypeErasedImpl
    : public NaryExpressionBase<sizeof...(Args)> {
 public:
  static constexpr size_t N = sizeof...(Args);
  using Base = NaryExpressionBase<N>;
  using Children = typename Base::Children;
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
  Function function_;
  Getters getters_;

 public:
  // Construct from an array of `N` child expressions, as well as the
  // `function` and `getters`.
  explicit NaryExpressionTypeErasedImpl(Function function, Getters getters,
                                        Children&& children)
      : Base{std::move(children)},
        function_{std::move(function)},
        getters_{std::move(getters)} {}

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
    return std::apply(
        [&](auto&&... child) {
          return evaluateOnChildrenOperands(context,
                                            child->evaluate(context)...);
        },
        this->children_);
  }

  // _________________________________________________________________________
  [[nodiscard]] std::string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    const auto& signatureId = typeid(*this);
    const auto& functionId = function_.target_type();

    std::string key =
        absl::StrCat(signatureId.name(), "_", signatureId.hash_code(), "_",
                     functionId.name(), "_", functionId.hash_code(), "_");

    return key + this->getCacheKeyForChildren(varColMap);
  }

 private:
  // Evaluate the `naryOperation` on the `operands` using the `context`.
  CPP_variadic_template(typename... Operands)(
      requires(...&& std::is_same_v<ExpressionResult, Operands>))
  ExpressionResult evaluateOnChildrenOperands(EvaluationContext* context,
                                              Operands... operands) const {
    // We have to first determine the number of results the expression will
    // produce.
    auto targetSize = context->size();
    bool resultIsConstant = (... && isConstantExpressionResult(operands));
    if (resultIsConstant) {
      targetSize = 1;
    }

    // A `zip_view` of the result of all the value getters applied to their
    // respective child result.
    auto zipper = std::apply(
        [&](const auto&... getters) {
          return ::ranges::views::zip(
              getters(std::move(operands), context, targetSize)...);
        },
        getters_);

    // Apply the `function_` on a tuple of arguments (the `zipper` above has
    // tuples as value and reference type).
    auto onTuple = [&](auto&& tuple) {
      return promoteToLocalVocabEntry(
          std::apply(
              [this](auto&&... args) { return function_(AD_FWD(args)...); },
              AD_FWD(tuple)),
          context->getLocalVocabContext());
    };
    auto resultGenerator =
        ql::views::transform(ql::ranges::ref_view(zipper), onTuple);
    // Compute the result.
    VectorWithMemoryLimit<PromoteToLocalVocabEntry<std::decay_t<Ret>>> result{
        context->_allocator};
    result.reserve(targetSize);
    ql::ranges::move(resultGenerator, std::back_inserter(result));

    if (resultIsConstant) {
      AD_CORRECTNESS_CHECK(result.size() == 1);
      return std::move(result[0]);
    } else {
      return result;
    }
  }
};

// ============================================================================
// Implementation of `NaryExpressionTypeErased` using the
// `NaryExpressionTypeErasedImpl` from above: It has the same template
// argument as `NaryExpression`, but uses type erasure for the function and
// value getters.
// ============================================================================

// Helper: given a Function type and a tuple of (strongly typed) function
// getters, compute the corresponding `NaryExpressionTypeErasedImpl` type and
// provide a helper function to create the tuple of type-erased value getters.
template <typename Func, typename VGTuple>
struct TypeErasedNaryHelper;

template <typename Func, typename... VGs>
struct TypeErasedNaryHelper<Func, std::tuple<VGs...>> {
  // `std::decay_t` ensures a value type, not a reference. Without this,
  // expressions that use `ql:identity` as their functor, because the
  // `ValueGetter`s do all the work would lead to dangling stack references.
  // The strongly-typed path handles the same issue in `applyFunction`.
  using Res = std::decay_t<std::invoke_result_t<Func, typename VGs::Value...>>;
  using BaseType = NaryExpressionTypeErasedImpl<Res, typename VGs::Value...>;
  static auto makeGetters() {
    return typename BaseType::Getters{TypeErasedValueGetter<VGs>{}...};
  }
};

// Forward declaration, because we implement pattern matching using partial
// specialization below.
template <typename NaryOperation>
class NaryExpressionTypeErased;

// Partial specialization for `Operation<N, FV<Function, ValueGetters...>,
// SpecializedFunctions...>` As that is exactly the pattern the strongly typed
// `NaryExpression` uses. Note: The `SpezializedFunctions` (which implement
// more efficient evaluation in some circumstances, but are not required for
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
  using Base = typename Helper::BaseType;
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
  }

// Two short aliases to make the instantiations more readable.
template <typename... T>
using FV = FunctionAndValueGetters<T...>;

template <size_t N, typename X, typename... T>
using NARY = NaryExpression<Operation<N, X, T...>>;

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
}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NARYEXPRESSIONIMPL_H
