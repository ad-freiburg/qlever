//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

// Several templated helper functions that are used for the Expression module

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORS_H

#include <absl/functional/bind_front.h>

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/Iterators.h"

namespace sparqlExpression::detail {

/// Convert a variable to a vector of all the Ids it is bound to in the
/// `context`.
inline ql::span<const ValueId> getIdsFromVariable(
    const ::Variable& variable, const EvaluationContext* context,
    size_t beginIndex, size_t endIndex) {
  const auto& inputTable = context->_inputTable;

  const auto& varToColMap = context->_variableToColumnMap;
  auto it = varToColMap.find(variable);
  AD_CONTRACT_CHECK(it != varToColMap.end());

  const size_t columnIndex = it->second.columnIndex_;

  ql::span<const ValueId> completeColumn = inputTable.getColumn(columnIndex);

  AD_CONTRACT_CHECK(beginIndex <= endIndex &&
                    endIndex <= completeColumn.size());
  return {completeColumn.begin() + beginIndex,
          completeColumn.begin() + endIndex};
}

// Overload that reads the `beginIndex` and the `endIndex` directly from the
// `context
inline ql::span<const ValueId> getIdsFromVariable(
    const ::Variable& variable, const EvaluationContext* context) {
  return getIdsFromVariable(variable, context, context->_beginIndex,
                            context->_endIndex);
}

/// Generators that yield `numItems` items for the various
/// `SingleExpressionResult`s after applying a `Transformation` to them.
/// Typically, this transformation is one of the value getters from
/// `SparqlExpressionValueGetters` with an already bound `EvaluationContext`.
CPP_template(typename T, typename Transformation = std::identity)(
    requires SingleExpressionResult<T> CPP_and isConstantResult<T> CPP_and
        ranges::invocable<Transformation, T>)
    ad_utility::InputRangeTypeErased<std::decay_t<std::invoke_result_t<
        Transformation, T>>> resultGenerator(T constant, size_t numItems,
                                             Transformation transformation =
                                                 {}) {
  using ResultType = std::decay_t<std::invoke_result_t<Transformation, T>>;
  struct State {
    ResultType value_;
    size_t remaining_;

    State(T constant, size_t numItems, Transformation transformation)
        : value_(transformation(constant)), remaining_(numItems) {}
  };
  auto state =
      std::make_shared<State>(constant, numItems, std::move(transformation));
  auto valueProducer = [state]() mutable -> std::optional<ResultType> {
    if (state->remaining_ == 0) {
      return std::nullopt;
    }
    --state->remaining_;
    return state->value_;
  };

  return ad_utility::InputRangeTypeErased<ResultType>{
      ad_utility::InputRangeFromGetCallable(std::move(valueProducer))};
}

CPP_template(typename T, typename Transformation = std::identity)(
    requires ql::ranges::input_range<
        T>) auto resultGenerator(T&& vector, size_t numItems,
                                 Transformation transformation = {}) {
  AD_CONTRACT_CHECK(numItems == vector.size());
  return ad_utility::allView(AD_FWD(vector)) |
         ql::views::transform(std::move(transformation));
}

template <typename Transformation = std::identity>
inline ad_utility::InputRangeTypeErased<
    std::decay_t<std::invoke_result_t<Transformation, Id>>>
resultGenerator(ad_utility::SetOfIntervals set, size_t targetSize,
                Transformation transformation = {}) {
  using ResultType = std::decay_t<std::invoke_result_t<Transformation, Id>>;
  struct State {
    ad_utility::SetOfIntervals set_;
    size_t targetSize_;
    ResultType trueTransformed_;
    ResultType falseTransformed_;
    size_t currentIndex_ = 0;
    size_t currentIntervalIdx_ = 0;
    State(ad_utility::SetOfIntervals set, size_t targetSize,
          Transformation transformation)
        : set_(std::move(set)),
          targetSize_(targetSize),
          trueTransformed_(transformation(Id::makeFromBool(true))),
          falseTransformed_(transformation(Id::makeFromBool(false))) {}
  };

  auto state = std::make_shared<State>(std::move(set), targetSize,
                                       std::move(transformation));

  auto valueProducer = [state]() mutable -> std::optional<ResultType> {
    if (state->currentIndex_ >= state->targetSize_) {
      return std::nullopt;
    }

    while (state->currentIntervalIdx_ < state->set_._intervals.size() &&
           state->currentIndex_ >=
               state->set_._intervals[state->currentIntervalIdx_].second) {
      state->currentIntervalIdx_++;
    }

    bool inInterval =
        state->currentIntervalIdx_ < state->set_._intervals.size() &&
        state->currentIndex_ >=
            state->set_._intervals[state->currentIntervalIdx_].first &&
        state->currentIndex_ <
            state->set_._intervals[state->currentIntervalIdx_].second;

    state->currentIndex_++;
    return inInterval ? state->trueTransformed_ : state->falseTransformed_;
  };

  return ad_utility::InputRangeTypeErased<ResultType>{
      ad_utility::InputRangeFromGetCallable(std::move(valueProducer))};
}

/// Return a generator that yields `numItems` many items for the various
/// `SingleExpressionResult`
CPP_template(typename Input, typename Transformation = std::identity)(
    requires SingleExpressionResult<
        Input>) auto makeGenerator(Input&& input, size_t numItems,
                                   const EvaluationContext* context,
                                   Transformation transformation = {}) {
  if constexpr (ad_utility::isSimilar<::Variable, Input>) {
    return resultGenerator(getIdsFromVariable(AD_FWD(input), context), numItems,
                           transformation);
  } else {
    return resultGenerator(AD_FWD(input), numItems, transformation);
  }
}

/// Generate `numItems` many values from the `input` and apply the
/// `valueGetter` to each of the values.
inline auto valueGetterGenerator =
    CPP_template_lambda()(typename ValueGetter, typename Input)(
        size_t numElements, EvaluationContext* context, Input&& input,
        ValueGetter&& valueGetter)(requires SingleExpressionResult<Input>) {
  auto transformation =
      CPP_template_lambda(context, valueGetter)(typename I)(I && i)(
          requires ranges::invocable<ValueGetter, I&&, EvaluationContext*>) {
    context->cancellationHandle_->throwIfCancelled();
    return valueGetter(AD_FWD(i), context);
  };

  return makeGenerator(AD_FWD(input), numElements, context, transformation);
};

template <typename FunctionType, typename... Generators>
struct ApplyFunctionState {
  using ResultType = std::decay_t<std::invoke_result_t<
      FunctionType, ql::ranges::range_value_t<Generators>...>>;

  std::decay_t<FunctionType> function_;
  std::tuple<std::decay_t<Generators>...> generators_;
  std::tuple<ql::ranges::iterator_t<std::decay_t<Generators>>...> iterators_;
  size_t remaining_;

  ApplyFunctionState(FunctionType f, size_t n, Generators... gens)
      : function_(std::move(f)),
        generators_(std::move(gens)...),
        remaining_(n) {
    iterators_ = std::apply(
        [](auto&... gens) { return std::tuple{gens.begin()...}; }, generators_);
  }
};

template <typename StateType>
auto createValueProducer(std::shared_ptr<StateType> state) {
  using ResultType = typename StateType::ResultType;
  return [state]() mutable -> std::optional<ResultType> {
    if (state->remaining_ == 0) {
      return std::nullopt;
    }
    auto functionOnIterators = [&](auto&&... its) {
      return state->function_(AD_MOVE(*its)...);
    };
    ResultType result = std::apply(functionOnIterators, state->iterators_);
    std::apply([](auto&&... its) { (..., ++its); }, state->iterators_);
    --state->remaining_;
    return result;
  };
}

/// Do the following `numItems` times: Obtain the next elements e_1, ..., e_n
/// from the `generators` and yield `function(e_1, ..., e_n)`, also as a
/// generator.
inline auto applyFunction = [](auto&& function, size_t numItems,
                               auto... generators) {
  using StateType =
      ApplyFunctionState<decltype(function), decltype(generators)...>;
  using ResultType = typename StateType::ResultType;

  auto state =
      std::make_shared<StateType>(std::forward<decltype(function)>(function),
                                  numItems, std::move(generators)...);

  auto valueProducer = createValueProducer(state);

  return ad_utility::InputRangeTypeErased<ResultType>{
      ad_utility::InputRangeFromGetCallable(std::move(valueProducer))};
};

/// Return a generator that returns the `numElements` many results of the
/// `Operation` applied to the `operands`
CPP_template(typename Operation, typename... Operands)(requires(
    SingleExpressionResult<
        Operands>&&...)) auto applyOperation(size_t numElements, Operation&&,
                                             EvaluationContext* context,
                                             Operands&&... operands) {
  using ValueGetters = typename std::decay_t<Operation>::ValueGetters;
  using Function = typename std::decay_t<Operation>::Function;
  static_assert(std::tuple_size_v<ValueGetters> == sizeof...(Operands));

  // Function that takes a single operand and a single value getter and computes
  // the corresponding generator.
  auto getValue = absl::bind_front(valueGetterGenerator, numElements, context);

  // Function that takes all the generators as a parameter pack and computes the
  // generator for the operation result;
  auto getResultFromGenerators =
      absl::bind_front(applyFunction, Function{}, numElements);

  /// The `ValueGetters` are stored in a `std::tuple`, so we have to extract
  /// them via `std::apply`. First set up a lambda that performs the actual
  /// logic on a parameter pack of `ValueGetters`
  auto getResultFromValueGetters = [&](auto&&... valueGetters) {
    // Both `operands` and `valueGetters` are parameter packs of equal size,
    // so there will be one call to `getValue` for each pair of
    // (`operands`, `valueGetter`)
    return getResultFromGenerators(
        getValue(std::forward<Operands>(operands), valueGetters)...);
  };

  return std::apply(getResultFromValueGetters, ValueGetters{});
}

// Return a lambda that takes a `LiteralOrIri` and converts it to an `Id` by
// adding it to the `localVocab`.
inline auto makeStringResultGetter(LocalVocab* localVocab) {
  return [localVocab](const ad_utility::triple_component::LiteralOrIri& str) {
    auto localVocabIndex = localVocab->getIndexAndAddIfNotContained(str);
    return ValueId::makeFromLocalVocabIndex(localVocabIndex);
  };
}

// Return the `Id` if the passed `value` contains one, alternatively add the
// literal or iri in the `value` to the `localVocab` and return the newly
// created `Id` instead.
inline Id idOrLiteralOrIriToId(const IdOrLiteralOrIri& value,
                               LocalVocab* localVocab) {
  return std::visit(
      ad_utility::OverloadCallOperator{[](ValueId id) { return id; },
                                       makeStringResultGetter(localVocab)},
      value);
}

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORS_H
