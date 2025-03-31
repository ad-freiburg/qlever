//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

// Several templated helper functions that are used for the Expression module

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORS_H

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/Generator.h"

namespace sparqlExpression::detail {

/// Convert a variable to a vector of all the Ids it is bound to in the
/// `context`.
inline std::span<const ValueId> getIdsFromVariable(
    const ::Variable& variable, const EvaluationContext* context,
    size_t beginIndex, size_t endIndex) {
  const auto& inputTable = context->_inputTable;

  const auto& varToColMap = context->_variableToColumnMap;
  auto it = varToColMap.find(variable);
  AD_CONTRACT_CHECK(it != varToColMap.end());

  const size_t columnIndex = it->second.columnIndex_;

  std::span<const ValueId> completeColumn = inputTable.getColumn(columnIndex);

  AD_CONTRACT_CHECK(beginIndex <= endIndex &&
                    endIndex <= completeColumn.size());
  return {completeColumn.begin() + beginIndex,
          completeColumn.begin() + endIndex};
}

// Overload that reads the `beginIndex` and the `endIndex` directly from the
// `context
inline std::span<const ValueId> getIdsFromVariable(
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
    cppcoro::generator<const std::decay_t<std::invoke_result_t<
        Transformation, T>>> resultGenerator(T constant, size_t numItems,
                                             Transformation transformation =
                                                 {}) {
  auto transformed = transformation(constant);
  for (size_t i = 0; i < numItems; ++i) {
    co_yield transformed;
  }
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
inline cppcoro::generator<
    const std::decay_t<std::invoke_result_t<Transformation, Id>>>
resultGenerator(ad_utility::SetOfIntervals set, size_t targetSize,
                Transformation transformation = {}) {
  size_t i = 0;
  const auto trueTransformed = transformation(Id::makeFromBool(true));
  const auto falseTransformed = transformation(Id::makeFromBool(false));
  for (const auto& [begin, end] : set._intervals) {
    while (i < begin) {
      co_yield falseTransformed;
      ++i;
    }
    while (i < end) {
      co_yield trueTransformed;
      ++i;
    }
  }
  while (i++ < targetSize) {
    co_yield falseTransformed;
  }
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

/// Do the following `numItems` times: Obtain the next elements e_1, ..., e_n
/// from the `generators` and yield `function(e_1, ..., e_n)`, also as a
/// generator.
inline auto applyFunction = []<typename Function, typename... Generators>(
                                Function&& function, size_t numItems,
                                Generators... generators)
    -> cppcoro::generator<std::invoke_result_t<
        Function, ql::ranges::range_value_t<Generators>...>> {
  // A tuple holding one iterator to each of the generators.
  std::tuple iterators{generators.begin()...};

  auto functionOnIterators = [&function](auto&&... iterators) {
    return function(AD_MOVE(*iterators)...);
  };

  for (size_t i = 0; i < numItems; ++i) {
    co_yield std::apply(functionOnIterators, iterators);

    // Increase all the iterators.
    std::apply([](auto&&... its) { (..., ++its); }, iterators);
  }
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
  auto getValue = std::bind_front(valueGetterGenerator, numElements, context);

  // Function that takes all the generators as a parameter pack and computes the
  // generator for the operation result;
  auto getResultFromGenerators =
      std::bind_front(applyFunction, Function{}, numElements);

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
