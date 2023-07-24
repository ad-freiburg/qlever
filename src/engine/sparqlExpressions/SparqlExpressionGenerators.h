//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 15.09.21.
// Several templated helper functions that are used for the Expression module

#ifndef QLEVER_SPARQLEXPRESSIONGENERATORS_H
#define QLEVER_SPARQLEXPRESSIONGENERATORS_H

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/Generator.h"

namespace sparqlExpression::detail {

/// Convert a variable to a vector of all the Ids it is bound to in the
/// `context`.
inline std::span<const ValueId> getIdsFromVariable(
    const ::Variable& variable, const EvaluationContext* context,
    size_t beginIndex, size_t endIndex) {
  const auto& inputTable = context->_inputTable;

  if (!context->_variableToColumnMap.contains(variable)) {
    throw std::runtime_error(
        "Variable " + variable.name() +
        " could not be mapped to context column of expression evaluation");
  }

  const size_t columnIndex =
      context->_variableToColumnMap.at(variable).columnIndex_;

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
/// `SingleExpressionResult`s.
template <SingleExpressionResult T>
requires isConstantResult<T>
cppcoro::generator<T> resultGenerator(T constant, size_t numItems) {
  for (size_t i = 0; i < numItems; ++i) {
    co_yield constant;
  }
}

template <typename T>
requires isVectorResult<T> auto resultGenerator(T vector, size_t numItems)
    -> cppcoro::generator<std::remove_reference_t<decltype(vector[0])>> {
  AD_CONTRACT_CHECK(numItems == vector.size());
  for (auto& element : vector) {
    auto cpy = std::move(element);
    co_yield cpy;
  }
}

inline cppcoro::generator<Id> resultGenerator(ad_utility::SetOfIntervals set,
                                              size_t targetSize) {
  size_t i = 0;
  for (const auto& [begin, end] : set._intervals) {
    while (i < begin) {
      co_yield Id::makeFromBool(false);
      ++i;
    }
    while (i < end) {
      co_yield Id::makeFromBool(true);
      ++i;
    }
  }
  while (i++ < targetSize) {
    co_yield Id::makeFromBool(false);
  }
}

/// Return a generator that yields `numItems` many items for the various
/// `SingleExpressionResult`
template <SingleExpressionResult Input>
auto makeGenerator(Input&& input, size_t numItems,
                   const EvaluationContext* context) {
  if constexpr (ad_utility::isSimilar<::Variable, Input>) {
    std::span<const ValueId> inputWithVariableResolved{
        getIdsFromVariable(std::forward<Input>(input), context)};
    return resultGenerator(inputWithVariableResolved, numItems);
  } else {
    return resultGenerator(std::forward<Input>(input), numItems);
  }
}

/// Generate `numItems` many values from the `input` and apply the
/// `valueGetter` to each of the values.
inline auto valueGetterGenerator =
    []<typename ValueGetter, SingleExpressionResult Input>(
        size_t numElements, EvaluationContext* context, Input&& input,
        ValueGetter&& valueGetter)
    -> cppcoro::generator<std::invoke_result_t<
        ValueGetter,
        typename decltype(makeGenerator(AD_FWD(input), 0, nullptr))::value_type,
        EvaluationContext*>> {
  for (auto singleInput :
       makeGenerator(std::forward<Input>(input), numElements, context)) {
    co_yield valueGetter(std::move(singleInput), context);
  }
};

/// Do the following `numItems` times: Obtain the next elements e_1, ..., e_n
/// from the `generators` and yield `function(e_1, ..., e_n)`, also as a
/// generator.
inline auto applyFunction = []<typename Function, typename... Generators>(
                                Function&& function, size_t numItems,
                                Generators... generators)
    -> cppcoro::generator<std::invoke_result_t<
        std::decay_t<Function>, typename Generators::value_type...>> {
  // A tuple holding one iterator to each of the generators.
  std::tuple iterators{generators.begin()...};

  auto functionOnIterators = [&function](auto&&... iterators) {
    return function(*iterators...);
  };

  for (size_t i = 0; i < numItems; ++i) {
    co_yield std::apply(functionOnIterators, iterators);

    // Increase all the iterators.
    std::apply([](auto&&... its) { (..., ++its); }, iterators);
  }
};

/// Return a generator that returns the `numElements` many results of the
/// `Operation` applied to the `operands`
template <typename Operation, SingleExpressionResult... Operands>
auto applyOperation(size_t numElements, Operation&&, EvaluationContext* context,
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

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SPARQLEXPRESSIONGENERATORS_H
