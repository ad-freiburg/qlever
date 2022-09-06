//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 15.09.21.
// Several templated helper functions that are used for the Expression module

#ifndef QLEVER_SPARQLEXPRESSIONGENERATORS_H
#define QLEVER_SPARQLEXPRESSIONGENERATORS_H

#include "../../util/Generator.h"
#include "./SparqlExpression.h"

namespace sparqlExpression::detail {

// Internal implementation of `getIdsFromVariable` (see below).
// It is required because of the `CALL_FIXED_SIZE` mechanism for the `IdTable`s.
template <size_t WIDTH>
void getIdsFromVariableImpl(VectorWithMemoryLimit<StrongId>& result,
                            const Variable& variable,
                            EvaluationContext* context) {
  AD_CHECK(result.empty());
  const auto inputTable = context->_inputTable.asStaticView<WIDTH>();

  const size_t beginIndex = context->_beginIndex;
  const size_t endIndex = context->_endIndex;

  if (!context->_variableToColumnAndResultTypeMap.contains(
          variable._variable)) {
    throw std::runtime_error(
        "Variable " + variable._variable +
        " could not be mapped to context column of expression evaluation");
  }

  const size_t columnIndex =
      context->_variableToColumnAndResultTypeMap.at(variable._variable).first;

  result.reserve(endIndex - endIndex);
  for (size_t i = beginIndex; i < endIndex; ++i) {
    result.push_back(StrongId{inputTable(i, columnIndex)});
  }
}

/// Convert a variable to a vector of all the Ids it is bound to in the
/// `context`.
// TODO<joka921> Restructure QLever to column based design, then this will
// become a noop;
inline VectorWithMemoryLimit<StrongId> getIdsFromVariable(
    const Variable& variable, EvaluationContext* context) {
  auto cols = context->_inputTable.cols();
  VectorWithMemoryLimit<StrongId> result{context->_allocator};
  CALL_FIXED_SIZE_1(cols, getIdsFromVariableImpl, result, variable, context);
  return result;
}

/// Generators that yield `numItems` items for the various
/// `SingleExpressionResult`s.
template <SingleExpressionResult T>
requires isConstantResult<T> cppcoro::generator<T> resultGenerator(
    T constant, size_t numItems) {
  for (size_t i = 0; i < numItems; ++i) {
    co_yield constant;
  }
}

template <SingleExpressionResult T>
requires isVectorResult<T> cppcoro::generator<typename T::value_type>
resultGenerator(T vector, size_t numItems) {
  AD_CHECK(numItems == vector.size());
  for (auto& element : vector) {
    co_yield std::move(element);
  }
}

inline cppcoro::generator<StrongIdWithResultType> resultGenerator(
    StrongIdsWithResultType ids, size_t targetSize) {
  AD_CHECK(targetSize == ids.size());
  for (const auto& strongId : ids._ids) {
    co_yield StrongIdWithResultType{strongId, ids._type};
  }
}

inline cppcoro::generator<Bool> resultGenerator(ad_utility::SetOfIntervals set,
                                                size_t targetSize) {
  size_t i = 0;
  for (const auto& [begin, end] : set._intervals) {
    while (i < begin) {
      co_yield false;
      ++i;
    }
    while (i < end) {
      co_yield true;
      ++i;
    }
  }
  while (i++ < targetSize) {
    co_yield false;
  }
}

/// Return a generator that yields `numItems` many items for the various
/// `SingleExpressionResult`
template <SingleExpressionResult Input>
auto makeGenerator(Input&& input, [[maybe_unused]] size_t numItems,
                   EvaluationContext* context) {
  if constexpr (ad_utility::isSimilar<Variable, Input>) {
    // TODO: Also directly write a generator that lazily gets the Ids in chunks.
    StrongIdsWithResultType inputWithVariableResolved{
        getIdsFromVariable(std::forward<Input>(input), context),
        context->_variableToColumnAndResultTypeMap.at(input._variable).second};
    return resultGenerator(std::move(inputWithVariableResolved), numItems);
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
        typename decltype(makeGenerator(Input{}, 0, nullptr))::value_type,
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
