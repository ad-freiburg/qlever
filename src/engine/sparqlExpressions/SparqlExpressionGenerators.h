//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

// Several templated helper functions that are used for the Expression module

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONGENERATORS_H

// We currently have two implementations for the generators in this file, one
// for the C++20 mode (using generator coroutines), and one for the C++17 mode
// using ql::ranges + type erasure (the type erasure currently is needed to keep
// the compile-times reasonable). We currently keep both implementations because
// the C++17 implementation is slower than the generator-based type erasure for
// reasons we yet have to explore.
#ifndef QLEVER_EXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17
#include <absl/container/inlined_vector.h>
#endif

#include <absl/functional/bind_front.h>

#ifndef QLEVER_EXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17
#include "backports/functional.h"
#endif
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/Generator.h"

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
#ifdef QLEVER_EXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17
// Use range-based implementation for C++17 compatibility.
CPP_template(typename T, typename Transformation = ql::identity)(
    requires SingleExpressionResult<T> CPP_and isConstantResult<T> CPP_and
        ranges::invocable<
            Transformation,
            T>) auto resultGeneratorImpl(T constant, size_t numItems,
                                         Transformation transformation = {}) {
  // We have to use `range-v3` as `views::repeat` is a C++23 feature.
  return ::ranges::repeat_n_view(transformation(constant), numItems);
}
#else
// Use faster coroutine-based implementation.
CPP_template(typename T, typename Transformation = ql::identity)(
    requires SingleExpressionResult<T> CPP_and isConstantResult<T> CPP_and
        ranges::invocable<Transformation, T>)
    cppcoro::generator<const std::decay_t<std::invoke_result_t<
        Transformation, T>>> resultGeneratorImpl(T constant, size_t numItems,
                                                 Transformation transformation =
                                                     {}) {
  auto transformed = transformation(constant);
  for (size_t i = 0; i < numItems; ++i) {
    co_yield transformed;
  }
}
#endif

CPP_template(typename T, typename Transformation = ql::identity)(
    requires ql::ranges::input_range<
        T>) auto resultGeneratorImpl(T&& vector, size_t numItems,
                                     Transformation transformation = {}) {
  AD_CONTRACT_CHECK(numItems == vector.size());
  return ad_utility::allView(AD_FWD(vector)) |
         ql::views::transform(std::move(transformation));
}

#ifdef QLEVER_EXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17
// Use range-based implementation for C++17 compatibility.
template <typename Transformation = ql::identity>
inline auto resultGeneratorImpl(const ad_utility::SetOfIntervals& set,
                                size_t targetSize,
                                Transformation transformation = {}) {
  struct Bounds {
    size_t num_;
    bool value_;
  };
  absl::InlinedVector<Bounds, 10> bounds;
  bounds.reserve(set._intervals.size() * 2 + 1);
  size_t last = 0;
  for (const auto& [lower, upper] : set._intervals) {
    AD_CONTRACT_CHECK(upper <= targetSize,
                      "The size of a `SetOfIntervals` exceeds the total size "
                      "of the evaluation context.");
    if (lower != last) {
      bounds.push_back(Bounds{lower - last, false});
    }
    if (lower != upper) {
      bounds.push_back(Bounds{upper - lower, true});
    }
    last = upper;
  }
  if (last < targetSize) {
    bounds.push_back(Bounds{targetSize - last, false});
  }
  // We have to use `range-v3` as `views::repeat` is a C++23 feature.
  return ad_utility::OwningView{std::move(bounds)} |
         ::ranges::views::transform([transformation](const auto& bound) {
           return ::ranges::views::repeat_n(
               transformation(Id::makeFromBool(bound.value_)), bound.num_);
         }) |
         ::ranges::views::join;
}
#else
// Use faster coroutine-based implementation.
template <typename Transformation = ql::identity>
inline cppcoro::generator<
    const std::decay_t<std::invoke_result_t<Transformation, Id>>>
resultGeneratorImpl(ad_utility::SetOfIntervals set, size_t targetSize,
                    Transformation transformation = {}) {
  size_t i = 0;
  const auto trueTransformed = transformation(Id::makeFromBool(true));
  const auto falseTransformed = transformation(Id::makeFromBool(false));
  if (!set._intervals.empty()) {
    AD_CONTRACT_CHECK(set._intervals.back().second <= targetSize,
                      "The size of a `SetOfIntervals` exceeds the total size "
                      "of the evaluation context.");
  }
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
#endif

// The actual `resultGenerator` that uses type erasure (if not specified
// otherwise) to the `resultGeneratorImpl` to keep the compile times reasonable.
template <typename S, typename Transformation = ql::identity>
inline auto resultGenerator(S&& input, size_t targetSize,
                            Transformation transformation = {}) {
  auto gen =
      resultGeneratorImpl(AD_FWD(input), targetSize, std::move(transformation));
#ifndef QLEVER_EXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17
  // `gen` is already (at least in many cases) type erased because of the nature
  // of C++20 coroutines.
  return gen;
#else
  // Without type erasure, compiling the `sparqlExpressions` module takes a lot
  // of time and memory. In the future we can evaluate the performance of
  // deactivating the type erasure for certain expressions + datatypes (e.g.
  // addition of IDs) etc.
  using V = ql::ranges::range_value_t<decltype(gen)>;

  return ad_utility::InputRangeTypeErased<V>(std::move(gen));
#endif
}
/// Return a generator that yields `numItems` many items for the various
/// `SingleExpressionResult`
CPP_template(typename Input, typename Transformation = ql::identity)(
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
#ifdef QLEVER_EXPRESSION_GENERATOR_BACKPORTS_FOR_CPP17
// Use range-based implementation for C++17 compatibility.
inline auto applyFunction = [](auto function, [[maybe_unused]] size_t numItems,
                               auto... generators) {
  // We have to use `range-v3` as `std::views::zip` is not available in our
  // toolchains.
  return ::ranges::views::zip(ad_utility::RvalueView{
             ad_utility::OwningView{std::move(generators)}}...) |
         ::ranges::views::transform(
             [f = std::move(function)](auto&& tuple) -> decltype(auto) {
               // If the transformation would return an rvalue reference,
               // return a plain value instead (obtained by moving the
               // reference) otherwise we get dangling references, but only
               // in Release builds.
               // TODO<joka921> I don't fully understand yet WHERE the
               // dangling reference comes from.
               using T = decltype(std::apply(f, AD_MOVE(tuple)));
               using R = std::conditional_t<std::is_rvalue_reference_v<T>,
                                            std::decay_t<T>, T>;
               return R{std::apply(f, AD_MOVE(tuple))};
             });
};
#else
// Use faster coroutine-based implementation.
inline auto applyFunction = [](auto function, size_t numItems,
                               auto... generators)
    -> cppcoro::generator<std::invoke_result_t<
        decltype(function),
        ql::ranges::range_value_t<decltype(generators)>...>> {
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
#endif

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
