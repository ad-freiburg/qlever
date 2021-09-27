//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

//
// Created by johannes on 15.09.21.
// Several templated helper functions that are used for the Expression module

#ifndef QLEVER_SPARQLEXPRESSIONHELPERS_H
#define QLEVER_SPARQLEXPRESSIONHELPERS_H

#include "./SparqlExpression.h"

namespace sparqlExpression::detail {
/// Convert a variable to a vector of all the Ids it is bound to in the
/// `context`.
// TODO<joka921> Restructure QLever to column based design, then this will
// become a noop;
VectorWithMemoryLimit<StrongId> getIdsFromVariable(const Variable& variable, EvaluationContext* context);

/// Internal helper function
/// Converts all the SingleExpressionResults to a vector-like type.
/// This means that variables are converted to the Ids they are bound to and setOfIntervals are
/// expanded to vector<bool>
///
/// \param targetSize The total size of the result vectors. Must be specified for the setOfInterval
/// case. \param context  Needed for the expansion from `Variable` to Id
template <SingleExpressionResult T>
auto possiblyExpand(T&& childResult, [[maybe_unused]] size_t targetSize,
                    EvaluationContext* context) {
  if constexpr (ad_utility::isSimilar<ad_utility::SetOfIntervals, T>) {
    VectorWithMemoryLimit<bool> vec{context->_allocator};
    vec.resize(targetSize);
    ad_utility::toBitContainer(std::move(childResult), targetSize, begin(vec));
    return vec;
  } else if constexpr (ad_utility::isSimilar<Variable, T>) {
    return StrongIdsWithResultType{getIdsFromVariable(std::forward<T>(childResult), context),
                                context->_variableToColumnAndResultTypeMap.at(childResult._variable).second};
  } else if constexpr (isVectorResult<T>) {
    AD_CHECK(childResult.size() == targetSize);
    return std::forward<T>(childResult);
  } else {
    static_assert(isConstantResult<T>);
    return std::forward<T>(childResult);
  }
};

/// Internal helper function
/// Converts a vector-like datatype (the result of `possiblyExpand`) into
/// a lambda which can be called with an integer index i to get the i-th value.
/// Note that for constants (vectors of size 1), we will always get the same
/// value.
template <typename T>
auto makeExtractor(T&& expandedResult) {
  return [expanded = std::move(expandedResult)](size_t index) mutable {
    if constexpr (ad_utility::isSimilar<T, StrongIdsWithResultType>) {
      // TODO<joka921> This is the cleanest, but not necessarily the fastest
      // way to deal with variable values.
      return StrongIdWithResultType{expanded._ids[index], expanded._type};
    } else if constexpr (ad_utility::isVector<T>) {
      // TODO<joka921>:: This branch is very easy to predict.
      // Still consider performing it somewhere else, when tuning the
      // expressions for performance.
      if (expanded.size() == 1) {
        /// This is actually a constant which always has the same value,
        /// independent of the index.
        index = 0;
      }
      if constexpr (ad_utility::isSimilar<VectorWithMemoryLimit<bool>, T>) {
        // special case because of the strange bit-references
        // TODO<joka921> Consider handling bools with another type, if this
        // has performance implications.
        return bool{expanded[index]};
      } else {
        return expanded[index];
      }
    } else {
      static_assert(!std::is_same_v<Variable, T>);
      return std::forward<T>(expanded);
    }
  };
}

/// Internal helper function
/// Apply a ValueExtractor (see file TODO for examples) to a single value
/// that was retrieved by calling the result of makeExtractor
template <typename T, typename ValueExtractor>
auto extractValue(T&& singleValue, ValueExtractor valueExtractor, EvaluationContext* context) {
  if constexpr (std::is_same_v<StrongIdsWithResultType, std::decay_t<T>>) {
    return valueExtractor(singleValue._ids, singleValue._type, context);
  } else if constexpr (std::is_same_v<StrongIdWithResultType, std::decay_t<T>>) {
    return valueExtractor(singleValue._id, singleValue._type, context);
  } else if constexpr (ad_utility::isSimilar<std::vector<bool>::reference, T>) {
    return valueExtractor(bool{singleValue}, context);
  } else {
    return valueExtractor(singleValue, context);
  }
};

/// The concatenation of `possiblyExpand` `makeExtractor` and `extractValue`
/// return a lambda that can be called with a single integer argument, the `index`
/// and returns the `index`-th argument from the childResult, possibly converted
/// by the `ValueExtractor`
template <SingleExpressionResult T, typename ValueExtractor>
auto makeExtractorFromChildResult(T&& childResult, [[maybe_unused]] size_t targetSize,
                                  EvaluationContext* context, ValueExtractor v) {
  auto extractor = makeExtractor(possiblyExpand(std::forward<T>(childResult), targetSize, context));
  return [ extractor = std::move(extractor), v = std::move(v),
           context ]<typename UseRawValue = std::false_type>(
      size_t index, UseRawValue u = UseRawValue{}) mutable {
    if constexpr (u.value) {
      return extractValue(extractor(index), ActualValueGetter{}, context);
    } else {
      return extractValue(extractor(index), v, context);
    }
  };
}

/// In the actual evaluation routines, we may use the cheaper RangeCalculation
/// if all the arguments are setOfIntervals::Set, and if a RangeCalculation
/// was actually specified. This variable performs this check.
template <typename RangeCalculation, typename... Results>
constexpr static bool rangeCalculationIsAllowed =
    !ad_utility::isSimilar<RangeCalculation, NoRangeCalculation> &&
    (... && ad_utility::isSimilar<Results, ad_utility::SetOfIntervals>);

// Get the number of values, that a SingleExpressionResult encodes, e.g. 1 for a
// constant
template <SingleExpressionResult T>
size_t getSizeOfSingleExpressionResult(const T& x,
                                       [[maybe_unused]] const EvaluationContext& context) {
  if constexpr (isVectorResult<T>) {
    return x.size();
  } else if constexpr (std::is_same_v<ad_utility::SetOfIntervals, T> ||
                       std::is_same_v<Variable, T>) {
    return context._endIndex - context._beginIndex;
  } else {
    static_assert(isConstantResult<T>);
    return 1;
  }
}

// Return the maximum size among the SingleExpressionResults and assert, that
// their sizes are compatible, meaning that they are either equal to the maximum
// size, or 1. (Else we cannot use these results as input to the same n-ary function, and throw an
// Exception
template <SingleExpressionResult... Ts>
size_t getAndVerifyResultSize(const EvaluationContext& context, const Ts&... inputs) {
  auto resultSize = std::max({getSizeOfSingleExpressionResult(inputs, context)...});
  AD_CHECK((... && (getSizeOfSingleExpressionResult(inputs, context) == resultSize ||
                    getSizeOfSingleExpressionResult(inputs, context) == 1)));
  return resultSize;
}

/// This is the visitor for the `evaluateNaryOperation`-function below.
/// it has exactly the same semantics, but takes the `SingleExpressionResult`s
/// (member types of the `ExpressionResult` variant) instead of `ExpressionResult`s
template <typename RangeCalculation, typename ValueExtractor, typename NaryOperation,
          SingleExpressionResult... Inputs>
ExpressionResult evaluateNaryOperationOnSingleExpressionResults(RangeCalculation rangeCalculation,
                                                                ValueExtractor valueExtractor,
                                                                NaryOperation naryOperation,
                                                                EvaluationContext* context,
                                                                Inputs&&... childResults) {
  // Perform the more efficient range calculation if it is possible.
  if constexpr (rangeCalculationIsAllowed<RangeCalculation, Inputs...>) {
    return rangeCalculation(std::forward<Inputs>(childResults)...);
  }

  // We have to first determine the number of results we will produce.
  auto targetSize = getAndVerifyResultSize(*context, childResults...);

  // The result is a constant iff all the results are constants.
  constexpr static bool resultIsConstant = (... && isConstantResult<Inputs>);

  //  We have to convert the arguments which are variables or sets into
  //  std::vector,
  // the following lambda is able to perform this task

  auto extractorsAndVariableType = std::make_tuple(makeExtractorFromChildResult(
      std::move(childResults), targetSize, context, valueExtractor)...);

  /// This lambda takes all the previously created extractors as context and
  /// creates the actual result of the computation.
  auto create = [&](auto&&... extractors) {
    using ResultType = std::decay_t<decltype(naryOperation(extractors(0)...))>;
    VectorWithMemoryLimit<ResultType> result{context->_allocator};
    result.reserve(targetSize);
    for (size_t i = 0; i < targetSize; ++i) {
      result.push_back(naryOperation(extractors(i)...));
    }

    if constexpr (resultIsConstant) {
      AD_CHECK(result.size() == 1);
      return result[0];
    } else {
      return result;
    }
  };

  // calculate the actual result (the extractors are stored in a tuple, so
  // we have to use std::apply)
  auto result = std::apply(create, std::move(extractorsAndVariableType));

  // We always return a vector. In the case of a constant result, it only
  // has one element.
  return ExpressionResult(std::move(result));
}

/// Evaluate a n-ary operation on n `ExpressionResults`
/// For the meaning of the template arguments, see the `SparqlExpression.h` header
template <typename RangeCalculation, typename ValueExtractor, typename NaryOperation,
          typename... Expr>
requires(...&& ad_utility::isSimilar<ExpressionResult, Expr>) ExpressionResult
    evaluateNaryOperation(RangeCalculation rangeCalculation, ValueExtractor valueExtractor,
                          NaryOperation naryOperation, EvaluationContext* context,
                          Expr&&... childResults) {
  auto visitor = [&]<SingleExpressionResult... E>(E && ... children) {
    return evaluateNaryOperationOnSingleExpressionResults(
        rangeCalculation, valueExtractor, naryOperation, context, std::forward<E>(children)...);
  };
  return std::visit(visitor, std::move(childResults)...);
}
}  // namespace sparqlExpression::detail

#endif  // QLEVER_SPARQLEXPRESSIONHELPERS_H
