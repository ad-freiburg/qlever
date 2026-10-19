// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_HOMOGENEOUSDATEEXPRESSIONHELPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_HOMOGENEOUSDATEEXPRESSIONHELPERS_H

#include <optional>
#include <type_traits>

#include "engine/sparqlExpressions/ExpressionResultHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/ChunkedForLoop.h"

namespace sparqlExpression::detail::homogeneousDate {

// Helpers for evaluating date-aware binary expressions directly on
// DateYearOrDuration values after classifying their operands.
//
// `NumericOrDateValueGetter` returns a variant that requires dispatch for every
// result row. These helpers bypass the getter and variant dispatch when both
// operands contain only Datatype::Date IDs. This storage tag includes dates,
// dateTimes, durations and large years; the original typed operation preserves
// their distinctions and arithmetic semantics. Other inputs retain the existing
// evaluation paths.

// Return whether the getter and operand representation support typed date
// execution. Only `NumericOrDateValueGetter` with Id constants or vectors of
// Ids is supported, because its Date alternative is DateYearOrDuration.
template <typename Getter, typename Operand>
constexpr bool supportsDateFastPath() {
  using T = std::decay_t<Operand>;
  if constexpr (!std::is_same_v<Getter, NumericOrDateValueGetter>) {
    return false;
  } else if constexpr (isVectorResult<T>) {
    return std::is_same_v<ql::ranges::range_value_t<T>, Id>;
  } else {
    return std::is_same_v<T, Id>;
  }
}

// Return whether a supported operand contains only `Datatype::Date` IDs.
// Vectors must match the context size; empty vectors return false.
// Classification stops at the first non-date ID and checks cancellation while
// scanning.
template <typename Operand>
bool isDateOperand(const Operand& operand, EvaluationContext* context) {
  if constexpr (isVectorResult<std::decay_t<Operand>>) {
    AD_CORRECTNESS_CHECK(operand.size() == context->size());
    // Avoid scanning ordinary numeric columns and don't classify empty input.
    if (operand.empty() || operand[0].getDatatype() != Datatype::Date) {
      return false;
    }
    bool allDates = true;
    ad_utility::chunkedForLoop<1000>(
        0, operand.size(),
        [&operand, &allDates](size_t i, const auto& stop) {
          if (operand[i].getDatatype() != Datatype::Date) {
            allDates = false;
            stop();
          }
        },
        [context]() { context->cancellationHandle_->throwIfCancelled(); });
    return allDates;
  } else {
    return operand.getDatatype() == Datatype::Date;
  }
}

// Return an indexed getter that extracts DateYearOrDuration values from an
// already-classified operand. Vector elements are extracted on demand; a
// constant is extracted once and reused for every index. The operand must
// outlive the returned getter.
template <typename Operand>
auto makeDateGetter(const Operand& operand) {
  if constexpr (isVectorResult<std::decay_t<Operand>>) {
    return [&operand](size_t i) { return operand[i].getDate(); };
  } else {
    return [value = operand.getDate()](size_t) { return value; };
  }
}

// Try to evaluate a binary operation directly on DateYearOrDuration values.
// Both getters and operand representations must support typed date execution,
// all operand IDs must have `Datatype::Date`, and at least one operand must be
// a vector. On success, return a result vector using the original typed
// function. Otherwise return std::nullopt so the caller can use the existing
// evaluation paths. This helper never invokes the generic value getters.
template <typename Function, typename LeftGetter, typename RightGetter,
          typename Left, typename Right>
std::optional<ExpressionResult> tryEvaluate(const Left& left,
                                            const Right& right,
                                            EvaluationContext* context) {
  if constexpr (supportsDateFastPath<LeftGetter, Left>() &&
                supportsDateFastPath<RightGetter, Right>() &&
                (isVectorResult<std::decay_t<Left>> ||
                 isVectorResult<std::decay_t<Right>>)) {
    if (isDateOperand(left, context) && isDateOperand(right, context)) {
      auto getLeft = makeDateGetter(left);
      auto getRight = makeDateGetter(right);
      Function function;
      VectorWithMemoryLimit<Id> result{context->_allocator};
      result.resize(context->size());
      ad_utility::chunkedForLoop<1000>(
          0, context->size(),
          [&result, &function, &getLeft, &getRight](size_t i) {
            result[i] = function(getLeft(i), getRight(i));
          },
          [context]() { context->cancellationHandle_->throwIfCancelled(); });
      return ExpressionResult{std::move(result)};
    }
  }
  return std::nullopt;
}

}  // namespace sparqlExpression::detail::homogeneousDate

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_HOMOGENEOUSDATEEXPRESSIONHELPERS_H
