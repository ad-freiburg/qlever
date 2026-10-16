// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EFFECTIVEBOOLEANVALUEHELPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EFFECTIVEBOOLEANVALUEHELPERS_H

#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

namespace sparqlExpression::detail {

// Call `onTrueRow(i)` for each row `i` in `[0, numRows)` for which the
// effective boolean value of `result` is true, as in a `FILTER`. According to
// the standard, values like zero, UNDEF, and empty strings are false. Check
// the cancellation for each row.
CPP_template(typename T,
             typename F)(requires SingleExpressionResult<
                         T>) void forEachTrueRow(T&& result, size_t numRows,
                                                 EvaluationContext& context,
                                                 const F& onTrueRow) {
  using ValueGetter = EffectiveBooleanValueGetter;
  ValueGetter valueGetter{};
  size_t row = 0;
  for (auto&& value : makeGenerator(AD_FWD(result), numRows, &context)) {
    if (valueGetter(value, &context) == ValueGetter::Result::True) {
      onTrueRow(row);
    }
    context.cancellationHandle_->throwIfCancelled();
    ++row;
  }
}

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EFFECTIVEBOOLEANVALUEHELPERS_H
