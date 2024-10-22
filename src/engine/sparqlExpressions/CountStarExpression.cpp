//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/CountStarExpression.h"

#include <absl/strings/str_cat.h>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {

// _____________________________________________________________________________
CountStarExpression::CountStarExpression(bool distinct) : distinct_{distinct} {
  setIsInsideAggregate();
}

// _____________________________________________________________________________
ExpressionResult CountStarExpression::evaluate(
    sparqlExpression::EvaluationContext* ctx) const {
  // The case of a simple `COUNT *` is trivial, just return the size
  // of the evaluation context.
  if (!distinct_) {
    return Id::makeFromInt(static_cast<int64_t>(ctx->size()));
  }

  // For the distinct case, we make a deep copy of the IdTable, sort it,
  // and then count the number of distinct elements. This could be more
  // efficient if we knew that the input was already sorted, but we leave that
  // open for another time.
  IdTable table{ctx->_allocator};

  // We only need to copy columns that are actually visible. Columns that are
  // hidden, e.g. because they weren't selected in a subquery, shouldn't be
  // part of the DISTINCT computation.

  auto varToColNoInternalVariables =
      ctx->_variableToColumnMap | std::views::filter([](const auto& varAndIdx) {
        return !varAndIdx.first.name().starts_with(INTERNAL_VARIABLE_PREFIX);
      });
  table.setNumColumns(std::ranges::distance(varToColNoInternalVariables));
  table.resize(ctx->size());
  auto checkCancellation = [ctx]() {
    ctx->cancellationHandle_->throwIfCancelled();
  };
  size_t targetColIdx = 0;
  for (const auto& [sourceColIdx, _] :
       varToColNoInternalVariables | std::views::values) {
    const auto& sourceColumn = ctx->_inputTable.getColumn(sourceColIdx);
    std::ranges::copy(sourceColumn.begin() + ctx->_beginIndex,
                      sourceColumn.begin() + ctx->_endIndex,
                      table.getColumn(targetColIdx).begin());
    ++targetColIdx;
    checkCancellation();
  }
  ctx->_qec.getSortPerformanceEstimator().throwIfEstimateTooLong(
      table.numRows(), table.numColumns(), ctx->deadline_,
      "Sort for COUNT(DISTINCT *)");
  ad_utility::callFixedSize(table.numColumns(), [&table]<int I>() {
    Engine::sort<I>(&table, std::ranges::lexicographical_compare);
  });
  return Id::makeFromInt(
      static_cast<int64_t>(Engine::countDistinct(table, checkCancellation)));
}

// _____________________________________________________________________________
SparqlExpression::AggregateStatus CountStarExpression::isAggregate() const {
  return distinct_ ? AggregateStatus::DistinctAggregate
                   : AggregateStatus::NonDistinctAggregate;
}

// _____________________________________________________________________________
string CountStarExpression::getCacheKey(
    [[maybe_unused]] const VariableToColumnMap& varColMap) const {
  return absl::StrCat("COUNT * with DISTINCT = ", distinct_);
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeCountStarExpression(bool distinct) {
  return std::make_unique<CountStarExpression>(distinct);
}
}  // namespace sparqlExpression
