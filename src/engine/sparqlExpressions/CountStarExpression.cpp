//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <absl/strings/str_cat.h>

#include "engine/Engine.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "global/RuntimeParameters.h"

namespace sparqlExpression {
class CountStarExpression : public SparqlExpression {
 private:
  bool distinct_;

 public:
  explicit CountStarExpression(bool distinct) : distinct_{distinct} {}

  // _________________________________________________________________________
  ExpressionResult evaluate(
      sparqlExpression::EvaluationContext* ctx) const override {
    if (!distinct_) {
      return Id::makeFromInt(static_cast<int64_t>(ctx->size()));
    }
    // For the distinct case, we make a deep copy of the IdTable, sort it,
    // and then count the number of distinct elements.
    IdTable table{ctx->_allocator};
    table.setNumColumns(ctx->_inputTable.numColumns());
    table.insertAtEnd(ctx->_inputTable.begin() + ctx->_beginIndex,
                      ctx->_inputTable.begin() + ctx->_endIndex);
    std::vector<ColumnIndex> indices;
    // TODO<joka921> Implement a `sort`, that simply sorts by all the columns.
    indices.reserve(table.numColumns());
    for (auto i : ad_utility::integerRange(table.numColumns())) {
      indices.push_back(i);
    }
    // TODO<joka921> proper timeout for sorting operations
    /*
    auto sortEstimateCancellationFactor =
        RuntimeParameters().get<"sort-estimate-cancellation-factor">();
    if (ctx->_qec.getSortPerformanceEstimator().estimatedSortTime(
            table.numRows(), table.numColumns()) >
        remainingTime() * sortEstimateCancellationFactor) {
      // The estimated time for this sort is much larger than the actually
      // remaining time, cancel this operation
      throw ad_utility::CancellationException(
          "Sort operation was canceled, because time estimate exceeded "
          "remaining time by a factor of " +
          std::to_string(sortEstimateCancellationFactor));
    }
     */
    Engine::sort(table, indices);
    auto checkCancellation = [ctx]() {
      ctx->cancellationHandle_->throwIfCancelled();
    };
    return Id::makeFromInt(
        static_cast<int64_t>(Engine::countDistinct(table, checkCancellation)));
  }

  // COUNT * technically is an aggregate.
  bool isAggregate() const override { return true; }

  // No variables.
  std::vector<Variable> getUnaggregatedVariables() override { return {}; }

  bool isDistinct() const override { return distinct_; }

  string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    return absl::StrCat("COUNT * with DISTINCT = ", distinct_);
  }

  std::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};

SparqlExpression::Ptr makeCountStarExpression(bool distinct) {
  return std::make_unique<CountStarExpression>(distinct);
}
}  // namespace sparqlExpression
