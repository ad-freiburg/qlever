//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <absl/strings/str_cat.h>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/sparqlExpressions/SparqlExpression.h"

// The implementation of `COUNT *`, deliberately hidden in a `.cpp` file.
namespace sparqlExpression {
class CountStarExpression : public SparqlExpression {
 private:
  bool distinct_;

 public:
  // _________________________________________________________________________
  explicit CountStarExpression(bool distinct) : distinct_{distinct} {}

  // _________________________________________________________________________
  ExpressionResult evaluate(
      sparqlExpression::EvaluationContext* ctx) const override {
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
    // hidden, e.g. because they weren't selectedd in a subquery shouldn't be
    // part of the DISTINCT computation.
    table.setNumColumns(ctx->_variableToColumnMap.size());
    table.resize(ctx->size());
    size_t targetIdx = 0;
    for (const auto& [sourceIdx, _] :
         ctx->_variableToColumnMap | std::views::values) {
      const auto& sourceColumn = ctx->_inputTable.getColumn(sourceIdx);
      std::ranges::copy(sourceColumn.begin() + ctx->_beginIndex,
                        sourceColumn.begin() + ctx->_endIndex,
                        table.getColumn(targetIdx).begin());
      ++targetIdx;
    }
    auto checkCancellation = [ctx]() {
      ctx->cancellationHandle_->throwIfCancelled();
    };
    checkCancellation();
    ctx->_qec.getSortPerformanceEstimator().throwIfEstimateTooLong(
        table.numRows(), table.numColumns(), ctx->deadline_,
        "Sort for COUNT(DISTINCT *)");
    ad_utility::callFixedSize(table.numColumns(), [&table]<int I>() {
      Engine::sort<I>(&table, std::ranges::lexicographical_compare);
    });
    return Id::makeFromInt(
        static_cast<int64_t>(Engine::countDistinct(table, checkCancellation)));
  }

  // COUNT * technically is an aggregate.
  bool isAggregate() const override { return true; }

  // No variables.
  std::vector<Variable> getUnaggregatedVariables() override { return {}; }

  // __________________________________________________________________
  bool isDistinct() const override { return distinct_; }

  // __________________________________________________________________
  string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    return absl::StrCat("COUNT * with DISTINCT = ", distinct_);
  }

  // __________________________________________________________________
  std::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};

// __________________________________________________________________
SparqlExpression::Ptr makeCountStarExpression(bool distinct) {
  return std::make_unique<CountStarExpression>(distinct);
}
}  // namespace sparqlExpression
