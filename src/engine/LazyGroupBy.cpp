//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "LazyGroupBy.h"

using groupBy::detail::VectorOfAggregationData;

// _____________________________________________________________________________
LazyGroupBy::LazyGroupBy(
    LocalVocab& localVocab,
    std::vector<GroupBy::HashMapAliasInformation> aggregateAliases,
    const ad_utility::AllocatorWithLimit<Id>& allocator, size_t numGroupColumns)
    : localVocab_{localVocab},
      aggregateAliases_{std::move(aggregateAliases)},
      aggregationData_{allocator, aggregateAliases_, numGroupColumns} {
  for (const auto& alias : aggregateAliases_) {
    for (const auto& aggregateInfo : alias.aggregateInfo_) {
      std::visit(
          [&aggregateInfo]<VectorOfAggregationData T>(T& arg) {
            if constexpr (std::same_as<typename T::value_type,
                                       GroupConcatAggregationData>) {
              arg.resize(1,
                         GroupConcatAggregationData{
                             aggregateInfo.aggregateType_.separator_.value()});
            } else {
              arg.resize(1);
            }
          },
          aggregationData_.getAggregationDataVariant(
              aggregateInfo.aggregateDataIndex_));
    }
  }
}

// _____________________________________________________________________________
void LazyGroupBy::resetAggregationData() {
  for (const auto& alias : aggregateAliases_) {
    for (const auto& aggregateInfo : alias.aggregateInfo_) {
      std::visit([]<VectorOfAggregationData T>(T& arg) { arg.at(0).reset(); },
                 aggregationData_.getAggregationDataVariant(
                     aggregateInfo.aggregateDataIndex_));
    }
  }
}

// _____________________________________________________________________________
void LazyGroupBy::commitRow(
    IdTable& resultTable,
    sparqlExpression::EvaluationContext& evaluationContext,
    const std::vector<std::pair<size_t, Id>>& currentGroupBlock,
    const GroupBy& groupBy) {
  resultTable.emplace_back();
  size_t colIdx = 0;
  for (const auto& [_, value] : currentGroupBlock) {
    resultTable.back()[colIdx] = value;
    ++colIdx;
  }

  evaluationContext._beginIndex = resultTable.size() - 1;
  evaluationContext._endIndex = resultTable.size();

  for (auto& alias : aggregateAliases_) {
    groupBy.evaluateAlias(alias, &resultTable, evaluationContext,
                          aggregationData_, &localVocab_);
  }
  resetAggregationData();
}

// _____________________________________________________________________________
void LazyGroupBy::processNextBlock(
    sparqlExpression::EvaluationContext& evaluationContext, size_t beginIndex,
    size_t endIndex) {
  size_t blockSize = endIndex - beginIndex;
  evaluationContext._beginIndex = beginIndex;
  evaluationContext._endIndex = endIndex;

  for (auto& aggregateAlias : aggregateAliases_) {
    for (auto& aggregate : aggregateAlias.aggregateInfo_) {
      // Evaluate child expression on block
      auto exprChildren = aggregate.expr_->children();
      AD_CORRECTNESS_CHECK(exprChildren.size() == 1);
      sparqlExpression::ExpressionResult expressionResult =
          exprChildren[0]->evaluate(&evaluationContext);

      std::visit(
          [blockSize,
           &evaluationContext]<sparqlExpression::SingleExpressionResult T>(
              T&& singleResult, VectorOfAggregationData auto& aggregateData) {
            auto generator = sparqlExpression::detail::makeGenerator(
                std::forward<T>(singleResult), blockSize, &evaluationContext);

            for (const auto& val : generator) {
              aggregateData.at(0).addValue(val, &evaluationContext);
            }
          },
          std::move(expressionResult),
          aggregationData_.getAggregationDataVariant(
              aggregate.aggregateDataIndex_));
    }
  }
}
