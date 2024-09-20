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
  for (const auto& aggregateInfo : allAggregateInfoView()) {
    visitAggregate(
        [&aggregateInfo]<VectorOfAggregationData T>(T& arg) {
          if constexpr (std::same_as<typename T::value_type,
                                     GroupConcatAggregationData>) {
            arg.emplace_back(aggregateInfo.aggregateType_.separator_.value());
          } else {
            arg.emplace_back();
          }
        },
        aggregateInfo);
  }
}

// _____________________________________________________________________________
void LazyGroupBy::resetAggregationData() {
  for (const auto& aggregateInfo : allAggregateInfoView()) {
    visitAggregate([]<VectorOfAggregationData T>(T& arg) { arg.at(0).reset(); },
                   aggregateInfo);
  }
}

// _____________________________________________________________________________
void LazyGroupBy::commitRow(
    IdTable& resultTable,
    sparqlExpression::EvaluationContext& evaluationContext,
    const GroupBy::GroupBlock& currentGroupBlock, const GroupBy& groupBy) {
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
void LazyGroupBy::processBlock(
    sparqlExpression::EvaluationContext& evaluationContext, size_t beginIndex,
    size_t endIndex) {
  size_t blockSize = endIndex - beginIndex;
  evaluationContext._beginIndex = beginIndex;
  evaluationContext._endIndex = endIndex;

  for (const auto& aggregateInfo : allAggregateInfoView()) {
    // Evaluate child expression on block
    auto exprChildren = aggregateInfo.expr_->children();
    AD_CORRECTNESS_CHECK(exprChildren.size() == 1);
    sparqlExpression::ExpressionResult expressionResult =
        exprChildren[0]->evaluate(&evaluationContext);

    visitAggregate(
        [blockSize,
         &evaluationContext]<sparqlExpression::SingleExpressionResult T>(
            VectorOfAggregationData auto& aggregateData, T&& singleResult) {
          auto generator = sparqlExpression::detail::makeGenerator(
              std::forward<T>(singleResult), blockSize, &evaluationContext);

          for (const auto& val : generator) {
            aggregateData.at(0).addValue(val, &evaluationContext);
          }
        },
        aggregateInfo, std::move(expressionResult));
  }
}

// _____________________________________________________________________________
void LazyGroupBy::visitAggregate(
    const auto& visitor,
    const GroupBy::HashMapAggregateInformation& aggregateInfo,
    auto&&... additionalVariants) {
  std::visit(visitor,
             aggregationData_.getAggregationDataVariant(
                 aggregateInfo.aggregateDataIndex_),
             AD_FWD(additionalVariants)...);
}
