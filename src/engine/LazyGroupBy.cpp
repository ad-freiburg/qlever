//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "LazyGroupBy.h"

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "global/Constants.h"

using groupBy::detail::VectorOfAggregationData;

template <size_t NUM_GROUP_COLUMNS>
LazyGroupBy<NUM_GROUP_COLUMNS>::LazyGroupBy(
    LocalVocab& localVocab,
    std::vector<GroupBy::HashMapAliasInformation> aggregateAliases,
    const std::vector<size_t>& groupByCols,
    const ad_utility::AllocatorWithLimit<Id>& allocator)
    : localVocab_{localVocab},
      aggregateAliases_{std::move(aggregateAliases)},
      groupByCols_{groupByCols},
      aggregationData_{allocator, aggregateAliases_, NUM_GROUP_COLUMNS} {
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
template <size_t NUM_GROUP_COLUMNS>
void LazyGroupBy<NUM_GROUP_COLUMNS>::resetAggregationData() {
  for (const auto& alias : aggregateAliases_) {
    for (const auto& aggregateInfo : alias.aggregateInfo_) {
      std::visit([]<VectorOfAggregationData T>(T& arg) { arg.at(0).reset(); },
                 aggregationData_.getAggregationDataVariant(
                     aggregateInfo.aggregateDataIndex_));
    }
  }
}

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
void LazyGroupBy<NUM_GROUP_COLUMNS>::commitRow(
    IdTable& resultTable,
    sparqlExpression::EvaluationContext& evaluationContext,
    const GroupBy& groupBy) {
  resultTable.emplace_back();
  for (size_t rowIndex = 0; rowIndex < groupByCols_.size(); ++rowIndex) {
    resultTable.back()[rowIndex] = currentGroupBlock_.at(rowIndex).second;
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
template <size_t NUM_GROUP_COLUMNS>
void LazyGroupBy<NUM_GROUP_COLUMNS>::processNextBlock(
    sparqlExpression::EvaluationContext& evaluationContext, size_t beginIndex,
    size_t endIndex) {
  size_t blockSize = endIndex - beginIndex;
  evaluationContext._beginIndex = beginIndex;
  evaluationContext._endIndex = endIndex;

  for (auto& aggregateAlias : aggregateAliases_) {
    for (auto& aggregate : aggregateAlias.aggregateInfo_) {
      // Evaluate child expression on block
      auto exprChildren = aggregate.expr_->children();
      sparqlExpression::ExpressionResult expressionResult =
          exprChildren[0]->evaluate(&evaluationContext);

      std::visit(
          [this, blockSize, &evaluationContext,
           &aggregate]<sparqlExpression::SingleExpressionResult T>(
              T&& singleResult) {
            auto generator = sparqlExpression::detail::makeGenerator(
                std::forward<T>(singleResult), blockSize, &evaluationContext);

            for (const auto& val : generator) {
              addToAggregateFunction(aggregate.aggregateDataIndex_, val,
                                     &evaluationContext);
            }
          },
          std::move(expressionResult));
    }
  }
}

// _____________________________________________________________________________

template <size_t NUM_GROUP_COLUMNS>
void LazyGroupBy<NUM_GROUP_COLUMNS>::populateGroupBlock(const IdTable& idTable,
                                                        size_t row) {
  if (currentGroupBlock_.empty()) {
    for (size_t col : groupByCols_) {
      currentGroupBlock_.emplace_back(col, idTable(0, col));
    }
  } else {
    for (auto& columnPair : currentGroupBlock_) {
      columnPair.second = idTable(row, columnPair.first);
    }
  }
}

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
bool LazyGroupBy<NUM_GROUP_COLUMNS>::rowMatchesCurrentBlock(
    const IdTable& idTable, size_t row) {
  return std::all_of(currentGroupBlock_.begin(), currentGroupBlock_.end(),
                     [&](const auto& columns) {
                       return idTable(row, columns.first) == columns.second;
                     });
}

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
std::vector<Id> LazyGroupBy<NUM_GROUP_COLUMNS>::getCurrentRow(
    size_t outputSize) const {
  std::vector result(outputSize, Id::makeUndefined());
  for (const auto& columnPair : currentGroupBlock_) {
    result.at(columnPair.first) = columnPair.second;
  }
  return result;
}

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
void LazyGroupBy<NUM_GROUP_COLUMNS>::substituteGroupVariable(
    const std::vector<GroupBy::ParentAndChildIndex>& occurrences,
    ValueId value) {
  for (const auto& occurrence : occurrences) {
    auto newExpression =
        std::make_unique<sparqlExpression::IdExpression>(std::move(value));

    occurrence.parent_->replaceChild(occurrence.nThChild_,
                                     std::move(newExpression));
  }
}

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
ValueId LazyGroupBy<NUM_GROUP_COLUMNS>::calculateAggregateResult(
    size_t aggregateIndex) {
  return std::visit(
      [this](const auto& aggregateData) {
        return aggregateData.at(0).calculateResult(&localVocab_);
      },
      aggregationData_.getAggregationDataVariant(aggregateIndex));
}

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
void LazyGroupBy<NUM_GROUP_COLUMNS>::addToAggregateFunction(
    size_t aggregateIndex, const std::variant<ValueId, LocalVocabEntry>& id,
    const sparqlExpression::EvaluationContext* ctx) {
  std::visit(
      [&id, ctx](auto& aggregateData) {
        return aggregateData.at(0).addValue(id, ctx);
      },
      aggregationData_.getAggregationDataVariant(aggregateIndex));
}

// _____________________________________________________________________________

template class LazyGroupBy<0>;
template class LazyGroupBy<1>;
template class LazyGroupBy<2>;
template class LazyGroupBy<3>;
template class LazyGroupBy<4>;
template class LazyGroupBy<5>;
static_assert(
    5 == DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE,
    "Please update the explicit template instantiations in `LazyGroupBy.cpp`.");
