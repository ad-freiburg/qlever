//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "LazyGroupBy.h"

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "global/Constants.h"

template <size_t NUM_GROUP_COLUMNS>
LazyGroupBy<NUM_GROUP_COLUMNS>::LazyGroupBy(
    LocalVocab& localVocab,
    std::vector<GroupBy::HashMapAliasInformation> aggregateAliases,
    const std::vector<size_t>& groupByCols)
    : localVocab_{localVocab},
      aggregateAliases_{std::move(aggregateAliases)},
      groupByCols_{groupByCols},
      aggregationData_{ad_utility::makeAllocatorWithLimit<Id>(
                           1_B * sizeof(Id) * NUM_GROUP_COLUMNS),
                       aggregateAliases_, NUM_GROUP_COLUMNS} {}

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
void LazyGroupBy<NUM_GROUP_COLUMNS>::resetAggregationData() {
  for (const auto& alias : aggregateAliases_) {
    for (const auto& aggregateInfo : alias.aggregateInfo_) {
      // TODO<RobinTF> replace typename with SupportedAggregates
      std::visit([]<typename T>(T& arg) { arg.at(0).reset(); },
                 aggregationData_.getAggregationDataVariant(
                     aggregateInfo.aggregateDataIndex_));
    }
  }
}

// _____________________________________________________________________________
template <size_t NUM_GROUP_COLUMNS>
void LazyGroupBy<NUM_GROUP_COLUMNS>::initializeAggregationData() {
  for (const auto& alias : aggregateAliases_) {
    for (const auto& aggregateInfo : alias.aggregateInfo_) {
      // TODO<RobinTF> replace typename with SupportedAggregates
      std::visit(
          [&aggregateInfo]<typename T>(T& arg) {
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
void LazyGroupBy<NUM_GROUP_COLUMNS>::commitRow(
    IdTable& resultTable,
    sparqlExpression::EvaluationContext& evaluationContext) {
  resultTable.emplace_back();
  for (size_t rowIndex = 0; rowIndex < groupByCols_.size(); ++rowIndex) {
    resultTable.back()[rowIndex] = currentGroupBlock_.at(rowIndex).second;
  }

  for (auto& alias : aggregateAliases_) {
    auto& info = alias.aggregateInfo_;

    // Either:
    // - One of the variables occurs at the top. This can be copied as the
    // result
    // - There is only one aggregate, and it appears at the top. No
    // substitutions necessary, can evaluate aggregate and copy results
    // - Possibly multiple aggregates and occurrences of grouped variables.
    // All have to be substituted away before evaluation

    auto substitutions = alias.groupedVariables_;
    auto topLevelGroupedVariable = std::ranges::find_if(
        substitutions, [](GroupBy::HashMapGroupedVariableInformation& val) {
          return std::get_if<GroupBy::OccurAsRoot>(&val.occurrences_);
        });

    if (topLevelGroupedVariable != substitutions.end()) {
      AD_CORRECTNESS_CHECK(!groupByCols_.empty());
      // If the aggregate is at the top of the alias, e.g. `SELECT (?a as ?x)
      // WHERE {...} GROUP BY ?a`, we can copy values directly from the column
      // of the grouped variable
      resultTable.back()[alias.outCol_] =
          currentGroupBlock_.at(topLevelGroupedVariable->resultColumnIndex_)
              .second;

      evaluationContext._previousResultsFromSameGroup.at(alias.outCol_) =
          sparqlExpression::copyExpressionResult(
              sparqlExpression::ExpressionResult{
                  resultTable.back()[alias.outCol_]});
    } else if (info.size() == 1 && !info.at(0).parentAndIndex_.has_value()) {
      // Only one aggregate, and it is at the top of the alias, e.g.
      // `(AVG(?x) as ?y)`. The grouped by variable cannot occur inside
      // an aggregate, hence we don't need to substitute anything here
      auto& aggregate = info.at(0);

      resultTable.back()[alias.outCol_] =
          calculateAggregateResult(aggregate.aggregateDataIndex_);

      // Copy the result so that future aliases may reuse it
      evaluationContext._previousResultsFromSameGroup.at(alias.outCol_) =
          sparqlExpression::copyExpressionResult(
              sparqlExpression::ExpressionResult{
                  resultTable.back()[alias.outCol_]});
    } else {
      for (const auto& substitution : substitutions) {
        const auto& occurrences =
            get<std::vector<GroupBy::ParentAndChildIndex>>(
                substitution.occurrences_);
        substituteGroupVariable(
            occurrences, resultTable.back()[substitution.resultColumnIndex_]);
      }

      // Substitute in the results of all aggregates contained in the
      // expression of the current alias, if `info` is non-empty.
      // Substitute in the results of all aggregates of `info`.
      std::vector<std::unique_ptr<sparqlExpression::SparqlExpression>>
          originalChildren;
      originalChildren.reserve(info.size());
      for (auto& aggregate : info) {
        // Substitute the resulting vector as a literal
        auto newExpression = std::make_unique<sparqlExpression::IdExpression>(
            calculateAggregateResult(aggregate.aggregateDataIndex_));

        AD_CONTRACT_CHECK(aggregate.parentAndIndex_.has_value());
        auto parentAndIndex = aggregate.parentAndIndex_.value();
        originalChildren.push_back(std::move(
            parentAndIndex.parent_->children()[parentAndIndex.nThChild_]));
        parentAndIndex.parent_->replaceChild(parentAndIndex.nThChild_,
                                             std::move(newExpression));
      }

      // Evaluate top-level alias expression
      sparqlExpression::ExpressionResult expressionResult =
          alias.expr_.getPimpl()->evaluate(&evaluationContext);

      // Restore original children
      for (size_t i = 0; i < info.size(); ++i) {
        auto& aggregate = info.at(i);
        auto parentAndIndex = aggregate.parentAndIndex_.value();
        parentAndIndex.parent_->replaceChild(parentAndIndex.nThChild_,
                                             std::move(originalChildren.at(i)));
      }

      // Copy the result so that future aliases may reuse it
      evaluationContext._previousResultsFromSameGroup.at(alias.outCol_) =
          sparqlExpression::copyExpressionResult(expressionResult);

      // Extract values
      resultTable.back()[alias.outCol_] = std::visit(
          [this]<sparqlExpression::SingleExpressionResult T>(
              T&& singleResult) -> Id {
            constexpr static bool isStrongId = std::is_same_v<T, Id>;
            AD_CONTRACT_CHECK(sparqlExpression::isConstantResult<T>);
            if constexpr (isStrongId) {
              return singleResult;
            } else if constexpr (sparqlExpression::isConstantResult<T>) {
              return sparqlExpression::detail::constantExpressionResultToId(
                  AD_FWD(singleResult), localVocab_);
            } else {
              // This should never happen since aggregates always return
              // constants.
              AD_FAIL();
            }
          },
          std::move(expressionResult));
    }
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
