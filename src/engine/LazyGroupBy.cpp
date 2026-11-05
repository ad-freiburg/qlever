//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "engine/LazyGroupBy.h"

#include "util/ChunkedForLoop.h"
#include "util/views/ChunkedIotaView.h"

using groupBy::detail::VectorOfAggregationData;

// _____________________________________________________________________________
LazyGroupBy::LazyGroupBy(
    LocalVocab& localVocab,
    std::vector<GroupByImpl::HashMapAliasInformation> aggregateAliases,
    const ad_utility::AllocatorWithLimit<Id>& allocator, size_t numGroupColumns)
    : localVocab_{localVocab},
      aggregateAliases_{std::move(aggregateAliases)},
      allocator_{allocator},
      aggregationData_{allocator, aggregateAliases_, numGroupColumns} {
  for (const auto& aggregateInfo : allAggregateInfoView()) {
    visitAggregate(
        [&aggregateInfo](auto& arg) {
          using T = std::decay_t<decltype(arg)>;
          static_assert(VectorOfAggregationData<T>);
          if constexpr (ql::concepts::same_as<typename T::value_type,
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
    visitAggregate(
        [](auto& arg) {
          using T = std::decay_t<decltype(arg)>;
          static_assert(VectorOfAggregationData<T>);
          arg.at(0).reset();
        },
        aggregateInfo);
  }
}

// _____________________________________________________________________________
void LazyGroupBy::commitRow(
    IdTable& resultTable,
    sparqlExpression::EvaluationContext& evaluationContext,
    const GroupByImpl::GroupBlock& currentGroupBlock) {
  resultTable.emplace_back();
  size_t colIdx = 0;
  for (const auto& [_, value] : currentGroupBlock) {
    resultTable.back()[colIdx] = value;
    ++colIdx;
  }

  evaluationContext._beginIndex = resultTable.size() - 1;
  evaluationContext._endIndex = resultTable.size();

  for (auto& alias : aggregateAliases_) {
    GroupByImpl::evaluateAlias(alias, &resultTable, evaluationContext,
                               aggregationData_,
                               evaluationContext._qec.getLocalVocabContext(),
                               &localVocab_, allocator_);
  }
  resetAggregationData();
}

// _____________________________________________________________________________
void LazyGroupBy::processBlock(
    sparqlExpression::EvaluationContext& evaluationContext, size_t beginIndex,
    size_t endIndex) {
  // A block can be large, and `addValue` might look up strings in the
  // vocabulary (e.g. for `GROUP_CONCAT`), so check for cancellation per chunk.
  for (auto [chunkBegin, chunkEnd] : ad_utility::chunkedIotaView(
           beginIndex, endIndex, ad_utility::CANCELLATION_CHECK_CHUNK_SIZE)) {
    evaluationContext.cancellationHandle_->throwIfCancelled();
    processChunk(evaluationContext, chunkBegin, chunkEnd);
  }
}

// _____________________________________________________________________________
void LazyGroupBy::processChunk(
    sparqlExpression::EvaluationContext& evaluationContext, size_t beginIndex,
    size_t endIndex) {
  size_t blockSize = endIndex - beginIndex;
  evaluationContext._beginIndex = beginIndex;
  evaluationContext._endIndex = endIndex;

  for (const auto& aggregateInfo : allAggregateInfoView()) {
    sparqlExpression::ExpressionResult expressionResult =
        GroupByImpl::evaluateChildExpressionOfAggregateFunction(
            aggregateInfo, evaluationContext);

    visitAggregate(
        CPP_template_lambda(blockSize, &evaluationContext)(
            typename A, typename T)(A & aggregateData, T && singleResult)(
            requires VectorOfAggregationData<A> &&
            sparqlExpression::SingleExpressionResult<T>) {
          auto generator = sparqlExpression::detail::makeGenerator(
              AD_FWD(singleResult), blockSize, &evaluationContext);

          for (const auto& val : generator) {
            aggregateData.at(0).addValue(val, &evaluationContext);
          }
        },
        aggregateInfo, std::move(expressionResult));
  }
}

// _____________________________________________________________________________
template <typename Visitor, typename... Args>
void LazyGroupBy::visitAggregate(
    const Visitor& visitor,
    const GroupByImpl::HashMapAggregateInformation& aggregateInfo,
    Args&&... additionalVariants) {
  std::visit(visitor,
             aggregationData_.getAggregationDataVariant(
                 aggregateInfo.aggregateDataIndex_),
             AD_FWD(additionalVariants)...);
}
