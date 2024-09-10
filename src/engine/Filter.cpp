// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2020-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "./Filter.h"

#include <algorithm>
#include <sstream>

#include "engine/CallFixedSize.h"
#include "engine/QueryExecutionTree.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

using std::endl;
using std::string;

// _____________________________________________________________________________
size_t Filter::getResultWidth() const { return _subtree->getResultWidth(); }

// _____________________________________________________________________________
Filter::Filter(QueryExecutionContext* qec,
               std::shared_ptr<QueryExecutionTree> subtree,
               sparqlExpression::SparqlExpressionPimpl expression)
    : Operation(qec),
      _subtree(std::move(subtree)),
      _expression{std::move(expression)} {}

// _____________________________________________________________________________
string Filter::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "FILTER " << _subtree->getCacheKey();
  os << " with " << _expression.getCacheKey(_subtree->getVariableColumns());
  return std::move(os).str();
}

string Filter::getDescriptor() const {
  return absl::StrCat("Filter ", _expression.getDescriptor());
}

// _____________________________________________________________________________
ProtoResult Filter::computeResult(bool requestLaziness) {
  LOG(DEBUG) << "Getting sub-result for Filter result computation..." << endl;
  std::shared_ptr<const Result> subRes = _subtree->getResult(true);
  LOG(DEBUG) << "Filter result computation..." << endl;
  checkCancellation();

  auto localVocab = subRes->getSharedLocalVocab();
  if (subRes->isFullyMaterialized()) {
    IdTable result = filterIdTable(subRes, subRes->idTable());
    LOG(DEBUG) << "Filter result computation done." << endl;

    return {std::move(result), resultSortedOn(), std::move(localVocab)};
  }
  if (requestLaziness) {
    return {[](auto subRes, auto* self) -> cppcoro::generator<IdTable> {
              for (IdTable& idTable : subRes->idTables()) {
                IdTable result = self->filterIdTable(subRes, idTable);
                co_yield result;
              }
            }(std::move(subRes), this),
            resultSortedOn(), std::move(localVocab)};
  }

  size_t width = getSubtree().get()->getResultWidth();
  IdTable result =
      ad_utility::callFixedSize(width, [this, &subRes, width]<int WIDTH>() {
        IdTable result{width, getExecutionContext()->getAllocator()};
        IdTableStatic<WIDTH> output =
            std::move(result).toStatic<static_cast<size_t>(WIDTH)>();

        for (IdTable& idTable : subRes->idTables()) {
          computeFilterImpl(output, idTable, subRes->localVocab(),
                            subRes->sortedBy());
          checkCancellation();
        }

        return std::move(output).toDynamic();
      });

  LOG(DEBUG) << "Filter result computation done." << endl;

  return {std::move(result), resultSortedOn(), std::move(localVocab)};
}

// _____________________________________________________________________________
IdTable Filter::filterIdTable(const std::shared_ptr<const Result>& subRes,
                              const IdTable& idTable) const {
  size_t width = idTable.numColumns();
  IdTable result = ad_utility::callFixedSize(
      width, [this, &idTable, width, &subRes]<int WIDTH>() {
        IdTable result{width, getExecutionContext()->getAllocator()};
        IdTableStatic<WIDTH> output =
            std::move(result).toStatic<static_cast<size_t>(WIDTH)>();
        computeFilterImpl(output, idTable, subRes->localVocab(),
                          subRes->sortedBy());

        return std::move(output).toDynamic();
      });
  checkCancellation();
  return result;
}

// _____________________________________________________________________________
template <int WIDTH>
void Filter::computeFilterImpl(IdTableStatic<WIDTH>& resultTable,
                               const IdTable& inputTable,
                               const LocalVocab& localVocab,
                               std::vector<ColumnIndex> sortedBy) const {
  AD_CONTRACT_CHECK(inputTable.numColumns() == WIDTH || WIDTH == 0);
  sparqlExpression::EvaluationContext evaluationContext(
      *getExecutionContext(), _subtree->getVariableColumns(), inputTable,
      getExecutionContext()->getAllocator(), localVocab, cancellationHandle_,
      deadline_);

  // TODO<joka921> This should be a mandatory argument to the
  // EvaluationContext constructor.
  evaluationContext._columnsByWhichResultIsSorted = std::move(sortedBy);
  const auto input =
      evaluationContext._inputTable.asStaticView<static_cast<size_t>(WIDTH)>();
  sparqlExpression::ExpressionResult expressionResult =
      _expression.getPimpl()->evaluate(&evaluationContext);

  auto visitor =
      [this, &resultTable, &input,
       &evaluationContext]<sparqlExpression::SingleExpressionResult T>(
          T&& singleResult) {
        if constexpr (std::is_same_v<T, ad_utility::SetOfIntervals>) {
          auto totalSize = std::accumulate(
              singleResult._intervals.begin(), singleResult._intervals.end(),
              resultTable.size(), [](const auto& sum, const auto& interval) {
                return sum + (interval.second - interval.first);
              });
          resultTable.reserve(totalSize);
          checkCancellation();
          for (auto [beg, end] : singleResult._intervals) {
            AD_CONTRACT_CHECK(end <= input.size());
            resultTable.insertAtEnd(input.cbegin() + beg, input.cbegin() + end);
            checkCancellation();
          }
          AD_CONTRACT_CHECK(resultTable.size() == totalSize);
        } else {
          // All other results are converted to boolean values via the
          // `EffectiveBooleanValueGetter`. This means for example, that zero,
          // UNDEF, and empty strings are filtered out.
          // TODO<joka921> Check whether it's feasible to precompute and reserve
          // the total size. This depends on the expensiveness of the
          // `EffectiveBooleanValueGetter`.
          auto resultGenerator = sparqlExpression::detail::makeGenerator(
              std::forward<T>(singleResult), input.size(), &evaluationContext);
          size_t i = 0;

          using EBV = sparqlExpression::detail::EffectiveBooleanValueGetter;
          for (auto&& resultValue : resultGenerator) {
            if (EBV{}(resultValue, &evaluationContext) == EBV::Result::True) {
              resultTable.push_back(input[i]);
            }
            checkCancellation();
            ++i;
          }
        }
      };

  std::visit(visitor, std::move(expressionResult));
}

// _____________________________________________________________________________
uint64_t Filter::getSizeEstimateBeforeLimit() {
  return _expression
      .getEstimatesForFilterExpression(
          _subtree->getSizeEstimate(),
          _subtree->getRootOperation()->getPrimarySortKeyVariable())
      .sizeEstimate;
}

// _____________________________________________________________________________
size_t Filter::getCostEstimate() {
  return _subtree->getCostEstimate() +
         _expression
             .getEstimatesForFilterExpression(
                 _subtree->getSizeEstimate(),
                 _subtree->getRootOperation()->getPrimarySortKeyVariable())
             .costEstimate;
}
