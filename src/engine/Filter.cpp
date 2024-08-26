// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2020-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "./Filter.h"

#include <algorithm>
#include <optional>
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
  std::shared_ptr<const Result> subRes = _subtree->getResult(requestLaziness);
  LOG(DEBUG) << "Filter result computation..." << endl;
  checkCancellation();

  if (subRes->isFullyMaterialized()) {
    IdTable result = filterIdTable(subRes, subRes->idTable());
    LOG(DEBUG) << "Filter result computation done." << endl;

    return {std::move(result), resultSortedOn(), subRes->getSharedLocalVocab()};
  }
  auto localVocab = subRes->getSharedLocalVocab();
  return {[](auto subRes, auto* self) -> cppcoro::generator<IdTable> {
            for (IdTable& idTable : subRes->idTables()) {
              IdTable result = self->filterIdTable(subRes, idTable);
              co_yield result;
            }
          }(std::move(subRes), this),
          resultSortedOn(), std::move(localVocab)};
}

// _____________________________________________________________________________
IdTable Filter::filterIdTable(const std::shared_ptr<const Result>& subRes,
                              const IdTable& idTable) {
  sparqlExpression::EvaluationContext evaluationContext(
      *getExecutionContext(), _subtree->getVariableColumns(), idTable,
      getExecutionContext()->getAllocator(), subRes->localVocab(),
      cancellationHandle_, deadline_);

  // TODO<joka921> This should be a mandatory argument to the
  // EvaluationContext constructor.
  evaluationContext._columnsByWhichResultIsSorted = subRes->sortedBy();

  size_t width = evaluationContext._inputTable.numColumns();
  IdTable result = CALL_FIXED_SIZE(width, &Filter::computeFilterImpl, this,
                                   evaluationContext);
  checkCancellation();
  return result;
}

// _____________________________________________________________________________
template <size_t WIDTH>
IdTable Filter::computeFilterImpl(
    sparqlExpression::EvaluationContext& evaluationContext) {
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(evaluationContext._inputTable.numColumns());

  sparqlExpression::ExpressionResult expressionResult =
      _expression.getPimpl()->evaluate(&evaluationContext);

  const auto input = evaluationContext._inputTable.asStaticView<WIDTH>();
  auto output = std::move(idTable).toStatic<WIDTH>();
  // Clang 17 seems to incorrectly deduce the type, so try to trick it
  std::remove_const_t<decltype(output)>& output2 = output;

  auto visitor =
      [this, &output2, &input,
       &evaluationContext]<sparqlExpression::SingleExpressionResult T>(
          T&& singleResult) {
        if constexpr (std::is_same_v<T, ad_utility::SetOfIntervals>) {
          auto totalSize = std::accumulate(
              singleResult._intervals.begin(), singleResult._intervals.end(),
              0ul, [](const auto& sum, const auto& interval) {
                return sum + (interval.second - interval.first);
              });
          output2.reserve(totalSize);
          checkCancellation();
          for (auto [beg, end] : singleResult._intervals) {
            AD_CONTRACT_CHECK(end <= input.size());
            output2.insertAtEnd(input.cbegin() + beg, input.cbegin() + end);
            checkCancellation();
          }
          AD_CONTRACT_CHECK(output2.size() == totalSize);
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
              output2.push_back(input[i]);
            }
            checkCancellation();
            ++i;
          }
        }
      };

  std::visit(visitor, std::move(expressionResult));

  return std::move(output).toDynamic();
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
