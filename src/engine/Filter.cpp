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

  if (subRes->isFullyMaterialized()) {
    IdTable result = filterIdTable(subRes->sortedBy(), subRes->idTable(),
                                   subRes->localVocab());
    LOG(DEBUG) << "Filter result computation done." << endl;

    return {std::move(result), resultSortedOn(), subRes->getSharedLocalVocab()};
  }

  if (requestLaziness) {
    return {[](auto subRes, auto* self) -> Result::Generator {
              for (auto& [idTable, localVocab] : subRes->idTables()) {
                IdTable result = self->filterIdTable(subRes->sortedBy(),
                                                     idTable, localVocab);
                co_yield {std::move(result), std::move(localVocab)};
              }
            }(std::move(subRes), this),
            resultSortedOn()};
  }

  // If we receive a generator of IdTables, we need to materialize it into a
  // single IdTable.
  size_t width = getSubtree().get()->getResultWidth();
  IdTable result{width, getExecutionContext()->getAllocator()};
  std::vector<LocalVocab> localVocabs;
  ad_utility::callFixedSize(
      width, [this, &subRes, &result, &localVocabs]<int WIDTH>() {
        for (Result::IdTableVocabPair& pair : subRes->idTables()) {
          computeFilterImpl<WIDTH>(result, pair.idTable_, pair.localVocab_,
                                   subRes->sortedBy());
          localVocabs.emplace_back(std::move(pair.localVocab_));
        }
      });

  LocalVocab resultLocalVocab{};
  resultLocalVocab.mergeWith(localVocabs);

  LOG(DEBUG) << "Filter result computation done." << endl;

  return {std::move(result), resultSortedOn(), std::move(resultLocalVocab)};
}

// _____________________________________________________________________________
IdTable Filter::filterIdTable(std::vector<ColumnIndex> sortedBy,
                              const IdTable& idTable,
                              const LocalVocab& localVocab) const {
  size_t width = idTable.numColumns();
  IdTable result{width, getExecutionContext()->getAllocator()};
  CALL_FIXED_SIZE(width, &Filter::computeFilterImpl, this, result, idTable,
                  localVocab, std::move(sortedBy));
  return result;
}

// _____________________________________________________________________________
template <int WIDTH>
void Filter::computeFilterImpl(IdTable& dynamicResultTable,
                               const IdTable& inputTable,
                               const LocalVocab& localVocab,
                               std::vector<ColumnIndex> sortedBy) const {
  AD_CONTRACT_CHECK(inputTable.numColumns() == WIDTH || WIDTH == 0);
  IdTableStatic<WIDTH> resultTable =
      std::move(dynamicResultTable).toStatic<static_cast<size_t>(WIDTH)>();
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

  // Note: the explicit (seemingly redundant) capture of `resultTable` is
  // required to work around a bug in Clang 17, see
  // https://github.com/llvm/llvm-project/issues/61267
  auto visitor =
      [this, &resultTable = resultTable, &input,
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

  dynamicResultTable = std::move(resultTable).toDynamic();
  checkCancellation();
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
