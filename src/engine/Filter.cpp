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

  LocalVocab resultLocalVocab{};
  ad_utility::callFixedSize(
      width, [this, &subRes, &result, &resultLocalVocab]<int WIDTH>() {
        for (Result::IdTableVocabPair& pair : subRes->idTables()) {
          computeFilterImpl<WIDTH>(result, std::move(pair.idTable_),
                                   pair.localVocab_, subRes->sortedBy());
          resultLocalVocab.mergeWith(std::span{&pair.localVocab_, 1});
        }
      });

  LOG(DEBUG) << "Filter result computation done." << endl;

  return {std::move(result), resultSortedOn(), std::move(resultLocalVocab)};
}

// _____________________________________________________________________________
template <ad_utility::SimilarTo<IdTable> Table>
IdTable Filter::filterIdTable(std::vector<ColumnIndex> sortedBy,
                              Table&& idTable,
                              const LocalVocab& localVocab) const {
  size_t width = idTable.numColumns();
  IdTable result{width, getExecutionContext()->getAllocator()};

  auto impl = [this, &result, &idTable, &localVocab, &sortedBy]<int WIDTH> {
    return this->computeFilterImpl<WIDTH>(result, AD_FWD(idTable), localVocab,
                                          std::move(sortedBy));
  };
  ad_utility::callFixedSize(width, impl);
  return result;
}

// _____________________________________________________________________________
template <int WIDTH, ad_utility::SimilarTo<IdTable> Table>
void Filter::computeFilterImpl(IdTable& dynamicResultTable, Table&& inputTable,
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

  // Filter `input` by `expressionResult` and store the result in `resultTable`.
  // This is a lambda because `expressionResult` is a `std::variant`.
  //
  // NOTE: the explicit (seemingly redundant) capture of `resultTable` is
  // required to work around a bug in Clang 17, see
  // https://github.com/llvm/llvm-project/issues/61267
  auto computeResult =
      [this, &resultTable = resultTable, &input, &inputTable,
       &dynamicResultTable,
       &evaluationContext]<sparqlExpression::SingleExpressionResult T>(
          T&& singleResult) {
        if constexpr (std::is_same_v<T, ad_utility::SetOfIntervals>) {
          AD_CONTRACT_CHECK(input.size() == evaluationContext.size());
          // If the expression result is given as a set of intervals, we copy
          // the corresponding parts of `input` to `resultTable`.
          //
          // NOTE: One of the interval ends may be larger than `input.size()`
          // (as the result of a negation).
          auto totalSize = std::accumulate(
              singleResult._intervals.begin(), singleResult._intervals.end(),
              resultTable.size(),
              [&input](const auto& sum, const auto& interval) {
                size_t intervalBegin = interval.first;
                size_t intervalEnd = std::min(interval.second, input.size());
                return sum + (intervalEnd - intervalBegin);
              });
          if (resultTable.empty() && totalSize == inputTable.size()) {
            // The binary filter contains all elements of the input, and we have
            // no previous results, so we can simply copy or move the complete
            // table.
            dynamicResultTable = AD_FWD(inputTable).moveOrClone();
            return;
          }
          checkCancellation();
          for (auto [intervalBegin, intervalEnd] : singleResult._intervals) {
            intervalEnd = std::min(intervalEnd, input.size());
            resultTable.insertAtEnd(inputTable, intervalBegin, intervalEnd);
            checkCancellation();
          }
          AD_CORRECTNESS_CHECK(resultTable.size() == totalSize);
        } else {
          // In the general case, we generate all expression results and apply
          // the `EffectiveBooleanValueGetter` to each.
          //
          // NOTE: According to the standard, this means that values like zero,
          // UNDEF, and empty strings are converted to `false` and hence the
          // corresponding rows from `input` are filtered out.
          //
          // TODO<joka921> Check whether it is feasible to precompute the
          // number of `true` values and use that to reserve the right
          // amount of space for `resultTable`, like we do it for the set of
          // intervals above. This depends on how expensive the evaluation with
          // the `EffectiveBooleanValueGetter` is.
          auto resultGenerator = sparqlExpression::detail::makeGenerator(
              std::forward<T>(singleResult), input.size(), &evaluationContext);
          size_t i = 0;

          using ValueGetter =
              sparqlExpression::detail::EffectiveBooleanValueGetter;
          ValueGetter valueGetter{};
          for (auto&& resultValue : resultGenerator) {
            if (valueGetter(resultValue, &evaluationContext) ==
                ValueGetter::Result::True) {
              resultTable.push_back(input[i]);
            }
            checkCancellation();
            ++i;
          }
        }
      };
  std::visit(computeResult, std::move(expressionResult));

  // Detect the case that we have directly written the `dynamicResultTable`
  // in the binary search filter case.
  if (dynamicResultTable.empty()) {
    dynamicResultTable = std::move(resultTable).toDynamic();
  }
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
