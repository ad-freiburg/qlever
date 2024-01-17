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
ResultTable Filter::computeResult() {
  LOG(DEBUG) << "Getting sub-result for Filter result computation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
  LOG(DEBUG) << "Filter result computation..." << endl;

  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(subRes->idTable().numColumns());

  size_t width = idTable.numColumns();
  CALL_FIXED_SIZE(width, &Filter::computeFilterImpl, this, &idTable, *subRes);
  LOG(DEBUG) << "Filter result computation done." << endl;

  return {std::move(idTable), resultSortedOn(), subRes->getSharedLocalVocab()};
}

// _____________________________________________________________________________
template <size_t WIDTH>
void Filter::computeFilterImpl(IdTable* outputIdTable,
                               const ResultTable& inputResultTable) {
  sparqlExpression::EvaluationContext evaluationContext(
      *getExecutionContext(), _subtree->getVariableColumns(),
      inputResultTable.idTable(), getExecutionContext()->getAllocator(),
      inputResultTable.localVocab());

  // TODO<joka921> This should be a mandatory argument to the EvaluationContext
  // constructor.
  evaluationContext._columnsByWhichResultIsSorted = inputResultTable.sortedBy();

  sparqlExpression::ExpressionResult expressionResult =
      _expression.getPimpl()->evaluate(&evaluationContext);

  const auto input = inputResultTable.idTable().asStaticView<WIDTH>();
  auto output = std::move(*outputIdTable).toStatic<WIDTH>();

  auto visitor =
      [&]<sparqlExpression::SingleExpressionResult T>(T&& singleResult) {
        if constexpr (std::is_same_v<T, ad_utility::SetOfIntervals>) {
          auto totalSize = std::accumulate(
              singleResult._intervals.begin(), singleResult._intervals.end(),
              0ul, [](const auto& sum, const auto& interval) {
                return sum + (interval.second - interval.first);
              });
          output.reserve(totalSize);
          for (auto [beg, end] : singleResult._intervals) {
            AD_CONTRACT_CHECK(end <= input.size());
            output.insertAtEnd(input.cbegin() + beg, input.cbegin() + end);
          }
          AD_CONTRACT_CHECK(output.size() == totalSize);
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
              output.push_back(input[i]);
            }
            ++i;
          }
        }
      };

  std::visit(visitor, std::move(expressionResult));

  *outputIdTable = std::move(output).toDynamic();
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
