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
string Filter::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "FILTER " << _subtree->asString(indent);
  os << " with " << _expression.getCacheKey(_subtree->getVariableColumns());
  return std::move(os).str();
}

string Filter::getDescriptor() const {
  return absl::StrCat("Filter ", _expression.getDescriptor());
}

// _____________________________________________________________________________
void Filter::computeResult(ResultTable* result) {
  LOG(DEBUG) << "Getting sub-result for Filter result computation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
  LOG(DEBUG) << "Filter result computation..." << endl;
  result->_idTable.setNumColumns(subRes->_idTable.numColumns());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  result->_localVocab = subRes->_localVocab;

  int width = result->_idTable.numColumns();
  CALL_FIXED_SIZE(width, &Filter::computeFilterImpl, this, result, *subRes);
  LOG(DEBUG) << "Filter result computation done." << endl;
}

// _____________________________________________________________________________
template <int WIDTH>
void Filter::computeFilterImpl(ResultTable* outputResultTable,
                               const ResultTable& inputResultTable) {
  sparqlExpression::VariableToColumnAndResultTypeMap columnMap;
  for (const auto& [variable, columnIndex] : _subtree->getVariableColumns()) {
    // TODO<joka921> The "ResultType" is currently unused, but we can use it in
    // the future for optimizations (in the style of "we know that this complete
    // column consists of floats").
    columnMap[variable] = std::pair(columnIndex, qlever::ResultType::KB);
  }

  sparqlExpression::EvaluationContext evaluationContext(
      *getExecutionContext(), columnMap, inputResultTable._idTable,
      getExecutionContext()->getAllocator(), *inputResultTable._localVocab);

  // TODO<joka921> This should be a mandatory argument to the EvaluationContext
  // constructor.
  evaluationContext._columnsByWhichResultIsSorted = inputResultTable._sortedBy;

  sparqlExpression::ExpressionResult expressionResult =
      _expression.getPimpl()->evaluate(&evaluationContext);

  const auto input = inputResultTable._idTable.asStaticView<WIDTH>();
  auto output = std::move(outputResultTable->_idTable).toStatic<WIDTH>();

  auto visitor =
      [&]<sparqlExpression::SingleExpressionResult T>(T&& singleResult) {
        if constexpr (std::is_same_v<T, sparqlExpression::VectorWithMemoryLimit<
                                            sparqlExpression::Bool>>) {
          AD_CONTRACT_CHECK(singleResult.size() == input.size());
          auto totalSize =
              std::accumulate(singleResult.begin(), singleResult.end(), 0ul);
          output.reserve(totalSize);
          for (size_t i = 0; i < singleResult.size(); ++i) {
            if (singleResult[i]) {
              output.push_back(input[i]);
            }
          }
          AD_CONTRACT_CHECK(output.size() == totalSize);
        } else if constexpr (std::is_same_v<T, ad_utility::SetOfIntervals>) {
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
          // Default case for all other types. We currently implicitly convert
          // all kinds of results (strings, doubles, ints) to bools inside a
          // FILTER.
          // TODO<joka921> Read up in the standard what is correct here, and
          // then rewrite the expression module with the correct semantics.
          // TODO<joka921> Check whether it's feasible to precompute and reserve
          // the total size. This depends on the expensiveness of the
          // `EffectiveBooleanValueGetter`.
          auto resultGenerator = sparqlExpression::detail::makeGenerator(
              std::forward<T>(singleResult), input.size(), &evaluationContext);
          size_t i = 0;

          for (auto&& resultValue : resultGenerator) {
            if (sparqlExpression::detail::EffectiveBooleanValueGetter{}(
                    resultValue, &evaluationContext)) {
              output.push_back(input[i]);
            }
            ++i;
          }
        }
      };

  std::visit(visitor, std::move(expressionResult));

  outputResultTable->_idTable = std::move(output).toDynamic();
}

// _____________________________________________________________________________
size_t Filter::getSizeEstimate() {
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
