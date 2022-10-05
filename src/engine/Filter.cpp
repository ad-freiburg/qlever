// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "Filter.h"

#include <algorithm>
#include <optional>
#include <regex>
#include <sstream>

#include "engine/CallFixedSize.h"
#include "engine/QueryExecutionTree.h"
#include "engine/sparqlExpressions/SparqlExpression.h"

using std::string;

namespace {
// Convert a FilterType to the corresponding `Comparison`. Throws if no
// corresponding `Comparison` exists (supported are LE, LT, EQ, NE, GE, GT).
valueIdComparators::Comparison toComparison(
    SparqlFilter::FilterType filterType) {
  switch (filterType) {
    case SparqlFilter::LT:
      return valueIdComparators::Comparison::LT;
    case SparqlFilter::LE:
      return valueIdComparators::Comparison::LE;
    case SparqlFilter::EQ:
      return valueIdComparators::Comparison::EQ;
    case SparqlFilter::NE:
      return valueIdComparators::Comparison::NE;
    case SparqlFilter::GT:
      return valueIdComparators::Comparison::GT;
    case SparqlFilter::GE:
      return valueIdComparators::Comparison::GE;
    default:
      AD_FAIL();
  }
}
}  // namespace

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
  result->_idTable.setCols(subRes->_idTable.cols());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  result->_localVocab = subRes->_localVocab;

  int width = result->_idTable.cols();
  CALL_FIXED_SIZE_1(width, computeFilterImpl, result, *subRes);
  LOG(DEBUG) << "Filter result computation done." << endl;
}

template <int WIDTH>
void Filter::computeFilterImpl(ResultTable* outputResultTable,
                               const ResultTable& inputResultTable) {
  sparqlExpression::VariableToColumnAndResultTypeMap columnMap;
  for (const auto& [variable, columnIndex] : _subtree->getVariableColumns()) {
    // TODO<joka921> The "ResultType" is currently unused, but we can use it in
    // the future for optimizations (in the style of "we know that this complete
    // column consists of floats".
    columnMap[variable] = std::pair(columnIndex, qlever::ResultType::KB);
  }

  sparqlExpression::EvaluationContext evaluationContext(
      *getExecutionContext(), columnMap, inputResultTable._idTable,
      getExecutionContext()->getAllocator(), *inputResultTable._localVocab);

  sparqlExpression::ExpressionResult expressionResult =
      _expression.getPimpl()->evaluate(&evaluationContext);

  const auto input = inputResultTable._idTable.asStaticView<WIDTH>();
  auto output = outputResultTable->_idTable.moveToStatic<WIDTH>();

  auto visitor = [&]<sparqlExpression::SingleExpressionResult T>(
                     T&& singleResult) mutable {
    if constexpr (std::is_same_v<T, sparqlExpression::VectorWithMemoryLimit<
                                        sparqlExpression::Bool>>) {
      AD_CHECK(singleResult.size() == input.size());
      // TODO<joka921> First determine the total size, and then get the final
      // result.
      for (size_t i = 0; i < singleResult.size(); ++i) {
        if (singleResult[i]) {
          output.push_back(input[i]);
        }
      }
    } else if constexpr (std::is_same_v<T, ad_utility::SetOfIntervals>) {
      // TODO<joka921> First determine the total size, and then get the final
      // result.
      for (auto [beg, end] : singleResult._intervals) {
        AD_CHECK(end <= input.size());
        output.insert(output.end(), input.begin() + beg, input.begin() + end);
      }
    } else {
      throw std::runtime_error(
          "Evaluated a filter expression that did not evaluate to a set of "
          "boolean values This is not allowed. TODO(developers): Catch this "
          "error earlier, e.g. during parsing");
    };

    std::visit(visitor, std::move(expressionResult));

    outputResultTable->_idTable = output.moveToDynamic();
  }
