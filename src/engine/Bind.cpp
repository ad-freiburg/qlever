//
// Created by johannes on 19.04.20.
//

#include "Bind.h"

#include "../util/Exception.h"
#include "./sparqlExpressions/SparqlExpression.h"
#include "./sparqlExpressions/SparqlExpressionGenerators.h"
#include "CallFixedSize.h"
#include "QueryExecutionTree.h"

// BIND adds exactly one new column
size_t Bind::getResultWidth() const { return _subtree->getResultWidth() + 1; }

// BIND doesn't change the number of result rows
size_t Bind::getSizeEstimate() { return _subtree->getSizeEstimate(); }

// BIND has cost linear in the size of the input. Note that BIND operations are
// currently always executed at their position in the SPARQL query, so that this
// cost estimate has no effect on query optimization (there is only one
// alternative).
size_t Bind::getCostEstimate() {
  return _subtree->getCostEstimate() + _subtree->getSizeEstimate();
}

float Bind::getMultiplicity(size_t col) {
  // this is the newly added column
  if (col == getResultWidth() - 1) {
    // TODO<joka921> get a better multiplicity estimate for BINDs which are
    // variable renames or constants.
    return 1;
  }

  // one of the columns that was only copied from the input.
  return _subtree->getMultiplicity(col);
}

// _____________________________________________________________________________
string Bind::getDescriptor() const { return _bind.getDescriptor(); }

// _____________________________________________________________________________
[[nodiscard]] vector<size_t> Bind::resultSortedOn() const {
  // We always append the result column of the BIND at the end and this column
  // is not sorted, so the sequence of indices of the sorted columns do not
  // change.
  return _subtree->resultSortedOn();
}

// _____________________________________________________________________________
bool Bind::knownEmptyResult() { return _subtree->knownEmptyResult(); }

// _____________________________________________________________________________
string Bind::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }

  os << "BIND ";
  os << _bind._expression.getCacheKey(getVariableColumns());
  os << "\n" << _subtree->asString(indent);
  return std::move(os).str();
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t> Bind::getVariableColumns() const {
  auto res = _subtree->getVariableColumns();
  // The new variable is always appended at the end.
  res[_bind._target] = getResultWidth() - 1;
  return res;
}

// _____________________________________________________________________________
void Bind::setTextLimit(size_t limit) { _subtree->setTextLimit(limit); }

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> Bind::getChildren() {
  return {_subtree.get()};
}

// _____________________________________________________________________________
void Bind::computeResult(ResultTable* result) {
  LOG(DEBUG) << "Get input to BIND operation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();
  LOG(DEBUG) << "Got input to Bind operation." << endl;

  RuntimeInformation& runtimeInfo = getRuntimeInfo();
  runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());

  result->_idTable.setCols(getResultWidth());
  result->_resultTypes = subRes->_resultTypes;
  result->_localVocab = subRes->_localVocab;
  int inwidth = subRes->_idTable.cols();
  int outwidth = getResultWidth();

  result->_resultTypes.emplace_back();
  CALL_FIXED_SIZE_2(inwidth, outwidth, computeExpressionBind, result,
                    &(result->_resultTypes.back()), *subRes,
                    _bind._expression.getPimpl());

  result->_sortedBy = resultSortedOn();

  LOG(DEBUG) << "BIND result computation done." << endl;
}

// _____________________________________________________________________________
template <int IN_WIDTH, int OUT_WIDTH>
void Bind::computeExpressionBind(
    ResultTable* outputResultTable, ResultTable::ResultType* resultType,
    const ResultTable& inputResultTable,
    sparqlExpression::SparqlExpression* expression) const {
  sparqlExpression::VariableToColumnAndResultTypeMap columnMap;
  for (const auto& [variable, columnIndex] : getVariableColumns()) {
    // Ignore the added (bound) variable.
    if (columnIndex < inputResultTable.width()) {
      columnMap[variable] =
          std::pair(columnIndex, inputResultTable.getResultType(columnIndex));
    }
  }

  sparqlExpression::EvaluationContext evaluationContext(
      *getExecutionContext(), columnMap, inputResultTable._idTable,
      getExecutionContext()->getAllocator(), *inputResultTable._localVocab);

  sparqlExpression::ExpressionResult expressionResult =
      expression->evaluate(&evaluationContext);

  const auto input = inputResultTable._idTable.asStaticView<IN_WIDTH>();
  auto output = outputResultTable->_idTable.moveToStatic<OUT_WIDTH>();

  // first initialize the first columns (they remain identical)
  const auto inSize = input.size();
  output.reserve(inSize);
  const auto inCols = input.cols();
  // copy the input to the first cols;
  for (size_t i = 0; i < inSize; ++i) {
    output.emplace_back();
    for (size_t j = 0; j < inCols; ++j) {
      output(i, j) = input(i, j);
    }
  }

  auto visitor = [&]<sparqlExpression::SingleExpressionResult T>(
                     T&& singleResult) mutable {
    constexpr static bool isVariable =
        std::is_same_v<T, sparqlExpression::Variable>;
    constexpr static bool isStrongId =
        std::is_same_v<T, sparqlExpression::StrongIdWithResultType>;
    if constexpr (isVariable) {
      auto column = getVariableColumns().at(singleResult._variable);
      for (size_t i = 0; i < inSize; ++i) {
        output(i, inCols) = output(i, column);
      }
      *resultType = evaluationContext._variableToColumnAndResultTypeMap
                        .at(singleResult._variable)
                        .second;
    } else if constexpr (isStrongId) {
      for (size_t i = 0; i < inSize; ++i) {
        output(i, inCols) = singleResult._id._value;
      }
      *resultType = singleResult._type;
    } else {
      bool isConstant = sparqlExpression::isConstantResult<T>;

      auto resultGenerator = sparqlExpression::detail::makeGenerator(
          std::forward<T>(singleResult), inSize, &evaluationContext);
      *resultType =
          sparqlExpression::detail::expressionResultTypeToQleverResultType<T>();

      size_t i = 0;
      for (auto&& resultValue : resultGenerator) {
        output(i, inCols) =
            sparqlExpression::detail::constantExpressionResultToId(
                resultValue, *(outputResultTable->_localVocab),
                isConstant && i > 0);
        i++;
      }
    }
  };

  std::visit(visitor, std::move(expressionResult));

  outputResultTable->_idTable = output.moveToDynamic();
}
