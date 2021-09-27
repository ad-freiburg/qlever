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
    // TODO<joka921> get a better multiplicty estimate for BINDs which are
    // variable renames or Constants
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

  auto m = getVariableColumns();
  auto strings = _bind.strings();
  // TODO<joka921> Proper asString() method for the Expressions
  os << "BIND ("
     << "Complex expression"
     << ") on";
  // the random string to prevent false caching
  os << _bind._expressionVariant.asString(m);

  for (const auto& ptr : strings) {
    auto s = *ptr;

    // non-variables are added directly (constants etc.)
    if (!ad_utility::startsWith(s, "?")) {
      os << s << ' ';
      continue;
    }

    // variables are converted to the corresponding column index, to create the
    // same cache key for same query with changed variable names.
    if (!m.contains(s)) {
      AD_THROW(
          ad_semsearch::Exception::BAD_INPUT,
          "Variable"s + s + " could not be mapped to column of BIND input");
    }
    os << "(col " << m[s] << ") ";
  }

  os << "\n" << _subtree->asString(indent);
  return os.str();
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

  result->_data.setCols(getResultWidth());
  result->_resultTypes = subRes->_resultTypes;
  result->_localVocab = subRes->_localVocab;
  int inwidth = subRes->_data.cols();
  int outwidth = getResultWidth();

  result->_resultTypes.emplace_back();
  CALL_FIXED_SIZE_2(inwidth, outwidth, computeExpressionBind, result,
                    &(result->_resultTypes.back()), *subRes,
                    _bind._expressionVariant.getImpl());

  result->_sortedBy = resultSortedOn();

  LOG(DEBUG) << "BIND result computation done." << endl;
}

// _____________________________________________________________________________
template <int IN_WIDTH, int OUT_WIDTH>
void Bind::computeExpressionBind(
    ResultTable* outResult, ResultTable::ResultType* resultType,
    const ResultTable& inResult,
    sparqlExpression::SparqlExpression* expression) const {
  sparqlExpression::VariableToColumnAndResultTypeMap columnMap;
  for (const auto& [variable, columnIndex] : getVariableColumns()) {
    if (columnIndex < inResult.width()) {
      columnMap[variable] =
          std::pair(columnIndex, inResult.getResultType(columnIndex));
    }
  }

  sparqlExpression::EvaluationContext evaluationInput(
      *getExecutionContext(), columnMap, inResult._data,
      getExecutionContext()->getAllocator(), *inResult._localVocab);

  sparqlExpression::ExpressionResult expressionResult =
      expression->evaluate(&evaluationInput);

  const auto input = inResult._data.asStaticView<IN_WIDTH>();
  auto res = outResult->_data.moveToStatic<OUT_WIDTH>();

  // first initialize the first columns (they remain identical)
  const auto inSize = input.size();
  res.reserve(inSize);
  const auto inCols = input.cols();
  // copy the input to the first cols;
  for (size_t i = 0; i < inSize; ++i) {
    res.emplace_back();
    for (size_t j = 0; j < inCols; ++j) {
      res(i, j) = input(i, j);
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
        res(i, inCols) = res(i, column);
      }
      *resultType = evaluationInput._variableToColumnAndResultTypeMap
                        .at(singleResult._variable)
                        .second;
    } else if constexpr (isStrongId) {
      for (size_t i = 0; i < inSize; ++i) {
        res(i, inCols) = singleResult._id._value;
      }
      *resultType = singleResult._type;
    } else {
      bool isConstant = sparqlExpression::isConstantResult<T>;

      auto expanded = sparqlExpression::detail::makeGenerator(
          std::forward<T>(singleResult), inSize, &evaluationInput);
      *resultType =
          sparqlExpression::detail::expressionResultTypeToQleverResultType<T>();
      bool isFirst = true;

      size_t i = 0;
      for (auto&& singleResultValue : expanded) {
        res(i, inCols) = sparqlExpression::detail::constantExpressionResultToId(
            singleResultValue, *(outResult->_localVocab),
            isConstant && !isFirst);
        isFirst = false;
        i++;
      }
    }
  };

  std::visit(visitor, std::move(expressionResult));

  outResult->_data = res.moveToDynamic();
}
