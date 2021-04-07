//
// Created by johannes on 19.04.20.
//

#include "Bind.h"
#include "../util/Exception.h"
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
    // if we rename a column, we also preserve the multiplicity of this column
    if (auto ptr = std::get_if<GraphPatternOperation::Bind::Rename>(
            &(_bind._expressionVariant))) {
      auto incol = _subtree->getVariableColumn(ptr->_var);
      return _subtree->getMultiplicity(incol);
    }
    // only one value in the new column, high multiplicity
    if (std::get_if<GraphPatternOperation::Bind::Constant>(
            &(_bind._expressionVariant))) {
      return _subtree->getSizeEstimate();
    }

    // If sum (or arithmetic expression), we make the simplifying assumption
    // that all results values are different (which will indeed often be the
    // case).
    if (std::get_if<GraphPatternOperation::Bind::Sum>(
            &(_bind._expressionVariant))) {
      return 1;
    }
    throw std::runtime_error("Unknown type of BIND in getMultiplicity");
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
  os << "BIND (" << _bind.operationName() << ") on";

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

  if (auto ptr = std::get_if<GraphPatternOperation::Bind::Sum>(
          &_bind._expressionVariant)) {
    std::array<size_t, 2> columns{_subtree->getVariableColumn(ptr->_var1),
                                  _subtree->getVariableColumn(ptr->_var2)};
    array<ResultTable::ResultType, 2> inTypes{subRes->_resultTypes[columns[0]],
                                              subRes->_resultTypes[columns[1]]};
    // currently the result type for a SUM is always float, this will be changed
    // with proper datatype support.
    result->_resultTypes.push_back(ResultTable::ResultType::FLOAT);
    CALL_FIXED_SIZE_2(inwidth, outwidth, Bind::computeSumBind, &result->_data,
                      subRes->_data, columns, inTypes,
                      _subtree->getQec()->getIndex());
  } else if (auto ptr = std::get_if<GraphPatternOperation::Bind::Rename>(
                 &_bind._expressionVariant)) {
    size_t inColumn{_subtree->getVariableColumn(ptr->_var)};
    // copying a column also copies the result type
    result->_resultTypes.push_back(subRes->_resultTypes[inColumn]);
    CALL_FIXED_SIZE_2(inwidth, outwidth, Bind::computeRenameBind,
                      &result->_data, subRes->_data, inColumn);
  } else if (auto ptr = std::get_if<GraphPatternOperation::Bind::Constant>(
                 &_bind._expressionVariant)) {
    result->_resultTypes.push_back(ptr->_type);
    Id value;
    if (ptr->_type == ResultTable::ResultType::VERBATIM) {
      value = ptr->_intValue;
    } else if (ptr->_type == ResultTable::ResultType::KB) {
      if (!_executionContext->getIndex().getVocab().getId(ptr->_kbValue,
                                                          &value)) {
        throw std::runtime_error("BIND constant " + ptr->_kbValue +
                                 " is not part of the knowledge base. This is "
                                 "currently unsupported");
      }
    } else {
      throw std::runtime_error(
          "BIND currently only supported for integer constant and entities "
          "from the KB."
          "This should never happen, please report this");
    }
    CALL_FIXED_SIZE_2(inwidth, outwidth, Bind::computeConstantBind,
                      &result->_data, subRes->_data, value);
  } else {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY,
             "Currently only three types of BIND are implemented: Integer "
             "constant, rename, and sum.");
  }

  result->_sortedBy = resultSortedOn();

  LOG(DEBUG) << "BIND result computation done." << endl;
}

template <int IN_WIDTH, int OUT_WIDTH>
void Bind::computeSumBind(IdTable* dynRes, const IdTable& inputDyn,
                          std::array<size_t, 2> columns,
                          array<ResultTable::ResultType, 2> inputTypes,
                          const Index& index) {
  const auto input = inputDyn.asStaticView<IN_WIDTH>();
  auto result = dynRes->moveToStatic<OUT_WIDTH>();

  const auto inSize = input.size();
  result.reserve(inSize);
  const auto inCols = input.cols();
  // copy the input to the first cols;
  for (size_t i = 0; i < inSize; ++i) {
    result.emplace_back();
    for (size_t j = 0; j < inCols; ++j) {
      result(i, j) = input(i, j);
    }
    float singleRes = .0f;
    for (size_t colIdx = 0; colIdx < columns.size(); ++colIdx) {
      if (inputTypes[colIdx] == ResultTable::ResultType::VERBATIM) {
        singleRes += input(i, columns[colIdx]);
      } else if (inputTypes[colIdx] == ResultTable::ResultType::FLOAT) {
        // used to store the id value of the entry interpreted as a float
        float tmpF;
        std::memcpy(&tmpF, &input(i, colIdx), sizeof(float));
        singleRes += tmpF;
      } else if (inputTypes[colIdx] == ResultTable::ResultType::TEXT ||
                 inputTypes[colIdx] == ResultTable::ResultType::LOCAL_VOCAB) {
        singleRes = std::numeric_limits<float>::quiet_NaN();
      } else {
        // load the string, parse it as an xsd::int or float
        std::string entity =
            index.idToOptionalString(input(i, columns[colIdx])).value_or("");
        if (!ad_utility::startsWith(entity, VALUE_FLOAT_PREFIX)) {
          singleRes = std::numeric_limits<float>::quiet_NaN();
          break;
        } else {
          singleRes += ad_utility::convertIndexWordToFloat(entity);
        }
      }
    }
    std::memcpy(&result(i, inCols), &singleRes, sizeof(float));
  }
  *dynRes = result.moveToDynamic();
}

template <int IN_WIDTH, int OUT_WIDTH>
void Bind::computeRenameBind(IdTable* dynRes, const IdTable& inputDyn,
                             size_t column) {
  const auto input = inputDyn.asStaticView<IN_WIDTH>();
  auto res = dynRes->moveToStatic<OUT_WIDTH>();

  const auto inSize = input.size();
  res.reserve(inSize);
  const auto inCols = input.cols();
  // copy the input to the first cols;
  for (size_t i = 0; i < inSize; ++i) {
    res.emplace_back();
    for (size_t j = 0; j < inCols; ++j) {
      res(i, j) = input(i, j);
    }
    // simply copy
    res(i, inCols) = input(i, column);
  }
  *dynRes = res.moveToDynamic();
}

template <int IN_WIDTH, int OUT_WIDTH>
void Bind::computeConstantBind(IdTable* dynRes, const IdTable& inputDyn,
                               size_t targetVal) {
  const auto input = inputDyn.asStaticView<IN_WIDTH>();
  auto res = dynRes->moveToStatic<OUT_WIDTH>();

  const auto inSize = input.size();
  res.reserve(inSize);
  const auto inCols = input.cols();
  // copy the input to the first cols;
  for (size_t i = 0; i < inSize; ++i) {
    res.emplace_back();
    for (size_t j = 0; j < inCols; ++j) {
      res(i, j) = input(i, j);
    }
    // simply copy
    res(i, inCols) = targetVal;
  }
  *dynRes = res.moveToDynamic();
}
