//
// Created by johannes on 19.04.20.
//

#include "Bind.h"
#include "../util/Exception.h"
#include "CallFixedSize.h"
#include "QueryExecutionTree.h"

size_t Bind::getResultWidth() const { return _subtree->getResultWidth() + 1; }

size_t Bind::getSizeEstimate() { return _subtree->getSizeEstimate(); }

size_t Bind::getCostEstimate() { return _subtree->getCostEstimate(); }

float Bind::getMultiplicity(size_t col) {
  if (col == getResultWidth() - 1) {
    return 1;
  }
  return _subtree->getMultiplicity(col);
}

string Bind::getDescriptor() const { return _bind.getDescriptor(); }

[[nodiscard]] vector<size_t> Bind::resultSortedOn() const {
  return _subtree->resultSortedOn();
}
bool Bind::knownEmptyResult() { return _subtree->knownEmptyResult(); }

string Bind::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }

  auto m = getVariableColumns();
  auto strings = _bind.strings();
  os << "BIND (" << _bind.operationName() << ") on";

  for (const auto& ptr : strings) {
    auto s = *ptr;  // TODO<joka921> :: possibly avoid string copying by
                    // returning reference_wrappers from strings() method.
    if (!ad_utility::startsWith(s, "?")) {
      os << s << ' ';
      continue;
    }

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

ad_utility::HashMap<string, size_t> Bind::getVariableColumns() const {
  auto res = _subtree->getVariableColumns();
  res[_bind._target] = getResultWidth() - 1;
  return res;
}

void Bind::setTextLimit(size_t limit) { _subtree->setTextLimit(limit); }

std::vector<QueryExecutionTree*> Bind::getChildren() {
  return {_subtree.get()};
}

void Bind::computeResult(ResultTable* result) {
  LOG(DEBUG) << "Getting sub-result for Bind result computation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();

  RuntimeInformation& runtimeInfo = getRuntimeInfo();
  runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());

  LOG(DEBUG) << "Sort result computation..." << endl;
  result->_data.setCols(getResultWidth());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  result->_localVocab = subRes->_localVocab;
  int inwidth = subRes->_data.cols();
  int outwidth = getResultWidth();
  if (auto ptr = std::get_if<GraphPatternOperation::Bind::Sum>(&_bind._input);
      ptr) {
    std::array<size_t, 2> columns{_subtree->getVariableColumn(ptr->_var1),
                                  _subtree->getVariableColumn(ptr->_var2)};
    array<ResultTable::ResultType, 2> inTypes{subRes->_resultTypes[columns[0]],
                                              subRes->_resultTypes[columns[1]]};
    result->_resultTypes.push_back(ResultTable::ResultType::FLOAT);
    CALL_FIXED_SIZE_2(inwidth, outwidth, Bind::computeSumBind, &result->_data,
                      subRes->_data, columns, inTypes,
                      _subtree->getQec()->getIndex());
  } else if (auto ptr = std::get_if<GraphPatternOperation::Bind::Rename>(
                 &_bind._input);
             ptr) {
    size_t columns{_subtree->getVariableColumn(ptr->_var)};
    result->_resultTypes.push_back(subRes->_resultTypes[columns]);
    CALL_FIXED_SIZE_2(inwidth, outwidth, Bind::computeRenameBind,
                      &result->_data, subRes->_data, columns);
  } else if (auto ptr = std::get_if<GraphPatternOperation::Bind::Constant>(&_bind._input); ptr) {
    /*
    if (auto ptrI = std::get_if<string>(&(ptr->_value))) {
      result->_resultTypes.push_back(ResultTable::ResultType::LOCAL_VOCAB);
      auto targetVal = result->_localVocab->size();
      result->_localVocab->push_back(*ptrI);
      CALL_FIXED_SIZE_2(inwidth, outwidth, Bind::computeConstantBind,
                        &result->_data, subRes->_data, targetVal);
                        */
        result->_resultTypes.push_back(ptr->_type);
        CALL_FIXED_SIZE_2(inwidth, outwidth, Bind::computeConstantBind,
                          &result->_data, subRes->_data, ptr->_value);

  } else if (auto ptr = std::get_if<GraphPatternOperation::Bind::Expression>(&_bind._input); ptr) {
    result->_resultTypes.push_back(ResultTable::ResultType::FLOAT);
    ExpressionTree::Input inp;
    inp.input_ = result;
    inp.requireNumericResult = true;
    inp.qec_ = _executionContext;
    inp.variableColumnMap_ = &_subtree->getVariableColumns();

    CALL_FIXED_SIZE_2(inwidth, outwidth, Bind::computeExpressionBind,
                      &result->_data, inp, (ptr->_expr).get());

  } else {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY,
             "Currently only renaming and sum binds are implemented");
  }

  // TODO<joka921> : in case of a renaming operation we are also sorted on
  // the renamed result, implement this.
  result->_sortedBy = resultSortedOn();

  LOG(DEBUG) << "Sort result computation done." << endl;
}

template <int IN_WIDTH, int OUT_WIDTH>
void Bind::computeExpressionBind(IdTable* dynRes, ExpressionTree::Input input,
                                  const ExpressionTree* exprPtr
) {
  auto res = dynRes->moveToStatic<OUT_WIDTH>();
  auto resCol = exprPtr->evaluate(input);
  const auto& inp = input.input_->_data;
  const auto inSize = inp.size();
  res.reserve(inSize);
  const auto inCols = inp.cols();
  // copy the input to the first cols;
  for (size_t i = 0; i < inSize; ++i) {
    res.emplace_back();
    for (size_t j = 0; j < inCols; ++j) {
      res(i, j) = inp(i, j);
    }
    res(i, inSize) = resCol[i];
  }
  *dynRes = res.moveToDynamic();

}

template <int IN_WIDTH, int OUT_WIDTH>
void Bind::computeSumBind(IdTable* dynRes, const IdTable& inputDyn,
                          std::array<size_t, 2> columns,
                          array<ResultTable::ResultType, 2> inputTypes,
                          const Index& index) {
  throw std::runtime_error("Legacy sums are unsupported, please report this");
#if 0
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
    float singleRes = .0f;
    for (size_t colIdx = 0; colIdx < 1; ++colIdx) {
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
        // TODO(schnelle): What's the correct way to handle OPTIONAL here
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
    std::memcpy(&res(i, inCols), &singleRes, sizeof(float));
  }
  *dynRes = res.moveToDynamic();
#endif
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
    res(i, inCols) = fancyInt(targetVal);
  }
  *dynRes = res.moveToDynamic();
}
