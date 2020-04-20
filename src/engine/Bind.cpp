//
// Created by johannes on 19.04.20.
//

#include "Bind.h"
#include "QueryExecutionTree.h"

size_t Bind::getResultWidth() const {
  return _subtree->getResultWidth();
}

size_t Bind::getSizeEstimate() {
  return _subtree->getSizeEstimate();
}

size_t Bind::getCostEstimate() {
  return _subtree->getCostEstimate();
}

float Bind::getMultiplicity(size_t col) {
  if (col == getResultWidth() - 1) {
    return 1;
  }
  return _subtree->getMultiplicity(col);
}

string Bind::getDescriptor() const {
  return "BIND (" + _var1 + " " + _var2 + " AS " + _targetVar;
}

[[nodiscard]] vector<size_t> Bind::resultSortedOn() const {
  return _subtree->resultSortedOn();
}
bool Bind::knownEmptyResult() {
  return _subtree->knownEmptyResult();
}

string Bind::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "BIND (SUM) on columns:" << _var1 << " " << _var2 << "\n" << _subtree->asString(indent);
  return os.str();
}

ad_utility::HashMap<string, size_t> Bind::getVariableColumns() const {
  auto res = _subtree->getVariableColumns();
  res[_targetVar] = getResultWidth() - 1;
  return res;
}


void Bind::setTextLimit(size_t limit) {
  _subtree->setTextLimit(limit);
}

std::vector<QueryExecutionTree*> Bind::getChildren() {
  return {_subtree.get()};
}

void Bind::computeResult(ResultTable *result) {
  LOG(DEBUG) << "Getting sub-result for Bind result computation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();

  RuntimeInformation& runtimeInfo = getRuntimeInfo();
  runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());

  LOG(DEBUG) << "Sort result computation..." << endl;
  result->_data.setCols(subRes->_data.cols());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  result->_localVocab = subRes->_localVocab;
  result->_data.insert(result->_data.end(), subRes->_data.begin(),
                       subRes->_data.end());
  int width = result->_data.cols();
  CALL_FIXED_SIZE_1(width, Engine::sort, &result->_data, _sortCol);
  result->_sortedBy = resultSortedOn();

  LOG(DEBUG) << "Sort result computation done." << endl;

}

template <int IN_WIDTH, int OUT_WIDTH>
void Bind::computeBind(IdTable* dynRes, const IdTable& inputDyn,
                       std::array<size_t, 2> columns, array<ResultTable::ResultType, 2> inputTypes) {
  const auto input = inputDyn.asStaticView<IN_WIDTH>();
  auto res = dynRes->moveToStatic<OUT_WIDTH>();

  const auto inSize = input.size();
  res.reserve(inSize);
  const auto inCols = input.cols();
  for (size_t i = 0; i < inSize; ++i) {
    res.emplace_back();
    for (size_t j = 0; j < inCols; ++j ) {
      res(i, j) = input(i, j);
    }
    res(i, inCols + 1) = input(i, columns[0]) + input(i, columns[1]); // todo;
      float res = .0f;
      for (size_t colIdx = 0; colIdx < 1; ++colIdx) {
      if (inputTypes[colIdx] == ResultTable::ResultType::VERBATIM) {
              res += input(i, columns[colIdx]);
      } else if (inputTypes[colIdx] == ResultTable::ResultType::FLOAT) {
        // used to store the id value of the entry interpreted as a float
        float tmpF;
        std::memcpy(&tmpF, &input(i, colIdx), sizeof(float));
        if (a._distinct) {
          for (size_t i = blockStart; i <= blockEnd; i++) {
            const auto it = distinctHashSet.find(input(i, a._inCol));
            if (it == distinctHashSet.end()) {
              distinctHashSet.insert(input(i, a._inCol));
              res += tmpF;
            }
          }
          distinctHashSet.clear();
        } else {
          for (size_t i = blockStart; i <= blockEnd; i++) {
            std::memcpy(&tmpF, &input(i, a._inCol), sizeof(float));
            res += tmpF;
          }
        }
      } else if (inputTypes[a._inCol] == ResultTable::ResultType::TEXT ||
                 inputTypes[a._inCol] == ResultTable::ResultType::LOCAL_VOCAB) {
        res = std::numeric_limits<float>::quiet_NaN();
      } else {
        if (a._distinct) {
          for (size_t i = blockStart; i <= blockEnd; i++) {
            const auto it = distinctHashSet.find(input(i, a._inCol));
            if (it == distinctHashSet.end()) {
              distinctHashSet.insert(input(i, a._inCol));
              // load the string, parse it as an xsd::int or float
              // TODO(schnelle): What's the correct way to handle OPTIONAL here
              std::string entity =
                  index.idToOptionalString(input(i, a._inCol)).value_or("");
              if (!ad_utility::startsWith(entity, VALUE_FLOAT_PREFIX)) {
                res = std::numeric_limits<float>::quiet_NaN();
                break;
              } else {
                res += ad_utility::convertIndexWordToFloat(entity);
              }
            }
          }
          distinctHashSet.clear();
        } else {
          for (size_t i = blockStart; i <= blockEnd; i++) {
            // load the string, parse it as an xsd::int or float
            // TODO(schnelle): What's the correct way to handle OPTIONAL here
            std::string entity =
                index.idToOptionalString(input(i, a._inCol)).value_or("");
            if (!ad_utility::startsWith(entity, VALUE_FLOAT_PREFIX)) {
              res = std::numeric_limits<float>::quiet_NaN();
              break;
            } else {
              res += ad_utility::convertIndexWordToFloat(entity);
            }
          }
        }
      }
      (*result)(resultRow, a._outCol) = 0;
      std::memcpy(&(*result)(resultRow, a._outCol), &res, sizeof(float));
  }
  *dynRes = res.moveToDynamic();
}
