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
