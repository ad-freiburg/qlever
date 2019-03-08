// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Distinct.h"
#include <sstream>
#include "./QueryExecutionTree.h"
#include "CallFixedSize.h"

using std::string;

// _____________________________________________________________________________
size_t Distinct::getResultWidth() const { return _subtree->getResultWidth(); }

// _____________________________________________________________________________
Distinct::Distinct(QueryExecutionContext* qec,
                   std::shared_ptr<QueryExecutionTree> subtree,
                   const vector<size_t>& keepIndices)
    : Operation(qec), _subtree(subtree), _keepIndices(keepIndices) {}

// _____________________________________________________________________________
string Distinct::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "Distinct " << _subtree->asString(indent);
  return os.str();
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t> Distinct::getVariableColumns() const {
  ad_utility::HashMap<string, size_t> map;
  for (size_t index : _keepIndices) {
    for (const auto& it : _subtree->getVariableColumnMap()) {
      if (it.second == index) {
        map.insert(it);
      }
    }
  }
  return map;
}

// _____________________________________________________________________________
void Distinct::computeResult(ResultTable* result) {
  LOG(DEBUG) << "Getting sub-result for distinct result computation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();

  RuntimeInformation& runtimeInfo = getRuntimeInfo();
  runtimeInfo.setDescriptor("DISTINCT");
  runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());
  LOG(DEBUG) << "Distinct result computation..." << endl;
  result->_data.setCols(subRes->_data.cols());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  result->_localVocab = subRes->_localVocab;
  int width = subRes->_data.size();
  CALL_FIXED_SIZE_1(width, getEngine().distinct, subRes->_data, _keepIndices,
                    &result->_data);
  LOG(DEBUG) << "Distinct result computation done." << endl;
}
