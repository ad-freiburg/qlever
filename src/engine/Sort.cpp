// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Sort.h"
#include <sstream>
#include "QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
size_t Sort::getResultWidth() const { return _subtree->getResultWidth(); }

// _____________________________________________________________________________
Sort::Sort(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree, size_t sortCol)
    : Operation(qec), _subtree(subtree), _sortCol(sortCol) {}

// _____________________________________________________________________________
string Sort::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "SORT on column:" << _sortCol << "\n" << _subtree->asString(indent);
  return os.str();
}

// _____________________________________________________________________________
void Sort::computeResult(ResultTable* result) {
  LOG(DEBUG) << "Getting sub-result for Sort result computation..." << endl;
  shared_ptr<const ResultTable> subRes = _subtree->getResult();

  std::string orderByVars = "";
  for (auto p : _subtree->getVariableColumnMap()) {
    if (p.second == _sortCol) {
      orderByVars = "ASC(" + p.first + ")";
      break;
    }
  }
  RuntimeInformation& runtimeInfo = getRuntimeInfo();
  runtimeInfo.setDescriptor("Sort on " + orderByVars);
  runtimeInfo.addChild(_subtree->getRootOperation()->getRuntimeInfo());

  LOG(DEBUG) << "Sort result computation..." << endl;
  result->_nofColumns = subRes->_nofColumns;
  result->_data.setCols(result->_nofColumns);
  result->_resultTypes.insert(result->_resultTypes.end(),
                              subRes->_resultTypes.begin(),
                              subRes->_resultTypes.end());
  result->_localVocab = subRes->_localVocab;
  result->_data.insert(result->_data.end(), subRes->_data.begin(),
                       subRes->_data.end());
  // std::cout << "before\n" << subRes->_data << std::endl;
  // std::cout << "After insert\n" << result->_data << std::endl;
  getEngine().sort(&result->_data, _sortCol);
  result->_sortedBy = resultSortedOn();

  LOG(DEBUG) << "Sort result computation done." << endl;
}
