// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <string>
#include <sstream>
#include "./QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
string QueryExecutionTree::asString() const {
  std::ostringstream os;
  os << "{" << _rootOperation->asString() << " | width: " << getResultWidth()
      << "}";
  return os.str();
}

// _____________________________________________________________________________
void QueryExecutionTree::setOperation(QueryExecutionTree::OperationType type,
    Operation* op) {
  _type = type;
  switch (type) {
    case OperationType::SCAN:
      delete _rootOperation;
      _rootOperation = new IndexScan(*reinterpret_cast<IndexScan*>(op));
      break;
    case OperationType::JOIN:
      delete _rootOperation;
      _rootOperation = new Join(*reinterpret_cast<Join*>(op));
      break;
    case OperationType::SORT:
      delete _rootOperation;
      _rootOperation = new Sort(*reinterpret_cast<Sort*>(op));
      break;
    default: AD_THROW(ad_semsearch::Exception::ASSERT_FAILED, "Cannot set "
        "an operation with undefined type.");
  }
}

// _____________________________________________________________________________
void QueryExecutionTree::setVariableColumn(const string& variable, int column) {
  _variableColumnMap[variable] = column;
}

// _____________________________________________________________________________
size_t QueryExecutionTree::getVariableColumn(const string& variable) const {
  AD_CHECK(_variableColumnMap.count(variable) > 0);
  return _variableColumnMap.find(variable)->second;
}

// _____________________________________________________________________________
void QueryExecutionTree::setVariableColumns(
    unordered_map<string, size_t> const& map) {
  _variableColumnMap = map;
}
