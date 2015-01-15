// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./QueryExecutionTree.h"


// _____________________________________________________________________________
string QueryExecutionTree::asString() const {
  return "";
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
    default: AD_THROW(ad_semsearch::Exception::ASSERT_FAILED, "Cannot set "
        "an operation with undefined type.");
  }
}