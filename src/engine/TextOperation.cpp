// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>
#include <unordered_map>
#include "./QueryExecutionTree.h"
#include "./TextOperation.h"


using std::string;
using std::unordered_map;

// _____________________________________________________________________________
size_t TextOperation::getResultWidth() const {
  size_t width = 1;
  for (size_t i = 0; i < _subtrees.size(); ++i) {
    width += _subtrees[i].getRootOperation()->getResultWidth();
  }
  return width;
}

// _____________________________________________________________________________
TextOperation::TextOperation(QueryExecutionContext* qec, const string& words,
                             const vector<QueryExecutionTree>& subtrees) :
    Operation(qec), _words(words), _subtrees(subtrees) { }

// _____________________________________________________________________________
string TextOperation::asString() const {
  std::ostringstream os;
  os << "TextOperation TODO";
  return os.str();
}

// _____________________________________________________________________________
void TextOperation::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "TextOperation result computation..." << endl;
  result->_status = ResultTable::FINISHED;
  LOG(DEBUG) << "TextOperation result computation done." << endl;
}
