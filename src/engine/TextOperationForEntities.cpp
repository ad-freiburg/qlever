// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>
#include <unordered_map>
#include "./QueryExecutionTree.h"
#include "TextOperationForEntities.h"


using std::string;
using std::unordered_map;

// _____________________________________________________________________________
size_t TextOperationForEntities::getResultWidth() const {
  size_t width = 3;
  for (size_t i = 0; i < _subtrees.size(); ++i) {
    width += _subtrees[i].first.getRootOperation()->getResultWidth();
  }
  return width;
}

// _____________________________________________________________________________
TextOperationForEntities::TextOperationForEntities(
    QueryExecutionContext* qec,
    const string& words,
    const vector<pair<QueryExecutionTree, size_t>>& subtrees) :
    Operation(qec), _words(words), _subtrees(subtrees) { }

// _____________________________________________________________________________
string TextOperationForEntities::asString() const {
  std::ostringstream os;
  os << "TEXT OPERATION FOR ENTITIES:" << " co-occurrence with words: \"" <<
  _words << "\"";
  for (size_t i = 0; i < _subtrees.size(); ++i) {
    os << "\n\tand " << _subtrees[i].first.asString() << " [" <<
    _subtrees[i].second << "]";
  }
  return os.str();
}

// _____________________________________________________________________________
void TextOperationForEntities::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "TextOperationForEntities result computation..." << endl;
  if (_subtrees.size() == 0) {
    result->_nofColumns = 3;
    result->_fixedSizeData = new vector<array<Id, 2>>;
    getExecutionContext()->getIndex().getECListForWords(
        _words,
        reinterpret_cast<vector<array<Id, 3>>*>(result->_fixedSizeData));
  } else {
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             "Complex text query is a todo for the future.");
  }
  result->_status = ResultTable::FINISHED;
  LOG(DEBUG) << "TextOperationForEntities result computation done." << endl;
}
