// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>
#include <unordered_map>
#include "./QueryExecutionTree.h"
#include "TextOperationForContexts.h"


using std::string;
using std::unordered_map;

// _____________________________________________________________________________
size_t TextOperationForContexts::getResultWidth() const {
  size_t width = 2;
  for (size_t i = 0; i < _subtrees.size(); ++i) {
    width += _subtrees[i].first.getRootOperation()->getResultWidth();
  }
  return width;
}

// _____________________________________________________________________________
TextOperationForContexts::TextOperationForContexts(
    QueryExecutionContext* qec,
    const string& words,
    const vector<pair<QueryExecutionTree, size_t>>& subtrees,
    size_t textLimit) :
    Operation(qec), _words(words), _subtrees(subtrees), _textLimit(textLimit) {
}

// _____________________________________________________________________________
string TextOperationForContexts::asString() const {
  std::ostringstream os;
  os << "TEXT OPERATION FOR CONTEXTS:" << " co-occurrence with words: \"" <<
  _words << "\"";
  for (size_t i = 0; i < _subtrees.size(); ++i) {
    os << "\n\tand " << _subtrees[i].first.asString() << " [" <<
    _subtrees[i].second << "]";
  }
  os << " with textLimit = " << _textLimit;
  return os.str();
}

// _____________________________________________________________________________
void TextOperationForContexts::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "TextOperationForContexts result computation..." << endl;
  if (_subtrees.size() == 0) {
    result->_nofColumns = 2;
    result->_fixedSizeData = new vector<array<Id, 2>>;
    getExecutionContext()->getIndex().getContextListForWords(
        _words,
        reinterpret_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  } else {
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             "Complex text query is a todo for the future.");
  }
  result->_status = ResultTable::FINISHED;
  LOG(DEBUG) << "TextOperationForContexts result computation done." << endl;
}
