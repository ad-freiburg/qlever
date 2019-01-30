// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>

#include "./QueryExecutionTree.h"
#include "TextOperationForContexts.h"

using std::string;

// _____________________________________________________________________________
size_t TextOperationForContexts::getResultWidth() const {
  size_t width = 2;
  for (size_t i = 0; i < _subtrees.size(); ++i) {
    width += _subtrees[i].first->getRootOperation()->getResultWidth();
  }
  return width;
}

// _____________________________________________________________________________
TextOperationForContexts::TextOperationForContexts(
    QueryExecutionContext* qec, const string& words,
    const vector<pair<std::shared_ptr<QueryExecutionTree>, size_t>>& subtrees,
    size_t textLimit)
    : Operation(qec),
      _words(words),
      _subtrees(subtrees),
      _textLimit(textLimit) {}

// _____________________________________________________________________________
string TextOperationForContexts::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "TEXT OPERATION FOR CONTEXTS:"
     << " co-occurrence with words: \"" << _words << "\"";
  for (size_t i = 0; i < _subtrees.size(); ++i) {
    os << "\n";
    for (size_t i = 0; i < indent; ++i) {
      os << " ";
    }
    os << "and\n" << _subtrees[i].first->asString(indent) << "\n";
    for (size_t i = 0; i < indent; ++i) {
      os << " ";
    }
    os << "[" << _subtrees[i].second << "]";
  }
  return os.str();
}

// _____________________________________________________________________________
void TextOperationForContexts::computeResult(ResultTable* result) {
  LOG(DEBUG) << "TextOperationForContexts result computation..." << endl;
  if (_subtrees.size() == 0) {
    result->_nofColumns = 2;
    result->_resultTypes.push_back(ResultTable::ResultType::TEXT);
    result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
    result->_fixedSizeData = new vector<array<Id, 2>>;
    getExecutionContext()->getIndex().getContextListForWords(
        _words,
        reinterpret_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  } else {
    AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
             "Complex text query is a todo for the future.");
  }
  result->finish();
  LOG(DEBUG) << "TextOperationForContexts result computation done." << endl;
}
