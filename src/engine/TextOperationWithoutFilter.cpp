// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>
#include <unordered_map>
#include "./QueryExecutionTree.h"
#include "TextOperationWithoutFilter.h"


using std::string;
using std::unordered_map;

// _____________________________________________________________________________
size_t TextOperationWithoutFilter::getResultWidth() const {
  size_t width = 2 + _nofVars;
  return width;
}

// _____________________________________________________________________________
TextOperationWithoutFilter::TextOperationWithoutFilter(
    QueryExecutionContext *qec, const string& words,
    size_t nofVars, size_t textLimit) :
    Operation(qec), _words(words), _nofVars(nofVars), _textLimit(textLimit) { }

// _____________________________________________________________________________
string TextOperationWithoutFilter::asString() const {
  std::ostringstream os;
  os << "TEXT OPERATION WITHOUT FILTER:" << " co-occurrence with words: \"" <<
  _words << "\" and " << _nofVars << " variables";;
  os << " with textLimit = " << _textLimit;
  return os.str();
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResult(ResultTable *result) const {
  LOG(DEBUG) << "TextOperationWithoutFilter result computation..." << endl;
  if (_nofVars == 0) {
    computeResultNoVar(result);
  } else if (_nofVars == 1) {
    computeResultOneVar(result);
  } else {
    computeResultMultVars(result);
  }
  result->_status = ResultTable::FINISHED;
  LOG(DEBUG) << "TextOperationWithoutFilter result computation done." << endl;
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResultNoVar(ResultTable *result) const {
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "TODO");
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResultOneVar(
    ResultTable *result) const {
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "TODO");
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResultMultVars(
    ResultTable *result) const {
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "TODO");
}
