// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>
#include <unordered_map>
#include "./QueryExecutionTree.h"
#include "TextOperationWithFilter.h"


using std::string;
using std::unordered_map;

// _____________________________________________________________________________
size_t TextOperationWithFilter::getResultWidth() const {
  return 1 + _nofVars + _filterResult->getResultWidth();
}

// _____________________________________________________________________________
TextOperationWithFilter::TextOperationWithFilter(
    QueryExecutionContext *qec, const string& words, size_t nofVars,
    const QueryExecutionTree *filterResult, size_t filterColumn, size_t textLimit) :
    Operation(qec), _words(words), _nofVars(nofVars),
    _filterResult(new QueryExecutionTree(*filterResult)),
    _filterColumn(filterColumn) {
  setTextLimit(textLimit);
}

// _____________________________________________________________________________
TextOperationWithFilter::TextOperationWithFilter(
    const TextOperationWithFilter& other) :
    Operation(other._executionContext), _words(other._words),
    _nofVars(other._nofVars), _textLimit(other._textLimit),
    _filterResult(new QueryExecutionTree(*other._filterResult)),
    _filterColumn(other._filterColumn) {
}

// _____________________________________________________________________________
TextOperationWithFilter& TextOperationWithFilter::operator=(
    const TextOperationWithFilter& other) {
  _executionContext = other._executionContext;
  _words = other._words;
  _nofVars = other._nofVars;
  _textLimit = other._textLimit;
  delete _filterResult;
  _filterResult = new QueryExecutionTree(*other._filterResult);
  _filterColumn = other._filterColumn;
  return *this;
}

// _____________________________________________________________________________
TextOperationWithFilter::~TextOperationWithFilter() {
  delete _filterResult;
}

// _____________________________________________________________________________
string TextOperationWithFilter::asString() const {
  std::ostringstream os;
  os << "TEXT OPERATION WITH FILTER:" << " co-occurrence with words: \"" <<
  _words << "\" and " << _nofVars << " variables with textLimit = " <<
  _textLimit << " filtered by " <<
  _filterResult->asString() << " on column " << _filterColumn;
  return os.str();
}

// _____________________________________________________________________________
void TextOperationWithFilter::computeResult(ResultTable *result) const {
  LOG(DEBUG) << "TextOperationWithFilter result computation..." << endl;
  AD_CHECK_GE(_nofVars, 1);
  if (_nofVars == 1) {
    computeResultOneVar(result);
  } else {
    computeResultMultVars(result);
  }
  result->_status = ResultTable::FINISHED;
  LOG(DEBUG) << "TextOperationWithFilter result computation done." << endl;
}

// _____________________________________________________________________________
void TextOperationWithFilter::computeResultOneVar(
    ResultTable *result) const {
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "TODO");
}

// _____________________________________________________________________________
void TextOperationWithFilter::computeResultMultVars(
    ResultTable *result) const {
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "TODO");
}