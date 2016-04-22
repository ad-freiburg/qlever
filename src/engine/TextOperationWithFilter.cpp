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
    QueryExecutionContext* qec, const string& words, size_t nofVars,
    const QueryExecutionTree* filterResult, size_t filterColumn,
    size_t textLimit) :
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
void TextOperationWithFilter::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "TextOperationWithFilter result computation..." << endl;
  AD_CHECK_GE(_nofVars, 1);
  result->_nofColumns = 2 + _filterResult->getResultWidth();
  if (_filterResult->getResultWidth() == 1) {
    AD_CHECK_GE(result->_nofColumns, 3);
    if (result->_nofColumns == 3) {
      result->_fixedSizeData = new vector<array<Id, 3>>;
      getExecutionContext()->getIndex().getFilteredECListForWords(
          _words,
          *static_cast<vector<array<Id, 1>>*>(_filterResult->getResult()._fixedSizeData),
          _filterColumn,
          _nofVars,
          _textLimit,
          *reinterpret_cast<vector<array<Id, 3>>*>(result->_fixedSizeData));
    } else if (result->_nofColumns == 4) {
      result->_fixedSizeData = new vector<array<Id, 4>>;
      getExecutionContext()->getIndex().getFilteredECListForWords(
          _words,
          *static_cast<vector<array<Id, 1>>*>(_filterResult->getResult()._fixedSizeData),
          _filterColumn,
          _nofVars,
          _textLimit,
          *reinterpret_cast<vector<array<Id, 4>>*>(result->_fixedSizeData));
    } else if (result->_nofColumns == 5) {
      result->_fixedSizeData = new vector<array<Id, 5>>;
      getExecutionContext()->getIndex().getFilteredECListForWords(
          _words,
          *static_cast<vector<array<Id, 1>>*>(_filterResult->getResult()._fixedSizeData),
          _filterColumn,
          _nofVars,
          _textLimit,
          *reinterpret_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else {
      getExecutionContext()->getIndex().getFilteredECListForWords(
          _words,
          *static_cast<vector<array<Id, 1>>*>(_filterResult->getResult()._fixedSizeData),
          _filterColumn,
          _nofVars,
          _textLimit,
          result->_varSizeData);
    }
  } else if (_filterResult->getResultWidth() == 2) {
    AD_CHECK_GE(result->_nofColumns, 4);
    if (result->_nofColumns == 4) {
      result->_fixedSizeData = new vector<array<Id, 4>>;
      getExecutionContext()->getIndex().getFilteredECListForWords(
          _words,
          *static_cast<vector<array<Id, 2>>*>(_filterResult->getResult()._fixedSizeData),
          _filterColumn,
          _nofVars,
          _textLimit,
          *reinterpret_cast<vector<array<Id, 4>>*>(result->_fixedSizeData));
    } else if (result->_nofColumns == 5) {
      result->_fixedSizeData = new vector<array<Id, 5>>;
      getExecutionContext()->getIndex().getFilteredECListForWords(
          _words,
          *static_cast<vector<array<Id, 2>>*>(_filterResult->getResult()._fixedSizeData),
          _filterColumn,
          _nofVars,
          _textLimit,
          *reinterpret_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else {
      getExecutionContext()->getIndex().getFilteredECListForWords(
          _words,
          *static_cast<vector<array<Id, 2>>*>(_filterResult->getResult()._fixedSizeData),
          _filterColumn,
          _nofVars,
          _textLimit,
          result->_varSizeData);
    }
  } else if (_filterResult->getResultWidth() == 3) {
    AD_CHECK_GE(result->_nofColumns, 5);
    if (result->_nofColumns == 5) {
      result->_fixedSizeData = new vector<array<Id, 5>>;
      getExecutionContext()->getIndex().getFilteredECListForWords(
          _words,
          *static_cast<vector<array<Id, 3>>*>(_filterResult->getResult()._fixedSizeData),
          _filterColumn,
          _nofVars,
          _textLimit,
          *reinterpret_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else {
      getExecutionContext()->getIndex().getFilteredECListForWords(
          _words,
          *static_cast<vector<array<Id, 3>>*>(_filterResult->getResult()._fixedSizeData),
          _filterColumn,
          _nofVars,
          _textLimit,
          result->_varSizeData);
    }
  } else if (_filterResult->getResultWidth() == 4) {
    getExecutionContext()->getIndex().getFilteredECListForWords(
        _words,
        *static_cast<vector<array<Id, 4>>*>(_filterResult->getResult()._fixedSizeData),
        _filterColumn,
        _nofVars,
        _textLimit,
        result->_varSizeData);
  } else if (_filterResult->getResultWidth() == 5) {
    getExecutionContext()->getIndex().getFilteredECListForWords(
        _words,
        *static_cast<vector<array<Id, 5>>*>(_filterResult->getResult()._fixedSizeData),
        _filterColumn,
        _nofVars,
        _textLimit,
        result->_varSizeData);
  } else {
    getExecutionContext()->getIndex().getFilteredECListForWords(
        _words,
        _filterResult->getResult()._varSizeData,
        _filterColumn,
        _nofVars,
        _textLimit,
        result->_varSizeData);
  }
  result->_status = ResultTable::FINISHED;
  LOG(DEBUG) << "TextOperationWithFilter result computation done." << endl;
}