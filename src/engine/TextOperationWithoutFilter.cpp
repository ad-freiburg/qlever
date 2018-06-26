// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "TextOperationWithoutFilter.h"
#include <sstream>
#include "./QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
size_t TextOperationWithoutFilter::getResultWidth() const {
  size_t width = 2 + _nofVars;
  return width;
}

// _____________________________________________________________________________
TextOperationWithoutFilter::TextOperationWithoutFilter(
    QueryExecutionContext* qec, const string& words, size_t nofVars,
    size_t textLimit)
    : Operation(qec),
      _words(words),
      _nofVars(nofVars),
      _textLimit(textLimit),
      _sizeEstimate(std::numeric_limits<size_t>::max()) {}

// _____________________________________________________________________________
string TextOperationWithoutFilter::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "TEXT OPERATION WITHOUT FILTER:"
     << " co-occurrence with words: \"" << _words << "\" and " << _nofVars
     << " variables";
  ;
  os << " with textLimit = " << _textLimit;
  return os.str();
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "TextOperationWithoutFilter result computation..." << endl;
  if (_nofVars == 0) {
    computeResultNoVar(result);
  } else if (_nofVars == 1) {
    computeResultOneVar(result);
  } else {
    computeResultMultVars(result);
  }
  result->finish();
  LOG(DEBUG) << "TextOperationWithoutFilter result computation done." << endl;
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResultNoVar(ResultTable* result) const {
  result->_nofColumns = 2;
  result->_fixedSizeData = new vector<array<Id, 2>>;
  result->_resultTypes.push_back(ResultTable::ResultType::TEXT);
  result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
  getExecutionContext()->getIndex().getContextListForWords(
      _words, reinterpret_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResultOneVar(
    ResultTable* result) const {
  result->_nofColumns = 3;
  result->_fixedSizeData = new vector<array<Id, 3>>;
  result->_resultTypes.push_back(ResultTable::ResultType::TEXT);
  result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  getExecutionContext()->getIndex().getECListForWords(
      _words, _textLimit,
      *reinterpret_cast<vector<array<Id, 3>>*>(result->_fixedSizeData));
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResultMultVars(
    ResultTable* result) const {
  if (_nofVars == 2) {
    result->_fixedSizeData = new vector<array<Id, 4>>;
    result->_nofColumns = 4;
    result->_resultTypes.push_back(ResultTable::ResultType::TEXT);
    result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    getExecutionContext()->getIndex().getECListForWords(
        _words, _nofVars, _textLimit,
        *reinterpret_cast<vector<array<Id, 4>>*>(result->_fixedSizeData));
  } else if (_nofVars == 3) {
    result->_fixedSizeData = new vector<array<Id, 5>>;
    result->_nofColumns = 5;
    result->_resultTypes.push_back(ResultTable::ResultType::TEXT);
    result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    getExecutionContext()->getIndex().getECListForWords(
        _words, _nofVars, _textLimit,
        *reinterpret_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
  } else {
    result->_nofColumns = _nofVars + 2;
    result->_resultTypes.reserve(result->_nofColumns);
    result->_resultTypes.push_back(ResultTable::ResultType::TEXT);
    result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
    for (size_t i = 2; i < result->_nofColumns; i++) {
      result->_resultTypes.push_back(ResultTable::ResultType::KB);
    }
    getExecutionContext()->getIndex().getECListForWords(
        _words, _nofVars, _textLimit, result->_varSizeData);
  }
}

// _____________________________________________________________________________
size_t TextOperationWithoutFilter::getSizeEstimate() {
  if (_sizeEstimate == std::numeric_limits<size_t>::max()) {
    double nofEntitiesSingleVar;
    if (_executionContext) {
      nofEntitiesSingleVar =
          _executionContext->getIndex().getSizeEstimate(_words) *
          std::min(float(_textLimit),
                   _executionContext->getIndex().getAverageNofEntityContexts());
    } else {
      nofEntitiesSingleVar = 10000 * 0.8;
    }
    _sizeEstimate = static_cast<size_t>(pow(nofEntitiesSingleVar, _nofVars));
  }
  return _sizeEstimate;
}

// _____________________________________________________________________________
size_t TextOperationWithoutFilter::getCostEstimate() {
  if (_executionContext) {
    return static_cast<size_t>(
        _executionContext->getCostFactor("NO_FILTER_PUNISH") *
        (getSizeEstimate() * _nofVars));
  } else {
    return getSizeEstimate() * _nofVars;
  }
}

// _____________________________________________________________________________
float TextOperationWithoutFilter::getMultiplicity(size_t col) {
  if (_multiplicities.size() == 0) {
    computeMultiplicities();
  }
  AD_CHECK_LT(col, _multiplicities.size());
  return _multiplicities[col];
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeMultiplicities() {
  for (size_t i = 0; i < getResultWidth(); ++i) {
    double nofEntitiesSingleVar;
    if (_executionContext) {
      nofEntitiesSingleVar =
          _executionContext->getIndex().getSizeEstimate(_words) *
          std::min(float(_textLimit),
                   _executionContext->getIndex().getAverageNofEntityContexts());
    } else {
      nofEntitiesSingleVar = 10000 * 0.8;
    }
    _multiplicities.emplace_back(pow(nofEntitiesSingleVar, _nofVars - 1));
  }
  assert(getResultWidth() == _multiplicities.size());
}
