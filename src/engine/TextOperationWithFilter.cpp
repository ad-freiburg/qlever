// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>

#include "./QueryExecutionTree.h"
#include "TextOperationWithFilter.h"


using std::string;


// _____________________________________________________________________________
size_t TextOperationWithFilter::getResultWidth() const {
  return 1 + _nofVars + _filterResult->getResultWidth();
}

// _____________________________________________________________________________
TextOperationWithFilter::TextOperationWithFilter(
    QueryExecutionContext* qec, const string& words, size_t nofVars,
    std::shared_ptr<QueryExecutionTree> filterResult, size_t filterColumn,
    size_t textLimit) :
    Operation(qec), _words(words), _nofVars(nofVars),
    _filterResult(filterResult),
    _filterColumn(filterColumn) {
  setTextLimit(textLimit);
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
  result->_nofColumns = 1 + _filterResult->getResultWidth() + _nofVars;
  if (_filterResult->getResultWidth() == 1) {
    AD_CHECK_GE(result->_nofColumns, 3);
    if (result->_nofColumns == 3) {
      result->_fixedSizeData = new vector<array<Id, 3>>;
      getExecutionContext()->getIndex().getFilteredECListForWords(
          _words,
          *static_cast<vector<array<Id, 1>>*>(_filterResult->getResult()._fixedSizeData),
          _nofVars,
          _textLimit,
          *reinterpret_cast<vector<array<Id, 3>>*>(result->_fixedSizeData));
    } else if (result->_nofColumns == 4) {
      result->_fixedSizeData = new vector<array<Id, 4>>;
      getExecutionContext()->getIndex().getFilteredECListForWords(
          _words,
          *static_cast<vector<array<Id, 1>>*>(_filterResult->getResult()._fixedSizeData),
          _nofVars,
          _textLimit,
          *reinterpret_cast<vector<array<Id, 4>>*>(result->_fixedSizeData));
    } else if (result->_nofColumns == 5) {
      result->_fixedSizeData = new vector<array<Id, 5>>;
      getExecutionContext()->getIndex().getFilteredECListForWords(
          _words,
          *static_cast<vector<array<Id, 1>>*>(_filterResult->getResult()._fixedSizeData),
          _nofVars,
          _textLimit,
          *reinterpret_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else {
      getExecutionContext()->getIndex().getFilteredECListForWords(
          _words,
          *static_cast<vector<array<Id, 1>>*>(_filterResult->getResult()._fixedSizeData),
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


// _____________________________________________________________________________
float TextOperationWithFilter::getMultiplicity(size_t col) {
  if (_multiplicities.size() == 0) {
    computeMultiplicities();
  }
  AD_CHECK_LT(col, _multiplicities.size());
  return _multiplicities[col];
}

// _____________________________________________________________________________
void TextOperationWithFilter::computeMultiplicities() {
  if (_executionContext) {
    // Like without filter
    vector<float> multiplicitiesNoFilter;
    for (size_t i = 0;
         i < getResultWidth() + 1 - _filterResult->getResultWidth(); ++i) {
      double nofEntitiesSingleVar;
      if (_executionContext) {
        nofEntitiesSingleVar =
            _executionContext->getIndex().getSizeEstimate(_words) *
            std::min(float(_textLimit),
                     _executionContext->getIndex().getAverageNofEntityContexts());
      } else {
        nofEntitiesSingleVar = 10000 * 0.8;
      }
      multiplicitiesNoFilter.emplace_back(
          pow(nofEntitiesSingleVar, _nofVars - 1));
    }

    assert(multiplicitiesNoFilter.size() > 2);

    // Like joins
    float _leftJcM = multiplicitiesNoFilter[2];
    float _rightJcM = _filterResult->getMultiplicity(_filterColumn);
    for (size_t i = 0; i < multiplicitiesNoFilter.size() - 1; ++i) {
      _multiplicities.emplace_back(multiplicitiesNoFilter[i] * _rightJcM);
    }
    for (size_t i = 0; i < _filterResult->getResultWidth(); ++i) {
      _multiplicities.emplace_back(
          _filterResult->getMultiplicity(i) * _leftJcM);
    }
  } else {
    for (size_t i = 0; i < getResultWidth(); ++i) {
      _multiplicities.emplace_back(1);
    }
  }
  assert(_multiplicities.size() == getResultWidth());
}

// _____________________________________________________________________________
size_t TextOperationWithFilter::getSizeEstimate() {
  if (_sizeEstimate == std::numeric_limits<size_t>::max()) {
    if (_executionContext) {
      // NEW at 05 Dec 2016:
      // Estimate the size of the result like the equivalent text without filter
      // plus join.
      double nofEntitiesSingleVar;
      if (_executionContext) {
        nofEntitiesSingleVar =
            _executionContext->getIndex().getSizeEstimate(_words) *
            std::min(float(_textLimit),
                     _executionContext->getIndex().getAverageNofEntityContexts());
      } else {
        nofEntitiesSingleVar = 10000 * 0.8;
      }

      auto estNoFil = static_cast<size_t>(pow(nofEntitiesSingleVar, _nofVars));

      size_t nofDistinctFilter = static_cast<size_t>(
          _filterResult->getSizeEstimate() /
          _filterResult->getMultiplicity(_filterColumn));

      float joinColMultiplicity = getMultiplicity(
          2 + (_nofVars - 1) + _filterColumn);

      _sizeEstimate = static_cast<size_t>(
          _executionContext->getCostFactor(
              "JOIN_SIZE_ESTIMATE_CORRECTION_FACTOR") *
          joinColMultiplicity *
          std::min(nofDistinctFilter, estNoFil));
    }
    _sizeEstimate = size_t(10000 * 0.8);
  }
  return _sizeEstimate;
}

// _____________________________________________________________________________
size_t TextOperationWithFilter::getCostEstimate() {
  if (_filterResult->knownEmptyResult()) {
    return 0;
  }
  if (_executionContext) {
    return static_cast<size_t>(
        _executionContext->getCostFactor("FILTER_PUNISH") * (
            getSizeEstimate() * _nofVars +
            _filterResult->getSizeEstimate() *
            _executionContext->getCostFactor("HASH_MAP_OPERATION_COST") +
            _filterResult->getCostEstimate()));
  } else {
    return _filterResult->getSizeEstimate() * 2 +
           _filterResult->getCostEstimate();
  }
}
