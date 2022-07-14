// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "TextOperationWithoutFilter.h"

#include <sstream>

#include "./QueryExecutionTree.h"

using std::string;

// _____________________________________________________________________________
size_t TextOperationWithoutFilter::getResultWidth() const {
  size_t width = 2 + getNofVars();
  return width;
}

// _____________________________________________________________________________
TextOperationWithoutFilter::TextOperationWithoutFilter(
    QueryExecutionContext* qec, const string& words,
    const std::set<string>& variables, const string& cvar, size_t textLimit)
    : Operation(qec),
      _words(words),
      _variables(variables),
      _cvar(cvar),
      _textLimit(textLimit),
      _sizeEstimate(std::numeric_limits<size_t>::max()) {}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t>
TextOperationWithoutFilter::getVariableColumns() const {
  ad_utility::HashMap<string, size_t> vcmap;
  size_t index = 0;
  vcmap[_cvar] = index++;
  vcmap[absl::StrCat(TEXTSCORE_VARIABLE_PREFIX, _cvar.substr(1))] = index++;
  for (const auto& var : _variables) {
    if (var != _cvar) {
      vcmap[var] = index++;
    }
  }
  return vcmap;
}
// _____________________________________________________________________________
string TextOperationWithoutFilter::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "TEXT OPERATION WITHOUT FILTER:"
     << " co-occurrence with words: \"" << _words << "\" and " << getNofVars()
     << " variables";
  ;
  os << " with textLimit = " << _textLimit;
  return std::move(os).str();
}

// _____________________________________________________________________________
string TextOperationWithoutFilter::getDescriptor() const {
  return "TextOperationWithoutFilter with " + _words;
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResult(ResultTable* result) {
  LOG(DEBUG) << "TextOperationWithoutFilter result computation..." << endl;
  if (getNofVars() == 0) {
    computeResultNoVar(result);
  } else if (getNofVars() == 1) {
    computeResultOneVar(result);
  } else {
    computeResultMultVars(result);
  }

  LOG(DEBUG) << "TextOperationWithoutFilter result computation done." << endl;
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResultNoVar(ResultTable* result) const {
  result->_idTable.setCols(2);
  result->_resultTypes.push_back(ResultTable::ResultType::TEXT);
  result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
  getExecutionContext()->getIndex().getContextListForWords(_words,
                                                           &result->_idTable);
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResultOneVar(
    ResultTable* result) const {
  result->_idTable.setCols(3);
  result->_resultTypes.push_back(ResultTable::ResultType::TEXT);
  result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  getExecutionContext()->getIndex().getECListForWordsOneVar(_words, _textLimit,
                                                            &result->_idTable);
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResultMultVars(
    ResultTable* result) const {
  result->_idTable.setCols(getNofVars() + 2);
  result->_resultTypes.reserve(result->_idTable.cols());
  result->_resultTypes.push_back(ResultTable::ResultType::TEXT);
  result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
  for (size_t i = 2; i < result->_idTable.cols(); i++) {
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
  }
  getExecutionContext()->getIndex().getECListForWords(
      _words, getNofVars(), _textLimit, &result->_idTable);
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
    _sizeEstimate =
        static_cast<size_t>(pow(nofEntitiesSingleVar, getNofVars()));
  }
  return _sizeEstimate;
}

// _____________________________________________________________________________
size_t TextOperationWithoutFilter::getCostEstimate() {
  if (_executionContext) {
    return static_cast<size_t>(
        _executionContext->getCostFactor("NO_FILTER_PUNISH") *
        (getSizeEstimate() * getNofVars()));
  } else {
    return getSizeEstimate() * getNofVars();
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
    _multiplicities.emplace_back(pow(nofEntitiesSingleVar, getNofVars() - 1));
  }
  assert(getResultWidth() == _multiplicities.size());
}
