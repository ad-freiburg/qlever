// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "TextOperationWithoutFilter.h"

#include <sstream>

#include "./QueryExecutionTree.h"

using std::endl;
using std::string;

// _____________________________________________________________________________
size_t TextOperationWithoutFilter::getResultWidth() const {
  size_t width = 2 + getNofVars() + getNofPrefixedTerms();
  return width;
}

// _____________________________________________________________________________
size_t TextOperationWithoutFilter::getNofPrefixedTerms() const {
  // Returns the number of words in _words that end with '*'
  // TODO<C++23>: This is a one-liner using `std::ranges::fold`.
  size_t nPrefixedTerms = 0;
  for (std::string_view s : absl::StrSplit(_words, ' ')) {
    if (s.ends_with('*')) {
      nPrefixedTerms++;
    }
  }
  return nPrefixedTerms;
}

// _____________________________________________________________________________
TextOperationWithoutFilter::TextOperationWithoutFilter(
    QueryExecutionContext* qec, const std::vector<std::string>& words,
    SetOfVariables variables, Variable cvar, size_t textLimit)
    : Operation(qec),
      _words(absl::StrJoin(words, " ")),
      _variables(std::move(variables)),
      _cvar(std::move(cvar)),
      _textLimit(textLimit),
      _sizeEstimate(std::numeric_limits<size_t>::max()) {}

// _____________________________________________________________________________
VariableToColumnMap TextOperationWithoutFilter::computeVariableToColumnMap()
    const {
  VariableToColumnMap vcmap;
  auto addDefinedVar = [&vcmap,
                        index = ColumnIndex{0}](const Variable& var) mutable {
    vcmap[var] = makeAlwaysDefinedColumn(index);
    ++index;
  };
  addDefinedVar(_cvar);
  addDefinedVar(_cvar.getTextScoreVariable());
  // TODO<joka921> The order of the variables is not deterministic, check
  // whether this is correct.
  // TODO<joka921> These variables seem to be newly created an never contain
  // undefined values. However I currently don't understand their semantics
  // which should be documented.
  for (const auto& var : _variables) {
    if (var != _cvar) {
      addDefinedVar(var);
    }
  }
  for (std::string s : std::vector<std::string>(absl::StrSplit(_words, ' '))) {
    if (!s.ends_with('*')) {
      continue;
    }
    s.pop_back();
    addDefinedVar(_cvar.getMatchingWordVariable(s));
  }
  return vcmap;
}
// _____________________________________________________________________________
string TextOperationWithoutFilter::getCacheKeyImpl() const {
  std::ostringstream os;
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
ResultTable TextOperationWithoutFilter::computeResult() {
  LOG(DEBUG) << "TextOperationWithoutFilter result computation..." << endl;
  IdTable table{getExecutionContext()->getAllocator()};
  if (getNofVars() == 0) {
    computeResultNoVar(&table);
  } else if (getNofVars() == 1) {
    computeResultOneVar(&table);
  } else {
    computeResultMultVars(&table);
  }
  LOG(DEBUG) << "TextOperationWithoutFilter result computation done." << endl;
  return {std::move(table), resultSortedOn(), LocalVocab{}};
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResultNoVar(IdTable* idTable) const {
  idTable->setNumColumns(2 + getNofPrefixedTerms());
  getExecutionContext()->getIndex().getContextListForWords(_words, idTable);
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResultOneVar(IdTable* idTable) const {
  idTable->setNumColumns(3 + getNofPrefixedTerms());
  getExecutionContext()->getIndex().getECListForWordsOneVar(_words, _textLimit,
                                                            idTable);
}

// _____________________________________________________________________________
void TextOperationWithoutFilter::computeResultMultVars(IdTable* idTable) const {
  idTable->setNumColumns(2 + getNofVars() + getNofPrefixedTerms());
  getExecutionContext()->getIndex().getECListForWords(_words, getNofVars(),
                                                      _textLimit, idTable);
}

// _____________________________________________________________________________
uint64_t TextOperationWithoutFilter::getSizeEstimateBeforeLimit() {
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
        (getSizeEstimateBeforeLimit() * getNofVars()));
  } else {
    return getSizeEstimateBeforeLimit() * getNofVars();
  }
}

// _____________________________________________________________________________
float TextOperationWithoutFilter::getMultiplicity(size_t col) {
  if (_multiplicities.size() == 0) {
    computeMultiplicities();
  }
  AD_CONTRACT_CHECK(col < _multiplicities.size());
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
