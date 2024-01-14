// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "TextOperationWithFilter.h"

#include <sstream>

#include "./QueryExecutionTree.h"

using std::endl;
using std::string;

// _____________________________________________________________________________
size_t TextOperationWithFilter::getResultWidth() const {
  return 1 + getNofPrefixedTerms() + getNofVars() +
         _filterResult->getResultWidth();
}

// _____________________________________________________________________________
size_t TextOperationWithFilter::getNofPrefixedTerms() const {
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
TextOperationWithFilter::TextOperationWithFilter(
    QueryExecutionContext* qec, const string& words,
    const SetOfVariables& variables, const Variable& cvar,
    std::shared_ptr<QueryExecutionTree> filterResult, size_t filterColumn,
    size_t textLimit)
    : Operation(qec),
      _words(words),
      _variables(variables),
      _cvar(cvar),
      _filterResult(filterResult),
      _filterColumn(filterColumn) {
  setTextLimit(textLimit);
}

// _____________________________________________________________________________
VariableToColumnMap TextOperationWithFilter::computeVariableToColumnMap()
    const {
  VariableToColumnMap vcmap;
  // Subtract one because the entity that we filtered on
  // is provided by the filter table and still has the same place there.
  vcmap[_cvar] = makeAlwaysDefinedColumn(ColumnIndex{0});
  vcmap[_cvar.getTextScoreVariable()] = makeAlwaysDefinedColumn(ColumnIndex{1});
  auto colN = ColumnIndex{2};
  const auto& filterColumns = _filterResult->getVariableColumns();
  // TODO<joka921> The order of the `_variables` is not deterministic,
  // check whether this is correct (especially in the presence of caching).
  for (const auto& var : _variables) {
    if (var == _cvar) {
      continue;
    }
    if (!filterColumns.contains(var)) {
      // TODO<joka921> These variables seem to be newly created an never contain
      // undefined values. However I currently don't understand their semantics
      // which should be documented.
      vcmap[var] = makeAlwaysDefinedColumn(colN);
      ++colN;
    }
  }
  for (const auto& varcol : filterColumns) {
    // TODO<joka921> It is possible that UNDEF values in the filter are never
    // propagated to the  result, but this has to be further examined.
    vcmap[varcol.first] =
        ColumnIndexAndTypeInfo{ColumnIndex{colN + varcol.second.columnIndex_},
                               varcol.second.mightContainUndef_};
  }
  for (std::string s : std::vector<std::string>(absl::StrSplit(_words, ' '))) {
    if (!s.ends_with('*')) {
      continue;
    }
    s.pop_back();
    vcmap[_cvar.getMatchingWordVariable(s)] = makeAlwaysDefinedColumn(
        ColumnIndex{colN + _filterResult->getResultWidth()});
    colN++;
  }
  return vcmap;
}

// _____________________________________________________________________________
string TextOperationWithFilter::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "TEXT OPERATION WITH FILTER:"
     << " co-occurrence with words: \"" << _words << "\" and " << getNofVars()
     << " variables with textLimit = " << _textLimit << " filtered by\n"
     << _filterResult->getCacheKey() << "\n";
  os << " filtered on column " << _filterColumn;
  return std::move(os).str();
}

// _____________________________________________________________________________
string TextOperationWithFilter::getDescriptor() const {
  return "TextOperationWithFilter with  " + _words;
}

// _____________________________________________________________________________
ResultTable TextOperationWithFilter::computeResult() {
  LOG(DEBUG) << "TextOperationWithFilter result computation..." << endl;
  AD_CONTRACT_CHECK(getNofVars() >= 1);
  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());
  shared_ptr<const ResultTable> filterResult = _filterResult->getResult();

  if (filterResult->idTable().numColumns() == 1) {
    getExecutionContext()->getIndex().getFilteredECListForWordsWidthOne(
        _words, filterResult->idTable(), getNofVars(), _textLimit, &idTable);
  } else {
    getExecutionContext()->getIndex().getFilteredECListForWords(
        _words, filterResult->idTable(), _filterColumn, getNofVars(),
        _textLimit, &idTable);
  }

  LOG(DEBUG) << "TextOperationWithFilter result computation done." << endl;
  return {std::move(idTable), resultSortedOn(),
          filterResult->getSharedLocalVocab()};
}

// _____________________________________________________________________________
float TextOperationWithFilter::getMultiplicity(size_t col) {
  if (_multiplicities.size() == 0) {
    computeMultiplicities();
  }
  AD_CONTRACT_CHECK(col < _multiplicities.size());
  return _multiplicities.at(col);
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
            std::min(
                float(_textLimit),
                _executionContext->getIndex().getAverageNofEntityContexts());
      } else {
        nofEntitiesSingleVar = 10000 * 0.8;
      }
      multiplicitiesNoFilter.emplace_back(
          pow(nofEntitiesSingleVar, getNofVars() - 1));
    }

    if (multiplicitiesNoFilter.size() <= 2) {
      AD_THROW(
          "One (out of more) reasons for this problem is if you connected"
          " a text record variable to other variables with"
          " a non-text predicate. "
          "One should always use ql:contains-entity for that.");
    }

    // Like joins
    float _leftJcM = multiplicitiesNoFilter[2];
    float _rightJcM = _filterResult->getMultiplicity(_filterColumn);
    for (size_t i = 0; i < multiplicitiesNoFilter.size() - 1; ++i) {
      _multiplicities.emplace_back(multiplicitiesNoFilter[i] * _rightJcM);
    }
    for (size_t i = 0; i < _filterResult->getResultWidth(); ++i) {
      _multiplicities.emplace_back(_filterResult->getMultiplicity(i) *
                                   _leftJcM);
    }
  } else {
    for (size_t i = 0; i < getResultWidth(); ++i) {
      _multiplicities.emplace_back(1);
    }
  }
  assert(_multiplicities.size() == getResultWidth());
}

// _____________________________________________________________________________
uint64_t TextOperationWithFilter::getSizeEstimateBeforeLimit() {
  if (_sizeEstimate == std::numeric_limits<size_t>::max()) {
    if (_executionContext) {
      // NEW at 05 Dec 2016:
      // Estimate the size of the result like the equivalent text without filter
      // plus join.
      double nofEntitiesSingleVar =
          _executionContext->getIndex().getSizeEstimate(_words) *
          std::min(float(_textLimit),
                   _executionContext->getIndex().getAverageNofEntityContexts());

      auto estNoFil =
          static_cast<size_t>(pow(nofEntitiesSingleVar, getNofVars()));

      size_t nofDistinctFilter =
          static_cast<size_t>(_filterResult->getSizeEstimate() /
                              _filterResult->getMultiplicity(_filterColumn));

      float joinColMultiplicity =
          getMultiplicity(2 + (getNofVars() - 1) + _filterColumn);

      _sizeEstimate = std::max(
          size_t(1),
          static_cast<size_t>(_executionContext->getCostFactor(
                                  "JOIN_SIZE_ESTIMATE_CORRECTION_FACTOR") *
                              joinColMultiplicity *
                              std::min(nofDistinctFilter, estNoFil)));
    } else {
      _sizeEstimate = size_t(10000 * 0.8);
    }
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
        _executionContext->getCostFactor("FILTER_PUNISH") *
        (getSizeEstimateBeforeLimit() * getNofVars() +
         _filterResult->getSizeEstimate() *
             _executionContext->getCostFactor("HASH_MAP_OPERATION_COST") +
         _filterResult->getCostEstimate()));
  } else {
    return _filterResult->getSizeEstimate() * 2 +
           _filterResult->getCostEstimate();
  }
}
