// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)

#include "CountAvailablePredicates.h"



// _____________________________________________________________________________
CountAvailablePredicates::CountAvailablePredicates(QueryExecutionContext *qec,
                                                   std::shared_ptr<QueryExecutionTree> subtree,
                                                   size_t subjectColumnIndex) :
  Operation(qec),
  _subtree(subtree),
  _subjectColumnIndex(subjectColumnIndex) { }


// _____________________________________________________________________________
string CountAvailablePredicates::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) { os << " "; }
  os << "COUNT_AVAILABLE_PREDICATES (col " << _subjectColumnIndex << ")\n"
     << _subtree->asString(indent);
  return os.str();
}

// _____________________________________________________________________________
size_t CountAvailablePredicates::getResultWidth() const {
  return 2;
}

// _____________________________________________________________________________
size_t CountAvailablePredicates::resultSortedOn() const {
  //TODO(florian): stub
  return 0;
}

// _____________________________________________________________________________
std::unordered_map<string, size_t> CountAvailablePredicates::getVariableColumns() const {
  std::unordered_map<string, size_t> varCols;
  //TODO(florian): stub
  varCols["predicate"] = 0;
  varCols["count"] = 1;
  return varCols;
}

// _____________________________________________________________________________
float CountAvailablePredicates::getMultiplicity(size_t col) {
  //TODO(florian): stub
  return 1;
}

// _____________________________________________________________________________
size_t CountAvailablePredicates::getSizeEstimate() {
  //TODO(florian): stub
  return _subtree->getSizeEstimate();
}

// _____________________________________________________________________________
size_t CountAvailablePredicates::getCostEstimate() {
  //TODO(florian): stub
  return 1;
}

// _____________________________________________________________________________
void CountAvailablePredicates::computeResult(ResultTable* result) const {
  // TODO(florian): make the loading of the has-pattern and has-relation relations cachable

  result->_nofColumns = 2;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 2>>();

  // load the has pattern table
  ResultTable hasPattern;
  hasPattern._nofColumns = 2;
  hasPattern._sortedBy = 0;
  hasPattern._fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanHasPattern(static_cast<vector<array<Id, 2>>*>(hasPattern._fixedSizeData));
  hasPattern._status = ResultTable::FINISHED;

  // load the has relation table
  ResultTable hasRelation;
  hasRelation._nofColumns = 2;
  hasRelation._sortedBy = 0;
  hasRelation._fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanHasRelation(static_cast<vector<array<Id, 2>>*>(hasRelation._fixedSizeData));
  hasRelation._status = ResultTable::FINISHED;

  const std::vector<Pattern>& patterns = _executionContext->getIndex().getPatterns();

  const ResultTable& subresult = _subtree->getResult();

  if (result->_nofColumns > 5) {
    Engine::computePatternTrick<vector<Id>>(&subresult._varSizeData,
                                            static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData),
                                            static_cast<vector<array<Id, 2>>*>(hasPattern._fixedSizeData),
                                            static_cast<vector<array<Id, 2>>*>(hasRelation._fixedSizeData),
                                            patterns,
                                            _subjectColumnIndex);
  } else {
    if (subresult._nofColumns == 1) {
      Engine::computePatternTrick<array<Id, 1>>(static_cast<vector<array<Id, 1>>*>(subresult._fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(hasPattern._fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(hasRelation._fixedSizeData),
                                                patterns,
                                                _subjectColumnIndex);
    } else if (subresult._nofColumns == 2) {
      Engine::computePatternTrick<array<Id, 2>>(static_cast<vector<array<Id, 2>>*>(subresult._fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(hasPattern._fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(hasRelation._fixedSizeData),
                                                patterns,
                                                _subjectColumnIndex);
    } else if (subresult._nofColumns == 3) {
      Engine::computePatternTrick<array<Id, 3>>(static_cast<vector<array<Id, 3>>*>(subresult._fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(hasPattern._fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(hasRelation._fixedSizeData),
                                                patterns,
                                                _subjectColumnIndex);
    } else if (subresult._nofColumns == 4) {
      Engine::computePatternTrick<array<Id, 4>>(static_cast<vector<array<Id, 4>>*>(subresult._fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(hasPattern._fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(hasRelation._fixedSizeData),
                                                patterns,
                                                _subjectColumnIndex);
    } else if (subresult._nofColumns == 5) {
      Engine::computePatternTrick<array<Id, 5>>(static_cast<vector<array<Id, 5>>*>(subresult._fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(hasPattern._fixedSizeData),
                                                static_cast<vector<array<Id, 2>>*>(hasRelation._fixedSizeData),
                                                patterns,
                                                _subjectColumnIndex);
    }
  }
}
