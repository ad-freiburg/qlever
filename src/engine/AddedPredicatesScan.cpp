// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Bürklin (buerklij@informatik.uni-freiburg.de)s

#include "AddedPredicatesScan.h"
#include <limits>
#include <string>
#include <vector>

// _____________________________________________________________________________
AddedPredicatesScan::AddedPredicatesScan(QueryExecutionContext* qec, Id statId,
                                         ScanType type)
    : Operation(qec),
      _sizeEstimate(std::numeric_limits<size_t>::max()),
      _statId(statId),
      _type(type) {}

// _____________________________________________________________________________
string AddedPredicatesScan::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }

  string stat;
  switch (_statId) {
    case 0:
      stat = NUM_TRIPLES_PREDICATE;
      break;
    case 1:
      stat = ENTITY_TYPE_PREDICATE;
      break;
    case 2:
      stat = NUM_OCCURRENCES_PREDICATE;
      break;
    default:
      AD_THROW(ad_semsearch::Exception::BAD_INPUT,
               "Added predicate not supported.");
  }

  os << "SCAN ADDED_PREDICATES_";
  switch (_type) {
    case POS_BOUND_O:
      os << "POS with stat = " << stat << ", O = \"" << _object << "\"";
      break;
    case PSO_BOUND_S:
      os << "PSO with stat = " << stat << ", S = \"" << _subject << "\"";
      break;
    case PSO_FREE_S:
    default:
      os << "PSO with stat = " << stat;
  }
  return os.str();
}

// _____________________________________________________________________________
size_t AddedPredicatesScan::getResultWidth() const {
  switch (_type) {
    case POS_BOUND_O:
    case PSO_BOUND_S:
      return 1;
    case PSO_FREE_S:
    case POS_FREE_O:
      return 2;
    default:
      AD_THROW(ad_semsearch::Exception::BAD_INPUT,
               "Added Predicate scan type not supported.");
  }
}

// _____________________________________________________________________________
void AddedPredicatesScan::determineMultiplicities() {
  _multiplicity.clear();
  if (getResultWidth() == 1) {
    _multiplicity.emplace_back(1);
  } else {
    switch (_type) {
      case PSO_FREE_S:
        _multiplicity = getIndex().getAddedPredicatesPsoMultiplicities(_statId);
        break;
      case POS_FREE_O:
        _multiplicity = getIndex().getAddedPredicatesPosMultiplicities(_statId);
        break;
      default:
        AD_THROW(ad_semsearch::Exception::ASSERT_FAILED,
                 "Switch reached default block unexpectedly!");
    }
  }
}

// _____________________________________________________________________________
size_t AddedPredicatesScan::computeSizeEstimate() const {
  return getIndex().addedPredicatesSizeEstimate(_statId);
}

// _____________________________________________________________________________
void AddedPredicatesScan::computeResult(ResultTable* result) const {
  switch (_type) {
    case POS_BOUND_O:
      computePOSboundO(result);
      break;
    case PSO_BOUND_S:
      computePSOboundS(result);
      break;
    case PSO_FREE_S:
      computePSOfreeS(result);
      break;
    case POS_FREE_O:
      computePOSfreeO(result);
      break;
  }
}

// _____________________________________________________________________________
void AddedPredicatesScan::computePSOfreeS(ResultTable* result) const {
  result->_nofColumns = 2;
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  if (_statId == Id(0) || _statId == Id(2)) {
    result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
  } else if (_statId == Id(1)) {
    result->_resultTypes.push_back(ResultTable::ResultType::ENTITY_TYPE);
  }
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanAddedPredicatesPso(
      _statId, static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  result->finish();
}

// _____________________________________________________________________________
void AddedPredicatesScan::computePOSfreeO(ResultTable* result) const {
  result->_nofColumns = 2;
  if (_statId == Id(0) || _statId == Id(2)) {
    result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
  } else if (_statId == Id(1)) {
    result->_resultTypes.push_back(ResultTable::ResultType::ENTITY_TYPE);
  }
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanAddedPredicatesPos(
      _statId, static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  result->finish();
}

// _____________________________________________________________________________
void AddedPredicatesScan::computePOSboundO(ResultTable* result) const {
  result->_nofColumns = 1;
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 1>>();
  _executionContext->getIndex().scanAddedPredicatesPos(
      _statId, _object,
      static_cast<vector<array<Id, 1>>*>(result->_fixedSizeData));
  result->finish();
}

// _____________________________________________________________________________
void AddedPredicatesScan::computePSOboundS(ResultTable* result) const {
  result->_nofColumns = 1;
  if (_statId == Id(0) || _statId == Id(2)) {
    result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
  } else if (_statId == Id(1)) {
    result->_resultTypes.push_back(ResultTable::ResultType::ENTITY_TYPE);
  }
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 1>>();
  _executionContext->getIndex().scanAddedPredicatesPso(
      _statId, _subject,
      static_cast<vector<array<Id, 1>>*>(result->_fixedSizeData));
  result->finish();
}
