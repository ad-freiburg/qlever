// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Bürklin (buerklij@informatik.uni-freiburg.de)s

#include "StatScan.h"

// _____________________________________________________________________________
StatScan::StatScan(QueryExecutionContext* qec, Id statId, ScanType type)
    : Operation(qec),
      _sizeEstimate(std::numeric_limits<size_t>::max()),
      _statId(statId),
      _type(type) {}

// _____________________________________________________________________________
string StatScan::asString(size_t indent) const {
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
               "Stat relation not supported.");
  }

  os << "SCAN STATS_";
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
size_t StatScan::getResultWidth() const {
  switch (_type) {
    case POS_BOUND_O:
    case PSO_BOUND_S:
      return 1;
    case PSO_FREE_S:
    case POS_FREE_O:
      return 2;
    default:
      AD_THROW(ad_semsearch::Exception::BAD_INPUT,
               "Stat scan type not supported.");
  }
}

// _____________________________________________________________________________
void StatScan::determineMultiplicities() {
  _multiplicity.clear();
  if (getResultWidth() == 1) {
    _multiplicity.emplace_back(1);
  } else {
    switch (_type) {
      case PSO_FREE_S:
        _multiplicity = getIndex().getStatsPsoMultiplicities(_statId);
        break;
      case POS_FREE_O:
        _multiplicity = getIndex().getStatsPosMultiplicities(_statId);
        break;
      default:
        AD_THROW(ad_semsearch::Exception::ASSERT_FAILED,
                 "Switch reached default block unexpectedly!");
    }
  }
}

// _____________________________________________________________________________
size_t StatScan::computeSizeEstimate() const {
  return getIndex().statSizeEstimate(_statId);
}

// _____________________________________________________________________________
void StatScan::computeResult(ResultTable* result) const {
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
void StatScan::computePSOfreeS(ResultTable* result) const {
  result->_nofColumns = 2;
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  if (_statId == Id(0) || _statId == Id(2)) {
    result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
  } else if (_statId == Id(1)) {
    result->_resultTypes.push_back(ResultTable::ResultType::ENTITY_TYPE);
  }
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanStatsPso(
      _statId, static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  result->finish();
}

// _____________________________________________________________________________
void StatScan::computePOSfreeO(ResultTable* result) const {
  result->_nofColumns = 2;
  if (_statId == Id(0) || _statId == Id(2)) {
    result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
  } else if (_statId == Id(1)) {
    result->_resultTypes.push_back(ResultTable::ResultType::ENTITY_TYPE);
  }
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanStatsPos(
      _statId, static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  result->finish();
}

// _____________________________________________________________________________
void StatScan::computePOSboundO(ResultTable* result) const {
  result->_nofColumns = 1;
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 1>>();
  _executionContext->getIndex().scanStatsPos(
      _statId, _object,
      static_cast<vector<array<Id, 1>>*>(result->_fixedSizeData));
  result->finish();
}

// _____________________________________________________________________________
void StatScan::computePSOboundS(ResultTable* result) const {
  result->_nofColumns = 1;
  if (_statId == Id(0) || _statId == Id(2)) {
    result->_resultTypes.push_back(ResultTable::ResultType::VERBATIM);
  } else if (_statId == Id(1)) {
    result->_resultTypes.push_back(ResultTable::ResultType::ENTITY_TYPE);
  }
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 1>>();
  _executionContext->getIndex().scanStatsPso(
      _statId, _subject,
      static_cast<vector<array<Id, 1>>*>(result->_fixedSizeData));
  result->finish();
}
