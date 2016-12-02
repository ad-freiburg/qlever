// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <string>
#include <sstream>
#include "./IndexScan.h"

using std::string;

// _____________________________________________________________________________
string IndexScan::asString() const {
  std::ostringstream os;
  switch (_type) {
    case PSO_BOUND_S:
      os << "SCAN PSO with P = \"" << _predicate << "\", S = \"" << _subject <<
         "\"";
      break;
    case POS_BOUND_O:
      os << "SCAN POS with P = \"" << _predicate << "\", O = \"" << _object <<
         "\"";
      break;
    case SOP_BOUND_O:
      os << "SCAN SOP with S = \"" << _subject << "\", O = \"" << _object <<
         "\"";
      break;
    case PSO_FREE_S:
      os << "SCAN PSO with P = \"" << _predicate << "\"";
      break;
    case POS_FREE_O:
      os << "SCAN POS with P = \"" << _predicate << "\"";
      break;
    case SPO_FREE_P:
      os << "SCAN SPO with S = \"" << _subject << "\"";
      break;
    case SOP_FREE_O:
      os << "SCAN SOP with S = \"" << _subject << "\"";
      break;
    case OPS_FREE_P:
      os << "SCAN OPS with O = \"" << _object << "\"";
      break;
    case OSP_FREE_S:
      os << "SCAN OSP with O = \"" << _object << "\"";
      break;
  }
  return os.str();
}

// _____________________________________________________________________________
size_t IndexScan::getResultWidth() const {
  switch (_type) {
    case PSO_BOUND_S:
    case POS_BOUND_O:
    case SOP_BOUND_O:
      return 1;
    case PSO_FREE_S:
    case POS_FREE_O:
    case SPO_FREE_P:
    case SOP_FREE_O:
    case OSP_FREE_S:
    case OPS_FREE_P:
      return 2;
    default: AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
                      "Should be unreachable.");
  }
}

// _____________________________________________________________________________
void IndexScan::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "IndexScan result computation...\n";
  switch (_type) {
    case PSO_BOUND_S:
      computePSOboundS(result);
      break;
    case POS_BOUND_O:
      computePOSboundO(result);
      break;
    case PSO_FREE_S:
      computePSOfreeS(result);
      break;
    case POS_FREE_O:
      computePOSfreeO(result);
      break;
    case SOP_BOUND_O:
      computeSOPboundO(result);
      break;
    case SPO_FREE_P:
      computeSPOfreeP(result);
      break;
    case SOP_FREE_O:
      computeSOPfreeO(result);
      break;
    case OSP_FREE_S:
      computeOSPfreeS(result);
      break;
    case OPS_FREE_P:
      computeOPSfreeP(result);
      break;
  }
  LOG(DEBUG) << "IndexScan result computation done.\n";
}

// _____________________________________________________________________________
void IndexScan::computePSOboundS(ResultTable* result) const {
  result->_nofColumns = 1;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 1>>();
  _executionContext->getIndex().scanPSO(
      _predicate, _subject,
      static_cast<vector<array<Id, 1>>*>(result->_fixedSizeData));
  result->_status = ResultTable::FINISHED;
}

// _____________________________________________________________________________
void IndexScan::computePSOfreeS(ResultTable* result) const {
  result->_nofColumns = 2;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanPSO(
      _predicate,
      static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  result->_status = ResultTable::FINISHED;
}

// _____________________________________________________________________________
void IndexScan::computePOSboundO(ResultTable* result) const {
  result->_nofColumns = 1;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 1>>();
  _executionContext->getIndex().scanPOS(
      _predicate, _object,
      static_cast<vector<array<Id, 1>>*>(result->_fixedSizeData));
  result->_status = ResultTable::FINISHED;
}

// _____________________________________________________________________________
void IndexScan::computePOSfreeO(ResultTable* result) const {
  result->_nofColumns = 2;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanPOS(
      _predicate,
      static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  result->_status = ResultTable::FINISHED;
}

// _____________________________________________________________________________
size_t IndexScan::computeSizeEstimate() const {
  if (_executionContext) {
    // Should always be in this branch. Else is only for test cases.
    return getIndex().sizeEstimate(_subject, _predicate, _object);
  } else {
    return 1000 + _subject.size() + _predicate.size() + _object.size();
  }
}

// _____________________________________________________________________________
void IndexScan::computeSPOfreeP(ResultTable* result) const {
  result->_nofColumns = 2;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanSPO(
      _subject,
      static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  result->_status = ResultTable::FINISHED;
}

// _____________________________________________________________________________
void IndexScan::computeSOPboundO(ResultTable* result) const {
  result->_nofColumns = 1;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 1>>();
  _executionContext->getIndex().scanSOP(
      _subject, _object,
      static_cast<vector<array<Id, 1>>*>(result->_fixedSizeData));
  result->_status = ResultTable::FINISHED;
}

// _____________________________________________________________________________
void IndexScan::computeSOPfreeO(ResultTable* result) const {
  result->_nofColumns = 2;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanSOP(
      _subject,
      static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  result->_status = ResultTable::FINISHED;
}

// _____________________________________________________________________________
void IndexScan::computeOPSfreeP(ResultTable* result) const {
  result->_nofColumns = 2;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanOPS(
      _object,
      static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  result->_status = ResultTable::FINISHED;
}

// _____________________________________________________________________________
void IndexScan::computeOSPfreeS(ResultTable* result) const {
  result->_nofColumns = 2;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanOSP(
      _object,
      static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  result->_status = ResultTable::FINISHED;
}

// _____________________________________________________________________________
void IndexScan::determineMultiplicities() {
  _multiplicity.clear();
  if (_executionContext) {
    if (getResultWidth() == 1) {
      _multiplicity.emplace_back(1);
    } else {
      switch (_type) {
        case PSO_FREE_S:
          _multiplicity = getIndex().getPSOMultiplicities(_predicate);
          break;
        case POS_FREE_O:
          _multiplicity = getIndex().getPOSMultiplicities(_predicate);
          break;
        case SPO_FREE_P:
          _multiplicity = getIndex().getSPOMultiplicities(_subject);
          break;
        case SOP_FREE_O:
          _multiplicity = getIndex().getSOPMultiplicities(_subject);
          break;
        case OSP_FREE_S:
          _multiplicity = getIndex().getOSPMultiplicities(_object);
          break;
        case OPS_FREE_P:
          _multiplicity = getIndex().getOPSMultiplicities(_object);
          break;
        default:
          AD_THROW(ad_semsearch::Exception::ASSERT_FAILED,
                   "Switch reached default block unexpectedly!");
      }
    }
  } else {
    _multiplicity.emplace_back(1);
    _multiplicity.emplace_back(1);
  }
  assert(_multiplicity.size() == 1 || _multiplicity.size() == 2);
}










