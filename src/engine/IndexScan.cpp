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
    case PSO_FREE_S:
      os << "SCAN PSO with P = \"" << _predicate << "\"";
      break;
    case POS_FREE_O:
      os << "SCAN POS with P = \"" << _predicate << "\"";
      break;
  }
  return os.str();
}

// _____________________________________________________________________________
size_t IndexScan::getResultWidth() const {
  switch (_type) {
    case PSO_BOUND_S:
    case POS_BOUND_O:
      return 1;
    case PSO_FREE_S:
    case POS_FREE_O:
      return 2;
    default:
      return 0;
  }
}

// _____________________________________________________________________________
void IndexScan::computeResult(ResultTable* result) const {
  LOG(INFO) << "IndexScan result computation...\n";
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
  }
  LOG(INFO) << "IndexScan result computation done.\n";
}

// _____________________________________________________________________________
void IndexScan::computePSOboundS(ResultTable* result) const {
  result->_nofColumns = 1;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 1>>();
  _executionContext->getIndex().scanPSO(_predicate, _subject,
      static_cast<vector<array<Id, 1>>*>(result->_fixedSizeData));
  result->_status = ResultTable::FINISHED;
}

// _____________________________________________________________________________
void IndexScan::computePSOfreeS(ResultTable* result) const {
  result->_nofColumns = 2;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanPSO(_predicate,
      static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  result->_status = ResultTable::FINISHED;
}

// _____________________________________________________________________________
void IndexScan::computePOSboundO(ResultTable* result) const {
  result->_nofColumns = 1;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 1>>();
  _executionContext->getIndex().scanPOS(_predicate, _object,
      static_cast<vector<array<Id, 1>>*>(result->_fixedSizeData));
  result->_status = ResultTable::FINISHED;
}

// _____________________________________________________________________________
void IndexScan::computePOSfreeO(ResultTable* result) const {
  result->_nofColumns = 2;
  result->_sortedBy = 0;
  result->_fixedSizeData = new vector<array<Id, 2>>();
  _executionContext->getIndex().scanPOS(_predicate,
      static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
  result->_status = ResultTable::FINISHED;
}
