// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./IndexScan.h"

#include <sstream>
#include <string>

using std::string;

// _____________________________________________________________________________
string IndexScan::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << ' ';
  }
  switch (_type) {
    case PSO_BOUND_S:
      os << "SCAN PSO with P = \"" << _predicate << "\", S = \"" << _subject
         << "\"";
      break;
    case POS_BOUND_O:
      os << "SCAN POS with P = \"" << _predicate << "\", O = \"" << _object
         << "\"";
      break;
    case SOP_BOUND_O:
      os << "SCAN SOP with S = \"" << _subject << "\", O = \"" << _object
         << "\"";
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
    case FULL_INDEX_SCAN_SPO:
      os << "SCAN FOR FULL INDEX SPO (DUMMY OPERATION)";
      break;
    case FULL_INDEX_SCAN_SOP:
      os << "SCAN FOR FULL INDEX SOP (DUMMY OPERATION)";
      break;
    case FULL_INDEX_SCAN_PSO:
      os << "SCAN FOR FULL INDEX PSO (DUMMY OPERATION)";
      break;
    case FULL_INDEX_SCAN_POS:
      os << "SCAN FOR FULL INDEX POS (DUMMY OPERATION)";
      break;
    case FULL_INDEX_SCAN_OSP:
      os << "SCAN FOR FULL INDEX OSP (DUMMY OPERATION)";
      break;
    case FULL_INDEX_SCAN_OPS:
      os << "SCAN FOR FULL INDEX OPS (DUMMY OPERATION)";
      break;
  }
  return os.str();
}

// _____________________________________________________________________________
string IndexScan::getDescriptor() const {
  return "IndexScan " + _subject + " " + _predicate + " " + _object;
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
    case FULL_INDEX_SCAN_SPO:
    case FULL_INDEX_SCAN_SOP:
    case FULL_INDEX_SCAN_PSO:
    case FULL_INDEX_SCAN_POS:
    case FULL_INDEX_SCAN_OSP:
    case FULL_INDEX_SCAN_OPS:
      return 3;
    default:
      AD_THROW(ad_semsearch::Exception::CHECK_FAILED, "Should be unreachable.");
  }
}

// _____________________________________________________________________________
vector<size_t> IndexScan::resultSortedOn() const {
  switch (_type) {
    case PSO_BOUND_S:
    case POS_BOUND_O:
    case SOP_BOUND_O:
      return {0};
    case PSO_FREE_S:
    case POS_FREE_O:
    case SPO_FREE_P:
    case SOP_FREE_O:
    case OSP_FREE_S:
    case OPS_FREE_P:
      return {0, 1};
    case FULL_INDEX_SCAN_SPO:
    case FULL_INDEX_SCAN_SOP:
    case FULL_INDEX_SCAN_PSO:
    case FULL_INDEX_SCAN_POS:
    case FULL_INDEX_SCAN_OSP:
    case FULL_INDEX_SCAN_OPS:
      return {0, 1, 2};
    default:
      AD_THROW(ad_semsearch::Exception::CHECK_FAILED, "Should be unreachable.");
  }
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t> IndexScan::getVariableColumns() const {
  ad_utility::HashMap<string, size_t> res;
  size_t col = 0;

  switch (_type) {
    case SPO_FREE_P:
    case FULL_INDEX_SCAN_SPO:
      if (_subject[0] == '?') {
        res[_subject] = col++;
      }
      if (_predicate[0] == '?') {
        res[_predicate] = col++;
      }

      if (_object[0] == '?') {
        res[_object] = col++;
      }
      return res;
    case SOP_FREE_O:
    case SOP_BOUND_O:
    case FULL_INDEX_SCAN_SOP:
      if (_subject[0] == '?') {
        res[_subject] = col++;
      }

      if (_object[0] == '?') {
        res[_object] = col++;
      }

      if (_predicate[0] == '?') {
        res[_predicate] = col++;
      }
      return res;
    case PSO_BOUND_S:
    case PSO_FREE_S:
    case FULL_INDEX_SCAN_PSO:
      if (_predicate[0] == '?') {
        res[_predicate] = col++;
      }
      if (_subject[0] == '?') {
        res[_subject] = col++;
      }

      if (_object[0] == '?') {
        res[_object] = col++;
      }
      return res;
    case POS_BOUND_O:
    case POS_FREE_O:
    case FULL_INDEX_SCAN_POS:
      if (_predicate[0] == '?') {
        res[_predicate] = col++;
      }

      if (_object[0] == '?') {
        res[_object] = col++;
      }

      if (_subject[0] == '?') {
        res[_subject] = col++;
      }
      return res;
    case OPS_FREE_P:
    case FULL_INDEX_SCAN_OPS:
      if (_object[0] == '?') {
        res[_object] = col++;
      }

      if (_predicate[0] == '?') {
        res[_predicate] = col++;
      }

      if (_subject[0] == '?') {
        res[_subject] = col++;
      }
      return res;
    case OSP_FREE_S:
    case FULL_INDEX_SCAN_OSP:
      if (_object[0] == '?') {
        res[_object] = col++;
      }

      if (_subject[0] == '?') {
        res[_subject] = col++;
      }

      if (_predicate[0] == '?') {
        res[_predicate] = col++;
      }
      return res;
    default:
      AD_THROW(ad_semsearch::Exception::CHECK_FAILED, "Should be unreachable.");
  }
}
// _____________________________________________________________________________
void IndexScan::computeResult(ResultTable* result) {
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
    case FULL_INDEX_SCAN_SPO:
    case FULL_INDEX_SCAN_SOP:
    case FULL_INDEX_SCAN_PSO:
    case FULL_INDEX_SCAN_POS:
    case FULL_INDEX_SCAN_OSP:
    case FULL_INDEX_SCAN_OPS:
      AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
               "Asked to execute a scan for the full index. "
               "This should never happen.");
  }
  LOG(DEBUG) << "IndexScan result computation done.\n";
}

// _____________________________________________________________________________
void IndexScan::computePSOboundS(ResultTable* result) const {
  result->_data.setCols(1);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_predicate, _subject, &result->_data, idx._PSO);
}

// _____________________________________________________________________________
void IndexScan::computePSOfreeS(ResultTable* result) const {
  result->_data.setCols(2);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0, 1};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_predicate, &result->_data, idx._PSO, _timeoutTimer);
}

// _____________________________________________________________________________
void IndexScan::computePOSboundO(ResultTable* result) const {
  result->_data.setCols(1);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_predicate, _object, &result->_data, idx._POS);
}

// _____________________________________________________________________________
void IndexScan::computePOSfreeO(ResultTable* result) const {
  result->_data.setCols(2);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0, 1};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_predicate, &result->_data, idx._POS, _timeoutTimer);
}

// _____________________________________________________________________________
size_t IndexScan::computeSizeEstimate() {
  if (_executionContext) {
    // Should always be in this branch. Else is only for test cases.

    // We have to do a simple scan anyway so might as well do it now
    if (getResultWidth() == 1) {
      auto key = asString();
      {
        auto rlock = getExecutionContext()->getPinnedSizes().rlock();
        if (rlock->count(key)) {
          return rlock->at(key);
        }
      }
      return getResult()->size();
    }
    if (_type == SPO_FREE_P || _type == SOP_FREE_O) {
      return getIndex().sizeEstimate(_subject, "", "");
    } else if (_type == POS_FREE_O || _type == PSO_FREE_S) {
      return getIndex().sizeEstimate("", _predicate, "");
    } else if (_type == OPS_FREE_P || _type == OSP_FREE_S) {
      return getIndex().sizeEstimate("", "", _object);
    }
    return getIndex().sizeEstimate("", "", "");
  } else {
    return 1000 + _subject.size() + _predicate.size() + _object.size();
  }
}

// _____________________________________________________________________________
void IndexScan::computeSPOfreeP(ResultTable* result) const {
  result->_data.setCols(2);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0, 1};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_subject, &result->_data, idx._SPO, _timeoutTimer);
}

// _____________________________________________________________________________
void IndexScan::computeSOPboundO(ResultTable* result) const {
  result->_data.setCols(1);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_subject, _object, &result->_data, idx._SOP);
}

// _____________________________________________________________________________
void IndexScan::computeSOPfreeO(ResultTable* result) const {
  result->_data.setCols(2);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0, 1};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_subject, &result->_data, idx._SOP, _timeoutTimer);
}

// _____________________________________________________________________________
void IndexScan::computeOPSfreeP(ResultTable* result) const {
  result->_data.setCols(2);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0, 1};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_object, &result->_data, idx._OPS);
}

// _____________________________________________________________________________
void IndexScan::computeOSPfreeS(ResultTable* result) const {
  result->_data.setCols(2);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_resultTypes.push_back(ResultTable::ResultType::KB);
  result->_sortedBy = {0, 1};
  const auto& idx = _executionContext->getIndex();
  idx.scan(_object, &result->_data, idx._OSP, _timeoutTimer);
}

// _____________________________________________________________________________
void IndexScan::determineMultiplicities() {
  _multiplicity.clear();
  if (_executionContext) {
    if (getResultWidth() == 1) {
      _multiplicity.emplace_back(1);
    } else {
      const auto& idx = getIndex();
      switch (_type) {
        case PSO_FREE_S:
          _multiplicity = idx.getMultiplicities(_predicate, idx._PSO);
          break;
        case POS_FREE_O:
          _multiplicity = idx.getMultiplicities(_predicate, idx._POS);
          break;
        case SPO_FREE_P:
          _multiplicity = idx.getMultiplicities(_subject, idx._SPO);
          break;
        case SOP_FREE_O:
          _multiplicity = idx.getMultiplicities(_subject, idx._SOP);
          break;
        case OSP_FREE_S:
          _multiplicity = idx.getMultiplicities(_object, idx._OSP);
          break;
        case OPS_FREE_P:
          _multiplicity = idx.getMultiplicities(_object, idx._OPS);
          break;
        case FULL_INDEX_SCAN_SPO:
          _multiplicity = idx.getMultiplicities(idx._SPO);
          break;
        case FULL_INDEX_SCAN_SOP:
          _multiplicity = idx.getMultiplicities(idx._SOP);
          break;
        case FULL_INDEX_SCAN_PSO:
          _multiplicity = idx.getMultiplicities(idx._PSO);
          break;
        case FULL_INDEX_SCAN_POS:
          _multiplicity = idx.getMultiplicities(idx._POS);
          break;
        case FULL_INDEX_SCAN_OSP:
          _multiplicity = idx.getMultiplicities(idx._OSP);
          break;
        case FULL_INDEX_SCAN_OPS:
          _multiplicity = idx.getMultiplicities(idx._OPS);
          break;
        default:
          AD_THROW(ad_semsearch::Exception::ASSERT_FAILED,
                   "Switch reached default block unexpectedly!");
      }
    }
  } else {
    _multiplicity.emplace_back(1);
    _multiplicity.emplace_back(1);
    if (getResultWidth() == 3) {
      _multiplicity.emplace_back(1);
    }
  }
  assert(_multiplicity.size() >= 1 || _multiplicity.size() <= 3);
}
