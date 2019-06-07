// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Join.h"
#include <functional>
#include <sstream>
#include <unordered_set>
#include "./QueryExecutionTree.h"
#include "CallFixedSize.h"

using std::string;

// _____________________________________________________________________________
Join::Join(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
           std::shared_ptr<QueryExecutionTree> t2, size_t t1JoinCol,
           size_t t2JoinCol, bool keepJoinColumn)
    : Operation(qec) {
  // Make sure subtrees are ordered so that identical queries can be identified.
  if (t1.get()->asString() < t2.get()->asString()) {
    _left = t1;
    _leftJoinCol = t1JoinCol;
    _right = t2;
    _rightJoinCol = t2JoinCol;
  } else {
    _left = t2;
    _leftJoinCol = t2JoinCol;
    _right = t1;
    _rightJoinCol = t1JoinCol;
  }
  _keepJoinColumn = keepJoinColumn;
  _sizeEstimate = 0;
  _sizeEstimateComputed = false;
  _multiplicities.clear();
}

// _____________________________________________________________________________
string Join::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "JOIN\n"
     << _left->asString(indent) << " join-column: [" << _leftJoinCol << "]\n";
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "|X|\n"
     << _right->asString(indent) << " join-column: [" << _rightJoinCol << "]";
  return os.str();
}

// _____________________________________________________________________________
string Join::getDescriptor() const {
  std::string joinVar = "";
  for (auto p : _left->getVariableColumns()) {
    if (p.second == _leftJoinCol) {
      joinVar = p.first;
      break;
    }
  }
  return "Join on " + joinVar;
}

// _____________________________________________________________________________
void Join::computeResult(ResultTable* result) {
  RuntimeInformation& runtimeInfo = getRuntimeInfo();

  LOG(DEBUG) << "Getting sub-results for join result computation..." << endl;
  size_t leftWidth = _left->getResultWidth();
  size_t rightWidth = _right->getResultWidth();

  // Checking this before calling getResult on the subtrees can
  // avoid the computation of an non-empty subtree.
  if (_left->knownEmptyResult() || _right->knownEmptyResult()) {
    LOG(TRACE) << "Either side is empty thus join result is empty" << endl;
    runtimeInfo.addDetail("Either side was empty", "");
    size_t resWidth = leftWidth + rightWidth - 1;
    result->_data.setCols(resWidth);
    result->_resultTypes.resize(result->_data.cols());
    result->_sortedBy = {_leftJoinCol};
    return;
  }

  // Check for joins with dummy
  if (isFullScanDummy(_left) || isFullScanDummy(_right)) {
    LOG(TRACE) << "Either side is Full Scan Dummy" << endl;
    computeResultForJoinWithFullScanDummy(result);
    return;
  }

  LOG(TRACE) << "Computing left side..." << endl;
  shared_ptr<const ResultTable> leftRes = _left->getResult();
  runtimeInfo.addChild(_left->getRootOperation()->getRuntimeInfo());

  // Check if we can stop early.
  if (leftRes->size() == 0) {
    LOG(TRACE) << "Left side empty thus join result is empty" << endl;
    runtimeInfo.addDetail("The left side was empty", "");
    size_t resWidth = leftWidth + rightWidth - 1;
    result->_data.setCols(resWidth);
    result->_resultTypes.resize(result->_data.cols());
    result->_sortedBy = {_leftJoinCol};
    return;
  }

  LOG(TRACE) << "Computing right side..." << endl;
  shared_ptr<const ResultTable> rightRes = _right->getResult();
  runtimeInfo.addChild(_right->getRootOperation()->getRuntimeInfo());

  LOG(DEBUG) << "Computing Join result..." << endl;

  AD_CHECK(result);

  result->_data.setCols(leftWidth + rightWidth - 1);
  result->_resultTypes.reserve(result->_data.cols());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              leftRes->_resultTypes.begin(),
                              leftRes->_resultTypes.end());
  for (size_t i = 0; i < rightRes->_data.cols(); i++) {
    if (i != _rightJoinCol) {
      result->_resultTypes.push_back(rightRes->_resultTypes[i]);
    }
  }
  result->_sortedBy = {_leftJoinCol};

  int lwidth = leftRes->_data.cols();
  int rwidth = rightRes->_data.cols();
  int reswidth = result->_data.cols();
  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, getEngine().join, leftRes->_data,
                    _leftJoinCol, rightRes->_data, _rightJoinCol,
                    &result->_data);

  LOG(DEBUG) << "Join result computation done." << endl;
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t> Join::getVariableColumns() const {
  ad_utility::HashMap<string, size_t> retVal;
  if (!isFullScanDummy(_left) && !isFullScanDummy(_right)) {
    retVal = _left->getVariableColumns();
    size_t leftSize = _left->getResultWidth();
    for (auto it = _right->getVariableColumns().begin();
         it != _right->getVariableColumns().end(); ++it) {
      if (it->second < _rightJoinCol) {
        retVal[it->first] = leftSize + it->second;
      }
      if (it->second > _rightJoinCol) {
        retVal[it->first] = leftSize + it->second - 1;
      }
    }
  } else {
    if (isFullScanDummy(_right)) {
      retVal = _left->getVariableColumns();
      size_t leftSize = _left->getResultWidth();
      for (auto it = _right->getVariableColumns().begin();
           it != _right->getVariableColumns().end(); ++it) {
        // Skip the first col for the dummy
        if (it->second != 0) {
          retVal[it->first] = leftSize + it->second - 1;
        }
      }
    } else {
      for (auto it = _left->getVariableColumns().begin();
           it != _left->getVariableColumns().end(); ++it) {
        // Skip+drop the first col for the dummy and subtract one from others.
        if (it->second != 0) {
          retVal[it->first] = it->second - 1;
        }
      }
      for (auto it = _right->getVariableColumns().begin();
           it != _right->getVariableColumns().end(); ++it) {
        retVal[it->first] = 2 + it->second;
      }
    }
  }
  return retVal;
}

// _____________________________________________________________________________
size_t Join::getResultWidth() const {
  size_t res = _left->getResultWidth() + _right->getResultWidth() -
               (_keepJoinColumn ? 1 : 2);
  AD_CHECK(res > 0);
  return res;
}

// _____________________________________________________________________________
vector<size_t> Join::resultSortedOn() const {
  if (!isFullScanDummy(_left)) {
    return {_leftJoinCol};
  } else {
    return {2 + _rightJoinCol};
  }
}

// _____________________________________________________________________________
std::unordered_set<string> Join::getContextVars() const {
  auto cvars = _left->getContextVars();
  cvars.insert(_right->getContextVars().begin(),
               _right->getContextVars().end());
  return cvars;
}

// _____________________________________________________________________________
float Join::getMultiplicity(size_t col) {
  if (_multiplicities.size() == 0) {
    computeSizeEstimateAndMultiplicities();
    _sizeEstimateComputed = true;
  }
  return _multiplicities[col];
}

// _____________________________________________________________________________
size_t Join::getCostEstimate() {
  float diskRandomAccessCost =
      _executionContext
          ? _executionContext->getCostFactor("DISK_RANDOM_ACCESS_COST")
          : 200000;
  size_t costJoin;
  if (isFullScanDummy(_left)) {
    size_t nofDistinctTabJc = static_cast<size_t>(
        _right->getSizeEstimate() / _right->getMultiplicity(_rightJoinCol));
    float averageScanSize = _left->getMultiplicity(_leftJoinCol);

    costJoin = nofDistinctTabJc *
               static_cast<size_t>(diskRandomAccessCost + averageScanSize);
  } else if (isFullScanDummy(_right)) {
    size_t nofDistinctTabJc = static_cast<size_t>(
        _left->getSizeEstimate() / _left->getMultiplicity(_leftJoinCol));
    float averageScanSize = _right->getMultiplicity(_rightJoinCol);
    costJoin = nofDistinctTabJc *
               static_cast<size_t>(diskRandomAccessCost + averageScanSize);
  } else {
    // Normal case:
    costJoin = _left->getSizeEstimate() + _right->getSizeEstimate();
  }
  return getSizeEstimate() + _left->getCostEstimate() +
         _right->getCostEstimate() + costJoin;
}

// _____________________________________________________________________________
void Join::computeResultForJoinWithFullScanDummy(ResultTable* result) const {
  LOG(DEBUG) << "Join by making multiple scans..." << endl;
  if (isFullScanDummy(_left)) {
    AD_CHECK(!isFullScanDummy(_right))
    result->_data.setCols(_right->getResultWidth() + 2);
    result->_sortedBy = {2 + _rightJoinCol};
    shared_ptr<const ResultTable> nonDummyRes = _right->getResult();
    result->_resultTypes.reserve(result->_data.cols());
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.insert(result->_resultTypes.end(),
                                nonDummyRes->_resultTypes.begin(),
                                nonDummyRes->_resultTypes.end());
    doComputeJoinWithFullScanDummyLeft(nonDummyRes->_data, &result->_data);
  } else {
    AD_CHECK(!isFullScanDummy(_left))
    result->_data.setCols(_left->getResultWidth() + 2);
    result->_sortedBy = {_leftJoinCol};

    shared_ptr<const ResultTable> nonDummyRes = _left->getResult();
    result->_resultTypes.reserve(result->_data.cols());
    result->_resultTypes.insert(result->_resultTypes.end(),
                                nonDummyRes->_resultTypes.begin(),
                                nonDummyRes->_resultTypes.end());
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);

    doComputeJoinWithFullScanDummyRight(nonDummyRes->_data, &result->_data);
  }

  LOG(DEBUG) << "Join (with dummy) done. Size: " << result->size() << endl;
}

// _____________________________________________________________________________
Join::ScanMethodType Join::getScanMethod(
    std::shared_ptr<QueryExecutionTree> fullScanDummyTree) const {
  ScanMethodType scanMethod;
  IndexScan& scan =
      *static_cast<IndexScan*>(fullScanDummyTree->getRootOperation().get());

  // this works because the join operations execution Context never changes
  // during its lifetime
  const auto& idx = _executionContext->getIndex();
  const auto scanLambda = [&idx](const auto& perm) {
    return
        [&idx, &perm](Id id, IdTable* idTable) { idx.scan(id, idTable, perm); };
  };

  switch (scan.getType()) {
    case IndexScan::FULL_INDEX_SCAN_SPO:
      scanMethod = scanLambda(idx._SPO);
      break;
    case IndexScan::FULL_INDEX_SCAN_SOP:
      scanMethod = scanLambda(idx._SOP);
      break;
    case IndexScan::FULL_INDEX_SCAN_PSO:
      scanMethod = scanLambda(idx._PSO);
      break;
    case IndexScan::FULL_INDEX_SCAN_POS:
      scanMethod = scanLambda(idx._POS);
      break;
    case IndexScan::FULL_INDEX_SCAN_OSP:
      scanMethod = scanLambda(idx._OSP);
      break;
    case IndexScan::FULL_INDEX_SCAN_OPS:
      scanMethod = scanLambda(idx._OPS);
      break;
    default:
      AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
               "Found non-dummy scan where one was expected.");
  }
  return scanMethod;
}

// _____________________________________________________________________________
void Join::doComputeJoinWithFullScanDummyLeft(const IdTable& ndr,
                                              IdTable* res) const {
  LOG(TRACE) << "Dummy on left side, other join op size: " << ndr.size()
             << endl;
  if (ndr.size() == 0) {
    return;
  }
  const ScanMethodType scan = getScanMethod(_left);
  // Iterate through non-dummy.
  Id currentJoinId = ndr(0, _rightJoinCol);
  auto joinItemFrom = ndr.begin();
  auto joinItemEnd = ndr.begin();
  for (size_t i = 0; i < ndr.size(); ++i) {
    // For each different element in the join column.
    if (ndr(i, _rightJoinCol) == currentJoinId) {
      ++joinItemEnd;
    } else {
      // Do a scan.
      LOG(TRACE) << "Inner scan with ID: " << currentJoinId << endl;
      IdTable jr(2);
      scan(currentJoinId, &jr);
      LOG(TRACE) << "Got #items: " << jr.size() << endl;
      // Build the cross product.
      appendCrossProduct(jr.begin(), jr.end(), joinItemFrom, joinItemEnd, res);
      // Reset
      currentJoinId = ndr(i, _rightJoinCol);
      joinItemFrom = joinItemEnd;
      ++joinItemEnd;
    }
  }
  // Do the scan for the final element.
  LOG(TRACE) << "Inner scan with ID: " << currentJoinId << endl;
  IdTable jr(2);
  scan(currentJoinId, &jr);
  LOG(TRACE) << "Got #items: " << jr.size() << endl;
  // Build the cross product.
  appendCrossProduct(jr.begin(), jr.end(), joinItemFrom, joinItemEnd, res);
}

// _____________________________________________________________________________
void Join::doComputeJoinWithFullScanDummyRight(const IdTable& ndr,
                                               IdTable* res) const {
  LOG(TRACE) << "Dummy on right side, other join op size: " << ndr.size()
             << endl;
  if (ndr.size() == 0) {
    return;
  }
  // Get the scan method (depends on type of dummy tree), use a function ptr.
  const ScanMethodType scan = getScanMethod(_right);
  // Iterate through non-dummy.
  Id currentJoinId = ndr(0, _leftJoinCol);
  auto joinItemFrom = ndr.begin();
  auto joinItemEnd = ndr.begin();
  for (size_t i = 0; i < ndr.size(); ++i) {
    // For each different element in the join column.
    if (ndr(i, _leftJoinCol) == currentJoinId) {
      ++joinItemEnd;
    } else {
      // Do a scan.
      LOG(TRACE) << "Inner scan with ID: " << currentJoinId << endl;
      IdTable jr(2);
      scan(currentJoinId, &jr);
      LOG(TRACE) << "Got #items: " << jr.size() << endl;
      // Build the cross product.
      appendCrossProduct(joinItemFrom, joinItemEnd, jr.begin(), jr.end(), res);
      // Reset
      currentJoinId = ndr[i][_leftJoinCol];
      joinItemFrom = joinItemEnd;
      ++joinItemEnd;
    }
  }
  // Do the scan for the final element.
  LOG(TRACE) << "Inner scan with ID: " << currentJoinId << endl;
  IdTable jr(2);
  scan(currentJoinId, &jr);
  LOG(TRACE) << "Got #items: " << jr.size() << endl;
  // Build the cross product.
  appendCrossProduct(joinItemFrom, joinItemEnd, jr.begin(), jr.end(), res);
}

// _____________________________________________________________________________
void Join::computeSizeEstimateAndMultiplicities() {
  _multiplicities.clear();
  if (_left->getSizeEstimate() == 0 || _right->getSizeEstimate() == 0) {
    for (size_t i = 0; i < getResultWidth(); ++i) {
      _multiplicities.emplace_back(1);
    }
    return;
  }

  size_t nofDistinctLeft = std::max(
      size_t(1), static_cast<size_t>(_left->getSizeEstimate() /
                                     _left->getMultiplicity(_leftJoinCol)));
  size_t nofDistinctRight = std::max(
      size_t(1), static_cast<size_t>(_right->getSizeEstimate() /
                                     _right->getMultiplicity(_rightJoinCol)));

  size_t nofDistinctInResult = std::min(nofDistinctLeft, nofDistinctRight);

  double adaptSizeLeft =
      _left->getSizeEstimate() *
      (static_cast<double>(nofDistinctInResult) / nofDistinctLeft);
  double adaptSizeRight =
      _right->getSizeEstimate() *
      (static_cast<double>(nofDistinctInResult) / nofDistinctRight);

  double corrFactor =
      _executionContext
          ? ((isFullScanDummy(_left) || isFullScanDummy(_right))
                 ? _executionContext->getCostFactor(
                       "DUMMY_JOIN_SIZE_ESTIMATE_CORRECTION_FACTOR")
                 : _executionContext->getCostFactor(
                       "JOIN_SIZE_ESTIMATE_CORRECTION_FACTOR"))
          : 1;

  double jcMultiplicityInResult = _left->getMultiplicity(_leftJoinCol) *
                                  _right->getMultiplicity(_rightJoinCol);
  _sizeEstimate = std::max(
      size_t(1), static_cast<size_t>(corrFactor * jcMultiplicityInResult *
                                     nofDistinctInResult));

  LOG(TRACE) << "Estimated size as: " << _sizeEstimate << " := " << corrFactor
             << " * " << jcMultiplicityInResult << " * " << nofDistinctInResult
             << std::endl;

  for (size_t i = isFullScanDummy(_left) ? 1 : 0; i < _left->getResultWidth();
       ++i) {
    double oldMult = _left->getMultiplicity(i);
    double m = std::max(
        1.0, oldMult * _right->getMultiplicity(_rightJoinCol) * corrFactor);
    if (i != _leftJoinCol && nofDistinctLeft != nofDistinctInResult) {
      double oldDist = _left->getSizeEstimate() / oldMult;
      double newDist = std::min(oldDist, adaptSizeLeft);
      m = (_sizeEstimate / corrFactor) / newDist;
    }
    _multiplicities.emplace_back(m);
  }
  for (size_t i = 0; i < _right->getResultWidth(); ++i) {
    if (i == _rightJoinCol && !isFullScanDummy(_left)) {
      continue;
    }
    double oldMult = _right->getMultiplicity(i);
    double m = std::max(
        1.0, oldMult * _left->getMultiplicity(_leftJoinCol) * corrFactor);
    if (i != _rightJoinCol && nofDistinctRight != nofDistinctInResult) {
      double oldDist = _right->getSizeEstimate() / oldMult;
      double newDist = std::min(oldDist, adaptSizeRight);
      m = (_sizeEstimate / corrFactor) / newDist;
    }
    _multiplicities.emplace_back(m);
  }

  assert(_multiplicities.size() == getResultWidth());
}

// ______________________________________________________________________________
void Join::appendCrossProduct(const IdTable::const_iterator& leftBegin,
                              const IdTable::const_iterator& leftEnd,
                              const IdTable::const_iterator& rightBegin,
                              const IdTable::const_iterator& rightEnd,
                              IdTable* res) const {
  for (auto itl = leftBegin; itl != leftEnd; ++itl) {
    for (auto itr = rightBegin; itr != rightEnd; ++itr) {
      const auto& l = *itl;
      const auto& r = *itr;
      res->push_back();
      size_t backIdx = res->size() - 1;
      for (size_t i = 0; i < itl.cols(); i++) {
        (*res)(backIdx, i) = l[i];
      }
      for (size_t i = 0; i < itr.cols(); i++) {
        (*res)(backIdx, itl.cols() + i) = r[i];
      }
    }
  }
}
