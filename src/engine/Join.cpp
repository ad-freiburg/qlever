// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Join.h"

#include <functional>
#include <sstream>
#include <type_traits>
#include <unordered_set>

#include "./QueryExecutionTree.h"
#include "CallFixedSize.h"

using std::string;

// _____________________________________________________________________________
Join::Join(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
           std::shared_ptr<QueryExecutionTree> t2, size_t t1JoinCol,
           size_t t2JoinCol, bool keepJoinColumn)
    : Operation(qec) {
  AD_CHECK(t1 && t2);
  // Make sure subtrees are ordered so that identical queries can be identified.
  if (t1->asString() > t2->asString()) {
    std::swap(t1, t2);
    std::swap(t1JoinCol, t2JoinCol);
  }
  _left = std::move(t1);
  _leftJoinCol = t1JoinCol;
  _right = std::move(t2);
  _rightJoinCol = t2JoinCol;
  _keepJoinColumn = keepJoinColumn;
  _sizeEstimate = 0;
  _sizeEstimateComputed = false;
  _multiplicities.clear();
}

// _____________________________________________________________________________
string Join::asStringImpl(size_t indent) const {
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
  return std::move(os).str();
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
  LOG(DEBUG) << "Getting sub-results for join result computation..." << endl;
  size_t leftWidth = _left->getResultWidth();
  size_t rightWidth = _right->getResultWidth();

  // TODO<joka921> Currently the _resultTypes are set incorrectly in case
  // of early stopping. For now, early stopping is thus disabled.
  // TODO: Implement getting the result types without calculating the result;
  /*
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
   */

  // Check for joins with dummy
  if (isFullScanDummy(_left) || isFullScanDummy(_right)) {
    LOG(TRACE) << "Either side is Full Scan Dummy" << endl;
    computeResultForJoinWithFullScanDummy(result);
    return;
  }

  LOG(TRACE) << "Computing left side..." << endl;
  shared_ptr<const ResultTable> leftRes = _left->getResult();

  // TODO<joka921> Currently the _resultTypes are set incorrectly in case
  // of early stopping. For now, early stopping is thus disabled.
  // TODO: Implement getting the result types without calculating the result;
  // Check if we can stop early.
  /*
  if (leftRes->size() == 0) {
    LOG(TRACE) << "Left side empty thus join result is empty" << endl;
    runtimeInfo.addDetail("The left side was empty", "");
    size_t resWidth = leftWidth + rightWidth - 1;
    result->_data.setCols(resWidth);
    result->_resultTypes.resize(result->_data.cols());
    result->_sortedBy = {_leftJoinCol};
    return;
  }
   */

  LOG(TRACE) << "Computing right side..." << endl;
  shared_ptr<const ResultTable> rightRes = _right->getResult();

  LOG(DEBUG) << "Computing Join result..." << endl;

  AD_CHECK(result);

  result->_idTable.setCols(leftWidth + rightWidth - 1);
  result->_resultTypes.reserve(result->_idTable.cols());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              leftRes->_resultTypes.begin(),
                              leftRes->_resultTypes.end());
  for (size_t i = 0; i < rightRes->_idTable.cols(); i++) {
    if (i != _rightJoinCol) {
      result->_resultTypes.push_back(rightRes->_resultTypes[i]);
    }
  }
  result->_sortedBy = {_leftJoinCol};

  int lwidth = leftRes->_idTable.cols();
  int rwidth = rightRes->_idTable.cols();
  int reswidth = result->_idTable.cols();
  CALL_FIXED_SIZE_3(lwidth, rwidth, reswidth, join, leftRes->_idTable,
                    _leftJoinCol, rightRes->_idTable, _rightJoinCol,
                    &result->_idTable);

  LOG(DEBUG) << "Join result computation done." << endl;
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t> Join::getVariableColumns() const {
  ad_utility::HashMap<string, size_t> retVal;
  if (!isFullScanDummy(_left) && !isFullScanDummy(_right)) {
    retVal = _left->getVariableColumns();
    size_t leftSize = _left->getResultWidth();
    for (const auto& [variable, column] : _right->getVariableColumns()) {
      if (column < _rightJoinCol) {
        retVal[variable] = leftSize + column;
      }
      if (column > _rightJoinCol) {
        retVal[variable] = leftSize + column - 1;
      }
    }
  } else {
    if (isFullScanDummy(_right)) {
      retVal = _left->getVariableColumns();
      size_t leftSize = _left->getResultWidth();
      for (const auto& [variable, column] : _right->getVariableColumns()) {
        // Skip the first col for the dummy
        if (column != 0) {
          retVal[variable] = leftSize + column - 1;
        }
      }
    } else {
      for (const auto& [variable, column] : _left->getVariableColumns()) {
        // Skip+drop the first col for the dummy and subtract one from others.
        if (column != 0) {
          retVal[variable] = column - 1;
        }
      }
      for (const auto& [variable, column] : _right->getVariableColumns()) {
        retVal[variable] = 2 + column;
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
void Join::computeResultForJoinWithFullScanDummy(ResultTable* result) {
  LOG(DEBUG) << "Join by making multiple scans..." << endl;
  if (isFullScanDummy(_left)) {
    AD_CHECK(!isFullScanDummy(_right))
    result->_idTable.setCols(_right->getResultWidth() + 2);
    result->_sortedBy = {2 + _rightJoinCol};
    shared_ptr<const ResultTable> nonDummyRes = _right->getResult();
    result->_resultTypes.reserve(result->_idTable.cols());
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.insert(result->_resultTypes.end(),
                                nonDummyRes->_resultTypes.begin(),
                                nonDummyRes->_resultTypes.end());
    doComputeJoinWithFullScanDummyLeft(nonDummyRes->_idTable,
                                       &result->_idTable);
  } else {
    AD_CHECK(!isFullScanDummy(_left))
    result->_idTable.setCols(_left->getResultWidth() + 2);
    result->_sortedBy = {_leftJoinCol};

    shared_ptr<const ResultTable> nonDummyRes = _left->getResult();
    result->_resultTypes.reserve(result->_idTable.cols());
    result->_resultTypes.insert(result->_resultTypes.end(),
                                nonDummyRes->_resultTypes.begin(),
                                nonDummyRes->_resultTypes.end());
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);

    doComputeJoinWithFullScanDummyRight(nonDummyRes->_idTable,
                                        &result->_idTable);
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
      IdTable jr(2, _executionContext->getAllocator());
      // The scan is a relatively expensive disk operation, so we can afford to
      // check for timeouts before each call.
      checkTimeout();
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
  IdTable jr(2, _executionContext->getAllocator());
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
      // The scan is a relatively expensive disk operation, so we can afford to
      // check for timeouts before each call.
      checkTimeout();
      IdTable jr(2, _executionContext->getAllocator());
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
  IdTable jr(2, _executionContext->getAllocator());
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

// ______________________________________________________________________________

template <int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
void Join::join(const IdTable& dynA, size_t jc1, const IdTable& dynB,
                size_t jc2, IdTable* dynRes) {
  const IdTableView<L_WIDTH> a = dynA.asStaticView<L_WIDTH>();
  const IdTableView<R_WIDTH> b = dynB.asStaticView<R_WIDTH>();

  LOG(DEBUG) << "Performing join between two tables.\n";
  LOG(DEBUG) << "A: width = " << a.cols() << ", size = " << a.size() << "\n";
  LOG(DEBUG) << "B: width = " << b.cols() << ", size = " << b.size() << "\n";

  // Check trivial case.
  if (a.size() == 0 || b.size() == 0) {
    return;
  }

  IdTableStatic<OUT_WIDTH> result = dynRes->moveToStatic<OUT_WIDTH>();
  // Cannot just switch l1 and l2 around because the order of
  // items in the result tuples is important.
  if (a.size() / b.size() > GALLOP_THRESHOLD) {
    doGallopInnerJoin(LeftLargerTag{}, a, jc1, b, jc2, &result);
  } else if (b.size() / a.size() > GALLOP_THRESHOLD) {
    doGallopInnerJoin(RightLargerTag{}, a, jc1, b, jc2, &result);
  } else {
    auto checkTimeoutAfterNCalls = checkTimeoutAfterNCallsFactory();
    // Intersect both lists.
    size_t i = 0;
    size_t j = 0;
    // while (a(i, jc1) < sent1) {
    while (i < a.size() && j < b.size()) {
      while (a(i, jc1) < b(j, jc2)) {
        ++i;
        checkTimeoutAfterNCalls();
        if (i >= a.size()) {
          goto finish;
        }
      }

      while (b(j, jc2) < a(i, jc1)) {
        ++j;
        checkTimeoutAfterNCalls();
        if (j >= b.size()) {
          goto finish;
        }
      }

      while (a(i, jc1) == b(j, jc2)) {
        // In case of match, create cross-product
        // Always fix a and go through b.
        size_t keepJ = j;
        while (a(i, jc1) == b(j, jc2)) {
          result.push_back();
          const size_t backIndex = result.size() - 1;
          for (size_t h = 0; h < a.cols(); h++) {
            result(backIndex, h) = a(i, h);
          }

          // Copy bs columns before the join column
          for (size_t h = 0; h < jc2; h++) {
            result(backIndex, h + a.cols()) = b(j, h);
          }

          // Copy bs columns after the join column
          for (size_t h = jc2 + 1; h < b.cols(); h++) {
            result(backIndex, h + a.cols() - 1) = b(j, h);
          }

          ++j;
          checkTimeoutAfterNCalls();
          if (j >= b.size()) {
            // The next i might still match
            break;
          }
        }
        ++i;
        checkTimeoutAfterNCalls();
        if (i >= a.size()) {
          goto finish;
        }
        // If the next i is still the same, reset j.
        if (a(i, jc1) == b(keepJ, jc2)) {
          j = keepJ;
        } else if (j >= b.size()) {
          // this check is needed because otherwise we might leak an out of
          // bounds value for j into the next loop which does not check it. this
          // fixes a bug that was not discovered by testing due to 0
          // initialization of IdTables used for testing and should not occur in
          // typical use cases but it is still wrong.
          goto finish;
        }
      }
    }
  }
finish:
  *dynRes = result.moveToDynamic();

  LOG(DEBUG) << "Join done.\n";
  LOG(DEBUG) << "Result: width = " << dynRes->cols()
             << ", size = " << dynRes->size() << "\n";
}

// _____________________________________________________________________________
template <typename TagType, int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
void Join::doGallopInnerJoin(const TagType, const IdTableView<L_WIDTH>& l1,
                             const size_t jc1, const IdTableView<R_WIDTH>& l2,
                             const size_t jc2,
                             IdTableStatic<OUT_WIDTH>* result) {
  LOG(DEBUG) << "Galloping case.\n";
  size_t i = 0;
  size_t j = 0;
  while (i < l1.size() && j < l2.size()) {
    if constexpr (std::is_same<TagType, RightLargerTag>::value) {
      while (l1(i, jc1) < l2(j, jc2)) {
        ++i;
        if (i >= l1.size()) {
          return;
        }
      }
      size_t step = 1;
      size_t last = j;
      while (l1(i, jc1) > l2(j, jc2)) {
        last = j;
        j += step;
        step *= 2;
        if (j >= l2.size()) {
          j = l2.size() - 1;
          if (l1(i, jc1) > l2(j, jc2)) {
            return;
          }
        }
      }
      if (l1(i, jc1) == l2(j, jc2)) {
        // We stepped into a block where l1 and l2 are equal. We need to
        // find the beginning of this block
        while (j > 0 && l1(i, jc1) == l2(j - 1, jc2)) {
          j--;
        }
      } else if (l1(i, jc1) < l2(j, jc2)) {
        // We stepped over the location where l1 and l2 may be equal.
        // Use binary search to locate that spot
        const Id needle = l1(i, jc1);
        j = std::lower_bound(l2.begin() + last, l2.begin() + j, needle,
                             [jc2](const auto& l, const Id needle) -> bool {
                               return l[jc2] < needle;
                             }) -
            l2.begin();
      }
    } else {
      size_t step = 1;
      size_t last = j;
      while (l2(j, jc2) > l1(i, jc1)) {
        last = i;
        i += step;
        step *= 2;
        if (i >= l1.size()) {
          i = l1.size() - 1;
          if (l2(j, jc2) > l1(i, jc1)) {
            return;
          }
        }
      }
      if (l2(j, jc2) == l1(i, jc1)) {
        // We stepped into a block where l1 and l2 are equal. We need to
        // find the beginning of this block
        while (i > 0 && l1(i - 1, jc1) == l2(j, jc2)) {
          i--;
        }
      } else if (l2(j, jc2) < l1(i, jc1)) {
        // We stepped over the location where l1 and l2 may be equal.
        // Use binary search to locate that spot
        const Id needle = l2(j, jc2);
        i = std::lower_bound(l1.begin() + last, l1.begin() + i, needle,
                             [jc1](const auto& l, const Id needle) -> bool {
                               return l[jc1] < needle;
                             }) -
            l1.begin();
      }
      while (l1(i, jc1) > l2(j, jc2)) {
        ++j;
        if (j >= l2.size()) {
          return;
        }
      }
    }
    while (l1(i, jc1) == l2(j, jc2)) {
      // In case of match, create cross-product
      // Always fix l1 and go through l2.
      const size_t keepJ = j;
      while (l1(i, jc1) == l2(j, jc2)) {
        size_t rowIndex = result->size();
        result->push_back();
        for (size_t h = 0; h < l1.cols(); h++) {
          (*result)(rowIndex, h) = l1(i, h);
        }
        // Copy l2s columns before the join column
        for (size_t h = 0; h < jc2; h++) {
          (*result)(rowIndex, h + l1.cols()) = l2(j, h);
        }

        // Copy l2s columns after the join column
        for (size_t h = jc2 + 1; h < l2.cols(); h++) {
          (*result)(rowIndex, h + l1.cols() - 1) = l2(j, h);
        }
        ++j;
        if (j >= l2.size()) {
          break;
        }
      }
      ++i;
      if (i >= l1.size()) {
        return;
      }
      // If the next i is still the same, reset j.
      if (l1(i, jc1) == l2(keepJ, jc2)) {
        j = keepJ;
      } else if (j >= l2.size()) {
        // this check is needed because otherwise we might leak an out of bounds
        // value for j into the next loop which does not check it.
        // this fixes a bug that was not discovered by testing due to 0
        // initialization of IdTables used for testing and should not occur in
        // typical use cases but it is still wrong.
        return;
      }
    }
  }
}
