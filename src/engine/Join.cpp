// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include <engine/CallFixedSize.h>
#include <engine/IndexScan.h>
#include <engine/Join.h>
#include <global/Constants.h>
#include <global/Id.h>
#include <util/HashMap.h>

#include <functional>
#include <sstream>
#include <type_traits>
#include <unordered_set>
#include <vector>

using std::string;

// _____________________________________________________________________________
Join::Join(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
           std::shared_ptr<QueryExecutionTree> t2, size_t t1JoinCol,
           size_t t2JoinCol, bool keepJoinColumn)
    : Operation(qec) {
  AD_CONTRACT_CHECK(t1 && t2);
  // Currently all join algorithms require both inputs to be sorted, so we
  // enforce the sorting here.
  t1 = QueryExecutionTree::createSortedTree(std::move(t1), {t1JoinCol});
  t2 = QueryExecutionTree::createSortedTree(std::move(t2), {t2JoinCol});

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
      joinVar = p.first.name();
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
    result->_data.setNumColumns(resWidth);
    result->_resultTypes.resize(result->_data.numColumns());
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
    result->_data.setNumColumns(resWidth);
    result->_resultTypes.resize(result->_data.numColumns());
    result->_sortedBy = {_leftJoinCol};
    return;
  }
   */

  LOG(TRACE) << "Computing right side..." << endl;
  shared_ptr<const ResultTable> rightRes = _right->getResult();

  LOG(DEBUG) << "Computing Join result..." << endl;

  AD_CONTRACT_CHECK(result);

  result->_idTable.setNumColumns(leftWidth + rightWidth - 1);
  result->_resultTypes.reserve(result->_idTable.numColumns());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              leftRes->_resultTypes.begin(),
                              leftRes->_resultTypes.end());
  for (size_t i = 0; i < rightRes->_idTable.numColumns(); i++) {
    if (i != _rightJoinCol) {
      result->_resultTypes.push_back(rightRes->_resultTypes[i]);
    }
  }
  result->_sortedBy = {_leftJoinCol};

  int lwidth = leftRes->_idTable.numColumns();
  int rwidth = rightRes->_idTable.numColumns();
  int reswidth = result->_idTable.numColumns();

  CALL_FIXED_SIZE((std::array{lwidth, rwidth, reswidth}), &Join::join, this,
                  leftRes->_idTable, _leftJoinCol, rightRes->_idTable,
                  _rightJoinCol, &result->_idTable);

  // If only one of the two operands has a local vocab, pass it on.
  result->_localVocab = LocalVocab::mergeLocalVocabsIfOneIsEmpty(
      leftRes->_localVocab, rightRes->_localVocab);

  LOG(DEBUG) << "Join result computation done" << endl;
}

// _____________________________________________________________________________
VariableToColumnMap Join::computeVariableToColumnMap() const {
  VariableToColumnMap retVal;
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
  AD_CONTRACT_CHECK(res > 0);
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
  size_t costJoin;

  // The cost estimates of the "Join with full scan" case must be consistent
  // with the estimates for the materialization of a full scan. For a detailed
  // discussion see the comments in `IndexScan::getCostEstimate()` (in
  // `IndexScan.cpp`)
  if (isFullScanDummy(_left)) {
    size_t nofDistinctTabJc = static_cast<size_t>(
        _right->getSizeEstimate() / _right->getMultiplicity(_rightJoinCol));
    float averageScanSize = _left->getMultiplicity(_leftJoinCol);

    costJoin = nofDistinctTabJc * averageScanSize * 10'000;
  } else if (isFullScanDummy(_right)) {
    size_t nofDistinctTabJc = static_cast<size_t>(
        _left->getSizeEstimate() / _left->getMultiplicity(_leftJoinCol));
    float averageScanSize = _right->getMultiplicity(_rightJoinCol);
    costJoin = nofDistinctTabJc * averageScanSize * 10'000;
  } else {
    // Normal case:
    costJoin = _left->getSizeEstimate() + _right->getSizeEstimate();
  }

  // TODO<joka921> once the `getCostEstimate` functions are `const`,
  // the argument can also be `const auto`
  auto costIfNotFullScan = [](auto& subtree) {
    return isFullScanDummy(subtree) ? size_t{0} : subtree->getCostEstimate();
  };

  return getSizeEstimate() + costJoin + costIfNotFullScan(_left) +
         costIfNotFullScan(_right);
}

// _____________________________________________________________________________
void Join::computeResultForJoinWithFullScanDummy(ResultTable* result) {
  LOG(DEBUG) << "Join by making multiple scans..." << endl;
  if (isFullScanDummy(_left)) {
    AD_CONTRACT_CHECK(!isFullScanDummy(_right));
    _left->getRootOperation()->updateRuntimeInformationWhenOptimizedOut({});
    result->_idTable.setNumColumns(_right->getResultWidth() + 2);
    result->_sortedBy = {2 + _rightJoinCol};
    shared_ptr<const ResultTable> nonDummyRes = _right->getResult();
    result->_resultTypes.reserve(result->_idTable.numColumns());
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.insert(result->_resultTypes.end(),
                                nonDummyRes->_resultTypes.begin(),
                                nonDummyRes->_resultTypes.end());
    doComputeJoinWithFullScanDummyLeft(nonDummyRes->_idTable,
                                       &result->_idTable);
  } else {
    AD_CONTRACT_CHECK(!isFullScanDummy(_left));
    _right->getRootOperation()->updateRuntimeInformationWhenOptimizedOut({});
    result->_idTable.setNumColumns(_left->getResultWidth() + 2);
    result->_sortedBy = {_leftJoinCol};

    shared_ptr<const ResultTable> nonDummyRes = _left->getResult();
    result->_resultTypes.reserve(result->_idTable.numColumns());
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
  const auto scanLambda = [&idx](const Index::Permutation perm) {
    return
        [&idx, perm](Id id, IdTable* idTable) { idx.scan(id, idTable, perm); };
  };

  using enum Index::Permutation;
  switch (scan.getType()) {
    case IndexScan::FULL_INDEX_SCAN_SPO:
      scanMethod = scanLambda(SPO);
      break;
    case IndexScan::FULL_INDEX_SCAN_SOP:
      scanMethod = scanLambda(SOP);
      break;
    case IndexScan::FULL_INDEX_SCAN_PSO:
      scanMethod = scanLambda(PSO);
      break;
    case IndexScan::FULL_INDEX_SCAN_POS:
      scanMethod = scanLambda(POS);
      break;
    case IndexScan::FULL_INDEX_SCAN_OSP:
      scanMethod = scanLambda(OSP);
      break;
    case IndexScan::FULL_INDEX_SCAN_OPS:
      scanMethod = scanLambda(OPS);
      break;
    default:
      AD_THROW("Found non-dummy scan where one was expected.");
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
      appendCrossProduct(jr.cbegin(), jr.cend(), joinItemFrom, joinItemEnd,
                         res);
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
  appendCrossProduct(jr.cbegin(), jr.cend(), joinItemFrom, joinItemEnd, res);
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
      appendCrossProduct(joinItemFrom, joinItemEnd, jr.cbegin(), jr.cend(),
                         res);
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
  appendCrossProduct(joinItemFrom, joinItemEnd, jr.cbegin(), jr.cend(), res);
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
      res->emplace_back();
      size_t backIdx = res->size() - 1;
      for (size_t i = 0; i < l.numColumns(); i++) {
        (*res)(backIdx, i) = l[i];
      }
      for (size_t i = 0; i < r.numColumns(); i++) {
        (*res)(backIdx, l.numColumns() + i) = r[i];
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
  LOG(DEBUG) << "A: width = " << a.numColumns() << ", size = " << a.size()
             << "\n";
  LOG(DEBUG) << "B: width = " << b.numColumns() << ", size = " << b.size()
             << "\n";

  // Check trivial case.
  if (a.size() == 0 || b.size() == 0) {
    return;
  }

  IdTableStatic<OUT_WIDTH> result = std::move(*dynRes).toStatic<OUT_WIDTH>();
  // Cannot just switch l1 and l2 around because the order of
  // items in the result tuples is important.
  if (a.size() / b.size() > GALLOP_THRESHOLD) {
    auto lessThan = [jc1, jc2](const auto& rowSmaller, const auto& rowLarger) {
      return rowSmaller[jc2] < rowLarger[jc1];
    };
    auto combineRows = [jc2, &result](const auto& rowSmaller,
                                      const auto& rowLarger) {
      addCombinedRowToIdTable(rowLarger, rowSmaller, jc2, &result);
    };
    ad_utility::gallopingJoin(b, a, lessThan, combineRows);
  } else if (b.size() / a.size() > GALLOP_THRESHOLD) {
    auto lessThan = [jc1, jc2](const auto& row1, const auto& row2) {
      return row1[jc1] < row2[jc2];
    };
    auto combineRows = [jc2, &result](const auto& row1, const auto& row2) {
      addCombinedRowToIdTable(row1, row2, jc2, &result);
    };
    ad_utility::gallopingJoin(a, b, lessThan, combineRows);
  } else {
    auto checkTimeoutAfterNCalls = checkTimeoutAfterNCallsFactory();

    auto lessThan = [jc1, jc2](const auto& row1, const auto& row2) {
      return row1[jc1] < row2[jc2];
    };
    auto lessThanReversed = [jc1, jc2](const auto& row1, const auto& row2) {
      return row1[jc2] < row2[jc1];
    };
    auto combineRows = [jc2, &result](const auto& row1, const auto& row2) {
      addCombinedRowToIdTable(row1, row2, jc2, &result);
    };
    auto joinColumnL = a.getColumn(jc1);
    auto joinColumnR = b.getColumn(jc2);
    auto aUndef = std::equal_range(joinColumnL.begin(), joinColumnL.end(),
                                   ValueId::makeUndefined());
    auto bUndef = std::equal_range(joinColumnR.begin(), joinColumnR.end(),
                                   ValueId::makeUndefined());
    // The undef values are right at the start, so this simplified calculation
    // should work.
    auto numUndefA = aUndef.first - aUndef.second;
    auto numUndefB = bUndef.first - bUndef.second;
    std::pair aUnd{a.begin(), a.begin() + numUndefA};
    std::pair bUnd{b.begin(), b.begin() + numUndefB};
    auto findSmallerUndefRangeLeft = [=](auto&&...) {
      return std::array{aUnd};
    };
    auto findSmallerUndefRangeRight = [=](auto&&...) {
      return std::array{bUnd};
    };

    // TODO<joka921> This does not yet respect the timeout.
    // ad_utility::zipperJoin(a, b, lessThan, combineRows);
    ad_utility::zipperJoinWithUndef(a, b, lessThan, lessThanReversed,
                                    combineRows, findSmallerUndefRangeLeft,
                                    findSmallerUndefRangeRight);
  }
  *dynRes = std::move(result).toDynamic();

  LOG(DEBUG) << "Join done.\n";
  LOG(DEBUG) << "Result: width = " << dynRes->numColumns()
             << ", size = " << dynRes->size() << "\n";
}

// ______________________________________________________________________________
template <int L_WIDTH, int R_WIDTH, int OUT_WIDTH>
void Join::hashJoinImpl(const IdTable& dynA, size_t jc1, const IdTable& dynB,
                        size_t jc2, IdTable* dynRes) {
  const IdTableView<L_WIDTH> a = dynA.asStaticView<L_WIDTH>();
  const IdTableView<R_WIDTH> b = dynB.asStaticView<R_WIDTH>();

  LOG(DEBUG) << "Performing hashJoin between two tables.\n";
  LOG(DEBUG) << "A: width = " << a.numColumns() << ", size = " << a.size()
             << "\n";
  LOG(DEBUG) << "B: width = " << b.numColumns() << ", size = " << b.size()
             << "\n";

  // Check trivial case.
  if (a.size() == 0 || b.size() == 0) {
    return;
  }

  IdTableStatic<OUT_WIDTH> result = std::move(*dynRes).toStatic<OUT_WIDTH>();

  // Puts the rows of the given table into a hash map, with the value of
  // the join column of a row as the key, and returns the hash map.
  auto idTableToHashMap = []<typename Table>(const Table& table,
                                             const size_t jc) {
    // This declaration works, because generic lambdas are just syntactic sugar
    // for templates.
    ad_utility::HashMap<Id, std::vector<typename Table::row_type>> map;
    for (const auto& row : table) {
      map[row[jc]].push_back(row);
    }
    return map;
  };

  /*
   * @brief Joins the two tables, putting the result in result. Creates a cross
   *  product for matching rows by putting the smaller IdTable in a hash map and
   *  using that, to faster find the matching rows.
   *
   * @tparam leftIsLarger If the left table in the join operation has more
   *  rows, or the right. True, if he has. False, if he hasn't.
   * @tparam LargerTableType, SmallerTableType The types of the tables given.
   *  Can be easily done with type interference, so no need to write them out.
   *
   * @param largerTable, smallerTable The two tables of the join operation.
   * @param largerTableJoinColumn, smallerTableJoinColumn The join columns
   *  of the tables
   */
  auto performHashJoin = [&idTableToHashMap,
                          &result]<bool leftIsLarger, typename LargerTableType,
                                   typename SmallerTableType>(
                             const LargerTableType& largerTable,
                             const size_t largerTableJoinColumn,
                             const SmallerTableType& smallerTable,
                             const size_t smallerTableJoinColumn) {
    // Put the smaller table into the hash table.
    auto map = idTableToHashMap(smallerTable, smallerTableJoinColumn);

    // Create cross product by going through the larger table.
    for (size_t i = 0; i < largerTable.size(); i++) {
      // Skip, if there is no matching entry for the join column.
      auto entry = map.find(largerTable(i, largerTableJoinColumn));
      if (entry == map.end()) {
        continue;
      }

      for (const auto& row : entry->second) {
        // Based on which table was larger, the arguments of
        // addCombinedRowToIdTable are different.
        // However this information is known at compile time, so the other
        // branch gets discarded at compile time, which makes this
        // condition have constant runtime.
        if constexpr (leftIsLarger) {
          addCombinedRowToIdTable(largerTable[i], row, smallerTableJoinColumn,
                                  &result);
        } else {
          addCombinedRowToIdTable(row, largerTable[i], largerTableJoinColumn,
                                  &result);
        }
      }
    }
  };

  // Cannot just switch a and b around because the order of
  // items in the result tuples is important.
  // Procceding with the actual hash join depended on which IdTableView
  // is bigger.
  if (a.size() >= b.size()) {
    performHashJoin.template operator()<true>(a, jc1, b, jc2);
  } else {
    performHashJoin.template operator()<false>(b, jc2, a, jc1);
  }
  *dynRes = std::move(result).toDynamic();

  LOG(DEBUG) << "HashJoin done.\n";
  LOG(DEBUG) << "Result: width = " << dynRes->numColumns()
             << ", size = " << dynRes->size() << "\n";
}

// ______________________________________________________________________________
void Join::hashJoin(const IdTable& dynA, size_t jc1, const IdTable& dynB,
                    size_t jc2, IdTable* dynRes) {
  CALL_FIXED_SIZE(
      (std::array{dynA.numColumns(), dynB.numColumns(), dynRes->numColumns()}),
      &Join::hashJoinImpl, this, dynA, jc1, dynB, jc2, dynRes);
}

// ___________________________________________________________________________
template <typename ROW_A, typename ROW_B, int TABLE_WIDTH>
void Join::addCombinedRowToIdTable(const ROW_A& rowA, const ROW_B& rowB,
                                   const size_t jcRowB,
                                   IdTableStatic<TABLE_WIDTH>* table) {
  // Add a new, empty row.
  const size_t backIndex = table->size();
  table->emplace_back();

  // Copy the entire rowA in the table.
  for (size_t h = 0; h < rowA.numColumns(); h++) {
    (*table)(backIndex, h) = rowA[h];
  }

  // Copy rowB columns before the join column.
  for (size_t h = 0; h < jcRowB; h++) {
    (*table)(backIndex, h + rowA.numColumns()) = rowB[h];
  }

  // Copy rowB columns after the join column.
  for (size_t h = jcRowB + 1; h < rowB.numColumns(); h++) {
    (*table)(backIndex, h + rowA.numColumns() - 1) = rowB[h];
  }
}
