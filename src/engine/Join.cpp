// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Join.h"
#include <sstream>
#include <unordered_set>
#include "./QueryExecutionTree.h"

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
void Join::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "Getting sub-results for join result computation..." << endl;
  size_t leftWidth = _left->getResultWidth();
  size_t rightWidth = _right->getResultWidth();

  // Checking this before calling getResult on the subtrees can
  // avoid the computation of an non-empty subtree.
  if (_left->knownEmptyResult() || _right->knownEmptyResult()) {
    LOG(TRACE) << "Either side is empty thus join result is empty" << endl;
    size_t resWidth = leftWidth + rightWidth - 1;
    result->_nofColumns = resWidth;
    result->_resultTypes.resize(result->_nofColumns);
    result->_sortedBy = {_leftJoinCol};
    if (resWidth == 1) {
      result->_fixedSizeData = new vector<array<Id, 1>>();
    } else if (resWidth == 2) {
      result->_fixedSizeData = new vector<array<Id, 2>>();
    } else if (resWidth == 3) {
      result->_fixedSizeData = new vector<array<Id, 3>>();
    } else if (resWidth == 4) {
      result->_fixedSizeData = new vector<array<Id, 4>>();
    } else if (resWidth == 5) {
      result->_fixedSizeData = new vector<array<Id, 5>>();
    }
    result->finish();
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

  // Check if we can stop early.
  if (leftRes->size() == 0) {
    LOG(TRACE) << "Left side empty thus join result is empty" << endl;
    size_t resWidth = leftWidth + rightWidth - 1;
    result->_nofColumns = resWidth;
    result->_resultTypes.resize(result->_nofColumns);
    result->_sortedBy = {_leftJoinCol};
    if (resWidth == 1) {
      result->_fixedSizeData = new vector<array<Id, 1>>();
    } else if (resWidth == 2) {
      result->_fixedSizeData = new vector<array<Id, 2>>();
    } else if (resWidth == 3) {
      result->_fixedSizeData = new vector<array<Id, 3>>();
    } else if (resWidth == 4) {
      result->_fixedSizeData = new vector<array<Id, 4>>();
    } else if (resWidth == 5) {
      result->_fixedSizeData = new vector<array<Id, 5>>();
    }
    result->finish();
    return;
  }

  LOG(TRACE) << "Computing right side..." << endl;
  shared_ptr<const ResultTable> rightRes = _right->getResult();

  LOG(DEBUG) << "Computing Join result..." << endl;

  AD_CHECK(result);
  AD_CHECK(!result->_fixedSizeData);

  result->_nofColumns = leftWidth + rightWidth - 1;
  result->_resultTypes.reserve(result->_nofColumns);
  result->_resultTypes.insert(result->_resultTypes.end(),
                              leftRes->_resultTypes.begin(),
                              leftRes->_resultTypes.end());
  for (size_t i = 0; i < rightRes->_nofColumns; i++) {
    if (i != _rightJoinCol) {
      result->_resultTypes.push_back(rightRes->_resultTypes[i]);
    }
  }
  result->_sortedBy = {_leftJoinCol};

  if (leftWidth == 1) {
    if (rightWidth == 1) {
      result->_fixedSizeData = new vector<array<Id, 1>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 1>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 1>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 1>>*>(result->_fixedSizeData));
    } else if (rightWidth == 2) {
      result->_fixedSizeData = new vector<array<Id, 2>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 1>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 2>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
    } else if (rightWidth == 3) {
      result->_fixedSizeData = new vector<array<Id, 3>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 1>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 3>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 3>>*>(result->_fixedSizeData));
    } else if (rightWidth == 4) {
      result->_fixedSizeData = new vector<array<Id, 4>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 1>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 4>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 4>>*>(result->_fixedSizeData));
    } else if (rightWidth == 5) {
      result->_fixedSizeData = new vector<array<Id, 5>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 1>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 5>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 1>>*>(leftRes->_fixedSizeData),
          _leftJoinCol, rightRes->_varSizeData, _rightJoinCol,
          &result->_varSizeData);
    }
  } else if (leftWidth == 2) {
    if (rightWidth == 1) {
      result->_fixedSizeData = new vector<array<Id, 2>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 2>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 1>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
      ;
    } else if (rightWidth == 2) {
      result->_fixedSizeData = new vector<array<Id, 3>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 2>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 2>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 3>>*>(result->_fixedSizeData));
    } else if (rightWidth == 3) {
      result->_fixedSizeData = new vector<array<Id, 4>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 2>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 3>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 4>>*>(result->_fixedSizeData));
    } else if (rightWidth == 4) {
      result->_fixedSizeData = new vector<array<Id, 5>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 2>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 4>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else if (rightWidth == 5) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 2>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 5>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 2>>*>(leftRes->_fixedSizeData),
          _leftJoinCol, rightRes->_varSizeData, _rightJoinCol,
          &result->_varSizeData);
    }
  } else if (leftWidth == 3) {
    if (rightWidth == 1) {
      result->_fixedSizeData = new vector<array<Id, 3>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 3>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 1>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 3>>*>(result->_fixedSizeData));
    } else if (rightWidth == 2) {
      result->_fixedSizeData = new vector<array<Id, 4>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 3>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 2>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 4>>*>(result->_fixedSizeData));
    } else if (rightWidth == 3) {
      result->_fixedSizeData = new vector<array<Id, 5>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 3>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 3>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else if (rightWidth == 4) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 3>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 4>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else if (rightWidth == 5) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 3>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 5>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 3>>*>(leftRes->_fixedSizeData),
          _leftJoinCol, rightRes->_varSizeData, _rightJoinCol,
          &result->_varSizeData);
    }
  } else if (leftWidth == 4) {
    if (rightWidth == 1) {
      result->_fixedSizeData = new vector<array<Id, 4>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 4>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 1>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 4>>*>(result->_fixedSizeData));
    } else if (rightWidth == 2) {
      result->_fixedSizeData = new vector<array<Id, 5>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 4>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 2>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else if (rightWidth == 3) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 4>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 3>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else if (rightWidth == 4) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 4>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 4>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else if (rightWidth == 5) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 4>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 5>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 4>>*>(leftRes->_fixedSizeData),
          _leftJoinCol, rightRes->_varSizeData, _rightJoinCol,
          &result->_varSizeData);
    }
  } else if (leftWidth == 5) {
    if (rightWidth == 1) {
      result->_fixedSizeData = new vector<array<Id, 5>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 5>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 1>>*>(rightRes->_fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else if (rightWidth == 2) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 5>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 2>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else if (rightWidth == 3) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 5>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 3>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else if (rightWidth == 4) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 5>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 4>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else if (rightWidth == 5) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 5>>*>(leftRes->_fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 5>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 5>>*>(leftRes->_fixedSizeData),
          _leftJoinCol, rightRes->_varSizeData, _rightJoinCol,
          &result->_varSizeData);
    }
  } else {
    if (rightWidth == 1) {
      _executionContext->getEngine().join(
          leftRes->_varSizeData, _leftJoinCol,
          *static_cast<const vector<array<Id, 1>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else if (rightWidth == 2) {
      _executionContext->getEngine().join(
          leftRes->_varSizeData, _leftJoinCol,
          *static_cast<const vector<array<Id, 2>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else if (rightWidth == 3) {
      _executionContext->getEngine().join(
          leftRes->_varSizeData, _leftJoinCol,
          *static_cast<const vector<array<Id, 3>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else if (rightWidth == 4) {
      _executionContext->getEngine().join(
          leftRes->_varSizeData, _leftJoinCol,
          *static_cast<const vector<array<Id, 4>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else if (rightWidth == 5) {
      _executionContext->getEngine().join(
          leftRes->_varSizeData, _leftJoinCol,
          *static_cast<const vector<array<Id, 5>>*>(rightRes->_fixedSizeData),
          _rightJoinCol, &result->_varSizeData);
    } else {
      _executionContext->getEngine().join(leftRes->_varSizeData, _leftJoinCol,
                                          rightRes->_varSizeData, _rightJoinCol,
                                          &result->_varSizeData);
    }
  }
  result->finish();
  LOG(DEBUG) << "Join result computation done." << endl;
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t> Join::getVariableColumns() const {
  ad_utility::HashMap<string, size_t> retVal;
  if (!isFullScanDummy(_left) && !isFullScanDummy(_right)) {
    retVal = _left->getVariableColumnMap();
    size_t leftSize = _left->getResultWidth();
    for (auto it = _right->getVariableColumnMap().begin();
         it != _right->getVariableColumnMap().end(); ++it) {
      if (it->second < _rightJoinCol) {
        retVal[it->first] = leftSize + it->second;
      }
      if (it->second > _rightJoinCol) {
        retVal[it->first] = leftSize + it->second - 1;
      }
    }
  } else {
    if (isFullScanDummy(_right)) {
      retVal = _left->getVariableColumnMap();
      size_t leftSize = _left->getResultWidth();
      for (auto it = _right->getVariableColumnMap().begin();
           it != _right->getVariableColumnMap().end(); ++it) {
        // Skip the first col for the dummy
        if (it->second != 0) {
          retVal[it->first] = leftSize + it->second - 1;
        }
      }
    } else {
      for (auto it = _left->getVariableColumnMap().begin();
           it != _left->getVariableColumnMap().end(); ++it) {
        // Skip+drop the first col for the dummy and subtract one from others.
        if (it->second != 0) {
          retVal[it->first] = it->second - 1;
        }
      }
      for (auto it = _right->getVariableColumnMap().begin();
           it != _right->getVariableColumnMap().end(); ++it) {
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
    result->_nofColumns = _right->getResultWidth() + 2;
    result->_sortedBy = {2 + _rightJoinCol};
    shared_ptr<const ResultTable> nonDummyRes = _right->getResult();
    result->_resultTypes.reserve(result->_nofColumns);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.insert(result->_resultTypes.end(),
                                nonDummyRes->_resultTypes.begin(),
                                nonDummyRes->_resultTypes.end());

    if (_right->getResultWidth() == 1) {
      const Index::WidthOneList& r =
          *static_cast<Index::WidthOneList*>(nonDummyRes->_fixedSizeData);
      result->_fixedSizeData = new Index::WidthThreeList();
      doComputeJoinWithFullScanDummyLeft(
          r, static_cast<Index::WidthThreeList*>(result->_fixedSizeData));
    } else if (_right->getResultWidth() == 2) {
      const Index::WidthTwoList& r =
          *static_cast<Index::WidthTwoList*>(nonDummyRes->_fixedSizeData);
      result->_fixedSizeData = new Index::WidthFourList();
      doComputeJoinWithFullScanDummyLeft(
          r, static_cast<Index::WidthFourList*>(result->_fixedSizeData));
    } else if (_right->getResultWidth() == 3) {
      const Index::WidthThreeList& r =
          *static_cast<Index::WidthThreeList*>(nonDummyRes->_fixedSizeData);
      result->_fixedSizeData = new Index::WidthFiveList();
      doComputeJoinWithFullScanDummyLeft(
          r, static_cast<Index::WidthFiveList*>(result->_fixedSizeData));
    } else if (_right->getResultWidth() == 4) {
      const Index::WidthFourList& r =
          *static_cast<Index::WidthFourList*>(nonDummyRes->_fixedSizeData);
      doComputeJoinWithFullScanDummyLeft(r, &result->_varSizeData);
    } else if (_right->getResultWidth() == 5) {
      const Index::WidthFiveList& r =
          *static_cast<Index::WidthFiveList*>(nonDummyRes->_fixedSizeData);
      doComputeJoinWithFullScanDummyLeft(r, &result->_varSizeData);
    } else {
      const Index::VarWidthList& r = nonDummyRes->_varSizeData;
      doComputeJoinWithFullScanDummyLeft(r, &result->_varSizeData);
    }
  } else {
    AD_CHECK(!isFullScanDummy(_left))
    result->_nofColumns = _left->getResultWidth() + 2;
    result->_sortedBy = {_leftJoinCol};

    shared_ptr<const ResultTable> nonDummyRes = _left->getResult();
    result->_resultTypes.reserve(result->_nofColumns);
    result->_resultTypes.insert(result->_resultTypes.end(),
                                nonDummyRes->_resultTypes.begin(),
                                nonDummyRes->_resultTypes.end());
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    result->_resultTypes.push_back(ResultTable::ResultType::KB);
    if (_left->getResultWidth() == 1) {
      const Index::WidthOneList& r =
          *static_cast<Index::WidthOneList*>(nonDummyRes->_fixedSizeData);
      result->_fixedSizeData = new Index::WidthThreeList();
      doComputeJoinWithFullScanDummyRight(
          r, static_cast<Index::WidthThreeList*>(result->_fixedSizeData));
    } else if (_left->getResultWidth() == 2) {
      const Index::WidthTwoList& r =
          *static_cast<Index::WidthTwoList*>(nonDummyRes->_fixedSizeData);
      result->_fixedSizeData = new Index::WidthFourList();
      doComputeJoinWithFullScanDummyRight(
          r, static_cast<Index::WidthFourList*>(result->_fixedSizeData));
    } else if (_left->getResultWidth() == 3) {
      const Index::WidthThreeList& r =
          *static_cast<Index::WidthThreeList*>(nonDummyRes->_fixedSizeData);
      result->_fixedSizeData = new Index::WidthFiveList();
      doComputeJoinWithFullScanDummyRight(
          r, static_cast<Index::WidthFiveList*>(result->_fixedSizeData));
    } else if (_left->getResultWidth() == 4) {
      const Index::WidthFourList& r =
          *static_cast<Index::WidthFourList*>(nonDummyRes->_fixedSizeData);
      doComputeJoinWithFullScanDummyRight(r, &result->_varSizeData);
    } else if (_left->getResultWidth() == 5) {
      const Index::WidthFiveList& r =
          *static_cast<Index::WidthFiveList*>(nonDummyRes->_fixedSizeData);
      doComputeJoinWithFullScanDummyRight(r, &result->_varSizeData);
    } else {
      const Index::VarWidthList& r = nonDummyRes->_varSizeData;
      doComputeJoinWithFullScanDummyRight(r, &result->_varSizeData);
    }
  }
  result->finish();
  LOG(DEBUG) << "Join (with dummy) done. Size: " << result->size() << endl;
}

// _____________________________________________________________________________
Join::ScanMethodType Join::getScanMethod(
    std::shared_ptr<QueryExecutionTree> fullScanDummyTree) const {
  ScanMethodType scanMethod;
  IndexScan& scan =
      *static_cast<IndexScan*>(fullScanDummyTree->getRootOperation().get());
  switch (scan.getType()) {
    case IndexScan::FULL_INDEX_SCAN_SPO:
      scanMethod = &Index::scanSPO;
      break;
    case IndexScan::FULL_INDEX_SCAN_SOP:
      scanMethod = &Index::scanSOP;
      break;
    case IndexScan::FULL_INDEX_SCAN_PSO:
      scanMethod = &Index::scanPSO;
      break;
    case IndexScan::FULL_INDEX_SCAN_POS:
      scanMethod = &Index::scanPOS;
      break;
    case IndexScan::FULL_INDEX_SCAN_OSP:
      scanMethod = &Index::scanOSP;
      break;
    case IndexScan::FULL_INDEX_SCAN_OPS:
      scanMethod = &Index::scanOPS;
      break;
    default:
      AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
               "Found non-dummy scan where one was expected.");
  }
  return scanMethod;
}

// _____________________________________________________________________________
template <typename NonDummyResultList, typename ResultList>
void Join::doComputeJoinWithFullScanDummyLeft(const NonDummyResultList& ndr,
                                              ResultList* res) const {
  LOG(TRACE) << "Dummy on left side, other join op size: " << ndr.size()
             << endl;
  if (ndr.size() == 0) {
    return;
  }
  const auto* index = &getIndex();
  const ScanMethodType scan = getScanMethod(_left);
  // Iterate through non-dummy.
  Id currentJoinId = ndr[0][_rightJoinCol];
  auto joinItemFrom = ndr.begin();
  auto joinItemEnd = ndr.begin();
  for (size_t i = 0; i < ndr.size(); ++i) {
    // For each different element in the join column.
    if (ndr[i][_rightJoinCol] == currentJoinId) {
      ++joinItemEnd;
    } else {
      // Do a scan.
      LOG(TRACE) << "Inner scan with ID: " << currentJoinId << endl;
      Index::WidthTwoList jr;
      (index->*scan)(currentJoinId, &jr);
      LOG(TRACE) << "Got #items: " << jr.size() << endl;
      // Build the cross product.
      appendCrossProduct(jr.begin(), jr.end(), joinItemFrom, joinItemEnd, res);
      // Reset
      currentJoinId = ndr[i][_rightJoinCol];
      joinItemFrom = joinItemEnd;
      ++joinItemEnd;
    }
  }
  // Do the scan for the final element.
  LOG(TRACE) << "Inner scan with ID: " << currentJoinId << endl;
  Index::WidthTwoList jr;
  (index->*scan)(currentJoinId, &jr);
  LOG(TRACE) << "Got #items: " << jr.size() << endl;
  // Build the cross product.
  appendCrossProduct(jr.begin(), jr.end(), joinItemFrom, joinItemEnd, res);
}

// _____________________________________________________________________________
template <typename NonDummyResultList, typename ResultList>
void Join::doComputeJoinWithFullScanDummyRight(const NonDummyResultList& ndr,
                                               ResultList* res) const {
  LOG(TRACE) << "Dummy on right side, other join op size: " << ndr.size()
             << endl;
  if (ndr.size() == 0) {
    return;
  }
  // Get the scan method (depends on type of dummy tree), use a function ptr.
  const ScanMethodType scan = getScanMethod(_right);
  const auto* index = &getIndex();
  // Iterate through non-dummy.
  Id currentJoinId = ndr[0][_leftJoinCol];
  auto joinItemFrom = ndr.begin();
  auto joinItemEnd = ndr.begin();
  for (size_t i = 0; i < ndr.size(); ++i) {
    // For each different element in the join column.
    if (ndr[i][_leftJoinCol] == currentJoinId) {
      ++joinItemEnd;
    } else {
      // Do a scan.
      LOG(TRACE) << "Inner scan with ID: " << currentJoinId << endl;
      Index::WidthTwoList jr;
      (index->*scan)(currentJoinId, &jr);
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
  Index::WidthTwoList jr;
  (index->*scan)(currentJoinId, &jr);
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
