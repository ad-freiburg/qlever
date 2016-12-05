// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <sstream>
#include <unordered_set>
#include "./QueryExecutionTree.h"
#include "./Join.h"
#include "Sort.h"

using std::string;

// _____________________________________________________________________________
Join::Join(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> t1,
           std::shared_ptr<QueryExecutionTree> t2,
           size_t t1JoinCol,
           size_t t2JoinCol,
           bool keepJoinColumn) : Operation(qec) {
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
string Join::asString() const {
  std::ostringstream os;
  os << "JOIN(\n" << _left->asString() << " [" << _leftJoinCol <<
     "]\n|X|\n" << _right->asString() << " [" << _rightJoinCol << "]\n)";
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
    size_t resWidth = leftWidth + rightWidth - 1;
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
    result->_status = ResultTable::FINISHED;
    return;
  }

  const ResultTable& leftRes = _left->getRootOperation()->getResult();

  // Check if we can stop early.
  if (leftRes.size() == 0) {
    size_t resWidth = leftWidth + rightWidth - 1;
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
    result->_status = ResultTable::FINISHED;
    return;
  }

  const ResultTable& rightRes = _right->getRootOperation()->getResult();

  LOG(DEBUG) << "Join result computation..." << endl;

  AD_CHECK(result);
  AD_CHECK(!result->_fixedSizeData);

  result->_nofColumns = leftWidth + rightWidth - 1;
  result->_sortedBy = _leftJoinCol;

  if (leftWidth == 1) {
    if (rightWidth == 1) {
      result->_fixedSizeData = new vector<array<Id, 1>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 1>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 1>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 1>>*>(result->_fixedSizeData));
    } else if (rightWidth == 2) {
      result->_fixedSizeData = new vector<array<Id, 2>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 1>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 2>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));
    } else if (rightWidth == 3) {
      result->_fixedSizeData = new vector<array<Id, 3>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 1>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 3>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 3>>*>(result->_fixedSizeData));
    } else if (rightWidth == 4) {
      result->_fixedSizeData = new vector<array<Id, 4>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 1>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 4>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 4>>*>(result->_fixedSizeData));
    } else if (rightWidth == 5) {
      result->_fixedSizeData = new vector<array<Id, 5>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 1>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 5>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 1>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          rightRes._varSizeData,
          _rightJoinCol,
          &result->_varSizeData);
    }
  } else if (leftWidth == 2) {
    if (rightWidth == 1) {
      result->_fixedSizeData = new vector<array<Id, 2>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 2>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 1>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 2>>*>(result->_fixedSizeData));;
    } else if (rightWidth == 2) {
      result->_fixedSizeData = new vector<array<Id, 3>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 2>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 2>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 3>>*>(result->_fixedSizeData));
    } else if (rightWidth == 3) {
      result->_fixedSizeData = new vector<array<Id, 4>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 2>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 3>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 4>>*>(result->_fixedSizeData));
    } else if (rightWidth == 4) {
      result->_fixedSizeData = new vector<array<Id, 5>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 2>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 4>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else if (rightWidth == 5) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 2>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 5>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 2>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          rightRes._varSizeData,
          _rightJoinCol,
          &result->_varSizeData);
    }
  } else if (leftWidth == 3) {
    if (rightWidth == 1) {
      result->_fixedSizeData = new vector<array<Id, 3>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 3>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 1>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 3>>*>(result->_fixedSizeData));
    } else if (rightWidth == 2) {
      result->_fixedSizeData = new vector<array<Id, 4>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 3>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 2>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 4>>*>(result->_fixedSizeData));
    } else if (rightWidth == 3) {
      result->_fixedSizeData = new vector<array<Id, 5>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 3>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 3>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else if (rightWidth == 4) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 3>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 4>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else if (rightWidth == 5) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 3>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 5>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 3>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          rightRes._varSizeData,
          _rightJoinCol,
          &result->_varSizeData);
    }
  } else if (leftWidth == 4) {
    if (rightWidth == 1) {
      result->_fixedSizeData = new vector<array<Id, 4>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 4>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 1>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 4>>*>(result->_fixedSizeData));
    } else if (rightWidth == 2) {
      result->_fixedSizeData = new vector<array<Id, 5>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 4>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 2>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else if (rightWidth == 3) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 4>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 3>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else if (rightWidth == 4) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 4>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 4>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else if (rightWidth == 5) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 4>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 5>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 4>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          rightRes._varSizeData,
          _rightJoinCol,
          &result->_varSizeData);
    }
  } else if (leftWidth == 5) {
    if (rightWidth == 1) {
      result->_fixedSizeData = new vector<array<Id, 5>>();
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 5>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 1>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          static_cast<vector<array<Id, 5>>*>(result->_fixedSizeData));
    } else if (rightWidth == 2) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 5>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 2>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else if (rightWidth == 3) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 5>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 3>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else if (rightWidth == 4) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 5>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 4>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else if (rightWidth == 5) {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 5>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          *static_cast<const vector<array<Id, 5>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else {
      _executionContext->getEngine().join(
          *static_cast<const vector<array<Id, 5>>*>(leftRes._fixedSizeData),
          _leftJoinCol,
          rightRes._varSizeData,
          _rightJoinCol,
          &result->_varSizeData);
    }
  } else {
    if (rightWidth == 1) {
      _executionContext->getEngine().join(
          leftRes._varSizeData,
          _leftJoinCol,
          *static_cast<const vector<array<Id, 1>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else if (rightWidth == 2) {
      _executionContext->getEngine().join(
          leftRes._varSizeData,
          _leftJoinCol,
          *static_cast<const vector<array<Id, 2>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else if (rightWidth == 3) {
      _executionContext->getEngine().join(
          leftRes._varSizeData,
          _leftJoinCol,
          *static_cast<const vector<array<Id, 3>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else if (rightWidth == 4) {
      _executionContext->getEngine().join(
          leftRes._varSizeData,
          _leftJoinCol,
          *static_cast<const vector<array<Id, 4>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else if (rightWidth == 5) {
      _executionContext->getEngine().join(
          leftRes._varSizeData,
          _leftJoinCol,
          *static_cast<const vector<array<Id, 5>>*>(rightRes._fixedSizeData),
          _rightJoinCol,
          &result->_varSizeData);
    } else {
      _executionContext->getEngine().join(
          leftRes._varSizeData,
          _leftJoinCol,
          rightRes._varSizeData,
          _rightJoinCol,
          &result->_varSizeData);
    }
  }
  result->_status = ResultTable::FINISHED;
  LOG(DEBUG) << "Join result computation done." << endl;
}

// _____________________________________________________________________________
std::unordered_map<string, size_t> Join::getVariableColumns() const {
  std::unordered_map<string, size_t> retVal(_left->getVariableColumnMap());
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
size_t Join::resultSortedOn() const {
  return _leftJoinCol;
}

// _____________________________________________________________________________
bool Join::isSelfJoin() const {
  // For efficiency reasons we only consider self joins directly between scans.
  if (_left->getResultWidth() == 2 && _right->getResultWidth() == 2
      && (_left->getType() == QueryExecutionTree::SCAN
          || _left->getType() == QueryExecutionTree::SORT)
      && (_right->getType() == QueryExecutionTree::SCAN
          || _right->getType() == QueryExecutionTree::SORT)) {
    return _left->asString() == _right->asString();
  }
  return false;
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
    computeMultiplicities();
  }
  AD_CHECK_LT(col, _multiplicities.size());
  return _multiplicities[col];
}

// _____________________________________________________________________________
size_t Join::computeSizeEstimate() {
  if (_left->getSizeEstimate() == 0
      || _right->getSizeEstimate() == 0) {
    return 0;
  }
  // NEW at 05 Dec 2016:
  // Estimate the size of the join result by the new multiplicity for the join
  // column (which is m = m1 * m2 where m1 and m2 are multiplicities of the
  // join columns in the two child trees) times the the number of distinct
  // elements estimated as  d = min(d1, d2).
  auto joinColInResult = _leftJoinCol;  // for readability
  size_t nofDistinctLeft = static_cast<size_t>(
      _left->getSizeEstimate() / _left->getMultiplicity(_leftJoinCol));
  size_t nofDistinctRight = static_cast<size_t>(
      _right->getSizeEstimate() / _right->getMultiplicity(_rightJoinCol));
  return static_cast<size_t>(getMultiplicity(joinColInResult) *
         std::min(nofDistinctLeft, nofDistinctRight));
}

// _____________________________________________________________________________
void Join::computeMultiplicities() {
  if (_executionContext) {
    float _leftJcM = _left->getMultiplicity(_leftJoinCol);
    float _rightJcM = _right->getMultiplicity(_rightJoinCol);
    for (size_t i = 0; i < _left->getResultWidth(); ++i) {
      _multiplicities.emplace_back(_left->getMultiplicity(i) * _rightJcM);
    }
    for (size_t i = 0; i < _right->getResultWidth(); ++i) {
      if (i == _rightJoinCol) { continue; }
      _multiplicities.emplace_back(_right->getMultiplicity(i) * _leftJcM);
    }
  } else {
    for (size_t i = 0; i < getResultWidth(); ++i) {
      _multiplicities.emplace_back(1);
    }
  }
  assert(_multiplicities.size() == getResultWidth());
}


