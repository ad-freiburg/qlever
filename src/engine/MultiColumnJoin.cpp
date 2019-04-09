// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "MultiColumnJoin.h"
#include "CallFixedSize.h"

using std::string;

// _____________________________________________________________________________
MultiColumnJoin::MultiColumnJoin(QueryExecutionContext* qec,
                                 std::shared_ptr<QueryExecutionTree> t1,
                                 std::shared_ptr<QueryExecutionTree> t2,
                                 const vector<array<Id, 2>>& jcs)
    : Operation(qec), _joinColumns(jcs), _multiplicitiesComputed(false) {
  // Make sure subtrees are ordered so that identical queries can be identified.
  AD_CHECK_GT(jcs.size(), 0);
  if (t1->asString() < t2->asString()) {
    _left = t1;
    _right = t2;
  } else {
    // Swap the two subtrees
    _left = t2;
    _right = t1;
    // As the subtrees have been swapped the join columns need to be swapped
    // as well.
    for (unsigned int i = 0; i < _joinColumns.size(); i++) {
      std::swap(_joinColumns[i][0], _joinColumns[i][1]);
    }
  }
}

// _____________________________________________________________________________
string MultiColumnJoin::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "MULTI_COLUMN_JOIN\n" << _left->asString(indent) << " ";
  os << "join-columns: [";
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    os << _joinColumns[i][0] << (i < _joinColumns.size() - 1 ? " & " : "");
  };
  os << "]\n";
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "|X|\n" << _right->asString(indent) << " ";
  os << "join-columns: [";
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    os << _joinColumns[i][1] << (i < _joinColumns.size() - 1 ? " & " : "");
  };
  os << "]";
  return os.str();
}

// _____________________________________________________________________________
void MultiColumnJoin::computeResult(ResultTable* result) {
  AD_CHECK(result);
  LOG(DEBUG) << "MultiColumnJoin result computation..." << endl;

  RuntimeInformation& runtimeInfo = getRuntimeInfo();
  std::string joinVars = "";
  for (auto p : _left->getVariableColumns()) {
    for (auto jc : _joinColumns) {
      // If the left join column matches the index of a variable in the left
      // subresult.
      if (jc[0] == p.second) {
        joinVars += p.first + " ";
      }
    }
  }
  runtimeInfo.setDescriptor("MultiColumnJoin on " + joinVars);

  result->_sortedBy = resultSortedOn();
  result->_data.setCols(getResultWidth());

  AD_CHECK_GE(result->_data.cols(), _joinColumns.size());

  const auto leftResult = _left->getResult();
  const auto rightResult = _right->getResult();

  runtimeInfo.addChild(_left->getRootOperation()->getRuntimeInfo());
  runtimeInfo.addChild(_right->getRootOperation()->getRuntimeInfo());

  LOG(DEBUG) << "MultiColumnJoin subresult computation done." << std::endl;

  // compute the result types
  result->_resultTypes.reserve(result->_data.cols());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              leftResult->_resultTypes.begin(),
                              leftResult->_resultTypes.end());
  for (size_t col = 0; col < rightResult->_data.cols(); col++) {
    bool isJoinColumn = false;
    for (const std::array<Id, 2>& a : _joinColumns) {
      if (a[1] == col) {
        isJoinColumn = true;
        break;
      }
    }
    if (!isJoinColumn) {
      result->_resultTypes.push_back(rightResult->_resultTypes[col]);
    }
  }

  LOG(DEBUG) << "Computing a multi column join between results of size "
             << leftResult->size() << " and " << rightResult->size() << endl;

  int leftWidth = leftResult->_data.cols();
  int rightWidth = rightResult->_data.cols();
  int resWidth = result->_data.cols();
  CALL_FIXED_SIZE_3(leftWidth, rightWidth, resWidth, computeMultiColumnJoin,
                    leftResult->_data, rightResult->_data, _joinColumns,
                    &result->_data);
  LOG(DEBUG) << "MultiColumnJoin result computation done." << endl;
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t> MultiColumnJoin::getVariableColumns()
    const {
  ad_utility::HashMap<string, size_t> retVal(_left->getVariableColumns());
  size_t columnIndex = retVal.size();
  for (const auto& it : _right->getVariableColumns()) {
    bool isJoinColumn = false;
    for (const std::array<Id, 2>& a : _joinColumns) {
      if (a[1] == it.second) {
        isJoinColumn = true;
        break;
      }
    }
    if (!isJoinColumn) {
      retVal[it.first] = columnIndex;
      columnIndex++;
    }
  }
  return retVal;
}

// _____________________________________________________________________________
size_t MultiColumnJoin::getResultWidth() const {
  size_t res =
      _left->getResultWidth() + _right->getResultWidth() - _joinColumns.size();
  AD_CHECK(res > 0);
  return res;
}

// _____________________________________________________________________________
vector<size_t> MultiColumnJoin::resultSortedOn() const {
  std::vector<size_t> sortedOn;
  // The result is sorted on all join columns from the left subtree.
  for (const auto& a : _joinColumns) {
    sortedOn.push_back(a[0]);
  }
  return sortedOn;
}

// _____________________________________________________________________________
float MultiColumnJoin::getMultiplicity(size_t col) {
  if (!_multiplicitiesComputed) {
    computeSizeEstimateAndMultiplicities();
  }
  return _multiplicities[col];
}

// _____________________________________________________________________________
size_t MultiColumnJoin::getSizeEstimate() {
  if (!_multiplicitiesComputed) {
    computeSizeEstimateAndMultiplicities();
  }
  return _sizeEstimate;
}

// _____________________________________________________________________________
size_t MultiColumnJoin::getCostEstimate() {
  size_t costEstimate =
      getSizeEstimate() + _left->getSizeEstimate() + _right->getSizeEstimate();
  // This join is slower than a normal join, due to
  // its increased complexity
  costEstimate *= 2;
  // Make the join 7% more expensive per join column
  costEstimate *= (1 + (_joinColumns.size() - 1) * 0.07);
  return _left->getCostEstimate() + _right->getCostEstimate() + costEstimate;
}

// _____________________________________________________________________________
void MultiColumnJoin::computeSizeEstimateAndMultiplicities() {
  // The number of distinct entries in the result is at most the minimum of
  // the numbers of distinct entries in all join columns.
  // The multiplicity in the result is approximated by the product of the
  // maximum of the multiplicities of each side.

  // compute the minimum number of distinct elements in the join columns
  size_t numDistinctLeft = std::numeric_limits<size_t>::max();
  size_t numDistinctRight = std::numeric_limits<size_t>::max();
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    size_t dl = std::max(1.0f, _left->getSizeEstimate() /
                                   _left->getMultiplicity(_joinColumns[i][0]));
    size_t dr = std::max(1.0f, _right->getSizeEstimate() /
                                   _right->getMultiplicity(_joinColumns[i][1]));
    numDistinctLeft = std::min(numDistinctLeft, dl);
    numDistinctRight = std::min(numDistinctRight, dr);
  }
  size_t numDistinctResult = std::min(numDistinctLeft, numDistinctRight);

  // compute an estimate for the results multiplicity
  float multLeft = std::numeric_limits<float>::max();
  float multRight = std::numeric_limits<float>::max();
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    multLeft = std::min(multLeft, _left->getMultiplicity(_joinColumns[i][0]));
    multRight = std::min(multLeft, _left->getMultiplicity(_joinColumns[i][1]));
  }
  float multResult = multLeft * multRight;

  _sizeEstimate = multResult * numDistinctResult;
  // Don't estimate 0 since then some parent operations
  // (in particular joins) using isKnownEmpty() will
  // will assume the size to be exactly zero
  _sizeEstimate += 1;

  // compute estimates for the multiplicities of the result columns
  _multiplicities.clear();

  for (size_t i = 0; i < _left->getResultWidth(); i++) {
    float mult = _left->getMultiplicity(i) * (multResult / multLeft);
    _multiplicities.push_back(mult);
  }

  for (size_t i = 0; i < _right->getResultWidth(); i++) {
    bool isJcl = false;
    for (size_t j = 0; j < _joinColumns.size(); j++) {
      if (_joinColumns[j][1] == i) {
        isJcl = true;
        break;
      }
    }
    if (isJcl) {
      continue;
    }
    float mult = _right->getMultiplicity(i) * (multResult / multRight);
    _multiplicities.push_back(mult);
  }
  _multiplicitiesComputed = true;
}

template <int A_WIDTH, int B_WIDTH, int OUT_WIDTH>
void MultiColumnJoin::computeMultiColumnJoin(
    const IdTable& dynA, const IdTable& dynB,
    const vector<array<Id, 2>>& joinColumns, IdTable* dynResult) {
  // check for trivial cases
  if (dynA.size() == 0 || dynB.size() == 0) {
    return;
  }

  // Marks the columns in b that are join columns. Used to skip these
  // when computing the result of the join
  int joinColumnBitmap_b = 0;
  for (const array<Id, 2>& jc : joinColumns) {
    joinColumnBitmap_b |= (1 << jc[1]);
  }

  IdTableStatic<A_WIDTH> a = dynA.asStaticView<A_WIDTH>();
  IdTableStatic<B_WIDTH> b = dynB.asStaticView<B_WIDTH>();
  IdTableStatic<OUT_WIDTH> result = dynResult->moveToStatic<OUT_WIDTH>();

  bool matched = false;
  size_t ia = 0, ib = 0;
  while (ia < a.size() && ib < b.size()) {
    // Join columns 0 are the primary sort columns
    while (a(ia, joinColumns[0][0]) < b(ib, joinColumns[0][1])) {
      ia++;
      if (ia >= a.size()) {
        goto finish;
      }
    }
    while (b(ib, joinColumns[0][1]) < a(ia, joinColumns[0][0])) {
      ib++;
      if (ib >= b.size()) {
        goto finish;
      }
    }

    // check if the rest of the join columns also match
    matched = true;
    for (size_t joinColIndex = 0; joinColIndex < joinColumns.size();
         joinColIndex++) {
      const array<Id, 2>& joinColumn = joinColumns[joinColIndex];
      if (a(ia, joinColumn[0]) < b(ib, joinColumn[1])) {
        ia++;
        matched = false;
        break;
      }
      if (b(ib, joinColumn[1]) < a(ia, joinColumn[0])) {
        ib++;
        matched = false;
        break;
      }
    }

    // Compute the cross product of the row in a and all matching
    // rows in b.
    while (matched && ia < a.size() && ib < b.size()) {
      // used to reset ib if another cross product needs to be computed
      size_t initIb = ib;

      while (matched) {
        result.emplace_back();
        size_t backIdx = result.size() - 1;

        // fill the result
        size_t rIndex = 0;
        for (size_t col = 0; col < a.cols(); col++) {
          result(backIdx, rIndex) = a(ia, col);
          rIndex++;
        }
        for (size_t col = 0; col < b.cols(); col++) {
          if ((joinColumnBitmap_b & (1 << col)) == 0) {
            result(backIdx, rIndex) = b(ib, col);
            rIndex++;
          }
        }

        ib++;

        // do the rows still match?
        for (const array<Id, 2>& jc : joinColumns) {
          if (ib >= b.size() || a(ia, jc[0]) != b(ib, jc[1])) {
            matched = false;
            break;
          }
        }
      }
      ia++;
      // Check if the next row in a also matches the initial row in b
      matched = true;
      for (const array<Id, 2>& jc : joinColumns) {
        if (ia >= a.size() || a(ia, jc[0]) != b(initIb, jc[1])) {
          matched = false;
          break;
        }
      }
      // If they match reset ib and compute another cross product
      if (matched) {
        ib = initIb;
      }
    }
  }
finish:
  *dynResult = result.moveToDynamic();
}
