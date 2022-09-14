// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "OptionalJoin.h"

#include "CallFixedSize.h"

using std::string;

// _____________________________________________________________________________
OptionalJoin::OptionalJoin(QueryExecutionContext* qec,
                           std::shared_ptr<QueryExecutionTree> t1,
                           bool t1Optional,
                           std::shared_ptr<QueryExecutionTree> t2,
                           bool t2Optional,
                           const vector<array<ColumnIndex, 2>>& jcs)
    : Operation(qec), _joinColumns(jcs), _multiplicitiesComputed(false) {
  // Make sure subtrees are ordered so that identical queries can be identified.
  AD_CHECK_GT(jcs.size(), 0);
  if (t1->asString() < t2->asString()) {
    _left = t1;
    _leftOptional = t1Optional;
    _right = t2;
    _rightOptional = t2Optional;
  } else {
    _left = t2;
    _leftOptional = t2Optional;
    _right = t1;
    _rightOptional = t1Optional;
    for (unsigned int i = 0; i < _joinColumns.size(); i++) {
      std::swap(_joinColumns[i][0], _joinColumns[i][1]);
    }
  }
}

// _____________________________________________________________________________
string OptionalJoin::asStringImpl(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) {
    os << " ";
  }
  os << "OPTIONAL_JOIN\n" << _left->asString(indent) << " ";
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
  return std::move(os).str();
}

// _____________________________________________________________________________
string OptionalJoin::getDescriptor() const {
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
  return "OptionalJoin on " + joinVars;
}

// _____________________________________________________________________________
void OptionalJoin::computeResult(ResultTable* result) {
  AD_CHECK(result);
  LOG(DEBUG) << "OptionalJoin result computation..." << endl;

  result->_sortedBy = resultSortedOn();
  result->_idTable.setCols(getResultWidth());

  AD_CHECK_GE(result->_idTable.cols(), _joinColumns.size());

  const auto leftResult = _left->getResult();
  const auto rightResult = _right->getResult();

  LOG(DEBUG) << "OptionalJoin subresult computation done." << std::endl;

  // compute the result types
  result->_resultTypes.reserve(result->_idTable.cols());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              leftResult->_resultTypes.begin(),
                              leftResult->_resultTypes.end());
  for (size_t col = 0; col < rightResult->_idTable.cols(); col++) {
    bool isJoinColumn = false;
    for (const std::array<ColumnIndex, 2>& a : _joinColumns) {
      if (a[1] == col) {
        isJoinColumn = true;
        break;
      }
    }
    if (!isJoinColumn) {
      result->_resultTypes.push_back(rightResult->_resultTypes[col]);
    }
  }

  LOG(DEBUG) << "Computing optional join between results of size "
             << leftResult->size() << " and " << rightResult->size() << endl;
  LOG(DEBUG) << "Left side optional: " << _leftOptional
             << " right side optional: " << _rightOptional << endl;

  int leftWidth = leftResult->_idTable.cols();
  int rightWidth = rightResult->_idTable.cols();
  int resWidth = result->_idTable.cols();
  CALL_FIXED_SIZE_3(leftWidth, rightWidth, resWidth, optionalJoin,
                    leftResult->_idTable, rightResult->_idTable, _leftOptional,
                    _rightOptional, _joinColumns, &result->_idTable);
  LOG(DEBUG) << "OptionalJoin result computation done." << endl;
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t> OptionalJoin::getVariableColumns() const {
  ad_utility::HashMap<string, size_t> retVal(_left->getVariableColumns());
  size_t leftSize = _left->getResultWidth();
  for (const auto& [variable, columnIndexRight] :
       _right->getVariableColumns()) {
    size_t columnIndex = leftSize + columnIndexRight;
    bool isJoinColumn = false;
    // Reduce the index for every column of _right that is beeing joined on,
    // and the index of which is smaller than the index of it.
    for (const std::array<ColumnIndex, 2>& a : _joinColumns) {
      if (a[1] < columnIndexRight) {
        columnIndex--;
      } else if (a[1] == columnIndexRight) {
        isJoinColumn = true;
        break;
      }
    }
    if (!isJoinColumn) {
      retVal[variable] = columnIndex;
    }
  }
  return retVal;
}

// _____________________________________________________________________________
size_t OptionalJoin::getResultWidth() const {
  size_t res =
      _left->getResultWidth() + _right->getResultWidth() - _joinColumns.size();
  AD_CHECK(res > 0);
  return res;
}

// _____________________________________________________________________________
vector<size_t> OptionalJoin::resultSortedOn() const {
  std::vector<size_t> sortedOn;
  // The result is sorted on all join columns from the left subtree.
  for (const auto& [joinColumnLeft, joinColumnRight] : _joinColumns) {
    (void)joinColumnRight;
    sortedOn.push_back(joinColumnLeft);
  }
  return sortedOn;
}

// _____________________________________________________________________________
float OptionalJoin::getMultiplicity(size_t col) {
  if (!_multiplicitiesComputed) {
    computeSizeEstimateAndMultiplicities();
  }
  return _multiplicities[col];
}

// _____________________________________________________________________________
size_t OptionalJoin::getSizeEstimate() {
  if (!_multiplicitiesComputed) {
    computeSizeEstimateAndMultiplicities();
  }
  return _sizeEstimate;
}

// _____________________________________________________________________________
size_t OptionalJoin::getCostEstimate() {
  size_t costEstimate =
      getSizeEstimate() + _left->getSizeEstimate() + _right->getSizeEstimate();
  // The optional join is about 3-7 times slower than a normal join, due to
  // its increased complexity
  costEstimate *= 4;
  // Make the join 7% more expensive per join column
  costEstimate *= (1 + (_joinColumns.size() - 1) * 0.07);
  return _left->getCostEstimate() + _right->getCostEstimate() + costEstimate;
}

// _____________________________________________________________________________
void OptionalJoin::computeSizeEstimateAndMultiplicities() {
  // The number of distinct entries in the result is at most the minimum of
  // the numbers of distinc entries in all join columns.
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
  // The number of distinct is at leat the number of distinct in a non optional
  // column, if the other one is optional.
  if (_leftOptional) {
    numDistinctResult = std::max(numDistinctLeft, numDistinctResult);
  }
  if (_rightOptional) {
    numDistinctRight = std::max(numDistinctRight, numDistinctResult);
  }

  // compute an estimate for the results multiplicity
  float multLeft = std::numeric_limits<float>::max();
  float multRight = std::numeric_limits<float>::max();
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    multLeft = std::min(multLeft, _left->getMultiplicity(_joinColumns[i][0]));
    multRight =
        std::min(multRight, _right->getMultiplicity(_joinColumns[i][1]));
  }
  float multResult = multLeft * multRight;

  if (_leftOptional && _rightOptional) {
    _sizeEstimate =
        (_left->getSizeEstimate() + _right->getSizeEstimate()) * multResult;
  } else if (_leftOptional) {
    _sizeEstimate = _right->getSizeEstimate() * multResult;
  } else if (_rightOptional) {
    _sizeEstimate = _left->getSizeEstimate() * multResult;
  } else {
    _sizeEstimate = multResult * numDistinctResult;
  }
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
void OptionalJoin::createOptionalResult(
    const IdTableView<A_WIDTH>& a, size_t aIdx, bool aEmpty,
    const IdTableView<B_WIDTH>& b, size_t bIdx, bool bEmpty,
    int joinColumnBitmap_a, int joinColumnBitmap_b,
    const std::vector<ColumnIndex>& joinColumnAToB,
    IdTableStatic<OUT_WIDTH>* res) {
  assert(!(aEmpty && bEmpty));
  res->emplace_back();
  size_t rIdx = res->size() - 1;
  if (aEmpty) {
    // Fill the columns of a with ID_NO_VALUE and the rest with b.
    size_t i = 0;
    for (size_t col = 0; col < a.cols(); col++) {
      if ((joinColumnBitmap_a & (1 << col)) == 0) {
        (*res)(rIdx, col) = ID_NO_VALUE;
      } else {
        // if this is one of the join columns use the value in b
        (*res)(rIdx, col) = b(bIdx, joinColumnAToB[col]);
      }
      i++;
    }
    for (size_t col = 0; col < b.cols(); col++) {
      if ((joinColumnBitmap_b & (1 << col)) == 0) {
        // only write the value if it is not one of the join columns in b
        (*res)(rIdx, i) = b(bIdx, col);
        i++;
      }
    }
  } else if (bEmpty) {
    // Fill the columns of b with ID_NO_VALUE and the rest with a
    for (size_t col = 0; col < a.cols(); col++) {
      (*res)(rIdx, col) = a(aIdx, col);
    }
    for (size_t col = a.cols(); col < res->cols(); col++) {
      (*res)(rIdx, col) = ID_NO_VALUE;
    }
  } else {
    // Use the values from both a and b
    size_t i = 0;
    for (size_t col = 0; col < a.cols(); col++) {
      (*res)(rIdx, col) = a(aIdx, col);
      i++;
    }
    for (size_t col = 0; col < b.cols(); col++) {
      if ((joinColumnBitmap_b & (1 << col)) == 0) {
        (*res)(rIdx, i) = b(bIdx, col);
        i++;
      }
    }
  }
}

template <int A_WIDTH, int B_WIDTH, int OUT_WIDTH>
void OptionalJoin::optionalJoin(
    const IdTable& dynA, const IdTable& dynB, bool aOptional, bool bOptional,
    const vector<array<ColumnIndex, 2>>& joinColumns, IdTable* dynResult) {
  // check for trivial cases
  if ((dynA.size() == 0 && dynB.size() == 0) ||
      (dynA.size() == 0 && !aOptional) || (dynB.size() == 0 && !bOptional)) {
    return;
  }

  const IdTableView<A_WIDTH> a = dynA.asStaticView<A_WIDTH>();
  const IdTableView<B_WIDTH> b = dynB.asStaticView<B_WIDTH>();
  IdTableStatic<OUT_WIDTH> result = dynResult->moveToStatic<OUT_WIDTH>();

  int joinColumnBitmapLeft = 0;
  int joinColumnBitmapRight = 0;
  for (const auto& [joinColumnLeft, joinColumnRight] : joinColumns) {
    joinColumnBitmapLeft |= (1 << joinColumnLeft);
    joinColumnBitmapRight |= (1 << joinColumnRight);
  }

  // When a is optional this is used to quickly determine
  // in which column of b the value of a joined column can be found.
  std::vector<ColumnIndex> joinColumnLeftToRight;
  if (aOptional) {
    uint32_t maxJoinColLeft = 0;
    for (const auto& [joinColumnLeft, joinColumnRight] : joinColumns) {
      if (joinColumnLeft > maxJoinColLeft) {
        maxJoinColLeft = joinColumnLeft;
      }
    }
    joinColumnLeftToRight.resize(maxJoinColLeft + 1);
    for (const auto& [joinColumnLeft, joinColumnRight] : joinColumns) {
      joinColumnLeftToRight[joinColumnLeft] = joinColumnRight;
    }
  }

  // Deal with one of the two tables beeing both empty and optional
  if (a.size() == 0 && aOptional) {
    for (size_t ib = 0; ib < b.size(); ib++) {
      createOptionalResult(a, 0, true, b, ib, false, joinColumnBitmapLeft,
                           joinColumnBitmapRight, joinColumnLeftToRight,
                           &result);
    }
    *dynResult = result.moveToDynamic();
    return;
  } else if (b.size() == 0 && bOptional) {
    for (size_t ia = 0; ia < a.size(); ia++) {
      createOptionalResult(a, ia, false, b, 0, true, joinColumnBitmapLeft,
                           joinColumnBitmapRight, joinColumnLeftToRight,
                           &result);
    }
    *dynResult = result.moveToDynamic();
    return;
  }

  bool matched = false;
  size_t ia = 0, ib = 0;
  while (ia < a.size() && ib < b.size()) {
    // Join columns 0 are the primary sort columns
    while (a(ia, joinColumns[0][0]) < b(ib, joinColumns[0][1])) {
      if (bOptional) {
        createOptionalResult(a, ia, false, b, ib, true, joinColumnBitmapLeft,
                             joinColumnBitmapRight, joinColumnLeftToRight,
                             &result);
      }
      ia++;
      if (ia >= a.size()) {
        goto finish;
      }
    }
    while (b[ib][joinColumns[0][1]] < a[ia][joinColumns[0][0]]) {
      if (aOptional) {
        createOptionalResult(a, ia, true, b, ib, false, joinColumnBitmapLeft,
                             joinColumnBitmapRight, joinColumnLeftToRight,
                             &result);
      }
      ib++;
      if (ib >= b.size()) {
        goto finish;
      }
    }

    // check if the rest of the join columns also match
    matched = true;
    for (size_t joinColIndex = 0; joinColIndex < joinColumns.size();
         joinColIndex++) {
      const auto& joinColumn = joinColumns[joinColIndex];
      if (a[ia][joinColumn[0]] < b[ib][joinColumn[1]]) {
        if (bOptional) {
          createOptionalResult(a, ia, false, b, ib, true, joinColumnBitmapLeft,
                               joinColumnBitmapRight, joinColumnLeftToRight,
                               &result);
        }
        ia++;
        matched = false;
        break;
      }
      if (b[ib][joinColumn[1]] < a[ia][joinColumn[0]]) {
        if (aOptional) {
          createOptionalResult(a, ia, true, b, ib, false, joinColumnBitmapLeft,
                               joinColumnBitmapRight, joinColumnLeftToRight,
                               &result);
        }
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
        createOptionalResult(a, ia, false, b, ib, false, joinColumnBitmapLeft,
                             joinColumnBitmapRight, joinColumnLeftToRight,
                             &result);

        ib++;

        // do the rows still match?
        for (const auto& [joinColumnLeft, joinColumnRight] : joinColumns) {
          if (ib >= b.size() ||
              a[ia][joinColumnLeft] != b[ib][joinColumnRight]) {
            matched = false;
            break;
          }
        }
      }
      ia++;
      // Check if the next row in a also matches the initial row in b
      matched = true;
      for (const auto& [joinColumnLeft, joinColumnRight] : joinColumns) {
        if (ia >= a.size() ||
            a[ia][joinColumnLeft] != b[initIb][joinColumnRight]) {
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

  // If the table of which we reached the end is optional, add all entries
  // of the other table.
  if (aOptional && ib < b.size()) {
    while (ib < b.size()) {
      createOptionalResult(a, ia, true, b, ib, false, joinColumnBitmapLeft,
                           joinColumnBitmapRight, joinColumnLeftToRight,
                           &result);

      ++ib;
    }
  }
  if (bOptional && ia < a.size()) {
    while (ia < a.size()) {
      createOptionalResult(a, ia, false, b, ib, true, joinColumnBitmapLeft,
                           joinColumnBitmapRight, joinColumnLeftToRight,
                           &result);
      ++ia;
    }
  }
  *dynResult = result.moveToDynamic();
}
