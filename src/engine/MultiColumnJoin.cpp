// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "./MultiColumnJoin.h"

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

// Used to generate all up to 125 combinations of left, right and result size.
template <size_t I, size_t J, size_t K>
struct MultiColumnJoinCaller {
  static void call(const size_t i, const size_t j, const size_t k,
                   shared_ptr<const ResultTable> leftResult,
                   shared_ptr<const ResultTable> rightResult,
                   const std::vector<std::array<Id, 2>>& joinColumns,
                   ResultTable* result, size_t resultSize) {
    if (I == i) {
      if (J == j) {
        if (K == k) {
          result->_fixedSizeData = new vector<array<Id, K>>;
          MultiColumnJoin::computeMultiColumnJoin<
              vector<array<Id, I>>, vector<array<Id, J>>, array<Id, K>>(
              *static_cast<vector<array<Id, I>>*>(leftResult->_fixedSizeData),
              *static_cast<vector<array<Id, J>>*>(rightResult->_fixedSizeData),
              joinColumns,
              static_cast<vector<array<Id, K>>*>(result->_fixedSizeData),
              resultSize);
        } else {
          MultiColumnJoinCaller<I, J, K + 1>::call(i, j, k, leftResult,
                                                   rightResult, joinColumns,
                                                   result, resultSize);
        }
      } else {
        MultiColumnJoinCaller<I, J + 1, K>::call(
            i, j, k, leftResult, rightResult, joinColumns, result, resultSize);
      }
    } else {
      // K has to be at least as large as I (otherwise we couldn't store
      // all columns).
      MultiColumnJoinCaller<I + 1, J, K + 1>::call(
          i, j, k, leftResult, rightResult, joinColumns, result, resultSize);
    }
  }
};

template <size_t I, size_t K>
struct MultiColumnJoinCaller<I, 6, K> {
  static void call(const size_t i, const size_t j, const size_t k,
                   shared_ptr<const ResultTable> leftResult,
                   shared_ptr<const ResultTable> rightResult,
                   const std::vector<std::array<Id, 2>>& joinColumns,
                   ResultTable* result, size_t resultSize) {
    // avoid unused warnings from the compiler (there would be a lot of them)
    (void)i;
    (void)j;
    (void)k;
    MultiColumnJoin::computeMultiColumnJoin<vector<array<Id, I>>,
                                            vector<vector<Id>>, vector<Id>>(
        *static_cast<vector<array<Id, I>>*>(leftResult->_fixedSizeData),
        rightResult->_varSizeData, joinColumns, &result->_varSizeData,
        resultSize);
  }
};

template <size_t I, size_t J>
struct MultiColumnJoinCaller<I, J, 6> {
  static void call(const size_t i, const size_t j, const size_t k,
                   shared_ptr<const ResultTable> leftResult,
                   shared_ptr<const ResultTable> rightResult,
                   const std::vector<std::array<Id, 2>>& joinColumns,
                   ResultTable* result, size_t resultSize) {
    // avoid unused warnings from the compiler (there would be a lot of them)
    (void)i;
    (void)j;
    (void)k;
    MultiColumnJoin::computeMultiColumnJoin<vector<array<Id, I>>,
                                            vector<array<Id, J>>, vector<Id>>(
        *static_cast<vector<array<Id, I>>*>(leftResult->_fixedSizeData),
        *static_cast<vector<array<Id, J>>*>(rightResult->_fixedSizeData),
        joinColumns, &result->_varSizeData, resultSize);
  }
};

template <size_t J>
struct MultiColumnJoinCaller<6, J, 6> {
  static void call(const size_t i, const size_t j, const size_t k,
                   shared_ptr<const ResultTable> leftResult,
                   shared_ptr<const ResultTable> rightResult,
                   const std::vector<std::array<Id, 2>>& joinColumns,
                   ResultTable* result, size_t resultSize) {
    // avoid unused warnings from the compiler (there would be a lot of them)
    (void)i;
    (void)j;
    (void)k;
    MultiColumnJoin::computeMultiColumnJoin<vector<vector<Id>>,
                                            vector<array<Id, J>>, vector<Id>>(
        leftResult->_varSizeData,
        *static_cast<vector<array<Id, J>>*>(rightResult->_fixedSizeData),
        joinColumns, &result->_varSizeData, resultSize);
  }
};

template <>
struct MultiColumnJoinCaller<6, 6, 6> {
  static void call(const size_t i, const size_t j, const size_t k,
                   shared_ptr<const ResultTable> leftResult,
                   shared_ptr<const ResultTable> rightResult,
                   const std::vector<std::array<Id, 2>>& joinColumns,
                   ResultTable* result, size_t resultSize) {
    // avoid unused warnings from the compiler (there would be a lot of them)
    (void)i;
    (void)j;
    (void)k;
    MultiColumnJoin::computeMultiColumnJoin<vector<vector<Id>>,
                                            vector<vector<Id>>, vector<Id>>(
        leftResult->_varSizeData, rightResult->_varSizeData, joinColumns,
        &result->_varSizeData, resultSize);
  }
};

// _____________________________________________________________________________
void MultiColumnJoin::computeResult(ResultTable* result) const {
  AD_CHECK(result);
  AD_CHECK(!result->_fixedSizeData);
  LOG(DEBUG) << "MultiColumnJoin result computation..." << endl;

  result->_sortedBy = resultSortedOn();
  result->_nofColumns = getResultWidth();

  AD_CHECK_GE(result->_nofColumns, _joinColumns.size());

  const auto leftResult = _left->getResult();
  const auto rightResult = _right->getResult();

  // compute the result types
  result->_resultTypes.reserve(result->_nofColumns);
  result->_resultTypes.insert(result->_resultTypes.end(),
                              leftResult->_resultTypes.begin(),
                              leftResult->_resultTypes.end());
  for (size_t col = 0; col < rightResult->_nofColumns; col++) {
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

  LOG(DEBUG) << "Computing optional join between results of size "
             << leftResult->size() << " and " << rightResult->size() << endl;

  // Calls computeMultiColumnJoin with the right values for the array sizes.
  MultiColumnJoinCaller<1, 1, 1>::call(
      leftResult->_nofColumns, rightResult->_nofColumns, result->_nofColumns,
      leftResult, rightResult, _joinColumns, result, result->_nofColumns);
  result->finish();
  LOG(DEBUG) << "MultiColumnJoin result computation done." << endl;
}

// _____________________________________________________________________________
ad_utility::HashMap<string, size_t> MultiColumnJoin::getVariableColumns()
    const {
  ad_utility::HashMap<string, size_t> retVal(_left->getVariableColumnMap());
  size_t leftSize = _left->getResultWidth();
  for (auto it = _right->getVariableColumnMap().begin();
       it != _right->getVariableColumnMap().end(); ++it) {
    size_t columnIndex = leftSize + it->second;
    bool isJoinColumn = false;
    // Reduce the index for every column of _right that is beeing joined on,
    // and the index of which is smaller than the index of it.
    for (const std::array<Id, 2>& a : _joinColumns) {
      if (a[1] < it->second) {
        columnIndex--;
      } else if (a[1] == it->second) {
        isJoinColumn = true;
        break;
      }
    }
    if (!isJoinColumn) {
      retVal[it->first] = columnIndex;
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

template <typename R>
struct MultiColumnJoinResultAllocator {
  static R allocate(size_t width) {
    (void)width;
    return R();
  }
};

template <>
struct MultiColumnJoinResultAllocator<std::vector<Id>> {
  static std::vector<Id> allocate(size_t width) {
    return std::vector<Id>(width);
  }
};

template <typename A, typename B, typename R>
void MultiColumnJoin::computeMultiColumnJoin(
    const A& a, const B& b, const vector<array<Id, 2>>& joinColumns,
    vector<R>* result, size_t resultWidth) {
  // check for trivial cases
  if (a.size() == 0 || b.size() == 0) {
    return;
  }

  // Marks the columns in b that are join columns. Used to skip these
  // when computing the result of the join
  int joinColumnBitmap_b = 0;
  for (const array<Id, 2>& jc : joinColumns) {
    joinColumnBitmap_b |= (1 << jc[1]);
  }

  // TODO(florian): Check if this actually improves the performance.
  // Cast away constness so we can add sentinels that will be removed
  // in the end and create and add those sentinels.
  A& l1 = const_cast<A&>(a);
  B& l2 = const_cast<B&>(b);
  Id sentVal = std::numeric_limits<Id>::max() - 1;
  auto v1 = l1[0];
  auto v2 = l2[0];
  for (size_t i = 0; i < v1.size(); i++) {
    v1[i] = sentVal;
  }
  for (size_t i = 0; i < v2.size(); i++) {
    v2[i] = sentVal;
  }
  l1.push_back(v1);
  l2.push_back(v2);

  bool matched = false;
  size_t ia = 0, ib = 0;
  while (ia < a.size() - 1 && ib < b.size() - 1) {
    // Join columns 0 are the primary sort columns
    while (a[ia][joinColumns[0][0]] < b[ib][joinColumns[0][1]]) {
      ia++;
    }
    while (b[ib][joinColumns[0][1]] < a[ia][joinColumns[0][0]]) {
      ib++;
    }

    // check if the rest of the join columns also match
    matched = true;
    for (size_t joinColIndex = 0; joinColIndex < joinColumns.size();
         joinColIndex++) {
      const array<Id, 2>& joinColumn = joinColumns[joinColIndex];
      if (a[ia][joinColumn[0]] < b[ib][joinColumn[1]]) {
        ia++;
        matched = false;
        break;
      }
      if (b[ib][joinColumn[1]] < a[ia][joinColumn[0]]) {
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
        R res = MultiColumnJoinResultAllocator<R>::allocate(resultWidth);

        // fill the result
        unsigned int rIndex = 0;
        const typename A::value_type& a_row = a[ia];
        const typename B::value_type& b_row = b[ib];
        for (size_t col = 0; col < a_row.size(); col++) {
          res[rIndex] = a_row[col];
          rIndex++;
        }
        for (size_t col = 0; col < b_row.size(); col++) {
          if ((joinColumnBitmap_b & (1 << col)) == 0) {
            res[rIndex] = b_row[col];
            rIndex++;
          }
        }

        result->push_back(res);
        ib++;

        // do the rows still match?
        for (const array<Id, 2>& jc : joinColumns) {
          if (ib == b.size() || a[ia][jc[0]] != b[ib][jc[1]]) {
            matched = false;
            break;
          }
        }
      }
      ia++;
      // Check if the next row in a also matches the initial row in b
      matched = true;
      for (const array<Id, 2>& jc : joinColumns) {
        if (ia == a.size() || a[ia][jc[0]] != b[initIb][jc[1]]) {
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

  // remove the sentinels
  l1.pop_back();
  l2.pop_back();
  if (result->back()[joinColumns[0][0]] == sentVal) {
    result->pop_back();
  }
}
