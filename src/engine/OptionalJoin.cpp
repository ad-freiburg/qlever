// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./OptionalJoin.h"

using std::string;

// _____________________________________________________________________________
OptionalJoin::OptionalJoin(QueryExecutionContext* qec,
                           std::shared_ptr<QueryExecutionTree> t1,
                           bool t1Optional,
                           std::shared_ptr<QueryExecutionTree> t2,
                           bool t2Optional,
                           const vector<array<size_t, 2>>& jcs) :
  Operation(qec),
  _joinColumns(jcs),
  _multiplicitiesComputed(false) {
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
string OptionalJoin::asString(size_t indent) const {
  std::ostringstream os;
  for (size_t i = 0; i < indent; ++i) { os << " "; }
  os << "OPTIONAL_JOIN\n"
     << _left->asString(indent) << " ";
  os << "join-columns: [";
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    os << _joinColumns[i][0] << (i < _joinColumns.size() - 1 ? " & " : "");
  };
  os << "]\n";
  for (size_t i = 0; i < indent; ++i) { os << " "; }
  os << "|X|\n"
     << _right->asString(indent) << " ";
  os << "join-columns: [";
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    os << _joinColumns[i][1] << (i < _joinColumns.size() - 1 ? " & " : "");
  };
  os << "]";
  return os.str();
}

// Used to generate all up to 125 combinations of left, right and result size.
template<int I, int J, int K>
struct meta_for {
  void operator()(int i, int j, int k,
                  const ResultTable& leftResult,
                  const ResultTable& rightResult,
                  bool leftOptional, bool rightOptional,
                  const std::vector<std::array<size_t, 2>>& joinColumns,
                  ResultTable *result,
                  int resultSize) const {
    if (I == i) {
      if (J == j) {
        if (K == k) {
          result->_fixedSizeData = new vector<array<Id, K>>;
          Engine::optionalJoin<vector<array<Id, I>>,
                               vector<array<Id, J>>,
                               array<Id, K>, K>(*static_cast<vector<array<Id, I>>*>
                                     (leftResult._fixedSizeData),
                                     *static_cast<vector<array<Id, J>>*>
                                     (rightResult._fixedSizeData),
                                     leftOptional, rightOptional,
                                     joinColumns,
                                     static_cast<vector<array<Id, K>>*>
                                     (result->_fixedSizeData),
                                     resultSize);
        } else {
          meta_for<I, J, K + 1>()(i, j, k, leftResult, rightResult,
                                  leftOptional, rightOptional, joinColumns,
                                  result, resultSize);
        }
      } else {
        meta_for<I, J + 1, K>()(i, j, k, leftResult, rightResult,
                                leftOptional, rightOptional, joinColumns,
                                result, resultSize);
      }
    } else {
      // K has to be at least as large as I (otherwise we couldn't store
      // all columns).
      meta_for<I + 1, J, K + 1>()(i, j, k, leftResult, rightResult,
                              leftOptional, rightOptional, joinColumns,
                              result, resultSize);
    }
  }
};


template <int I, int K>
struct meta_for<I, 6, K> {
  void operator()(int i, int j, int k,
                  const ResultTable& leftResult,
                  const ResultTable& rightResult,
                  bool leftOptional, bool rightOptional,
                  const std::vector<std::array<size_t, 2>>& joinColumns,
                  ResultTable *result, int resultSize) const {
    // avoid unused warnings from the compiler (there would be a lot of them)
    (void) i;
    (void) j;
    (void) k;
    Engine::optionalJoin<vector<array<Id, I>>,
        vector<vector<Id>>,
        vector<Id>, K>(*static_cast<vector<array<Id, I>>*>
                       (leftResult._fixedSizeData),
                       rightResult._varSizeData,
                       leftOptional, rightOptional,
                       joinColumns,
                       &result->_varSizeData,
                       resultSize);
  }
};

template <int I, int J>
struct meta_for<I, J, 6> {
  void operator()(int i, int j, int k,
                  const ResultTable& leftResult,
                  const ResultTable& rightResult,
                  bool leftOptional, bool rightOptional,
                  const std::vector<std::array<size_t, 2>>& joinColumns,
                  ResultTable *result, int resultSize) const {
    // avoid unused warnings from the compiler (there would be a lot of them)
    (void) i;
    (void) j;
    (void) k;
    Engine::optionalJoin<vector<array<Id, I>>,
        vector<array<Id, J>>,
        vector<Id>, 6>(*static_cast<vector<array<Id, I>>*>
                       (leftResult._fixedSizeData),
                       *static_cast<vector<array<Id, J>>*>
                       (rightResult._fixedSizeData),
                       leftOptional, rightOptional,
                       joinColumns,
                       &result->_varSizeData,
                       resultSize);
  }
};

template <int J>
struct meta_for<6, J, 6> {
  void operator()(int i, int j, int k,
                  const ResultTable& leftResult,
                  const ResultTable& rightResult,
                  bool leftOptional, bool rightOptional,
                  const std::vector<std::array<size_t, 2>>& joinColumns,
                  ResultTable *result, int resultSize) const {
    // avoid unused warnings from the compiler (there would be a lot of them)
    (void) i;
    (void) j;
    (void) k;
    Engine::optionalJoin<vector<vector<Id>>,
        vector<array<Id, J>>,
        vector<Id>, 6>(leftResult._varSizeData,
                       *static_cast<vector<array<Id, J>>*>
                       (rightResult._fixedSizeData),
                       leftOptional, rightOptional,
                       joinColumns,
                       &result->_varSizeData,
                       resultSize);
  }
};

template <>
struct meta_for<6, 6, 6> {
  void operator()(int i, int j, int k,
                  const ResultTable& leftResult,
                  const ResultTable& rightResult,
                  bool leftOptional, bool rightOptional,
                  const std::vector<std::array<size_t, 2>>& joinColumns,
                  ResultTable *result, int resultSize) const {
    // avoid unused warnings from the compiler (there would be a lot of them)
    (void) i;
    (void) j;
    (void) k;
    Engine::optionalJoin<vector<vector<Id>>,
        vector<vector<Id>>,
        vector<Id>, 6>(leftResult._varSizeData,
                       rightResult._varSizeData,
                       leftOptional, rightOptional,
                       joinColumns,
                       &result->_varSizeData,
                       resultSize);
  }
};

// _____________________________________________________________________________
void OptionalJoin::computeResult(ResultTable* result) const {
  AD_CHECK(result);
  AD_CHECK(!result->_fixedSizeData);
  LOG(DEBUG) << "OptionalJoin result computation..." << endl;

  result->_sortedBy = resultSortedOn();
  result->_nofColumns = getResultWidth();

  AD_CHECK_GE(result->_nofColumns, _joinColumns.size());

  const auto& leftResult = _left->getResult();
  const auto& rightResult = _right->getResult();

  LOG(DEBUG) << "Computing optional join between results of size "
             << leftResult.size() << " and " << rightResult.size() << endl;
  LOG(DEBUG) << "Left side optional: " << _leftOptional
             << " right side optional: " << _rightOptional << endl;

  // Calls Engine::optionalJoin with the right values for the array sizes.
  meta_for<1, 1, 1>()(leftResult._nofColumns,
                      rightResult._nofColumns,
                      result->_nofColumns,
                      leftResult,
                      rightResult,
                      _leftOptional, _rightOptional,
                      _joinColumns,
                      result,
                      result->_nofColumns);
  result->_status = ResultTable::FINISHED;
  LOG(DEBUG) << "OptionalJoin result computation done." << endl;
}

// _____________________________________________________________________________
std::unordered_map<string, size_t> OptionalJoin::getVariableColumns() const {
  std::unordered_map<string, size_t> retVal(_left->getVariableColumnMap());
  size_t leftSize = _left->getResultWidth();
  for (auto it = _right->getVariableColumnMap().begin();
       it != _right->getVariableColumnMap().end(); ++it) {
    size_t columnIndex = leftSize + it->second;
    bool isJoinColumn = false;
    // Reduce the index for every column of _right that is beeing joined on,
    // and the index of which is smaller than the index of it.
    for (const std::array<size_t, 2>& a : _joinColumns) {
      if (a[1] < it->second) {
        columnIndex--;
      } else if(a[1] == it->second) {
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
size_t OptionalJoin::getResultWidth() const {
  size_t res = _left->getResultWidth() + _right->getResultWidth()
               - _joinColumns.size();
  AD_CHECK(res > 0);
  return res;
}

// _____________________________________________________________________________
size_t OptionalJoin::resultSortedOn() const {
  return _joinColumns[0][0];
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
  size_t costEstimate = getSizeEstimate() + _left->getSizeEstimate()
                        + _right->getSizeEstimate();
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
    size_t dl = std::max(1.0f, _left->getSizeEstimate()
                              / _left->getMultiplicity(_joinColumns[i][0]));
    size_t dr = std::max(1.0f, _right->getSizeEstimate()
                              / _right->getMultiplicity(_joinColumns[i][1]));
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

  if (_leftOptional && _rightOptional) {
    _sizeEstimate = (_left->getSizeEstimate() + _right->getSizeEstimate())
                    * multResult;
  } else if (_leftOptional) {
    _sizeEstimate = _right->getSizeEstimate() * multResult;
  } else if (_rightOptional) {
    _sizeEstimate = _left->getSizeEstimate() * multResult;
  } else {
    _sizeEstimate = multResult * numDistinctResult;
  }

  // compute estimates for the multiplicities of the result columns
  _multiplicities.clear();

  // TODO(florian) improve the multiplicity estimation and handle optional sides
  for (size_t i = 0; i < _left->getResultWidth(); i++) {
    float mult = _left->getMultiplicity(i) * multResult;
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
    float mult = _right->getMultiplicity(i) * multResult;
    _multiplicities.push_back(mult);
  }
  _multiplicitiesComputed = true;
}
