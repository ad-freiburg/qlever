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
  _joinColumns(jcs) {
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
  os << "OPTIONAL_JOIN\n";
  for (size_t i = 0; i < indent; ++i) { os << " "; }
  os << _left->asString(indent) << "\n";
  for (size_t i = 0; i < indent; ++i) { os << " "; }
  os << "join-columns: [";
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    os << _joinColumns[i][0] << (i < _joinColumns.size() - 1 ? " & " : "");
  };
  os << "]\n";
  for (size_t i = 0; i < indent; ++i) { os << " "; }
  os << "|X|\n";
  for (size_t i = 0; i < indent; ++i) { os << " "; }
  os << _right->asString(indent) << "\n";
  for (size_t i = 0; i < indent; ++i) { os << " "; }
  os << "join-columns: [";
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    os << _joinColumns[i][1] << (i < _joinColumns.size() - 1 ? " & " : "");
  };
  os << "]\n";
  return os.str();
}

// Used to generate all 125 combinations of left, right and result size.
template<int I, int J, int K>
struct meta_for {
  void operator()(int i, int j, int k,
                  void *leftResult,
                  void *rightResult,
                  bool leftOptional, bool rightOptional,
                  const std::vector<std::array<size_t, 2>> &joinColumns,
                  void *result) const {
    if (I == i) {
      if (J == j) {
        if (K == k) {
          Engine::optionalJoin<vector<array<Id, I>>,
                               vector<array<Id, J>>,
                               Id, K>(*static_cast<vector<array<Id, I>>*>
                                     (leftResult),
                                     *static_cast<vector<array<Id, J>>*>
                                     (rightResult),
                                     leftOptional, rightOptional,
                                     joinColumns,
                                     static_cast<vector<array<Id, K>>*>
                                     (result));
        } else {
          meta_for<I, J, K + 1>()(i, j, k, leftResult, rightResult,
                                  leftOptional, rightOptional, joinColumns,
                                  result);
        }
      } else {
        meta_for<I, J + 1, K>()(i, j, k, leftResult, rightResult,
                                leftOptional, rightOptional, joinColumns,
                                result);
      }
    } else {
      // K has to be at least as large as I (otherwise we couldn't store
      // all columns).
      meta_for<I + 1, J, K + 1>()(i, j, k, leftResult, rightResult,
                              leftOptional, rightOptional, joinColumns,
                              result);
    }
  }
};

// only instantiate the template for ints up to 5
template <int J, int K>
struct meta_for<6, J, K> {
  void operator()(int i, int j, int k,
                  void *leftResult,
                  void *rightResult,
                  bool leftOptional, bool rightOptional,
                  const std::vector<std::array<size_t, 2>> &joinColumns,
                  void *result) const {
    // avoid unused warnings from the compiler (there would be a lot of them)
    (void) i;
    (void) j;
    (void) k;
    (void) leftResult;
    (void) rightResult;
    (void) leftOptional;
    (void) rightOptional;
    (void) joinColumns;
    (void) result;
  }
};

template <int I, int K>
struct meta_for<I, 6, K> {
  void operator()(int i, int j, int k,
                  void *leftResult,
                  void *rightResult,
                  bool leftOptional, bool rightOptional,
                  const std::vector<std::array<size_t, 2>> &joinColumns,
                  void *result) const {
    // avoid unused warnings from the compiler (there would be a lot of them)
    (void) i;
    (void) j;
    (void) k;
    (void) leftResult;
    (void) rightResult;
    (void) leftOptional;
    (void) rightOptional;
    (void) joinColumns;
    (void) result;
  }
};

template <int I, int J>
struct meta_for<I, J, 6> {
  void operator()(int i, int j, int k,
                  void *leftResult,
                  void *rightResult,
                  bool leftOptional, bool rightOptional,
                  const std::vector<std::array<size_t, 2>> &joinColumns,
                  void *result) const {
    // avoid unused warnings from the compiler (there would be a lot of them)
    (void) i;
    (void) j;
    (void) k;
    (void) leftResult;
    (void) rightResult;
    (void) leftOptional;
    (void) rightOptional;
    (void) joinColumns;
    (void) result;
  }
};

template <int J>
struct meta_for<6, J, 6> {
  void operator()(int i, int j, int k,
                  void *leftResult,
                  void *rightResult,
                  bool leftOptional, bool rightOptional,
                  const std::vector<std::array<size_t, 2>> &joinColumns,
                  void *result) const {
    // avoid unused warnings from the compiler (there would be a lot of them)
    (void) i;
    (void) j;
    (void) k;
    (void) leftResult;
    (void) rightResult;
    (void) leftOptional;
    (void) rightOptional;
    (void) joinColumns;
    (void) result;
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

  if (leftResult._nofColumns > 5 || rightResult._nofColumns > 5
      || result->_nofColumns > 5) {
      // TODO handle more than 5 columns
  } else {
    // Calls Engine::optionalJoin with the right values for the array sizes.
    meta_for<1, 1, 1>()(leftResult._nofColumns,
                      rightResult._nofColumns,
                      result->_nofColumns,
                      leftResult._fixedSizeData,
                      rightResult._fixedSizeData,
                      _leftOptional, _rightOptional,
                      _joinColumns,
                      result->_fixedSizeData);
  }
/*
   if (leftResult._nofColumns == 1) {
    if (rightResult._nofColumns == 1) {
      Engine::optionalJoin(*static_cast<vector<array<Id, 1>>*>
                           (leftResult._fixedSizeData),
                           *static_cast<vector<array<Id, 1>>*>
                           (rightResult._fixedSizeData),
                           _leftOptional, _rightOptional,
                           _joinColumns, result);
    } else if (rightResult._nofColumns == 2) {
      Engine::optionalJoin(*static_cast<vector<array<Id, 1>>*>
                           (leftResult._fixedSizeData),
                           *static_cast<vector<array<Id, 2>>*>
                           (rightResult._fixedSizeData),
                           _leftOptional, _rightOptional,
                           _joinColumns, result);
    } else if (rightResult._nofColumns == 3) {
      Engine::optionalJoin(*static_cast<vector<array<Id, 1>>*>
                           (leftResult._fixedSizeData),
                           *static_cast<vector<array<Id, 3>>*>
                           (rightResult._fixedSizeData),
                           _leftOptional, _rightOptional,
                           _joinColumns, result);
    } else if (rightResult._nofColumns == 4) {
      Engine::optionalJoin(*static_cast<vector<array<Id, 1>>*>
                           (leftResult._fixedSizeData),
                           *static_cast<vector<array<Id, 4>>*>
                           (rightResult._fixedSizeData),
                           _leftOptional, _rightOptional,
                           _joinColumns, result);
    } else if (rightResult._nofColumns == 5) {
      Engine::optionalJoin(*static_cast<vector<array<Id, 1>>*>
                           (leftResult._fixedSizeData),
                           *static_cast<vector<array<Id, 5>>*>
                           (rightResult._fixedSizeData),
                           _leftOptional, _rightOptional,
                           _joinColumns, result);
    }
  }*/

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
    // Reduce the index for every column of _right that is beeing joined on,
    // and the index of which is smaller than the index of it.
    for (const std::array<size_t, 2> &a : _joinColumns) {
      if (a[1] < it->second) {
        columnIndex--;
      }
    }
    retVal[it->first] = columnIndex;
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
  /*if (_multiplicities.size() == 0) {
    computeMultiplicities();
  }
  AD_CHECK_LT(col, _multiplicities.size());
  return _multiplicities[col];*/
  // TODO properly implement multiplicities
  return 0;
}
