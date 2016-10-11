// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./TwoColumnJoin.h"

using std::string;


// _____________________________________________________________________________
TwoColumnJoin::TwoColumnJoin(QueryExecutionContext* qec,
                             std::shared_ptr<QueryExecutionTree> t1,
                             std::shared_ptr<QueryExecutionTree> t2,
                             const vector<array<size_t, 2>>& jcs) : Operation(
    qec) {
  // Make sure subtrees are ordered so that identical queries can be identified.
  AD_CHECK_EQ(jcs.size(), 2);
  if (t1->asString() < t2->asString()) {
    _left = t1;
    _jc1Left = jcs[0][0];
    _jc2Left = jcs[1][0];
    _right = t2;
    _jc1Right = jcs[0][1];
    _jc2Right = jcs[1][1];
  } else {
    _left = t2;
    _jc1Left = jcs[0][1];
    _jc2Left = jcs[1][1];
    _right = t1;
    _jc1Right = jcs[0][0];
    _jc2Right = jcs[1][0];
  }
  // Now check if one of them is an index scan with width two.
  // If so, make sure that its first jc is 0 and the second 1.
  if (_left->getType() == QueryExecutionTree::SCAN &&
      _left->getResultWidth() == 2) {
    if (_jc1Left > _jc2Left) {
      std::swap(_jc1Left, _jc2Left);
      std::swap(_jc1Right, _jc2Right);
    }
  } else if (_right->getType() == QueryExecutionTree::SCAN &&
      _right->getResultWidth() == 2) {
    if (_jc1Right > _jc2Right) {
      std::swap(_jc1Left, _jc2Left);
      std::swap(_jc1Right, _jc2Right);
    }
  }
}

// _____________________________________________________________________________
string TwoColumnJoin::asString() const {
  std::ostringstream os;
  os << "TWO_COLUMN_JOIN(\n" << _left->asString() << " [" << _jc1Left <<
  " & " << _jc2Left <<
  "]\n|X|\n" << _right->asString() << " [" << _jc1Right << " & " <<
  _jc2Right << "]\n)";
  return os.str();
}

// _____________________________________________________________________________
void TwoColumnJoin::computeResult(ResultTable* result) const {
  AD_CHECK(result);
  AD_CHECK(!result->_fixedSizeData);
  LOG(DEBUG) << "TwoColumnJoin result computation..." << endl;

  // Deal with the case that one of the lists is width two and
  // with join columns 0 1. This means we can use the filter method.
  if ((_left->getResultWidth() == 2 && _jc1Left == 0 && _jc2Left == 1) ||
      (_right->getResultWidth() == 2 && _jc1Right == 0 && _jc2Right == 1)) {
    bool rightFilter = (_right->getResultWidth() == 2 && _jc1Right == 0 &&
                        _jc2Right == 1);
    const auto& v = rightFilter ? _left : _right;
    const auto& filter = *static_cast<vector<array<Id, 2>>*>(
        rightFilter ? _right->getResult()._fixedSizeData
                    : _left->getResult()._fixedSizeData);
    size_t jc1 = rightFilter ? _jc1Left : _jc1Right;
    size_t jc2 = rightFilter ? _jc2Left : _jc2Right;
    result->_sortedBy = jc1;
    result->_nofColumns = v->getResultWidth();

    AD_CHECK_GE(result->_nofColumns, 2);

    if (result->_nofColumns == 2) {
      using ResType = vector<array<Id, 2>>;
      result->_fixedSizeData = new ResType();
      getEngine().filter(*static_cast<ResType*>(v->getResult()._fixedSizeData),
                         jc1, jc2, filter,
                         static_cast<ResType*>(result->_fixedSizeData));
    } else if (result->_nofColumns == 3) {
      using ResType = vector<array<Id, 3>>;
      result->_fixedSizeData = new ResType();
      getEngine().filter(*static_cast<ResType*>(v->getResult()._fixedSizeData),
                         jc1, jc2, filter,
                         static_cast<ResType*>(result->_fixedSizeData));
    } else if (result->_nofColumns == 4) {
      using ResType = vector<array<Id, 4>>;
      result->_fixedSizeData = new ResType();
      getEngine().filter(*static_cast<ResType*>(v->getResult()._fixedSizeData),
                         jc1, jc2, filter,
                         static_cast<ResType*>(result->_fixedSizeData));
    } else if (result->_nofColumns == 5) {
      using ResType = vector<array<Id, 5>>;
      result->_fixedSizeData = new ResType();
      getEngine().filter(*static_cast<ResType*>(v->getResult()._fixedSizeData),
                         jc1, jc2, filter,
                         static_cast<ResType*>(result->_fixedSizeData));
    } else {
      getEngine().filter(v->getResult()._varSizeData, jc1, jc2, filter,
                         &result->_varSizeData);
    }

    result->_status = ResultTable::FINISHED;
    LOG(DEBUG) << "TwoColumnJoin result computation done." << endl;
    return;
  }
  // TOOD: implement the other case later
  AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED,
           "For now, prefer cyclic queries to be resolved using a single join.")
}

// _____________________________________________________________________________
std::unordered_map<string, size_t> TwoColumnJoin::getVariableColumns() const {
  std::unordered_map<string, size_t> retVal(_left->getVariableColumnMap());
  size_t leftSize = _left->getResultWidth();
  for (auto it = _right->getVariableColumnMap().begin();
       it != _right->getVariableColumnMap().end(); ++it) {
    if (it->second < _jc1Right) {
      if (it->second < _jc2Right) {
        retVal[it->first] = leftSize + it->second;
      } else if (it->second > _jc2Right) {
        retVal[it->first] = leftSize + it->second - 1;
      }
    }
    if (it->second > _jc1Right) {
      if (it->second < _jc2Right) {
        retVal[it->first] = leftSize + it->second - 1;
      } else if (it->second > _jc2Right) {
        retVal[it->first] = leftSize + it->second - 2;
      }
    }
  }
  return retVal;
}

// _____________________________________________________________________________
size_t TwoColumnJoin::getResultWidth() const {
  size_t res = _left->getResultWidth() + _right->getResultWidth() - 2;
  AD_CHECK(res > 0);
  return res;
}

// _____________________________________________________________________________
size_t TwoColumnJoin::resultSortedOn() const {
  return _jc1Left;
}
