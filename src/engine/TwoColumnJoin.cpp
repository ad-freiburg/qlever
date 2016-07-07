// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./TwoColumnJoin.h"

using std::string;
using std::unordered_map;

// _____________________________________________________________________________
TwoColumnJoin::TwoColumnJoin(QueryExecutionContext* qec,
                             const QueryExecutionTree& t1,
                             const QueryExecutionTree& t2,
                             const vector<array<size_t, 2>>& jcs) : Operation(
    qec) {
  // Make sure subtrees are ordered so that identical queries can be identified.
  AD_CHECK_EQ(jcs.size(), 2);
  if (t1.asString() < t2.asString()) {
    _left = new QueryExecutionTree(t1);
    _jc1Left = jcs[0][0];
    _jc2Left = jcs[1][0];
    _right = new QueryExecutionTree(t2);
    _jc1Right = jcs[0][1];
    _jc2Right = jcs[1][1];
  } else {
    _left = new QueryExecutionTree(t2);
    _jc1Left = jcs[0][1];
    _jc2Left = jcs[1][1];
    _right = new QueryExecutionTree(t1);
    _jc1Right = jcs[0][0];
    _jc2Right = jcs[1][0];
  }
}

// _____________________________________________________________________________
TwoColumnJoin::~TwoColumnJoin() {
  delete _left;
  delete _right;
}


// _____________________________________________________________________________
TwoColumnJoin::TwoColumnJoin(const TwoColumnJoin& other) :
    Operation(other._executionContext),
    _left(new QueryExecutionTree(*other._left)),
    _right(new QueryExecutionTree(*other._right)),
    _jc1Left(other._jc1Left),
    _jc2Left(other._jc2Left),
    _jc1Right(other._jc1Right),
    _jc2Right(other._jc2Right) {
}

// _____________________________________________________________________________
TwoColumnJoin& TwoColumnJoin::operator=(const TwoColumnJoin& other) {
  _executionContext = other._executionContext;
  delete _left;
  _left = new QueryExecutionTree(*other._left);
  delete _right;
  _right = new QueryExecutionTree(*other._right);
  _jc1Left = other._jc1Left;
  _jc2Left = other._jc2Left;
  _jc1Right = other._jc1Right;
  _jc2Right = other._jc2Right;
  return *this;
}

// _____________________________________________________________________________
string TwoColumnJoin::asString() const {
  std::ostringstream os;
  os << "TWO_COLUMN_JOIN(\n\t" << _left->asString() << " [" << _jc1Left <<
  " & " << _jc2Left <<
  "]\n\t|X|\n\t" << _right->asString() << " [" << _jc1Right << " & " <<
  _jc2Right << "]\n)";
  return os.str();
}

// _____________________________________________________________________________
void TwoColumnJoin::computeResult(ResultTable* result) const {
  LOG(DEBUG) << "Join result computation..." << endl;
  size_t leftWidth = _left->getResultWidth();
  size_t rightWidth = _right->getResultWidth();
  const ResultTable& leftRes = _left->getRootOperation()->getResult();
  const ResultTable& rightRes = _right->getRootOperation()->getResult();

  AD_CHECK(result);
  AD_CHECK(!result->_fixedSizeData);

  result->_nofColumns = leftWidth + rightWidth - 2;
  result->_sortedBy = _jc1Left;


  result->_status = ResultTable::FINISHED;
  LOG(DEBUG) << "Join result computation done." << endl;
}

// _____________________________________________________________________________
unordered_map<string, size_t> TwoColumnJoin::getVariableColumns() const {
  unordered_map<string, size_t> retVal(_left->getVariableColumnMap());
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
