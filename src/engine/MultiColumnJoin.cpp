// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "MultiColumnJoin.h"

#include "CallFixedSize.h"

using std::string;

// _____________________________________________________________________________
MultiColumnJoin::MultiColumnJoin(
    QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> t1,
    std::shared_ptr<QueryExecutionTree> t2,
    const std::vector<std::array<ColumnIndex, 2>>& jcs)
    : Operation(qec), _joinColumns(jcs), _multiplicitiesComputed(false) {
  // Make sure subtrees are ordered so that identical queries can be identified.
  AD_CONTRACT_CHECK(!jcs.empty());
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
string MultiColumnJoin::asStringImpl(size_t indent) const {
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
  return std::move(os).str();
}

// _____________________________________________________________________________
string MultiColumnJoin::getDescriptor() const {
  std::string joinVars = "";
  for (auto p : _left->getVariableColumns()) {
    for (auto jc : _joinColumns) {
      // If the left join column matches the index of a variable in the left
      // subresult.
      if (jc[0] == p.second.columnIndex_) {
        joinVars += p.first.name() + " ";
      }
    }
  }
  return "MultiColumnJoin on " + joinVars;
}

// _____________________________________________________________________________
ResultTable MultiColumnJoin::computeResult() {
  LOG(DEBUG) << "MultiColumnJoin result computation..." << endl;

  IdTable idTable{getExecutionContext()->getAllocator()};
  idTable.setNumColumns(getResultWidth());

  AD_CONTRACT_CHECK(idTable.numColumns() >= _joinColumns.size());

  const auto leftResult = _left->getResult();
  const auto rightResult = _right->getResult();

  LOG(DEBUG) << "MultiColumnJoin subresult computation done." << std::endl;

  LOG(DEBUG) << "Computing a multi column join between results of size "
             << leftResult->size() << " and " << rightResult->size() << endl;

  int leftWidth = leftResult->idTable().numColumns();
  int rightWidth = rightResult->idTable().numColumns();
  int resWidth = idTable.numColumns();
  CALL_FIXED_SIZE((std::array{leftWidth, rightWidth, resWidth}),
                  &MultiColumnJoin::computeMultiColumnJoin,
                  leftResult->idTable(), rightResult->idTable(), _joinColumns,
                  &idTable);

  LOG(DEBUG) << "MultiColumnJoin result computation done" << endl;
  // If only one of the two operands has a non-empty local vocabulary, share
  // with that one (otherwise, throws an exception).
  return {std::move(idTable), resultSortedOn(),
          ResultTable::getSharedLocalVocabFromNonEmptyOf(*leftResult,
                                                         *rightResult)};
}

// _____________________________________________________________________________
VariableToColumnMap MultiColumnJoin::computeVariableToColumnMap() const {
  return makeVarToColMapForJoinOperation(
      _left->getVariableColumns(), _right->getVariableColumns(), _joinColumns,
      BinOpType::Join, _left->getResultWidth());
}

// _____________________________________________________________________________
size_t MultiColumnJoin::getResultWidth() const {
  size_t res =
      _left->getResultWidth() + _right->getResultWidth() - _joinColumns.size();
  AD_CONTRACT_CHECK(res > 0);
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
size_t MultiColumnJoin::getSizeEstimateBeforeLimit() {
  if (!_multiplicitiesComputed) {
    computeSizeEstimateAndMultiplicities();
  }
  return _sizeEstimate;
}

// _____________________________________________________________________________
size_t MultiColumnJoin::getCostEstimate() {
  size_t costEstimate = getSizeEstimateBeforeLimit() +
                        _left->getSizeEstimate() + _right->getSizeEstimate();
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
    multRight =
        std::min(multRight, _right->getMultiplicity(_joinColumns[i][1]));
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
