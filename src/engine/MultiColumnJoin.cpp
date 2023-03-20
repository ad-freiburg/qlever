// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "MultiColumnJoin.h"

#include "engine/CallFixedSize.h"
#include "util/JoinAlgorithms.h"

using std::string;

// _____________________________________________________________________________
MultiColumnJoin::MultiColumnJoin(QueryExecutionContext* qec,
                                 std::shared_ptr<QueryExecutionTree> t1,
                                 std::shared_ptr<QueryExecutionTree> t2,
                                 const vector<array<ColumnIndex, 2>>& jcs)
    : Operation(qec),
      _left(std::move(t1)),
      _right(std::move(t2)),
      _joinColumns(jcs),
      _multiplicitiesComputed(false) {
  // Make sure subtrees are ordered so that identical queries can be identified.
  AD_CONTRACT_CHECK(!jcs.empty());
  if (_left->asString() > _right->asString()) {
    std::swap(_left, _right);
    // As the subtrees have been swapped the join columns need to be swapped
    // as well.
    for (auto& [leftCol, rightCol] : _joinColumns) {
      std::swap(leftCol, rightCol);
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
      if (jc[0] == p.second) {
        joinVars += p.first.name() + " ";
      }
    }
  }
  return "MultiColumnJoin on " + joinVars;
}

// _____________________________________________________________________________
void MultiColumnJoin::computeResult(ResultTable* result) {
  AD_CONTRACT_CHECK(result);
  LOG(DEBUG) << "MultiColumnJoin result computation..." << endl;

  result->_sortedBy = resultSortedOn();
  result->_idTable.setNumColumns(getResultWidth());

  AD_CONTRACT_CHECK(result->_idTable.numColumns() >= _joinColumns.size());

  const auto leftResult = _left->getResult();
  const auto rightResult = _right->getResult();

  LOG(DEBUG) << "MultiColumnJoin subresult computation done." << std::endl;

  // compute the result types
  result->_resultTypes.reserve(result->_idTable.numColumns());
  result->_resultTypes.insert(result->_resultTypes.end(),
                              leftResult->_resultTypes.begin(),
                              leftResult->_resultTypes.end());
  for (size_t col = 0; col < rightResult->_idTable.numColumns(); col++) {
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

  LOG(DEBUG) << "Computing a multi column join between results of size "
             << leftResult->size() << " and " << rightResult->size() << endl;

  int leftWidth = leftResult->_idTable.numColumns();
  int rightWidth = rightResult->_idTable.numColumns();
  int resWidth = result->_idTable.numColumns();
  CALL_FIXED_SIZE((std::array{leftWidth, rightWidth, resWidth}),
                  &MultiColumnJoin::computeMultiColumnJoin,
                  leftResult->_idTable, rightResult->_idTable, _joinColumns,
                  &result->_idTable);

  // If only one of the two operands has a non-empty local vocabulary, share
  // with that one (otherwise, throws an exception).
  result->shareLocalVocabFromNonEmptyOf(*leftResult, *rightResult);

  LOG(DEBUG) << "MultiColumnJoin result computation done" << endl;
}

// _____________________________________________________________________________
VariableToColumnMap MultiColumnJoin::computeVariableToColumnMap() const {
  VariableToColumnMap retVal(_left->getVariableColumns());
  size_t columnIndex = retVal.size();
  const auto variableColumnsRightSorted =
      copySortedByColumnIndex(_right->getVariableColumns());
  for (const auto& it : variableColumnsRightSorted) {
    bool isJoinColumn = false;
    for (const std::array<ColumnIndex, 2>& a : _joinColumns) {
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

// _______________________________________________________________________
template <int A_WIDTH, int B_WIDTH, int OUT_WIDTH>
void MultiColumnJoin::computeMultiColumnJoin(
    const IdTable& dynA, const IdTable& dynB,
    const vector<array<ColumnIndex, 2>>& joinColumns, IdTable* dynResult) {
  // check for trivial cases
  if (dynA.empty() || dynB.empty()) {
    return;
  }

  IdTableView<A_WIDTH> a = dynA.asStaticView<A_WIDTH>();
  IdTableView<B_WIDTH> b = dynB.asStaticView<B_WIDTH>();
  IdTableStatic<OUT_WIDTH> result = std::move(*dynResult).toStatic<OUT_WIDTH>();

  auto joinColumnData = ad_utility::prepareJoinColumns(
      joinColumns, a.numColumns(), b.numColumns());

  auto dynASubset = dynA.asColumnSubsetView(joinColumnData.jcsA_);
  auto dynBSubset = dynB.asColumnSubsetView(joinColumnData.jcsB_);

  auto dynAPermuted = dynA.asColumnSubsetView(joinColumnData.colsCompleteA_);
  auto dynBPermuted = dynB.asColumnSubsetView(joinColumnData.colsCompleteB_);

  auto lessThanBoth = std::ranges::lexicographical_compare;

  auto rowAdder = ad_utility::AddCombinedRowToIdTable(result.numColumns());
  auto addRow = [&, numJoinColumns = joinColumns.size()](const auto& rowA,
                                                         const auto& rowB) {
    const auto& a = *(dynAPermuted.begin() + rowA.rowIndex());
    const auto& b = *(dynBPermuted.begin() + rowB.rowIndex());
    rowAdder(a, b, numJoinColumns, &result);
  };

  auto findUndefDispatch = [](const auto& row, auto begin, auto end) {
    return ad_utility::findSmallerUndefRanges(row, begin, end);
  };
  ad_utility::zipperJoinWithUndef(dynASubset, dynBSubset, lessThanBoth, addRow,
                                  findUndefDispatch, findUndefDispatch);

  // The column order in the result is now
  // [joinColumns, non-join-columns-a, non-join-columns-b] (which makes the
  // algorithms above easier), be the order that is expected by the rest of the
  // code is [columns-a, non-join-columns-b]. Permute the columns to fix the
  // order.
  result.permuteColumns(joinColumnData.permutation_);

  *dynResult = std::move(result).toDynamic();
}
