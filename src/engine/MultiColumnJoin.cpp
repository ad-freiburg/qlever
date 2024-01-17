// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include "MultiColumnJoin.h"

#include "engine/AddCombinedRowToTable.h"
#include "engine/CallFixedSize.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"

using std::endl;
using std::string;

// _____________________________________________________________________________
MultiColumnJoin::MultiColumnJoin(QueryExecutionContext* qec,
                                 std::shared_ptr<QueryExecutionTree> t1,
                                 std::shared_ptr<QueryExecutionTree> t2)
    : Operation{qec} {
  // Make sure subtrees are ordered so that identical queries can be identified.
  if (t1->getCacheKey() > t2->getCacheKey()) {
    std::swap(t1, t2);
  }
  std::tie(_left, _right, _joinColumns) =
      QueryExecutionTree::getSortedSubtreesAndJoinColumns(std::move(t1),
                                                          std::move(t2));
}

// _____________________________________________________________________________
string MultiColumnJoin::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "MULTI_COLUMN_JOIN\n" << _left->getCacheKey() << " ";
  os << "join-columns: [";
  for (size_t i = 0; i < _joinColumns.size(); i++) {
    os << _joinColumns[i][0] << (i < _joinColumns.size() - 1 ? " & " : "");
  };
  os << "]\n";
  os << "|X|\n" << _right->getCacheKey() << " ";
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

  computeMultiColumnJoin(leftResult->idTable(), rightResult->idTable(),
                         _joinColumns, &idTable);

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
vector<ColumnIndex> MultiColumnJoin::resultSortedOn() const {
  std::vector<ColumnIndex> sortedOn;
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
uint64_t MultiColumnJoin::getSizeEstimateBeforeLimit() {
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

// _______________________________________________________________________
void MultiColumnJoin::computeMultiColumnJoin(
    const IdTable& left, const IdTable& right,
    const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
    IdTable* result) {
  // check for trivial cases
  if (left.empty() || right.empty()) {
    return;
  }

  ad_utility::JoinColumnMapping joinColumnData{joinColumns, left.numColumns(),
                                               right.numColumns()};

  IdTableView<0> leftJoinColumns =
      left.asColumnSubsetView(joinColumnData.jcsLeft());
  IdTableView<0> rightJoinColumns =
      right.asColumnSubsetView(joinColumnData.jcsRight());

  auto leftPermuted = left.asColumnSubsetView(joinColumnData.permutationLeft());
  auto rightPermuted =
      right.asColumnSubsetView(joinColumnData.permutationRight());

  auto rowAdder = ad_utility::AddCombinedRowToIdTable(
      joinColumns.size(), leftPermuted, rightPermuted, std::move(*result));
  auto addRow = [&rowAdder, beginLeft = leftJoinColumns.begin(),
                 beginRight = rightJoinColumns.begin()](const auto& itLeft,
                                                        const auto& itRight) {
    rowAdder.addRow(itLeft - beginLeft, itRight - beginRight);
  };

  auto findUndef = [](const auto& row, auto begin, auto end,
                      bool& resultMightBeUnsorted) {
    return ad_utility::findSmallerUndefRanges(row, begin, end,
                                              resultMightBeUnsorted);
  };

  // `isCheap` is true iff there are no UNDEF values in the join columns. In
  // this case we can use a much cheaper algorithm.
  // TODO<joka921> There are many other cases where a cheaper implementation can
  // be chosen, but we leave those for another PR, this is the most common case.
  namespace stdr = std::ranges;
  bool isCheap = stdr::none_of(joinColumns, [&](const auto& jcs) {
    auto [leftCol, rightCol] = jcs;
    return (stdr::any_of(right.getColumn(rightCol), &Id::isUndefined)) ||
           (stdr::any_of(left.getColumn(leftCol), &Id::isUndefined));
  });

  const size_t numOutOfOrder = [&]() {
    if (isCheap) {
      return ad_utility::zipperJoinWithUndef(
          leftJoinColumns, rightJoinColumns,
          std::ranges::lexicographical_compare, addRow, ad_utility::noop,
          ad_utility::noop);
    } else {
      return ad_utility::zipperJoinWithUndef(
          leftJoinColumns, rightJoinColumns,
          std::ranges::lexicographical_compare, addRow, findUndef, findUndef);
    }
  }();
  *result = std::move(rowAdder).resultTable();
  // If there were UNDEF values in the input, the result might be out of
  // order. Sort it, because this operation promises a sorted result in its
  // `resultSortedOn()` member function.
  // TODO<joka921> We only have to do this if the sorting is required (merge the
  // other PR first).
  if (numOutOfOrder > 0) {
    std::vector<ColumnIndex> cols;
    for (size_t i = 0; i < joinColumns.size(); ++i) {
      cols.push_back(i);
    }
    Engine::sort(*result, cols);
  }

  // The result that `zipperJoinWithUndef` produces has a different order of
  // columns than expected, permute them. See the documentation of
  // `JoinColumnMapping` for details.
  result->setColumnSubset(joinColumnData.permutationResult());
}
